"""Run evaluation framework against the API.

Measures:
1. Intent accuracy: does the orchestrator classify correctly?
2. SQL correctness: for structured queries, does the generated SQL return matching data?
3. Answer quality: for semantic queries, does the answer contain relevant content?
4. Latency: response time per query (with P50/P95/P99 percentiles)
5. LLM-as-judge: factuality, completeness, citation quality, context utilization (1-5 each)
6. Hallucination rate: % of answers with factuality < 3
7. Cost per query: estimated LLM calls × cost
8. Fallback rate: % of queries where agent falls back to legacy router
9. Tool utilization: which tools are called most often

Multi-model bake-off:
    python -m eval.run_eval --models mistral-large llama3.1-70b snowflake-arctic
    python -m eval.run_eval --models llama3.1-70b --sample 20
All candidates in one invocation share a RUN_ID in ANALYTICS.EVAL_RUNS.
The judge model is held constant (mistral-large2) to avoid self-preference bias.
"""

import argparse
import json
import os
import re
import sys
import time
import uuid
from collections import Counter
from datetime import datetime

import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

from eval.test_questions import EVAL_QUESTIONS

API_BASE = "http://localhost:8000"
COST_PER_COMPLETE_CALL = 0.003  # Approximate cost for mistral-large on Snowflake

# Judge held constant across candidates to eliminate self-preference bias.
# mistral-large2 is not in the candidate bake-off set.
JUDGE_MODEL = "mistral-large2"


def _question_type(qid: str) -> str:
    """Map question id prefix ('cat_01', 'prod_10', 'theme_15') to type."""
    prefix = qid.split("_", 1)[0] if "_" in qid else qid
    return {"cat": "category", "prod": "product", "theme": "theme"}.get(prefix, "unknown")


def stratified_sample(questions: list[dict], n: int) -> list[dict]:
    """Pick N questions preserving the category/product/theme ratio.

    Original split is 30/40/30. For N=20 → 6/8/6.
    """
    if n >= len(questions):
        return list(questions)
    buckets: dict[str, list[dict]] = {}
    for q in questions:
        buckets.setdefault(_question_type(q["id"]), []).append(q)
    total = sum(len(v) for v in buckets.values())
    sample: list[dict] = []
    for qtype, qs in buckets.items():
        take = round(n * len(qs) / total)
        sample.extend(qs[:max(1, take)])
    # Trim/pad to exactly n
    if len(sample) > n:
        sample = sample[:n]
    elif len(sample) < n:
        # Pad with whatever questions weren't picked
        seen = {q["id"] for q in sample}
        for q in questions:
            if q["id"] not in seen:
                sample.append(q)
                if len(sample) >= n:
                    break
    return sample


def get_snowflake_cursor():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )
    return conn, conn.cursor()


def run_ground_truth_sql(cur, sql):
    """Execute ground truth SQL and return results."""
    try:
        cur.execute(sql)
        return cur.fetchall()
    except Exception as e:
        return f"SQL_ERROR: {e}"


def check_numerical_match(api_data, gt_data, tolerance=0.05):
    """Check if API data matches ground truth within tolerance."""
    if not api_data or not gt_data:
        return False, "Missing data"

    if isinstance(gt_data, str) and gt_data.startswith("SQL_ERROR"):
        return False, gt_data

    # For single-value queries, compare the first value
    if len(gt_data) == 1 and len(gt_data[0]) == 1:
        gt_val = gt_data[0][0]
        for row in api_data:
            for val in row.values():
                try:
                    if abs(float(val) - float(gt_val)) / max(abs(float(gt_val)), 1e-10) < tolerance:
                        return True, f"Match: {gt_val}"
                except (ValueError, TypeError):
                    if str(val).lower() == str(gt_val).lower():
                        return True, f"Match: {gt_val}"

    # For multi-row queries, check if the same categories/ASINs appear
    gt_categories = set()
    for row in gt_data:
        for val in row:
            if isinstance(val, str):
                gt_categories.add(val.lower())

    api_categories = set()
    for row in api_data:
        for val in row.values():
            if isinstance(val, str):
                api_categories.add(val.lower())

    overlap = gt_categories & api_categories
    if overlap:
        return True, f"Category overlap: {overlap}"

    return False, "No match found"


# ============================================
# LLM-AS-JUDGE
# ============================================

JUDGE_PROMPT = """You are evaluating the quality of an AI-generated answer about Amazon Electronics product reviews.

Question: {question}
Data available: {data_summary}
Generated answer: {answer}

Rate the answer on these 4 dimensions (1-5 each):

1. FACTUALITY: Does the answer only state facts present in the data? No fabricated statistics or claims.
   5 = Every claim is grounded in data | 3 = Mostly grounded with minor unsupported claims | 1 = Fabricates numbers or facts

2. COMPLETENESS: Does the answer fully address the question asked?
   5 = Thoroughly addresses all aspects | 3 = Addresses the main point but misses details | 1 = Barely relevant to the question

3. CITATION_QUALITY: Does the answer cite specific numbers, review quotes, or data points?
   5 = Specific numbers with scope (e.g., "4.2 avg from 15K reviews") | 3 = Some numbers but vague | 1 = No citations at all

4. CONTEXT_UTILIZATION: Does the answer make good use of the available data, or does it ignore it?
   5 = Uses all relevant data provided | 3 = Uses some data but misses key information | 1 = Ignores available data entirely

Output ONLY valid JSON, no other text:
{{"factuality": X, "completeness": X, "citation_quality": X, "context_utilization": X, "reasoning": "1-2 sentence explanation"}}"""


def judge_answer(question, answer, data_summary, cur):
    """Use CORTEX.COMPLETE to score an answer on 4 quality dimensions."""
    if not answer or len(answer.strip()) < 10:
        return {
            "factuality": 1, "completeness": 1,
            "citation_quality": 1, "context_utilization": 1,
            "reasoning": "Answer too short or empty"
        }

    prompt = JUDGE_PROMPT.format(
        question=question,
        answer=answer[:1500],  # Truncate to save tokens
        data_summary=data_summary[:500],
    )

    try:
        cur.execute(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
            (JUDGE_MODEL, prompt)
        )
        response = cur.fetchone()[0].strip()

        # Parse JSON from response
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            scores = json.loads(json_match.group(0))
            # Validate scores are in range
            for key in ["factuality", "completeness", "citation_quality", "context_utilization"]:
                if key in scores:
                    scores[key] = max(1, min(5, int(scores[key])))
                else:
                    scores[key] = 3  # Default if missing
            return scores

        return {"factuality": 3, "completeness": 3, "citation_quality": 3,
                "context_utilization": 3, "reasoning": "Could not parse judge response"}

    except Exception as e:
        return {"factuality": 3, "completeness": 3, "citation_quality": 3,
                "context_utilization": 3, "reasoning": f"Judge error: {str(e)[:100]}"}


# ============================================
# COST ESTIMATION
# ============================================

def estimate_query_cost(api_response):
    """Estimate LLM cost based on the query path taken."""
    intent = api_response.get("intent", "")
    tools_used = api_response.get("tools_used", [])
    has_reflection = api_response.get("reflection") is not None
    fallback = api_response.get("fallback", False)

    if fallback:
        # Legacy path: Analyst (0 COMPLETE) or Search+COMPLETE (1) or Synthesis (2)
        if "structured" in intent:
            llm_calls = 0  # Analyst API handles it
        elif "semantic" in intent:
            llm_calls = 1  # Search + COMPLETE
        else:
            llm_calls = 2  # Synthesis (both paths)
    elif intent == "agent" or (tools_used and tools_used != ["structured"]):
        # Agent path: plan (1) + synthesis (1) + optional reflection (1)
        llm_calls = 3 if has_reflection else 2
    else:
        # Fast path or structured via agent
        llm_calls = 1

    return {
        "llm_calls": llm_calls,
        "estimated_cost": round(llm_calls * COST_PER_COMPLETE_CALL, 4),
        "path": "agent" if not fallback else f"legacy_{intent}",
    }


# ============================================
# MAIN EVALUATION
# ============================================

def evaluate_question(q, cur, model: str | None = None):
    """Evaluate a single question with all metrics.

    If `model` is provided, it is passed through to /query so the agent uses
    that Cortex LLM for planning, synthesis, reflection, and verify_claims.
    """
    result = {
        "id": q["id"],
        "question": q["question"],
        "expected_intent": q["expected_intent"],
        "candidate_model": model,
    }

    # Call the API
    start = time.time()
    try:
        payload = {"question": q["question"]}
        if model:
            payload["model"] = model
        resp = requests.post(
            f"{API_BASE}/query",
            json=payload,
            timeout=120,
        )
        latency = time.time() - start
        result["latency_s"] = round(latency, 2)

        if resp.status_code != 200:
            result["api_error"] = resp.text[:200]
            result["intent_correct"] = False
            result["data_correct"] = False
            return result

        data = resp.json()
        result["actual_intent"] = data.get("intent")
        # Functional routing accuracy: did the agent use appropriate tools?
        # "agent" intent is valid when the agent picked the right tool type
        actual = data.get("intent", "")
        expected = q["expected_intent"]
        tools = data.get("tools_used", [])
        if actual == expected:
            result["intent_correct"] = True
        elif actual == "agent" or actual.startswith("structured"):
            # Agent-first routing: check if tool choice matches expected intent
            if expected == "structured" and ("query_analyst" in tools or "structured" in tools):
                result["intent_correct"] = True
            elif expected == "semantic" and "search_reviews" in tools:
                result["intent_correct"] = True
            elif expected == "synthesis" and len(tools) >= 2:
                result["intent_correct"] = True
            else:
                result["intent_correct"] = False
        else:
            result["intent_correct"] = False
        result["answer_preview"] = data.get("answer", "")[:300]
        result["answer_length"] = len(data.get("answer", ""))
        result["sql"] = data.get("sql")
        result["has_sources"] = bool(data.get("sources"))
        result["has_data"] = bool(data.get("data"))
        result["tools_used"] = data.get("tools_used", [])
        result["fallback"] = data.get("fallback", False)

    except Exception as e:
        result["api_error"] = str(e)[:200]
        result["intent_correct"] = False
        result["data_correct"] = False
        return result

    # Check data correctness for SQL-verifiable questions
    if q["ground_truth_type"] == "sql":
        gt_sql = q.get("ground_truth_sql")
        if gt_sql:
            gt_data = run_ground_truth_sql(cur, gt_sql)
            api_data = data.get("data")
            match, detail = check_numerical_match(api_data, gt_data)

            # Secondary check: if data field didn't match, check if the answer TEXT
            # contains the ground truth values (Analyst often returns data in the answer)
            if not match and gt_data and not isinstance(gt_data, str):
                answer_text = data.get("answer", "").lower()
                for row in gt_data:
                    for val in row:
                        val_str = str(val).lower()
                        # Check numeric values with some formatting flexibility
                        try:
                            num_val = float(val)
                            # Check for the number in the answer (with rounding tolerance)
                            for fmt in [f"{num_val:.2f}", f"{num_val:.1f}", f"{num_val:.0f}",
                                       f"{int(num_val)}", f"{num_val:,.0f}"]:
                                if fmt in answer_text:
                                    match = True
                                    detail = f"Answer text contains: {fmt} (gt: {val})"
                                    break
                        except (ValueError, TypeError):
                            # String value — check if it appears in the answer
                            if len(val_str) > 2 and val_str in answer_text:
                                match = True
                                detail = f"Answer text contains: {val_str}"
                        if match:
                            break
                    if match:
                        break

            result["data_correct"] = match
            result["match_detail"] = detail
        else:
            result["data_correct"] = None
    else:
        has_content = len(data.get("answer", "")) > 50
        result["data_correct"] = has_content
        result["match_detail"] = "Qualitative: answer present" if has_content else "Answer too short"

    # LLM-as-judge scoring
    data_summary = ""
    if data.get("data"):
        data_summary = f"{len(data['data'])} data rows returned"
    if data.get("sources"):
        data_summary += f", {len(data['sources'])} review sources"
    if not data_summary:
        data_summary = "No data or sources returned"

    judge_scores = judge_answer(q["question"], data.get("answer", ""), data_summary, cur)
    result["judge_factuality"] = judge_scores.get("factuality", 3)
    result["judge_completeness"] = judge_scores.get("completeness", 3)
    result["judge_citation_quality"] = judge_scores.get("citation_quality", 3)
    result["judge_context_utilization"] = judge_scores.get("context_utilization", 3)
    result["judge_reasoning"] = judge_scores.get("reasoning", "")
    result["is_hallucination"] = judge_scores.get("factuality", 3) < 3

    # Cost estimation
    cost_info = estimate_query_cost(data)
    result["llm_calls"] = cost_info["llm_calls"]
    result["estimated_cost"] = cost_info["estimated_cost"]
    result["query_path"] = cost_info["path"]

    return result


def compute_percentile(values, percentile):
    """Compute percentile from a sorted list."""
    if not values:
        return 0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * percentile / 100)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


# ============================================
# SNOWFLAKE PERSISTENCE (MealMind-inspired)
# ============================================

EVAL_RUNS_INSERT = """
INSERT INTO ANALYTICS.EVAL_RUNS (
    RUN_ID, RUN_TS, MODEL, JUDGE_MODEL, QUESTION_ID, QUESTION_TYPE, QUESTION_TEXT,
    EXPECTED_INTENT, ACTUAL_INTENT, INTENT_CORRECT, DATA_CORRECT,
    JUDGE_FACTUALITY, JUDGE_COMPLETENESS, JUDGE_CITATION, JUDGE_CONTEXT,
    JUDGE_REASONING, IS_HALLUCINATION, LATENCY_MS, TOOLS_USED, FALLBACK_USED,
    ANSWER_PREVIEW, MATCH_DETAIL, LLM_CALLS, ESTIMATED_COST
)
SELECT
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, PARSE_JSON(%s), %s,
    %s, %s, %s, %s
"""


def _truncate(s, n):
    return (s or "")[:n]


def persist_eval_runs(cur, run_id: str, run_ts, model: str, results: list[dict]):
    """Insert per-question eval results into ANALYTICS.EVAL_RUNS.

    Uses single-row INSERT … SELECT (needed for PARSE_JSON on the TOOLS_USED
    ARRAY column). Snowflake's multi-row rewrite doesn't support expressions
    in VALUES, so we loop — negligible at 20-100 rows per run.
    """
    inserted = 0
    for r in results:
        if r.get("api_error"):
            continue
        row = (
            run_id, run_ts, model, JUDGE_MODEL,
            r["id"], _question_type(r["id"]), _truncate(r.get("question"), 2000),
            r.get("expected_intent"), _truncate(r.get("actual_intent"), 64),
            bool(r.get("intent_correct")) if r.get("intent_correct") is not None else None,
            bool(r.get("data_correct")) if r.get("data_correct") is not None else None,
            r.get("judge_factuality"), r.get("judge_completeness"),
            r.get("judge_citation_quality"), r.get("judge_context_utilization"),
            _truncate(r.get("judge_reasoning"), 2000),
            bool(r.get("is_hallucination")) if r.get("is_hallucination") is not None else None,
            float(r.get("latency_s", 0)) * 1000.0,
            json.dumps(r.get("tools_used") or []),
            bool(r.get("fallback", False)),
            _truncate(r.get("answer_preview"), 2000),
            _truncate(r.get("match_detail"), 500),
            r.get("llm_calls"), r.get("estimated_cost"),
        )
        cur.execute(EVAL_RUNS_INSERT, row)
        inserted += 1
    if inserted:
        print(f"  Persisted {inserted} rows to ANALYTICS.EVAL_RUNS (run_id={run_id[:8]}...)")


# ============================================
# MAIN
# ============================================

def parse_args():
    p = argparse.ArgumentParser(description="ReviewSense bake-off eval runner")
    p.add_argument(
        "--models", nargs="+", default=[None],
        help="Candidate Cortex LLM(s) to evaluate. Default: use settings.llm_model. "
             "Examples: --models mistral-large llama3.1-70b snowflake-arctic",
    )
    p.add_argument(
        "--sample", type=int, default=None,
        help="Stratified sample size (preserves cat/prod/theme ratio). Default: full 100.",
    )
    p.add_argument(
        "--run-id", default=None,
        help="Override run_id. Default: new uuid4 shared across all models.",
    )
    return p.parse_args()


def run_single_model(model: str | None, questions: list[dict], cur, run_id: str) -> dict:
    """Run the eval loop for one candidate model, return summary dict."""
    model_label = model or "default"
    print("\n" + "=" * 70)
    print(f"MODEL: {model_label}")
    print("=" * 70)

    results = []
    errors = 0
    run_ts = datetime.now()

    for i, q in enumerate(questions):
        print(f"\n[{i+1}/{len(questions)}] ({model_label}) {q['id']}: {q['question'][:60]}...")
        result = evaluate_question(q, cur, model=model)
        results.append(result)

        if result.get("api_error"):
            errors += 1
            print(f"  ERROR: {result['api_error'][:80]}")
        else:
            intent_mark = "PASS" if result["intent_correct"] else "FAIL"
            print(f"  Intent: {result['actual_intent']} (expected: {result['expected_intent']}) {intent_mark}")
            print(f"  Data: {result.get('match_detail', 'N/A')[:60]}")
            print(f"  Judge: F={result['judge_factuality']} C={result['judge_completeness']} "
                  f"CQ={result['judge_citation_quality']} CU={result['judge_context_utilization']}")
            print(f"  Latency: {result['latency_s']}s | Cost: ${result.get('estimated_cost', 0):.4f} | "
                  f"Path: {result.get('query_path', 'unknown')}")

    # ============================================
    # COMPUTE AGGREGATE METRICS
    # ============================================

    evaluated_results = [r for r in results if not r.get("api_error")]
    total = len(questions)
    evaluated = len(evaluated_results)

    # Core metrics
    intent_correct = sum(1 for r in evaluated_results if r["intent_correct"])
    data_correct = sum(1 for r in evaluated_results if r.get("data_correct"))

    # Latency percentiles
    latencies = [r["latency_s"] for r in evaluated_results]
    latency_p50 = compute_percentile(latencies, 50)
    latency_p95 = compute_percentile(latencies, 95)
    latency_p99 = compute_percentile(latencies, 99)

    # LLM Judge aggregates
    factuality_scores = [r["judge_factuality"] for r in evaluated_results]
    completeness_scores = [r["judge_completeness"] for r in evaluated_results]
    citation_scores = [r["judge_citation_quality"] for r in evaluated_results]
    context_scores = [r["judge_context_utilization"] for r in evaluated_results]

    avg_factuality = sum(factuality_scores) / max(len(factuality_scores), 1)
    avg_completeness = sum(completeness_scores) / max(len(completeness_scores), 1)
    avg_citation = sum(citation_scores) / max(len(citation_scores), 1)
    avg_context = sum(context_scores) / max(len(context_scores), 1)

    # Hallucination rate
    hallucinations = sum(1 for r in evaluated_results if r.get("is_hallucination"))
    hallucination_rate = hallucinations / max(evaluated, 1)

    # Fallback rate
    fallbacks = sum(1 for r in evaluated_results if r.get("fallback"))
    fallback_rate = fallbacks / max(evaluated, 1)

    # Cost
    total_cost = sum(r.get("estimated_cost", 0) for r in evaluated_results)
    avg_cost = total_cost / max(evaluated, 1)

    # Tool utilization
    tool_counter = Counter()
    for r in evaluated_results:
        for tool in r.get("tools_used", []):
            tool_counter[tool] += 1

    # Query path distribution
    path_counter = Counter(r.get("query_path", "unknown") for r in evaluated_results)

    # ============================================
    # PRINT SUMMARY
    # ============================================

    print("\n" + "=" * 70)
    print("EVALUATION SUMMARY")
    print("=" * 70)

    print(f"\n--- Core Metrics ---")
    print(f"Total questions:       {total}")
    print(f"API errors:            {errors}")
    print(f"Intent accuracy:       {intent_correct}/{evaluated} ({intent_correct*100//max(evaluated,1)}%)")
    print(f"Data correctness:      {data_correct}/{evaluated} ({data_correct*100//max(evaluated,1)}%)")

    print(f"\n--- LLM-as-Judge (1-5 scale) ---")
    print(f"Avg factuality:        {avg_factuality:.2f}")
    print(f"Avg completeness:      {avg_completeness:.2f}")
    print(f"Avg citation quality:  {avg_citation:.2f}")
    print(f"Avg context util:      {avg_context:.2f}")
    print(f"Hallucination rate:    {hallucinations}/{evaluated} ({hallucination_rate*100:.1f}%)")

    print(f"\n--- Latency ---")
    print(f"P50:                   {latency_p50:.1f}s")
    print(f"P95:                   {latency_p95:.1f}s")
    print(f"P99:                   {latency_p99:.1f}s")
    print(f"Avg:                   {sum(latencies)/max(len(latencies),1):.1f}s")

    print(f"\n--- Cost ---")
    print(f"Avg cost/query:        ${avg_cost:.4f}")
    print(f"Total eval cost:       ${total_cost:.2f}")

    print(f"\n--- System Health ---")
    print(f"Fallback rate:         {fallbacks}/{evaluated} ({fallback_rate*100:.1f}%)")
    print(f"Query paths:           {dict(path_counter)}")
    print(f"Tool utilization:      {dict(tool_counter.most_common(10))}")

    # Intent breakdown
    print(f"\n--- Intent Breakdown ---")
    for intent in ["structured", "semantic", "synthesis"]:
        subset = [r for r in evaluated_results if r["expected_intent"] == intent]
        if subset:
            correct = sum(1 for r in subset if r["intent_correct"])
            avg_f = sum(r["judge_factuality"] for r in subset) / len(subset)
            print(f"  {intent}: {correct}/{len(subset)} intent acc, {avg_f:.1f} avg factuality")

    # ============================================
    # SAVE RESULTS + PERSIST TO SNOWFLAKE
    # ============================================

    summary = {
        "total_questions": total,
        "api_errors": errors,
        "evaluated": evaluated,
        "intent_accuracy": round(intent_correct / max(evaluated, 1), 4),
        "data_correctness": round(data_correct / max(evaluated, 1), 4),
        "avg_factuality": round(avg_factuality, 2),
        "avg_completeness": round(avg_completeness, 2),
        "avg_citation_quality": round(avg_citation, 2),
        "avg_context_utilization": round(avg_context, 2),
        "hallucination_rate": round(hallucination_rate, 4),
        "hallucination_count": hallucinations,
        "fallback_rate": round(fallback_rate, 4),
        "fallback_count": fallbacks,
        "latency_p50": round(latency_p50, 2),
        "latency_p95": round(latency_p95, 2),
        "latency_p99": round(latency_p99, 2),
        "latency_avg": round(sum(latencies) / max(len(latencies), 1), 2),
        "avg_cost_per_query": round(avg_cost, 4),
        "total_eval_cost": round(total_cost, 2),
    }

    file_model_label = (model or "default").replace("/", "_").replace(" ", "_")
    output_path = os.path.join(os.path.dirname(__file__), f"eval_results_{file_model_label}.json")
    with open(output_path, "w") as f:
        json.dump({
            "run_id": run_id,
            "model": model,
            "judge_model": JUDGE_MODEL,
            "timestamp": run_ts.isoformat(),
            "summary": summary,
            "tool_utilization": dict(tool_counter.most_common()),
            "path_distribution": dict(path_counter),
            "intent_breakdown": {
                intent: {
                    "count": len([r for r in evaluated_results if r["expected_intent"] == intent]),
                    "intent_accuracy": round(
                        sum(1 for r in evaluated_results if r["expected_intent"] == intent and r["intent_correct"])
                        / max(len([r for r in evaluated_results if r["expected_intent"] == intent]), 1), 4
                    ),
                    "avg_factuality": round(
                        sum(r["judge_factuality"] for r in evaluated_results if r["expected_intent"] == intent)
                        / max(len([r for r in evaluated_results if r["expected_intent"] == intent]), 1), 2
                    ),
                }
                for intent in ["structured", "semantic", "synthesis"]
            },
            "results": results,
        }, f, indent=2, default=str)

    print(f"\nDetailed results saved to: {output_path}")

    # Persist to Snowflake (MealMind-inspired eval_logs pattern)
    try:
        persist_eval_runs(cur, run_id, run_ts, model or "default", results)
    except Exception as e:
        print(f"  WARN: Snowflake persistence failed: {e}")

    return {"model": model, "summary": summary, "output_path": output_path}


def main():
    args = parse_args()

    run_id = args.run_id or str(uuid.uuid4())
    questions = list(EVAL_QUESTIONS)
    if args.sample:
        questions = stratified_sample(questions, args.sample)

    print("ReviewSense AI — Bake-Off Evaluation")
    print(f"Run ID:          {run_id}")
    print(f"Judge model:     {JUDGE_MODEL}")
    print(f"Candidates:      {', '.join(m or 'default' for m in args.models)}")
    print(f"Questions:       {len(questions)} (of {len(EVAL_QUESTIONS)} total)")
    print(f"Started:         {datetime.now().isoformat()}")
    print("=" * 70)

    # Check API is up
    try:
        health = requests.get(f"{API_BASE}/health", timeout=10)
        print(f"API Health: {health.json()}")
    except Exception:
        print("ERROR: API is not running. Start it with: python -m uvicorn api.main:app")
        sys.exit(1)

    conn, cur = get_snowflake_cursor()
    try:
        bake_off_summary = []
        for model in args.models:
            try:
                s = run_single_model(model, questions, cur, run_id)
                bake_off_summary.append(s)
            except Exception as e:
                print(f"ERROR running {model or 'default'}: {e}")
                bake_off_summary.append({"model": model, "error": str(e)[:300]})

        # Final cross-model table (quick glance)
        print("\n" + "=" * 70)
        print("BAKE-OFF SUMMARY")
        print("=" * 70)
        print(f"{'Model':<22} {'Intent%':>8} {'Data%':>7} {'Fact':>5} {'Cite':>5} {'Hallu%':>7} {'P95s':>6}")
        for s in bake_off_summary:
            if "error" in s:
                print(f"{(s['model'] or 'default'):<22} ERROR: {s['error'][:80]}")
                continue
            summ = s["summary"]
            print(
                f"{(s['model'] or 'default'):<22} "
                f"{summ['intent_accuracy']*100:>7.1f}% "
                f"{summ['data_correctness']*100:>6.1f}% "
                f"{summ['avg_factuality']:>5.2f} "
                f"{summ['avg_citation_quality']:>5.2f} "
                f"{summ['hallucination_rate']*100:>6.1f}% "
                f"{summ['latency_p95']:>5.1f}"
            )
        print(f"\nRun ID: {run_id}  —  query ANALYTICS.V_EVAL_MODEL_SUMMARY for details")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
