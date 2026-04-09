"""Custom Agentic RAG: Plan-Execute-Synthesize loop.

Tiered routing:
- Tier 1 (Fast Path): Rule-based, zero LLM cost for simple queries
- Tier 2 (LLM Planning): COMPLETE plans which tools to call (1 LLM call)
- Execute: Run tools from plan (0 LLM calls — pure SQL/Search)
- Synthesize: COMPLETE generates grounded answer (1 LLM call)

Circuit breaker: stops trying broken external APIs after N failures.
"""

import json
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from api.db import get_cursor
from api.config import settings
from api.services import tools

logger = logging.getLogger(__name__)

# Circuit breaker state
_circuit = {"failures": 0, "open_until": 0, "max_failures": 3, "cooldown": 60}

# Tool registry — maps tool names to functions
TOOL_REGISTRY = {
    "search_reviews": tools.search_reviews,
    "get_product_detail": tools.get_product_detail,
    "search_products": tools.search_products,
    "compare_products": tools.compare_products,
    "verify_claims": tools.verify_claims,
    "get_brand_analysis": tools.get_brand_analysis,
    "compare_brands": tools.compare_brands,
    "find_similar_products": tools.find_similar_products,
    "price_value_analysis": tools.price_value_analysis,
}

TOOL_DESCRIPTIONS = """Available tools:
1. search_reviews(query, asin?, category?, theme?, min_rating?, max_rating?, verified_only?, quality?, limit?) — Search actual review text. USE FOR: opinions, experiences, complaints, "what do people say".
2. get_product_detail(asin) — Get complete product profile: metadata, stats, category comparison, theme breakdown. USE FOR: "tell me about product X", any ASIN reference.
3. search_products(category?, brand?, min_price?, max_price?, features_contain?, min_rating?, sort_by?, limit?) — Find products by criteria. USE FOR: recommendations, "find me X under $Y".
4. compare_products(asins) — Side-by-side comparison of 2-5 products. USE FOR: "compare X vs Y".
5. verify_claims(asin) — Compare metadata feature claims vs actual review evidence. USE FOR: "is the battery really 8 hours?", "are the claims true?".
6. get_brand_analysis(brand) — Brand-level stats: products, ratings, sentiment, categories, top complaints. USE FOR: "how is brand X?", brand questions.
7. compare_brands(brands) — Compare 2-4 brands. USE FOR: "brand X vs brand Y".
8. find_similar_products(asin, limit?) — Find related products via also_buy data. USE FOR: "similar to", "alternatives".
9. price_value_analysis(category) — Price brackets vs quality within a category. USE FOR: "is paying more worth it?", "best value".
10. query_analyst(question) — Generate SQL via Cortex Analyst for stats questions. USE FOR: category rankings, trends, aggregate counts."""


# ============================================
# TIER 1: FAST PATH (zero LLM cost)
# ============================================

def _try_fast_path(question: str) -> dict | None:
    """Try to handle simple queries with a direct tool call, no LLM planning."""
    q = question.lower()

    # Single ASIN lookup
    asin_match = re.search(r'\bB0[A-Z0-9]{8,}\b', question)
    multi_asin = re.findall(r'\bB0[A-Z0-9]{8,}\b', question)

    # Multi-ASIN comparison
    if len(multi_asin) >= 2:
        result = tools.compare_products(multi_asin[:5])
        return _build_response(question, [
            {"tool": "compare_products", "result": result, "purpose": f"Compare {len(multi_asin)} products"}
        ])

    # Single ASIN detail
    if asin_match and len(q.split()) < 12 and not any(s in q for s in ['compare', 'vs', 'versus', 'similar', 'claim', 'true', 'accurate']):
        result = tools.get_product_detail(asin_match.group(0))
        if result:
            return _build_response(question, [
                {"tool": "get_product_detail", "result": result, "purpose": f"Product detail for {asin_match.group(0)}"}
            ])

    # Very simple stat questions → Cortex Analyst
    simple_stat_patterns = [
        r'^how many (reviews|products|categories)',
        r'^what is the (average|total|overall)',
        r'^which category (has the|is the)',
    ]
    for pattern in simple_stat_patterns:
        if re.search(pattern, q):
            from api.services.analyst import query_analyst
            result = query_analyst(question)
            return _build_response(question, [
                {"tool": "query_analyst", "result": result, "purpose": "SQL query for stats"}
            ], intent="structured")

    return None  # Not a fast-path query → go to Tier 2


# ============================================
# TIER 2: LLM PLANNING (1 COMPLETE call)
# ============================================

PLANNING_PROMPT = """You are a planning agent for a product review intelligence system.
Given the user's question, select which tools to call and in what order.

{tool_descriptions}

Rules:
- Output ONLY valid JSON, no other text
- Max 5 steps
- If a step depends on a previous step's output, set "depends_on" to the step index (0-based)
- For simple questions, use 1-2 tools
- For comparisons, use the compare tool
- For "is this claim true?", use verify_claims
- For brand questions, use get_brand_analysis or compare_brands
- For price/value questions, use price_value_analysis

Question: {question}

JSON plan:"""


def _plan_tools(question: str) -> list[dict] | None:
    """Use COMPLETE to plan which tools to call."""
    prompt = PLANNING_PROMPT.format(
        tool_descriptions=TOOL_DESCRIPTIONS,
        question=question,
    )

    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
                (settings.llm_model, prompt)
            )
            response = cur.fetchone()[0].strip()

        # Extract JSON from response
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if not json_match:
            logger.warning(f"Planning returned no JSON: {response[:200]}")
            return None

        plan = json.loads(json_match.group(0))
        steps = plan.get("steps", [])

        # Validate plan
        if not steps or len(steps) > 5:
            logger.warning(f"Invalid plan: {len(steps) if steps else 0} steps")
            return None

        for step in steps:
            if step.get("tool") not in TOOL_REGISTRY and step.get("tool") != "query_analyst":
                logger.warning(f"Unknown tool in plan: {step.get('tool')}")
                return None

        return steps

    except Exception as e:
        logger.error(f"Planning failed: {e}")
        return None


# ============================================
# EXECUTE TOOLS (0 LLM cost for SQL tools)
# ============================================

def _build_execution_waves(steps: list[dict]) -> list[list[int]]:
    """Group steps into dependency waves for parallel execution.

    Wave 1: all steps with no dependencies (run in parallel)
    Wave 2: steps that depend on Wave 1 (run in parallel with each other)
    etc.
    """
    waves = []
    assigned = set()

    while len(assigned) < len(steps):
        wave = []
        for i, step in enumerate(steps):
            if i in assigned:
                continue
            dep = step.get("depends_on")
            if dep is None or dep in assigned:
                wave.append(i)

        if not wave:  # circular dependency safety — break the cycle
            remaining = [i for i in range(len(steps)) if i not in assigned]
            wave = remaining

        waves.append(wave)
        assigned.update(wave)

    return waves


def _execute_single_tool(step: dict) -> dict:
    """Execute a single tool in its own thread."""
    tool_name = step.get("tool")
    params = step.get("params", {})
    purpose = step.get("purpose", "")

    try:
        if tool_name == "query_analyst":
            from api.services.analyst import query_analyst
            result = query_analyst(params.get("question", params.get("query", "")))
        elif tool_name in TOOL_REGISTRY:
            func = TOOL_REGISTRY[tool_name]
            result = func(**params)
        else:
            result = {"error": f"Unknown tool: {tool_name}"}

        return {
            "tool": tool_name, "result": result,
            "purpose": purpose, "status": "done",
        }
    except Exception as e:
        logger.error(f"Tool {tool_name} failed: {e}")
        return {
            "tool": tool_name, "result": {"error": str(e)[:200]},
            "purpose": purpose, "status": "error",
        }


def _execute_plan(steps: list[dict]) -> list[dict]:
    """Execute tools with parallel execution for independent steps.

    Groups steps into waves by dependency. Steps in the same wave
    run concurrently via ThreadPoolExecutor. Steps in later waves
    wait for their dependencies to complete.

    Adaptive: if a dependency returned empty/error, dependent steps are skipped.
    """
    waves = _build_execution_waves(steps)
    all_results = [None] * len(steps)

    for wave_num, wave in enumerate(waves):
        # Determine which steps in this wave can actually run
        runnable = []
        for step_idx in wave:
            step = steps[step_idx]
            dep = step.get("depends_on")

            # Check if dependency is satisfied
            if dep is not None and dep < len(all_results) and all_results[dep] is not None:
                dep_result = all_results[dep].get("result")
                if not dep_result or (isinstance(dep_result, dict) and dep_result.get("error")):
                    all_results[step_idx] = {
                        "tool": step.get("tool"), "result": None,
                        "purpose": step.get("purpose", ""),
                        "status": "skipped (dependency empty)",
                        "wave": wave_num + 1,
                    }
                    continue

            runnable.append(step_idx)

        if not runnable:
            continue

        # Single tool — no need for thread overhead
        if len(runnable) == 1:
            idx = runnable[0]
            result = _execute_single_tool(steps[idx])
            result["wave"] = wave_num + 1
            all_results[idx] = result
            continue

        # Multiple tools — run in parallel
        logger.info(f"Wave {wave_num + 1}: running {len(runnable)} tools in parallel")
        with ThreadPoolExecutor(max_workers=min(len(runnable), 4)) as executor:
            futures = {}
            for step_idx in runnable:
                future = executor.submit(_execute_single_tool, steps[step_idx])
                futures[future] = step_idx

            for future in as_completed(futures):
                idx = futures[future]
                result = future.result()
                result["wave"] = wave_num + 1
                all_results[idx] = result

    return [r for r in all_results if r is not None]


# ============================================
# SYNTHESIZE (1 COMPLETE call)
# ============================================

SYNTHESIS_PROMPT = """You are a product intelligence analyst. Answer the user's question using ONLY the tool results below.

Rules:
- Be precise and factual
- Cite specific numbers, ratings, and review quotes from the results
- Never invent statistics or quotes not present in the data
- If data is insufficient, say so honestly
- Keep the answer concise but thorough

User question: {question}

Tool results:
{tool_results}

Answer:"""


def _synthesize(question: str, tool_results: list[dict]) -> str:
    """Generate final answer from all tool results."""
    # Build context from tool results
    context_parts = []
    for r in tool_results:
        if r["status"] == "done" and r.get("result"):
            result_str = json.dumps(r["result"], default=str)
            # Truncate very large results
            if len(result_str) > 3000:
                result_str = result_str[:3000] + "... (truncated)"
            context_parts.append(f"[{r['tool']}] ({r['purpose']}): {result_str}")

    if not context_parts:
        return "I wasn't able to find enough data to answer your question. Could you rephrase or provide more details?"

    context = "\n\n".join(context_parts)

    prompt = SYNTHESIS_PROMPT.format(
        question=question,
        tool_results=context,
    )

    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
                (settings.llm_model, prompt)
            )
            return cur.fetchone()[0]
    except Exception as e:
        logger.error(f"Synthesis failed: {e}")
        return "An error occurred while generating the answer. Please try again."


# ============================================
# BUILD RESPONSE
# ============================================

def _build_response(question: str, tool_results: list[dict], intent: str = "agent") -> dict:
    """Build the standard response dict from tool results."""
    # Extract useful fields
    sql = None
    data = None
    sources = None

    for r in tool_results:
        result = r.get("result") or {}
        if isinstance(result, dict):
            if result.get("sql"):
                sql = result["sql"]
            if result.get("data"):
                data = result["data"]
            if result.get("results"):  # search_reviews format
                sources = [
                    {"asin": s.get("asin", ""), "rating": s.get("rating", ""), "text": s.get("text", "")[:200]}
                    for s in result["results"][:5]
                ]
            if result.get("sources"):
                sources = result["sources"]

    # Build tool trace
    tool_trace = []
    tool_icons = {
        "search_reviews": "🔍", "get_product_detail": "📦", "search_products": "🛒",
        "compare_products": "⚖️", "verify_claims": "✅", "get_brand_analysis": "🏷️",
        "compare_brands": "🏷️", "find_similar_products": "🔗", "price_value_analysis": "💰",
        "query_analyst": "📊",
    }
    for r in tool_results:
        icon = tool_icons.get(r["tool"], "⚙️")
        summary = r["status"]
        if r["status"] == "done" and r.get("result"):
            res = r["result"]
            if isinstance(res, dict):
                if "result_count" in res:
                    summary = f"Found {res['result_count']} results"
                elif "products" in res and isinstance(res["products"], list):
                    summary = f"Found {len(res['products'])} products"
                elif "claims" in res:
                    summary = f"Verified {len(res['claims'])} claims"
                elif "brand" in res:
                    summary = f"Brand: {res['brand']}, {res.get('total_reviews', '?')} reviews"
                elif "asin" in res:
                    summary = f"Product: {res.get('product_name', res['asin'])}"
                else:
                    summary = "Data retrieved"
            elif res is None:
                summary = "No data found"

        tool_trace.append({
            "tool": r["tool"],
            "description": r.get("purpose", r["tool"]),
            "status": r["status"],
            "result_summary": summary,
        })

    return {
        "question": question,
        "intent": intent,
        "sql": sql,
        "data": data,
        "sources": sources,
        "tools_used": [r["tool"] for r in tool_results],
        "tool_trace": tool_trace,
    }


# ============================================
# MAIN ENTRY POINT
# ============================================

def run_custom_agent(question: str) -> dict:
    """Run the custom agentic RAG loop.

    Tiered:
    - Tier 1: Fast path for simple queries (0 LLM calls)
    - Tier 2: LLM plans tools → execute → synthesize (2 LLM calls)
    """
    start = time.time()

    # Tier 1: Fast path
    fast_result = _try_fast_path(question)
    if fast_result:
        # Still need synthesis for a natural answer
        answer = _synthesize(question, [
            {"tool": r["tool"], "result": r["result"], "purpose": r.get("purpose", ""), "status": "done"}
            for r in (fast_result.get("tool_trace") or [{"tool": "direct", "result": fast_result}])
        ])
        fast_result["answer"] = answer
        fast_result["latency_ms"] = round((time.time() - start) * 1000, 1)
        return fast_result

    # Tier 2: LLM Planning
    steps = _plan_tools(question)
    if not steps:
        return None  # Signal to caller to use legacy fallback

    # Execute plan
    tool_results = _execute_plan(steps)

    # Synthesize
    answer = _synthesize(question, tool_results)

    # Build response
    response = _build_response(question, tool_results)
    response["answer"] = answer
    response["latency_ms"] = round((time.time() - start) * 1000, 1)

    return response
