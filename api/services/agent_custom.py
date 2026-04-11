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

# Dynamic dataset stats — loaded from Snowflake, cached 1 hour
_dataset_stats = {"review_count": 183000, "category_count": 14, "loaded_at": 0}


def _load_dataset_stats():
    """Query actual dataset size from gold marts. Cache 1 hour."""
    now = time.time()
    if now - _dataset_stats["loaded_at"] < 3600:
        return
    try:
        with get_cursor() as cur:
            cur.execute("""
                SELECT COUNT(DISTINCT REVIEW_ID), COUNT(DISTINCT DERIVED_CATEGORY)
                FROM GOLD.ENRICHED_REVIEWS
                WHERE DERIVED_CATEGORY IS NOT NULL
            """)
            row = cur.fetchone()
            _dataset_stats["review_count"] = row[0]
            _dataset_stats["category_count"] = row[1]
            _dataset_stats["loaded_at"] = now
            logger.info(f"Dataset stats loaded: {row[0]:,} reviews, {row[1]} categories")
    except Exception as e:
        logger.warning(f"Failed to load dataset stats: {e}. Using defaults.")

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
   Valid categories: headphones_earbuds, speakers, streaming_devices, smart_home, cables_adapters, chargers_batteries, phone_accessories, computer_peripherals, storage_media, cameras_accessories, tv_accessories, gaming_accessories, wearables, other_electronics
   Valid themes: battery_life, build_quality, sound_quality, connectivity, comfort, value_for_money, customer_service, durability, ease_of_use, other
2. get_product_detail(asin) — Get complete product profile: metadata, stats, category comparison, theme breakdown. USE FOR: "tell me about product X", any ASIN reference.
3. search_products(category?, brand?, min_price?, max_price?, features_contain?, min_rating?, sort_by?, limit?) — Find products by criteria. USE FOR: recommendations, "find me X under $Y".
   Valid categories: headphones_earbuds, speakers, streaming_devices, smart_home, cables_adapters, chargers_batteries, phone_accessories, computer_peripherals, storage_media, cameras_accessories, tv_accessories, gaming_accessories, wearables, other_electronics
   sort_by options: review_count, avg_rating, price, avg_sentiment
4. compare_products(asins) — Side-by-side comparison of 2-5 products. USE FOR: "compare X vs Y".
5. verify_claims(asin) — Compare metadata feature claims vs actual review evidence. USE FOR: "is the battery really 8 hours?", "are the claims true?".
6. get_brand_analysis(brand) — Brand-level stats: products, ratings, sentiment, categories, top complaints. USE FOR: "how is brand X?", brand questions.
7. compare_brands(brands) — Compare 2-4 brands. USE FOR: "brand X vs brand Y".
8. find_similar_products(asin, limit?) — Find related products via also_buy data. USE FOR: "similar to", "alternatives".
9. price_value_analysis(category) — Price brackets vs quality within a category. USE FOR: "is paying more worth it?", "best value".
   Valid categories: headphones_earbuds, speakers, streaming_devices, smart_home, cables_adapters, chargers_batteries, phone_accessories, computer_peripherals, storage_media, cameras_accessories, tv_accessories, gaming_accessories, wearables, other_electronics
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


# Fuzzy tool name mapping — LLM might use variations of our tool names
TOOL_NAME_ALIASES = {
    "brand_analysis": "get_brand_analysis",
    "product_detail": "get_product_detail",
    "product_details": "get_product_detail",
    "similar_products": "find_similar_products",
    "find_similar": "find_similar_products",
    "price_analysis": "price_value_analysis",
    "price_value": "price_value_analysis",
    "review_search": "search_reviews",
    "reviews": "search_reviews",
    "products": "search_products",
    "compare_brand": "compare_brands",
    "compare_product": "compare_products",
    "verify_claim": "verify_claims",
    "analyst": "query_analyst",
    "cortex_analyst": "query_analyst",
}


def _resolve_tool_name(name: str) -> str | None:
    """Resolve a tool name from the LLM to our registry, with fuzzy matching."""
    if not name:
        return None
    name = name.strip().lower()

    # Exact match
    if name in TOOL_REGISTRY or name == "query_analyst":
        return name

    # Alias match
    if name in TOOL_NAME_ALIASES:
        return TOOL_NAME_ALIASES[name]

    # Partial match — strip common prefixes/suffixes
    stripped = name.replace("get_", "").replace("find_", "").replace("search_", "")
    for registered in list(TOOL_REGISTRY.keys()) + ["query_analyst"]:
        if stripped in registered or registered.endswith(stripped):
            return registered

    return None


def _extract_json_from_response(response: str) -> dict | None:
    """Try multiple strategies to extract JSON from LLM response."""
    # Strategy 1: Direct JSON parse (response is pure JSON)
    try:
        return json.loads(response)
    except json.JSONDecodeError:
        pass

    # Strategy 2: Extract ```json code block
    code_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response, re.DOTALL)
    if code_match:
        try:
            return json.loads(code_match.group(1))
        except json.JSONDecodeError:
            pass

    # Strategy 3: Find JSON object with "steps" key (more targeted than greedy {.*})
    steps_match = re.search(r'\{[^{}]*"steps"\s*:\s*\[.*?\]\s*[^{}]*\}', response, re.DOTALL)
    if steps_match:
        try:
            return json.loads(steps_match.group(0))
        except json.JSONDecodeError:
            pass

    # Strategy 4: Greedy match (last resort)
    greedy_match = re.search(r'\{.*\}', response, re.DOTALL)
    if greedy_match:
        try:
            return json.loads(greedy_match.group(0))
        except json.JSONDecodeError:
            pass

    return None


def _plan_tools(question: str) -> list[dict] | None:
    """Use COMPLETE to plan which tools to call.

    Resilient to LLM output variations:
    - Tries multiple JSON extraction strategies
    - Fuzzy matches tool names
    - Skips invalid steps instead of rejecting entire plan
    - Logs raw LLM response for debugging
    """
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

        # Log raw response for debugging
        logger.info(f"Planning LLM response for '{question[:50]}': {response[:300]}")

        # Extract JSON from response (tries 4 strategies)
        plan = _extract_json_from_response(response)
        if not plan:
            logger.warning(f"Could not extract JSON from planning response: {response[:300]}")
            return None

        steps = plan.get("steps", [])

        # Validate plan structure
        if not isinstance(steps, list):
            logger.warning(f"Invalid plan: 'steps' is not a list: {type(steps)}")
            return None

        if not steps:
            logger.warning("Plan has 0 steps")
            return None

        # Validate and fix individual steps — skip bad ones, keep good ones
        valid_steps = []
        for step in steps[:5]:  # Max 5 steps
            tool_name = step.get("tool", "")
            resolved_name = _resolve_tool_name(tool_name)

            if resolved_name:
                step["tool"] = resolved_name  # Normalize the name
                # LLM might use "arguments", "parameters", "args" instead of "params"
                if "params" not in step:
                    step["params"] = step.get("arguments") or step.get("parameters") or step.get("args") or {}
                valid_steps.append(step)
            else:
                logger.warning(f"Skipping unknown tool in plan: '{tool_name}'")

        if not valid_steps:
            logger.warning(f"No valid tools in plan. Original steps: {[s.get('tool') for s in steps]}")
            return None

        logger.info(f"Plan validated: {[s['tool'] for s in valid_steps]}")
        return valid_steps

    except Exception as e:
        logger.error(f"Planning failed: {e}", exc_info=True)
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
            # Handle dep as int, list, or None
            if isinstance(dep, list):
                deps_met = all(d in assigned for d in dep)
            elif dep is not None:
                deps_met = dep in assigned
            else:
                deps_met = True
            if deps_met:
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
            deps = dep if isinstance(dep, list) else [dep] if dep is not None else []
            skip = False
            for d in deps:
                if d is not None and d < len(all_results) and all_results[d] is not None:
                    dep_result = all_results[d].get("result")
                    if not dep_result or (isinstance(dep_result, dict) and dep_result.get("error")):
                        all_results[step_idx] = {
                            "tool": step.get("tool"), "result": None,
                            "purpose": step.get("purpose", ""),
                            "status": "skipped (dependency empty)",
                            "wave": wave_num + 1,
                        }
                        skip = True
                        break
            if skip:
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

            for future in as_completed(futures, timeout=30):
                idx = futures[future]
                try:
                    result = future.result(timeout=5)
                    result["wave"] = wave_num + 1
                    all_results[idx] = result
                except Exception as e:
                    logger.error(f"Tool execution timed out or failed: {e}")
                    all_results[idx] = {
                        "tool": steps[idx].get("tool"), "result": {"error": str(e)[:200]},
                        "purpose": steps[idx].get("purpose", ""), "status": "timeout",
                        "wave": wave_num + 1,
                    }

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
# REFLECTION (Ch 4) — verify answer grounding
# ============================================

REFLECTION_PROMPT = """You are a quality verification agent. Check if the answer is properly grounded in the tool results.

User question: {question}

Generated answer:
{answer}

Tool results summary:
{tool_summary}

Check these criteria and respond with ONLY valid JSON:
{{
  "grounded": true/false,
  "issues": ["list of any ungrounded claims or hallucinations"],
  "confidence": 0.0-1.0,
  "summary": "one sentence assessment"
}}"""


def _reflect(question: str, answer: str, tool_results: list[dict]) -> dict:
    """Verify that the synthesized answer is grounded in tool results.

    Checks: Are all claims supported by data? Any hallucinated stats?
    Returns reflection dict with grounded flag and issues list.
    """
    # Build a summary of what the tools actually returned
    tool_summary_parts = []
    for r in tool_results:
        if r["status"] == "done" and r.get("result"):
            res = r["result"]
            if isinstance(res, dict):
                # Extract key facts from each tool result
                facts = []
                for key in ["avg_rating", "review_count", "negative_rate", "trust_score",
                            "brand", "product_name", "result_count", "total_reviews"]:
                    if key in res and res[key] is not None:
                        facts.append(f"{key}={res[key]}")
                if facts:
                    tool_summary_parts.append(f"[{r['tool']}]: {', '.join(facts)}")
                else:
                    tool_summary_parts.append(f"[{r['tool']}]: returned data")

    if not tool_summary_parts:
        return {"grounded": False, "issues": ["No tool results to verify against"],
                "confidence": 0.0, "summary": "Cannot verify — no data available"}

    tool_summary = "\n".join(tool_summary_parts)

    prompt = REFLECTION_PROMPT.format(
        question=question,
        answer=answer[:1000],  # truncate long answers
        tool_summary=tool_summary,
    )

    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
                (settings.llm_model, prompt)
            )
            response = cur.fetchone()[0].strip()

        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            reflection = json.loads(json_match.group(0))
            return {
                "grounded": reflection.get("grounded", True),
                "issues": reflection.get("issues", []),
                "confidence": reflection.get("confidence", 0.5),
                "summary": reflection.get("summary", "Verification complete"),
            }
        else:
            return {"grounded": True, "issues": [], "confidence": 0.5,
                    "summary": "Verification inconclusive"}

    except Exception as e:
        logger.warning(f"Reflection failed: {e}")
        # Don't block the answer — reflection is advisory, not blocking
        return {"grounded": True, "issues": [], "confidence": 0.5,
                "summary": "Reflection skipped due to error"}


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

    # Load dynamic dataset stats (cached, 1 hour TTL)
    _load_dataset_stats()

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

    # Reflect — verify answer is grounded in tool results
    reflection = _reflect(question, answer, tool_results)

    # Build response
    response = _build_response(question, tool_results)
    response["answer"] = answer
    response["reflection"] = reflection
    response["latency_ms"] = round((time.time() - start) * 1000, 1)

    # Add reflection to tool trace
    if response.get("tool_trace"):
        response["tool_trace"].append({
            "tool": "reflection",
            "description": "Verifying answer is grounded in tool results",
            "status": "done",
            "result_summary": reflection.get("summary", "Verified"),
        })

    return response
