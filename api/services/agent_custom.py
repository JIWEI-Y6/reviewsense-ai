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
3. search_products(category?, brand?, min_price?, max_price?, features_contain?, min_rating?, sort_by?, limit?, review_theme?) — Find products by criteria. USE FOR: recommendations, "find me X under $Y", "comfortable headphones", "durable speakers".
   Valid categories: headphones_earbuds, speakers, streaming_devices, smart_home, cables_adapters, chargers_batteries, phone_accessories, computer_peripherals, storage_media, cameras_accessories, tv_accessories, gaming_accessories, wearables, other_electronics
   sort_by options: review_count, avg_rating, price, avg_sentiment, theme_sentiment
   review_theme: Filter to products with POSITIVE reviews about this theme. Valid: battery_life, build_quality, sound_quality, connectivity, comfort, value_for_money, customer_service, durability, ease_of_use, other. Ranked by theme sentiment.
4. compare_products(asins) — Side-by-side comparison of 2-5 products. USE FOR: "compare X vs Y".
5. verify_claims(asin) — Compare metadata feature claims vs actual review evidence. USE FOR: "is the battery really 8 hours?", "are the claims true?".
6. get_brand_analysis(brand) — Brand-level stats: products, ratings, sentiment, categories, top complaints. USE FOR: "how is brand X?", brand questions.
7. compare_brands(brands) — Compare 2-4 brands. USE FOR: "brand X vs brand Y".
8. find_similar_products(asin, limit?) — Find related products via also_buy data. USE FOR: "similar to", "alternatives".
9. price_value_analysis(category) — Price brackets vs quality within a category. USE FOR: "is paying more worth it?", "best value".
   Valid categories: headphones_earbuds, speakers, streaming_devices, smart_home, cables_adapters, chargers_batteries, phone_accessories, computer_peripherals, storage_media, cameras_accessories, tv_accessories, gaming_accessories, wearables, other_electronics
10. query_analyst(question) — Generate SQL via Cortex Analyst for stats questions. USE FOR: category rankings, trends, aggregate counts."""


# ============================================
# CONVERSATION CONTEXT
# ============================================

def _format_conversation_context(conversation_history=None, session_context=None) -> str:
    """Build concise context string from conversation history and session context.

    Two-layer approach:
    1. Structured entity extraction — scans FULL message text for ASINs and product
       names before any truncation, so entities are never lost regardless of response length.
    2. Sliding window — truncated raw text for general conversational context.
    """
    parts = []

    # Layer 1a: Entity memory from session context
    if session_context:
        entities = []
        if getattr(session_context, 'products_discussed', None):
            entities.append(f"Products: {', '.join(session_context.products_discussed[-5:])}")
        if getattr(session_context, 'brands_discussed', None):
            entities.append(f"Brands: {', '.join(session_context.brands_discussed[-3:])}")
        if getattr(session_context, 'categories_discussed', None):
            entities.append(f"Categories: {', '.join(session_context.categories_discussed[-3:])}")
        if entities:
            parts.append("Session entities: " + " | ".join(entities))

    if conversation_history:
        # Layer 1b: Extract entities from FULL text before truncation
        conv_products = []
        seen_asins = set()
        for msg in conversation_history:
            role = msg.role if hasattr(msg, 'role') else msg.get('role', '')
            content = msg.content if hasattr(msg, 'content') else msg.get('content', '')
            if role == "assistant" and content:
                # Extract ASINs with surrounding product name context
                for m in re.finditer(
                    r'(?:the |, )?([A-Z][\w\s\-]{3,40}?)\s*\(?(B0[A-Z0-9]{8,})\)?', content
                ):
                    name_hint = m.group(1).strip().rstrip('(')
                    asin = m.group(2)
                    if asin not in seen_asins:
                        conv_products.append(f"{asin} ({name_hint})")
                        seen_asins.add(asin)
                # Catch standalone ASINs without adjacent product names
                for asin in re.findall(r'\bB0[A-Z0-9]{8,}\b', content):
                    if asin not in seen_asins:
                        conv_products.append(asin)
                        seen_asins.add(asin)
        if conv_products:
            parts.append("Products referenced in conversation: " + ", ".join(conv_products))

        # Layer 2: Sliding window — last 2 exchanges, tiered truncation
        if len(conversation_history) >= 2:
            recent = conversation_history[-4:]
            # Find the last assistant message for higher truncation limit
            last_asst_idx = None
            for i in range(len(recent) - 1, -1, -1):
                role = recent[i].role if hasattr(recent[i], 'role') else recent[i].get('role', '')
                if role == 'assistant':
                    last_asst_idx = i
                    break
            lines = []
            for i, msg in enumerate(recent):
                role = msg.role if hasattr(msg, 'role') else msg.get('role', '')
                content = msg.content if hasattr(msg, 'content') else msg.get('content', '')
                # Last assistant message gets more room; others stay compact
                char_limit = 400 if i == last_asst_idx else 150
                truncated = content[:char_limit] + ("..." if len(content) > char_limit else "")
                lines.append(f"{'User' if role == 'user' else 'Assistant'}: {truncated}")
            parts.append("Recent conversation:\n" + "\n".join(lines))

    return "\n".join(parts) if parts else ""


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

PLANNING_PROMPT = """You are a planning agent for ReviewSenseAI, a product intelligence platform built on 183K Amazon Electronics reviews across 14 categories.

Available data:
- 183K reviews with sentiment scores, themes, and summaries (pre-computed)
- 14 product categories, 10 review themes
- 407 products with detailed stats (only ASINs with 20+ reviews)
- 25K ASINs with real metadata (brand, price, title, features)
- Gold mart tables: category_sentiment_summary, product_sentiment_summary, category_monthly_trends, theme_category_analysis, complaint_analysis

{tool_descriptions}

Before selecting tools, reason about:
1. What type of answer does the user need? (statistics, opinions, comparison, verification)
2. What entities are mentioned? (ASINs, brands, categories, themes)
3. Can this be answered with 1 tool, or does it need multiple?

Rules:
- Use the MINIMUM number of tools needed. Simple stat → 1 tool. Simple opinion → 1 tool. Only use 3+ for comparisons or multi-faceted questions.
- Use exact tool names from the list above. Do not invent tools like 'analyze_sentiment' or 'get_reviews'.
- Never call get_product_detail or compare_products without specific ASINs (format: B0XXXXXXXXX). If the user mentions a product by name, use search_products first to find the ASIN.
- query_analyst only answers statistical questions (counts, averages, rankings, trends). For opinions, experiences, or 'what do people say' questions, use search_reviews.
- For 'why' questions, use search_reviews to find review text that explains patterns. SQL returns numbers, not reasons.
- Use exact category names: headphones_earbuds (not 'headphones'), cables_adapters (not 'cables'). Use exact theme names: battery_life (not 'battery'), build_quality (not 'quality').
- If the user asks about a product type not in the 14 categories, acknowledge the limitation.
- For vague or open-ended questions, use query_analyst for a category overview OR search_reviews with a broad query. Do not plan 3+ tools for vague questions.
- When the user asks for a specific quality (comfortable, durable, good sound, long battery, waterproof), use search_products with the review_theme parameter to find products with positive reviews about that attribute. Map qualities to themes: comfortable→comfort, durable→durability, sound/audio→sound_quality, battery/long battery→battery_life, easy to use→ease_of_use, good value/cheap→value_for_money, build/sturdy→build_quality. Also use search_reviews to get actual review quotes. Do NOT search for "highest rated" first and then check for the attribute — that inverts the user's priority.
- When recommending products, prefer those with real metadata (brand, price) over products with brand "Unknown". If a product has no brand, present it by name only without saying "by Unknown".
- Max 5 steps. Set "depends_on" to the step index (0-based) if a step needs a previous step's output.
- If a step depends on finding an ASIN from search_products, mark that dependency.

Examples:
User: "What do people say about battery life in headphones?"
Analysis: Opinion question about a specific theme in a specific category. Needs search_reviews with filters.
---JSON---
{{"steps": [{{"tool": "search_reviews", "params": {{"query": "battery life", "category": "headphones_earbuds", "theme": "battery_life"}}, "purpose": "Find reviews discussing battery life in headphones"}}]}}

User: "Compare Sony vs Logitech"
Analysis: Brand comparison. Needs compare_brands with both brand names.
---JSON---
{{"steps": [{{"tool": "compare_brands", "params": {{"brands": ["sony", "logitech"]}}, "purpose": "Compare brand stats and review sentiment"}}]}}

User: "Which category has the worst reviews and why?"
Analysis: Two-part question: stat (which category) + explanation (why). Needs analyst for ranking, then search for reasons.
---JSON---
{{"steps": [{{"tool": "query_analyst", "params": {{"question": "Which category has the lowest average rating?"}}, "purpose": "Find worst-rated category"}}, {{"tool": "search_reviews", "params": {{"query": "problems issues complaints low rating"}}, "purpose": "Find reviews explaining why ratings are low", "depends_on": 0}}]}}

User: "Find me comfortable headphones"
Analysis: Product recommendation filtered by a specific quality (comfort). Use search_products with review_theme to find products with positive comfort reviews, and search_reviews for supporting quotes.
---JSON---
{{"steps": [{{"tool": "search_products", "params": {{"category": "headphones_earbuds", "review_theme": "comfort", "sort_by": "theme_sentiment", "limit": 5}}, "purpose": "Find headphones with best comfort reviews"}}, {{"tool": "search_reviews", "params": {{"query": "comfortable headphones", "category": "headphones_earbuds", "theme": "comfort", "limit": 5}}, "purpose": "Get actual comfort review quotes"}}]}}
{conversation_context_block}
Question: {question}

First, briefly analyze what the user needs (2-3 sentences). Then output your plan after the marker.
---JSON---"""


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
    # Strategy 0: Split on ---JSON--- delimiter (two-stage CoT)
    if "---JSON---" in response:
        parts = response.split("---JSON---")
        if len(parts) > 2:
            logger.warning(f"Multiple ---JSON--- delimiters found ({len(parts)-1}), using last")
        json_part = parts[-1].strip()
        try:
            return json.loads(json_part)
        except json.JSONDecodeError:
            # Fall through to other strategies on the json_part
            response = json_part

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


def _plan_tools(question: str, conversation_context: str = "", model: str | None = None) -> list[dict] | None:
    """Use COMPLETE to plan which tools to call.

    Resilient to LLM output variations:
    - Two-stage CoT: LLM reasons first, then JSON after ---JSON--- delimiter
    - Tries multiple JSON extraction strategies
    - Fuzzy matches tool names
    - Skips invalid steps instead of rejecting entire plan
    - Logs raw LLM response for debugging
    """
    if conversation_context:
        context_block = (
            "\n\nConversation context (use this to resolve follow-up references):\n"
            f"{conversation_context}\n\n"
            "IMPORTANT: If the user says \"both\", \"them\", \"those\", \"each of them\", "
            "or similar plural references, identify ALL referenced products from the "
            "\"Products referenced in conversation\" line above. Use their ASINs directly "
            "in tool calls (e.g., get_product_detail for each, or compare_products with all). "
            "Do NOT use placeholders like {{STEP_RESULTS}} — use the actual ASINs.\n"
        )
    else:
        context_block = ""

    prompt = PLANNING_PROMPT.format(
        tool_descriptions=TOOL_DESCRIPTIONS,
        conversation_context_block=context_block,
        question=question,
    )

    try:
        active_model = model or settings.llm_model
        with get_cursor() as cur:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
                (active_model, prompt)
            )
            response = cur.fetchone()[0].strip()

        # Log raw response for debugging
        logger.info(f"Planning LLM response ({active_model}) for '{question[:50]}': {response[:300]}")

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
# PARAMETER VALIDATION (zero LLM cost)
# ============================================

VALID_CATEGORIES = {
    "headphones_earbuds", "speakers", "streaming_devices", "smart_home",
    "cables_adapters", "chargers_batteries", "phone_accessories",
    "computer_peripherals", "storage_media", "cameras_accessories",
    "tv_accessories", "gaming_accessories", "wearables", "other_electronics"
}

VALID_THEMES = {
    "battery_life", "build_quality", "sound_quality", "connectivity",
    "comfort", "value_for_money", "customer_service", "durability",
    "ease_of_use", "other"
}

CATEGORY_ALIASES = {
    "headphones": "headphones_earbuds", "earbuds": "headphones_earbuds",
    "cables": "cables_adapters", "adapters": "cables_adapters",
    "chargers": "chargers_batteries", "batteries": "chargers_batteries",
    "phones": "phone_accessories", "phone": "phone_accessories",
    "computers": "computer_peripherals", "peripherals": "computer_peripherals",
    "storage": "storage_media", "cameras": "cameras_accessories",
    "tv": "tv_accessories", "gaming": "gaming_accessories",
    "smart home": "smart_home", "smarthome": "smart_home",
    "streaming": "streaming_devices",
}

THEME_ALIASES = {
    "battery": "battery_life", "build": "build_quality",
    "sound": "sound_quality", "audio": "sound_quality",
    "connection": "connectivity", "bluetooth": "connectivity", "wifi": "connectivity",
    "price": "value_for_money", "value": "value_for_money", "cost": "value_for_money",
    "support": "customer_service", "service": "customer_service",
    "durable": "durability",
    "ease of use": "ease_of_use", "usability": "ease_of_use",
}


def _validate_and_fix_params(tool_name: str, params: dict) -> dict:
    """Validate and auto-correct tool params. Returns fixed params."""
    fixed = dict(params)

    # Fix category param
    if "category" in fixed:
        cat = str(fixed["category"]).lower().strip()
        if cat not in VALID_CATEGORIES:
            fixed["category"] = CATEGORY_ALIASES.get(cat, cat)
            if fixed["category"] not in VALID_CATEGORIES:
                logger.warning(f"Unknown category '{cat}' for {tool_name}, removing filter")
                del fixed["category"]

    # Fix theme param (covers both 'theme' and 'review_theme')
    for theme_key in ["theme", "review_theme"]:
        if theme_key in fixed:
            theme = str(fixed[theme_key]).lower().strip()
            if theme not in VALID_THEMES:
                fixed[theme_key] = THEME_ALIASES.get(theme, theme)
                if fixed[theme_key] not in VALID_THEMES:
                    logger.warning(f"Unknown theme '{theme}' for {tool_name}, removing filter")
                    del fixed[theme_key]

    # Fix ASIN format
    if "asin" in fixed and fixed["asin"]:
        asin = str(fixed["asin"]).strip().upper()
        if not re.match(r'^B0[A-Z0-9]{8,}$', asin):
            logger.warning(f"Invalid ASIN '{asin}' for {tool_name}, removing")
            del fixed["asin"]

    # Fix rating ranges
    for key in ["min_rating", "max_rating"]:
        if key in fixed:
            try:
                val = int(fixed[key])
                fixed[key] = max(1, min(5, val))
            except (ValueError, TypeError):
                del fixed[key]

    return fixed


# ============================================
# ADAPTIVE REPLANNING (zero LLM cost)
# ============================================

TOOL_FALLBACK_STRATEGIES = {
    "search_products": [
        {"action": "broaden", "remove_params": ["min_price", "max_price", "brand", "review_theme"]},
        {"action": "switch_tool", "tool": "search_reviews"},
    ],
    "query_analyst": [
        {"action": "switch_tool", "tool": "search_reviews"},
    ],
    "get_product_detail": [
        {"action": "switch_tool", "tool": "search_reviews", "add_params": {"limit": 3}},
    ],
    "search_reviews": [
        {"action": "broaden", "remove_params": ["theme", "category", "quality"]},
    ],
}


def _execute_with_retry(step, model: str | None = None):
    """Execute a tool step. If empty, try rule-based fallback strategies.

    Safety: max 2 retries per step (one broaden + one switch_tool).
    No loops — flat iteration over strategies list.
    """
    # Validate and fix params first
    step["params"] = _validate_and_fix_params(step["tool"], step.get("params", {}))

    # First attempt
    result = _execute_single_tool(step, model=model)

    if result and result.get("result") and not (isinstance(result["result"], dict) and result["result"].get("error")):
        return result  # Success

    # Empty result — try fallback strategies
    strategies = TOOL_FALLBACK_STRATEGIES.get(step["tool"], [])
    original_context = f"{step['tool']}({step.get('params', {})})"

    for strategy in strategies:
        if strategy["action"] == "broaden":
            relaxed_params = {k: v for k, v in step["params"].items()
                           if k not in strategy["remove_params"]}
            retry_step = {**step, "params": relaxed_params}
            result = _execute_single_tool(retry_step, model=model)

        elif strategy["action"] == "switch_tool":
            query = (step["params"].get("query")
                     or step["params"].get("question")
                     or step.get("purpose", "")  # use purpose as fallback query context
                     or "product reviews")
            switch_params = {"query": query, **strategy.get("add_params", {})}
            # Carry over category if available
            if "category" in step["params"]:
                switch_params["category"] = step["params"]["category"]
            switch_step = {
                "tool": strategy["tool"],
                "params": switch_params,
                "purpose": f"Fallback for {step['tool']}"
            }
            result = _execute_single_tool(switch_step, model=model)

        if result and result.get("result") and not (isinstance(result["result"], dict) and result["result"].get("error")):
            result["retried"] = True
            result["original_query"] = f"No results for: {original_context}"
            logger.info(f"Fallback succeeded for {step['tool']}: {strategy['action']}")
            return result

    # All strategies exhausted
    logger.warning(f"All fallback strategies exhausted for {step['tool']}")
    return {
        "tool": step["tool"],
        "result": None,
        "status": "no_data",
        "purpose": step.get("purpose", ""),
        "original_query": f"No results for: {original_context}",
    }


# ============================================
# EXECUTE TOOLS (0 LLM cost for SQL tools)
# ============================================

def _extract_asins_from_result(result) -> list[str]:
    """Extract ASINs from a tool result (search_products, search_reviews, etc.)."""
    asins = []
    if not result or not isinstance(result, dict):
        return asins

    # search_products returns list of dicts with ASIN key
    products = result.get("products") or result.get("results") or []
    if isinstance(products, list):
        for p in products:
            if isinstance(p, dict):
                asin = p.get("ASIN") or p.get("asin")
                if asin and re.match(r'^B0[A-Z0-9]{8,}$', str(asin)):
                    asins.append(str(asin))

    # Direct ASIN field
    if not asins and result.get("asin"):
        asins.append(result["asin"])

    # search_reviews returns results with ASIN
    sources = result.get("sources") or result.get("reviews") or []
    if isinstance(sources, list):
        for s in sources:
            if isinstance(s, dict):
                asin = s.get("ASIN") or s.get("asin")
                if asin and re.match(r'^B0[A-Z0-9]{8,}$', str(asin)) and asin not in asins:
                    asins.append(str(asin))

    # Cortex Analyst data rows
    data = result.get("data") or []
    if isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                for v in row.values():
                    if isinstance(v, str) and re.match(r'^B0[A-Z0-9]{8,}$', v) and v not in asins:
                        asins.append(v)

    return asins


def _resolve_dependent_params(step: dict, all_results: list, step_idx: int) -> dict:
    """Resolve placeholder params in dependent steps using actual results from prior steps.

    Handles two cases:
    1. Explicit placeholders: {{STEP_RESULTS[0].ASINS[0]}} → actual ASIN from step 0
    2. Implicit: step needs an 'asin' param, depends on a search step, but has no valid ASIN
       → inject ASINs from the dependency's results
    """
    params = dict(step.get("params", {}))
    dep = step.get("depends_on")
    if dep is None:
        return params

    deps = dep if isinstance(dep, list) else [dep]

    # Collect all ASINs from dependency results, in order
    dep_asins = []
    for d in deps:
        if d is not None and d < len(all_results) and all_results[d] is not None:
            dep_result = all_results[d].get("result")
            dep_asins.extend(_extract_asins_from_result(dep_result))

    if not dep_asins:
        return params

    # Case 1: Replace explicit placeholder patterns like {{STEP_RESULTS[N].ASINS[M]}}
    for key, val in list(params.items()):
        if isinstance(val, str) and '{{' in val and 'STEP_RESULTS' in val:
            match = re.search(r'\{\{STEP_RESULTS\[(\d+)\]\.ASINS?\[(\d+)\]\}\}', val)
            if match:
                asin_idx = int(match.group(2))
                if asin_idx < len(dep_asins):
                    params[key] = dep_asins[asin_idx]
                    logger.info(f"Resolved placeholder {val} → {dep_asins[asin_idx]}")
                else:
                    logger.warning(f"Placeholder {val} requests index {asin_idx} but only {len(dep_asins)} ASINs available")

    # Case 2: Step needs 'asin' but doesn't have a valid one — inject from dependency
    tool_name = step.get("tool", "")
    if tool_name in ("get_product_detail", "verify_claims", "find_similar_products"):
        asin_val = params.get("asin", "")
        is_placeholder = isinstance(asin_val, str) and ('{{' in asin_val or not asin_val)
        is_invalid = isinstance(asin_val, str) and not re.match(r'^B0[A-Z0-9]{8,}$', asin_val)
        if (is_placeholder or is_invalid or not asin_val) and dep_asins:
            # Pick ASIN by sibling position: if steps 1,2 both depend on step 0,
            # step 1 gets dep_asins[0], step 2 gets dep_asins[1]
            # Formula: offset = step_idx - first_dependency_index - 1
            first_dep = min(deps) if deps else 0
            sibling_index = step_idx - first_dep - 1
            if 0 <= sibling_index < len(dep_asins):
                params["asin"] = dep_asins[sibling_index]
            else:
                params["asin"] = dep_asins[sibling_index % len(dep_asins)]
            logger.info(f"Injected ASIN {params['asin']} into {tool_name} (step {step_idx}, sibling {sibling_index})")

    # Case 3: compare_products needs 'asins' list
    if tool_name == "compare_products" and dep_asins:
        if not params.get("asins") or any('{{' in str(a) for a in params.get("asins", [])):
            params["asins"] = dep_asins[:5]
            logger.info(f"Injected {len(dep_asins[:5])} ASINs into compare_products")

    return params


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


# Tools that accept an optional `model` kwarg (injected at execute time
# so the candidate model flows end-to-end through the bake-off).
_MODEL_AWARE_TOOLS = {"verify_claims"}


def _execute_single_tool(step: dict, model: str | None = None) -> dict:
    """Execute a single tool in its own thread."""
    tool_name = step.get("tool")
    params = dict(step.get("params", {}))  # copy so we don't mutate the plan
    purpose = step.get("purpose", "")

    # Inject the candidate model into tools that support it
    if model and tool_name in _MODEL_AWARE_TOOLS:
        params.setdefault("model", model)

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


def _execute_plan(steps: list[dict], model: str | None = None) -> list[dict]:
    """Execute tools with parallel execution for independent steps.

    Groups steps into waves by dependency. Steps in the same wave
    run concurrently via ThreadPoolExecutor. Steps in later waves
    wait for their dependencies to complete.

    The `model` param flows into model-aware tools (e.g., verify_claims)
    so the candidate model is used end-to-end during a bake-off run.

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

        # Resolve dependent params before execution (inject ASINs from prior results)
        for step_idx in runnable:
            if steps[step_idx].get("depends_on") is not None:
                steps[step_idx]["params"] = _resolve_dependent_params(
                    steps[step_idx], all_results, step_idx
                )

        # Single tool — no need for thread overhead
        if len(runnable) == 1:
            idx = runnable[0]
            result = _execute_with_retry(steps[idx], model=model)
            result["wave"] = wave_num + 1
            all_results[idx] = result
            continue

        # Multiple tools — run in parallel
        logger.info(f"Wave {wave_num + 1}: running {len(runnable)} tools in parallel")
        with ThreadPoolExecutor(max_workers=min(len(runnable), 4)) as executor:
            futures = {}
            for step_idx in runnable:
                future = executor.submit(_execute_with_retry, steps[step_idx], model)
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

SYNTHESIS_PROMPT = """You are a product intelligence analyst for ReviewSenseAI. Answer the user's question using ONLY the tool results below.

Answer structure:
1. Lead with the direct answer (1-2 sentences)
2. Support with specific evidence (numbers, review quotes, comparisons)
3. Note caveats or limitations if data is sparse or conflicting

Citation rules:
- For statistics: include the number and scope, e.g. "Headphones have an average rating of 4.2 (based on 15,230 reviews)"
- For review quotes: use the reviewer's actual words, e.g. 'One reviewer noted: "battery lasts about 6 hours real-world use"'
- For comparisons: show both sides, e.g. "Sony (4.3 avg) outperforms Logitech (3.9 avg) by 0.4 points"
- Always note the data volume when it affects confidence. "3 reviews mention this" is weaker than "47 reviews mention this".

Conflict resolution:
- Review TEXT is the ground truth for specific claims about features, quality, and experiences. A review saying "battery dies in 2 hours" overrides a 4-star rating for battery-related questions.
- Aggregate RATINGS show overall satisfaction but mask feature-level problems. A 4.5 average can hide a serious battery issue if everything else is strong.
- When they conflict: present both, explain why they differ (e.g. "Despite a 4.5 average rating, 47 reviews specifically cite battery drain — the high rating likely reflects strong performance in other areas like sound quality").

Other rules:
- Never invent statistics or quotes not present in the tool results
- If tool results are empty or contain errors, say "I couldn't find data for that specific query" — never fabricate an answer from empty results
- If a product brand is "Unknown" or missing, omit the brand — just use the product name (e.g., "Wireless Earbuds" not "Wireless Earbuds by Unknown")
- Keep the answer concise but thorough
{conversation_context_block}
User question: {question}

Tool results:
{tool_results}

Answer:"""


def _synthesize(question: str, tool_results: list[dict], conversation_context: str = "", model: str | None = None) -> str:
    """Generate final answer from all tool results."""
    # Build context from tool results
    context_parts = []
    for r in tool_results:
        # Include done results and no_data results (for fallback context)
        if r.get("result"):
            result_str = json.dumps(r["result"], default=str)
            # Truncate very large results
            if len(result_str) > 3000:
                result_str = result_str[:3000] + "... (truncated)"
            context_parts.append(f"[{r['tool']}] ({r['purpose']}): {result_str}")
        elif r.get("original_query"):
            # Failed step with retry context — tell synthesis what wasn't found
            context_parts.append(f"[{r['tool']}] ({r['purpose']}): {r['original_query']}")

    if not context_parts:
        return "I wasn't able to find enough data to answer your question. Could you rephrase or provide more details?"

    context = "\n\n".join(context_parts)

    if conversation_context:
        context_block = (
            "\n- This is part of an ongoing conversation. "
            "Do not repeat information already provided. "
            "Reference prior context naturally.\n\n"
            f"Conversation context:\n{conversation_context}\n"
        )
    else:
        context_block = ""

    prompt = SYNTHESIS_PROMPT.format(
        question=question,
        tool_results=context,
        conversation_context_block=context_block,
    )

    try:
        active_model = model or settings.llm_model
        with get_cursor() as cur:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
                (active_model, prompt)
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


def _reflect(question: str, answer: str, tool_results: list[dict], model: str | None = None) -> dict:
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
        active_model = model or settings.llm_model
        with get_cursor() as cur:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
                (active_model, prompt)
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
        tool_name = r.get("tool", "")
        if isinstance(result, dict):
            if result.get("sql"):
                sql = result["sql"]
            if result.get("data"):
                data = result["data"]
            elif result.get("answer") and result.get("data") is None and tool_name == "query_analyst":
                # Analyst returned answer but no data rows — extract from answer
                pass
            elif tool_name in ("get_product_detail", "search_products", "compare_products",
                              "get_brand_analysis", "compare_brands", "price_value_analysis",
                              "find_similar_products", "verify_claims"):
                # These tools return flat dicts — wrap as data for the response
                if result and not result.get("error"):
                    data = [result] if not isinstance(result, list) else result
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
        icon = tool_icons.get(r.get("tool", ""), "⚙️")
        status = r.get("status", "done")
        summary = status
        if status == "done" and r.get("result"):
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
            "tool": r.get("tool", "unknown"),
            "description": r.get("purpose", r.get("tool", "")),
            "status": status,
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

def run_custom_agent(question: str, conversation_history=None, session_context=None, model: str | None = None) -> dict:
    """Run the custom agentic RAG loop.

    Tiered:
    - Tier 1: Fast path for simple queries (0 LLM calls)
    - Tier 2: LLM plans tools → execute → synthesize (2 LLM calls)

    Conversation-aware: uses conversation_history and session_context
    to plan better tools and generate coherent multi-turn answers.

    Model override: if `model` is provided, it's used for all COMPLETE calls
    in the agent path (planning, synthesis, reflection, verify_claims tool).
    Otherwise, falls back to `settings.llm_model`. Used by the bake-off eval.
    """
    start = time.time()
    active_model = model or settings.llm_model

    # Load dynamic dataset stats (cached, 1 hour TTL)
    _load_dataset_stats()

    # Build conversation context once, reuse in planning + synthesis
    conv_context = _format_conversation_context(conversation_history, session_context)

    # Tier 1: Fast path
    fast_result = _try_fast_path(question)
    if fast_result:
        # Still need synthesis for a natural answer
        fast_tool_results = []
        for key in ["sql", "data", "sources"]:
            if fast_result.get(key):
                fast_tool_results.append({
                    "tool": (fast_result.get("tools_used") or ["direct"])[0],
                    "result": {key: fast_result[key]},
                    "purpose": "Fast path result",
                    "status": "done",
                })
        if not fast_tool_results:
            fast_tool_results = [{"tool": "direct", "result": fast_result, "purpose": "Fast path", "status": "done"}]
        answer = _synthesize(question, fast_tool_results, conversation_context=conv_context, model=active_model)
        fast_result["answer"] = answer
        fast_result["model"] = active_model
        fast_result["latency_ms"] = round((time.time() - start) * 1000, 1)
        return fast_result

    # Tier 2: LLM Planning
    steps = _plan_tools(question, conversation_context=conv_context, model=active_model)
    if not steps:
        return None  # Signal to caller to use legacy fallback

    # Execute plan
    tool_results = _execute_plan(steps, model=active_model)

    # Synthesize
    answer = _synthesize(question, tool_results, conversation_context=conv_context, model=active_model)

    # Reflect — verify answer is grounded in tool results
    reflection = _reflect(question, answer, tool_results, model=active_model)

    # Build response
    response = _build_response(question, tool_results)
    response["answer"] = answer
    response["reflection"] = reflection
    response["model"] = active_model
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
