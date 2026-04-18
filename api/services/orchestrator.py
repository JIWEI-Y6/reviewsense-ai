"""Query orchestrator — routes queries through tools with trace logging.

Classifies intent, executes the appropriate path, and builds a tool trace
that shows which tools were used and what they found.

Features:
- Query result caching (5-min TTL, skip for follow-ups)
- Conversation compaction (summarize old messages, keep recent)
- Automatic fallback: custom agent → legacy orchestrator
- Analyst refuses → semantic search fallback
"""

import time
import hashlib
import logging
from api.services.agent import query_agent
from api.services.guardrails import check_input, sanitize_output

logger = logging.getLogger(__name__)

# Query result cache — 5 minute TTL
_query_cache = {}
_CACHE_TTL = 300  # seconds


def _get_cached(question: str) -> dict | None:
    """Return cached result if same question asked within TTL."""
    key = hashlib.md5(question.strip().lower().encode()).hexdigest()
    cached = _query_cache.get(key)
    if cached and time.time() - cached["timestamp"] < _CACHE_TTL:
        logger.info(f"Cache hit for: {question[:50]}")
        result = cached["result"].copy()
        result["cached"] = True
        return result
    return None


def _cache_result(question: str, result: dict):
    """Store result in cache. Evict old entries if cache grows too large."""
    key = hashlib.md5(question.strip().lower().encode()).hexdigest()
    _query_cache[key] = {"result": result, "timestamp": time.time()}
    # Evict oldest if cache > 100 entries
    if len(_query_cache) > 100:
        oldest_key = min(_query_cache, key=lambda k: _query_cache[k]["timestamp"])
        del _query_cache[oldest_key]


def _build_trace_step(tool: str, description: str, status: str = "done",
                      result_summary: str | None = None) -> dict:
    return {
        "tool": tool,
        "description": description,
        "status": status,
        "result_summary": result_summary,
    }


def _resolve_question_with_context(question: str, conversation_history=None, session_context=None) -> str:
    """Tag follow-up questions with a lightweight topic hint.

    Detects follow-up signals (pronouns, short questions) and prepends a brief
    topic hint from the last assistant message. Actual entity resolution (which
    products "both of them" refers to) is handled by the LLM planner, which
    receives full entity context via _format_conversation_context.
    """
    if not conversation_history:
        return question

    q = question.lower()
    followup_signals = ['it', 'they', 'them', 'this', 'that', 'those', 'the same',
                        'how about', 'what about', 'and the', 'also', 'its', 'their',
                        'both', 'each of them', 'all of them']

    has_signal = any(f' {s} ' in f' {q} ' for s in followup_signals)
    is_very_short = len(question.split()) <= 3  # "how much?", "is it good?"

    if not has_signal and not is_very_short:
        return question

    # Find a brief topic hint from the last assistant message
    for msg in reversed(conversation_history):
        role = msg.role if hasattr(msg, 'role') else msg.get('role', '')
        content = msg.content if hasattr(msg, 'content') else msg.get('content', '')
        if role == "assistant" and len(content) > 20:
            topic_hint = content[:100]
            return f"Follow-up on: {topic_hint}. Question: {question}"

    # Fallback: session context entities (when no conversation history text)
    if session_context:
        products = getattr(session_context, 'products_discussed', None) or \
                   (session_context.get('products_discussed') if isinstance(session_context, dict) else None) or []
        brands = getattr(session_context, 'brands_discussed', None) or \
                 (session_context.get('brands_discussed') if isinstance(session_context, dict) else None) or []
        if products:
            return f"Regarding product {products[-1]}: {question}"
        if brands:
            return f"Regarding {brands[-1]} products: {question}"

    return question


def _legacy_route(question: str, conversation_history=None, session_context=None) -> dict:
    """Manual intent classification + routing with tool trace."""
    from api.services.analyst import query_analyst
    from api.services.search import query_search
    from api.services.synthesis import query_synthesis

    # Resolve follow-ups with conversation context
    resolved_question = _resolve_question_with_context(question, conversation_history, session_context)

    q = question.lower()  # use original for intent classification
    trace = []

    # Step 1: Intent classification
    structured_signals = ['how many', 'average', 'rank', 'top ', 'worst', 'best',
                          'highest', 'lowest', 'percentage', 'trend', 'compare', 'count',
                          'which category', 'which product', 'number of', 'total']
    semantic_signals = ['what do people say', 'tell me about', 'what problems',
                        'recommend', 'worth buying', 'what do people think', 'experiences',
                        'what do customers', 'what do reviews', 'how do people']
    synthesis_signals = ['and what', 'numbers and', 'full analysis', 'complete picture',
                         'stats and', 'data and examples']

    if any(s in q for s in synthesis_signals):
        intent = "synthesis"
    elif any(s in q for s in semantic_signals):
        intent = "semantic"
    elif any(s in q for s in structured_signals):
        intent = "structured"
    else:
        intent = "structured"

    trace.append(_build_trace_step(
        "intent_classifier",
        f"Classified question as '{intent}'",
        result_summary=f"Intent: {intent}"
    ))

    # Step 2: Execute based on intent
    if intent == "synthesis":
        trace.append(_build_trace_step(
            "cortex_analyst",
            "Generating SQL query for structured data",
            status="running"
        ))
        analyst_result = query_analyst(resolved_question)
        row_count = len(analyst_result.get("data") or [])
        trace[-1]["status"] = "done"
        trace[-1]["result_summary"] = f"Generated SQL, returned {row_count} rows"

        trace.append(_build_trace_step(
            "cortex_search",
            "Searching reviews for relevant examples",
            status="running"
        ))
        search_result = query_search(resolved_question)
        source_count = len(search_result.get("sources") or [])
        trace[-1]["status"] = "done"
        trace[-1]["result_summary"] = f"Found {source_count} relevant reviews"

        trace.append(_build_trace_step(
            "cortex_complete",
            "Synthesizing data and reviews into final answer",
            status="running"
        ))
        result = query_synthesis(resolved_question)
        result["intent"] = "synthesis"
        trace[-1]["status"] = "done"
        trace[-1]["result_summary"] = "Answer synthesized from data + reviews"

    elif intent == "semantic":
        trace.append(_build_trace_step(
            "cortex_search",
            "Searching 183K reviews for relevant matches",
            status="running"
        ))
        result = query_search(resolved_question)
        result["intent"] = "semantic"
        source_count = len(result.get("sources") or [])
        trace[-1]["status"] = "done"
        trace[-1]["result_summary"] = f"Found {source_count} relevant reviews"

        trace.append(_build_trace_step(
            "cortex_complete",
            "Generating answer from review evidence",
            status="done",
            result_summary="Answer generated with review citations"
        ))

    else:  # structured
        trace.append(_build_trace_step(
            "cortex_analyst",
            "Translating question to SQL via semantic model",
            status="running"
        ))
        result = query_analyst(question)
        result["intent"] = "structured"

        # Check if Analyst refused
        answer_lower = (result.get("answer") or "").lower()
        refusal_signals = ["not possible", "i'm sorry", "cannot", "don't have", "does not have",
                           "not available", "unable to", "outside the scope"]

        if any(s in answer_lower for s in refusal_signals) and not result.get("data"):
            trace[-1]["status"] = "done"
            trace[-1]["result_summary"] = "Analyst couldn't answer — falling back to search"

            trace.append(_build_trace_step(
                "cortex_search",
                "Falling back to review search",
                status="running"
            ))
            result = query_search(resolved_question)
            result["intent"] = "semantic (fallback from analyst)"
            source_count = len(result.get("sources") or [])
            trace[-1]["status"] = "done"
            trace[-1]["result_summary"] = f"Found {source_count} relevant reviews"

            trace.append(_build_trace_step(
                "cortex_complete",
                "Generating answer from review evidence",
                status="done",
                result_summary="Answer generated with review citations"
            ))
        else:
            row_count = len(result.get("data") or [])
            has_sql = bool(result.get("sql"))
            trace[-1]["status"] = "done"
            trace[-1]["result_summary"] = f"{'Generated SQL, ' if has_sql else ''}{row_count} rows returned"

    result["question"] = question
    result["tools_used"] = [intent]
    result["tool_trace"] = trace
    return result


def route_query(question: str, conversation_history=None, session_context=None, model: str | None = None) -> dict:
    """Process a user question through guardrails and the orchestrator.

    The optional `model` parameter overrides the default Cortex LLM used by the
    custom agent (planning, synthesis, reflection, and eval-relevant tools).
    Legacy fallback paths still use `settings.llm_model` — this is intentional,
    since the bake-off measures the agent path, not the legacy path.
    """
    start = time.time()

    # Input guardrail — skip off-topic check if mid-conversation
    has_history = bool(conversation_history and len(conversation_history) > 0)
    check_input(question, has_conversation_history=has_history)

    # Cache key includes model so different candidates don't collide
    cache_key = f"{model or ''}::{question}" if model else question

    # Check cache (skip for follow-ups — they need fresh context)
    if not has_history:
        cached = _get_cached(cache_key)
        if cached:
            cached["latency_ms"] = round((time.time() - start) * 1000, 1)
            return cached

    # Resolve follow-up questions with conversation context BEFORE any routing
    resolved = _resolve_question_with_context(question, conversation_history, session_context)

    if resolved != question:
        logger.info(f"Follow-up resolved: {resolved[:100]}")

    # Agent-first: try custom agent for ALL queries (new + follow-ups)
    try:
        from api.services.agent_custom import run_custom_agent
        result = run_custom_agent(
            resolved,
            conversation_history=conversation_history,
            session_context=session_context,
            model=model,
        )

        if result and result.get("answer") and len(result["answer"].strip()) > 10:
            result["fallback"] = False
        else:
            logger.warning("Custom agent returned no result, falling back to legacy")
            result = _legacy_route(resolved, None, None)
            result["fallback"] = True

    except Exception as e:
        logger.error(f"Custom agent failed: {e}, falling back to legacy")
        result = _legacy_route(resolved, None, None)
        result["fallback"] = True

    # Output guardrail
    if result.get("answer"):
        result["answer"] = sanitize_output(result["answer"])

    if "latency_ms" not in result:
        result["latency_ms"] = round((time.time() - start) * 1000, 1)

    # Cache result for identical future queries (skip follow-ups)
    if not has_history and result.get("answer"):
        _cache_result(cache_key, result)

    return result
