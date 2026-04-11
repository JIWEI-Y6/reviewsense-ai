"""Query orchestrator — routes queries through tools with trace logging.

Classifies intent, executes the appropriate path, and builds a tool trace
that shows which tools were used and what they found.

Edge cases handled:
- Agent API unreachable → falls back to legacy orchestrator
- Agent returns empty answer → retry with legacy path
- Analyst refuses (can't answer) → falls back to semantic search
- Guardrail blocks input → return 400 before any processing
"""

import time
import logging
from api.services.agent import query_agent
from api.services.guardrails import check_input, sanitize_output

logger = logging.getLogger(__name__)


def _build_trace_step(tool: str, description: str, status: str = "done",
                      result_summary: str | None = None) -> dict:
    return {
        "tool": tool,
        "description": description,
        "status": status,
        "result_summary": result_summary,
    }


def _resolve_question_with_context(question: str, conversation_history=None, session_context=None) -> str:
    """Resolve follow-up questions using conversation history.

    If the question looks like a follow-up (short, uses pronouns, references 'it'/'they'),
    prepend context from recent conversation so the downstream tools understand what's being asked.
    """
    if not conversation_history:
        return question

    q = question.lower()
    followup_signals = ['it', 'they', 'them', 'this', 'that', 'those', 'the same',
                        'how about', 'what about', 'and the', 'also', 'its', 'their']

    has_signal = any(f' {s} ' in f' {q} ' for s in followup_signals)
    is_very_short = len(question.split()) <= 3  # "how much?", "is it good?"

    # Only treat as follow-up if it has explicit follow-up signals,
    # or is extremely short (3 words or less = almost certainly a follow-up)
    is_followup = has_signal or is_very_short

    if not is_followup:
        return question

    # Find the most recent topic from conversation
    last_topic = ""
    last_product = ""
    for msg in reversed(conversation_history):
        role = msg.role if hasattr(msg, 'role') else msg.get('role', '')
        content = msg.content if hasattr(msg, 'content') else msg.get('content', '')
        if role == "assistant":
            import re
            asin_match = re.search(r'\bB0[A-Z0-9]{8,}\b', content)
            if asin_match:
                last_product = asin_match.group(0)
            if not last_topic:
                last_topic = content[:200]
            break
        elif role == "user":
            if not last_topic and len(content) > 10:
                last_topic = content

    # Build a clean, self-contained rewritten question
    if last_product:
        return f"Regarding product {last_product}: {question}"
    elif last_topic:
        # Use session context for product/brand references
        if session_context and session_context.products_discussed:
            product = session_context.products_discussed[-1]
            return f"Regarding product {product}: {question}"
        elif session_context and session_context.brands_discussed:
            brand = session_context.brands_discussed[-1]
            return f"Regarding {brand} products: {question}"
        else:
            return f"In the context of: {last_topic[:100]}. {question}"

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


def route_query(question: str, conversation_history=None, session_context=None) -> dict:
    """Process a user question through guardrails and the orchestrator."""
    start = time.time()

    # Input guardrail — skip off-topic check if mid-conversation
    has_history = bool(conversation_history and len(conversation_history) > 0)
    check_input(question, has_conversation_history=has_history)

    # Resolve follow-up questions with conversation context BEFORE any routing
    resolved = _resolve_question_with_context(question, conversation_history, session_context)

    # If this is a follow-up (resolved != original), skip agent and use legacy directly
    # The agent doesn't handle contextual questions well
    if resolved != question:
        logger.info(f"Follow-up detected. Resolved: {resolved[:100]}")
        # Pass resolved question, None for history so _legacy_route doesn't re-resolve
        result = _legacy_route(resolved, None, None)
        result["fallback"] = False  # Not a fallback — intentional routing
        if result.get("answer"):
            result["answer"] = sanitize_output(result["answer"])
        if "latency_ms" not in result:
            result["latency_ms"] = round((time.time() - start) * 1000, 1)
        return result

    # Try custom agent loop first (our tools), then legacy as fallback
    try:
        from api.services.agent_custom import run_custom_agent
        result = run_custom_agent(resolved)

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

    return result
