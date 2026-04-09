"""Main query endpoint — routes through the Cortex Agent orchestrator."""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from api.models.requests import QueryRequest
from api.models.responses import QueryResponse
from api.services.orchestrator import route_query
from api.services.guardrails import GuardrailError, check_input
from api.services.agent import query_agent_stream

router = APIRouter(tags=["Query"])
limiter = Limiter(key_func=get_remote_address)


@router.post("/query", response_model=QueryResponse)
@limiter.limit("10/minute")
def query(request: Request, body: QueryRequest):
    """Ask a natural language question about product reviews.

    The Cortex Agent autonomously selects tools and chains results:
    - **Cortex Analyst**: SQL generation for numbers, rankings, trends
    - **Cortex Search**: Review retrieval for opinions, experiences
    - **Gold Mart UDFs**: Pre-computed category/product/theme/complaint stats
    - Falls back to legacy orchestrator if agent is unavailable
    """
    try:
        result = route_query(
            body.question,
            conversation_history=body.conversation_history,
            session_context=body.session_context,
        )
        return QueryResponse(**result)
    except GuardrailError as e:
        raise HTTPException(status_code=400, detail=e.message)


@router.post("/query/stream")
def query_stream(body: QueryRequest):
    """Stream a response from the Cortex Agent.

    Returns Server-Sent Events (SSE) with real-time tool usage and answer generation.
    """
    try:
        has_history = bool(body.conversation_history and len(body.conversation_history) > 0)
        check_input(body.question, has_conversation_history=has_history)
    except GuardrailError as e:
        raise HTTPException(status_code=400, detail=e.message)

    def event_generator():
        for event in query_agent_stream(body.question):
            yield f"{event}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")
