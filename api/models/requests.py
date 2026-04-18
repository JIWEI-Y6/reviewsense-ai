"""Pydantic request models."""

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class SessionContext(BaseModel):
    products_discussed: list[str] = Field(default_factory=list, description="ASINs mentioned in conversation")
    categories_discussed: list[str] = Field(default_factory=list, description="Categories mentioned")
    brands_discussed: list[str] = Field(default_factory=list, description="Brands mentioned")


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=1000, description="Natural language question")
    conversation_history: list[ChatMessage] | None = Field(
        default=None,
        description="Last 2-3 conversation exchanges for follow-up context",
    )
    session_context: SessionContext | None = Field(
        default=None,
        description="Products, categories, and brands discussed in this session",
    )
    model: str | None = Field(
        default=None,
        description="Cortex LLM model override (e.g., 'mistral-large', 'llama3.1-70b', 'snowflake-arctic'). "
                    "When None, falls back to settings.llm_model. Used by the eval bake-off harness.",
    )

    model_config = {"protected_namespaces": (), "json_schema_extra": {"examples": [
        {"question": "Which product categories have the worst reviews?"},
        {"question": "What do people complain about in headphones?"},
        {
            "question": "How about the battery life?",
            "conversation_history": [
                {"role": "user", "content": "What do people say about Echo Dot?"},
                {"role": "assistant", "content": "The Echo Dot is a popular smart speaker..."},
            ],
            "session_context": {
                "products_discussed": ["B01DFKC2SO"],
                "categories_discussed": ["smart_home"],
                "brands_discussed": ["Amazon"],
            },
        },
        {
            "question": "What's the average rating for headphones?",
            "model": "llama3.1-70b",
        },
    ]}}


class CompareRequest(BaseModel):
    categories: list[str] = Field(..., min_length=2, max_length=5, description="Categories to compare")
    metric: str = Field(default="sentiment", description="Metric to compare: sentiment, rating, negative_rate")
