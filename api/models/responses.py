"""Pydantic response models."""

from pydantic import BaseModel
from typing import Any


class ToolStep(BaseModel):
    tool: str
    description: str
    status: str  # "running", "done", "error"
    result_summary: str | None = None


class Reflection(BaseModel):
    grounded: bool = True
    issues: list[str] = []
    confidence: float = 0.5
    summary: str = ""


class QueryResponse(BaseModel):
    question: str
    intent: str  # "agent", "structured", "semantic", "synthesis"
    answer: str
    sql: str | None = None
    data: list[dict[str, Any]] | None = None
    sources: list[dict[str, Any]] | None = None
    tools_used: list[str] | None = None
    tool_trace: list[ToolStep] | None = None
    reflection: Reflection | None = None
    fallback: bool = False
    cached: bool = False
    latency_ms: float


class CategorySummary(BaseModel):
    derived_category: str
    review_count: int
    avg_rating: float
    avg_sentiment: float
    negative_rate: float


class CategoryDetail(BaseModel):
    derived_category: str
    review_count: int
    avg_rating: float
    avg_sentiment: float
    negative_rate: float
    top_themes: list[dict[str, Any]]
    top_complaints: list[dict[str, Any]]
    monthly_trends: list[dict[str, Any]]


class ProductSummary(BaseModel):
    asin: str
    derived_category: str | None
    review_count: int
    avg_rating: float
    avg_sentiment: float
    negative_rate: float
    top_theme: str | None


class HealthResponse(BaseModel):
    status: str
    snowflake_connected: bool
    analyst_available: bool
    search_available: bool
