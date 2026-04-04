"""ReviewSense AI — Product Intelligence API.

Three retrieval paths, one orchestrator:
- Structured: Cortex Analyst (natural language → SQL over aggregate marts)
- Semantic: Cortex Search Service (hybrid vector + keyword + RAG)
- Synthesis: Both paths combined, merged by Cortex COMPLETE
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import query, categories, products, compare, health, alerts

app = FastAPI(
    title="ReviewSense AI",
    description="Product Intelligence Platform — analyzes 200K+ Amazon Electronics reviews "
                "using Snowflake Cortex AI. Routes natural language questions through "
                "structured analytics (Cortex Analyst), semantic search (Cortex Search), "
                "or a synthesis of both.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(query.router)
app.include_router(categories.router)
app.include_router(products.router)
app.include_router(compare.router)
app.include_router(alerts.router)
