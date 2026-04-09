"""ReviewSense AI — Product Intelligence API.

Three retrieval paths, one orchestrator:
- Structured: Cortex Analyst (natural language → SQL over aggregate marts)
- Semantic: Cortex Search Service (hybrid vector + keyword + RAG)
- Synthesis: Both paths combined, merged by Cortex COMPLETE
"""

import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from api.routers import query, categories, products, compare, health, alerts, reports

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Rate limiter
limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="ReviewSense AI",
    description="Product Intelligence Platform — analyzes 200K+ Amazon Electronics reviews "
                "using Snowflake Cortex AI. Routes natural language questions through "
                "structured analytics (Cortex Analyst), semantic search (Cortex Search), "
                "or a synthesis of both.",
    version="1.0.0",
)

app.state.limiter = limiter

# Rate limit error handler
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded. Max 10 requests/minute on /query."})

# Global error handler
@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error on {request.url}: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal server error. Please try again."})

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
app.include_router(reports.router)
