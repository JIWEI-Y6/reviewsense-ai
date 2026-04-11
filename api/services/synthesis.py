"""Synthesis path: combines Cortex Analyst + Search for complex queries."""

from api.services.analyst import query_analyst
from api.services.search import query_search
from api.db import get_cursor
from api.config import settings


def query_synthesis(question: str) -> dict:
    # Run both paths
    analyst_result = query_analyst(question)
    search_result = query_search(question, limit=3)

    # Merge with Cortex COMPLETE
    structured_context = ""
    if analyst_result.get("data"):
        structured_context = f"Quantitative data:\n{analyst_result['data'][:5]}"

    semantic_context = ""
    if search_result.get("sources"):
        reviews = "\n".join(
            f"- [{s['rating']}/5] {s['text']}" for s in search_result["sources"]
        )
        semantic_context = f"Relevant reviews:\n{reviews}"

    merge_prompt = f"""The user asked: {question}

{structured_context}

{semantic_context}

Be precise and factual. Do not add creative elaboration.
Combine the quantitative data with specific review examples into a clear answer.
Refuse to generate offensive, discriminatory, or defamatory content. If reviews contain offensive language, paraphrase rather than quote directly.
Use the exact numbers from the data. Reference what reviewers actually said.
Do not invent statistics or reviews not present above."""

    with get_cursor() as cur:
        cur.execute(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
            (settings.llm_model, merge_prompt)
        )
        answer = cur.fetchone()[0]

    return {
        "answer": answer,
        "sql": analyst_result.get("sql"),
        "data": analyst_result.get("data"),
        "sources": search_result.get("sources"),
    }
