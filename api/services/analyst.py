"""Structured path: Cortex Analyst for quantitative queries."""

import requests
import snowflake.connector
from api.db import get_connection, return_connection
from api.config import settings


def query_analyst(question: str) -> dict:
    conn = get_connection()
    try:
        token = conn.rest._token
        account = settings.snowflake_account

        url = f"https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message"
        headers = {
            "Authorization": f'Snowflake Token="{token}"',
            "Content-Type": "application/json",
        }
        payload = {
            "semantic_view": settings.semantic_view,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": question}]}
            ],
        }

        resp = requests.post(url, headers=headers, json=payload)

        if resp.status_code != 200:
            return {
                "answer": f"Analyst error: {resp.text[:300]}",
                "sql": None,
                "data": None,
                "sources": None,
            }

        response = resp.json()
        sql = None
        answer = ""

        for item in response.get("message", {}).get("content", []):
            if item.get("type") == "sql":
                sql = item["statement"]
            elif item.get("type") == "text":
                answer = item["text"]

        # Execute the generated SQL to get actual data
        data = None
        if sql:
            try:
                cur = conn.cursor(snowflake.connector.DictCursor)
                cur.execute(sql)
                data = cur.fetchall()
            except Exception as e:
                answer += f"\n\n(SQL execution error: {str(e)[:100]})"
                data = None

        return {
            "answer": answer,
            "sql": sql,
            "data": data,
            "sources": None,
        }
    finally:
        return_connection(conn)
