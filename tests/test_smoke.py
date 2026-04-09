"""Smoke tests — run before every demo. 5 critical paths.

Requires: API running at localhost:8000 + Snowflake connection.
Run: python -m pytest tests/test_smoke.py -v
"""

import pytest
import requests

API = "http://localhost:8000"


class TestSmoke:

    def test_health(self):
        r = requests.get(f"{API}/health", timeout=10)
        assert r.status_code == 200
        data = r.json()
        assert data["snowflake_connected"] is True

    def test_categories(self):
        r = requests.get(f"{API}/categories", timeout=15)
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 14

    def test_product_lookup(self):
        r = requests.get(f"{API}/products/B01G8JO5F2", timeout=15)
        assert r.status_code == 200
        data = r.json()
        assert data["asin"] == "B01G8JO5F2"
        assert data["review_count"] > 0

    def test_structured_query(self):
        r = requests.post(f"{API}/query",
                          json={"question": "Which category has the most reviews?"},
                          timeout=60)
        assert r.status_code == 200
        data = r.json()
        assert len(data["answer"]) > 20

    def test_semantic_query(self):
        r = requests.post(f"{API}/query",
                          json={"question": "What do people say about headphone sound quality?"},
                          timeout=60)
        assert r.status_code == 200
        data = r.json()
        assert len(data["answer"]) > 20


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
