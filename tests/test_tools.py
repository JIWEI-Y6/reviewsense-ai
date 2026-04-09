"""Tests for custom agentic RAG tools."""

import os
import sys
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()


class TestSearchReviews:
    """Tests for search_reviews tool."""

    def test_basic_search(self):
        """Basic search with no filters returns results."""
        from api.services.tools import search_reviews
        result = search_reviews("battery life wireless earbuds")
        assert result["query_info"]["result_count"] > 0
        assert len(result["results"]) > 0
        assert "text" in result["results"][0]
        assert "rating" in result["results"][0]

    def test_search_with_asin_filter(self):
        """Search filtered by ASIN returns only that product's reviews."""
        from api.services.tools import search_reviews
        result = search_reviews("quality", asin="B01G8JO5F2")
        assert result["query_info"]["result_count"] > 0
        for r in result["results"]:
            assert r["asin"] == "B01G8JO5F2"

    def test_search_with_category_filter(self):
        """Search filtered by category returns only that category."""
        from api.services.tools import search_reviews
        result = search_reviews("sound quality", category="headphones_earbuds")
        assert result["query_info"]["result_count"] > 0
        for r in result["results"]:
            assert r["category"] == "headphones_earbuds"

    def test_search_with_theme_filter(self):
        """Search filtered by theme returns relevant theme."""
        from api.services.tools import search_reviews
        result = search_reviews("problems", theme="battery_life")
        assert result["query_info"]["result_count"] > 0
        for r in result["results"]:
            assert r["theme"] == "battery_life"

    def test_search_negative_reviews(self):
        """Search with max_rating=2 returns only negative reviews."""
        from api.services.tools import search_reviews
        result = search_reviews("disappointed", max_rating=2)
        assert result["query_info"]["result_count"] > 0
        for r in result["results"]:
            assert int(r["rating"]) <= 2

    def test_search_positive_reviews(self):
        """Search with min_rating=4 returns only positive reviews."""
        from api.services.tools import search_reviews
        result = search_reviews("amazing great love", min_rating=4)
        assert result["query_info"]["result_count"] > 0
        for r in result["results"]:
            assert int(r["rating"]) >= 4

    def test_search_high_quality_only(self):
        """Search with quality='high' returns detailed reviews."""
        from api.services.tools import search_reviews
        result = search_reviews("detailed review", quality="high")
        assert result["query_info"]["result_count"] > 0
        for r in result["results"]:
            assert r["quality"] == "high"

    def test_search_combined_filters(self):
        """Search with multiple filters narrows results correctly."""
        from api.services.tools import search_reviews
        result = search_reviews(
            "battery",
            category="headphones_earbuds",
            max_rating=2,
        )
        assert result["query_info"]["filters_applied"]["category"] == "headphones_earbuds"
        assert result["query_info"]["filters_applied"]["max_rating"] == 2
        for r in result["results"]:
            assert r["category"] == "headphones_earbuds"
            assert int(r["rating"]) <= 2

    def test_search_empty_results(self):
        """Search for nonsense returns empty results gracefully."""
        from api.services.tools import search_reviews
        result = search_reviews("xyznonexistentquery12345", asin="NONEXISTENT")
        assert result["query_info"]["result_count"] == 0
        assert result["results"] == []

    def test_search_limit(self):
        """Limit parameter controls result count."""
        from api.services.tools import search_reviews
        result = search_reviews("headphones", limit=3)
        assert len(result["results"]) <= 3

    def test_search_returns_all_fields(self):
        """Each result has all expected fields."""
        from api.services.tools import search_reviews
        result = search_reviews("good product")
        assert result["query_info"]["result_count"] > 0
        r = result["results"][0]
        assert "text" in r
        assert "rating" in r
        assert "asin" in r
        assert "category" in r
        assert "theme" in r
        assert "quality" in r

    def test_filters_tracked_in_query_info(self):
        """Applied filters are recorded in query_info."""
        from api.services.tools import search_reviews
        result = search_reviews(
            "test",
            asin="B01G8JO5F2",
            category="headphones_earbuds",
            theme="sound_quality",
            verified_only=True,
        )
        info = result["query_info"]
        assert info["filters_applied"]["asin"] == "B01G8JO5F2"
        assert info["filters_applied"]["category"] == "headphones_earbuds"
        assert info["filters_applied"]["theme"] == "sound_quality"
        assert info["filters_applied"]["verified_only"] == True


class TestGetProductDetail:
    """Tests for get_product_detail tool."""

    def test_known_product(self):
        """Known ASIN returns full product data."""
        from api.services.tools import get_product_detail
        result = get_product_detail("B01G8JO5F2")
        assert result is not None
        assert result["asin"] == "B01G8JO5F2"
        assert result["review_count"] > 0
        assert result["product_name"] is not None
        assert result["brand"] is not None
        assert result["category"] is not None

    def test_unknown_product(self):
        """Unknown ASIN returns None."""
        from api.services.tools import get_product_detail
        result = get_product_detail("NONEXISTENT999")
        assert result is None

    def test_has_review_stats(self):
        """Top product has review stats (20+ reviews)."""
        from api.services.tools import get_product_detail
        result = get_product_detail("B01G8JO5F2")
        assert result["avg_rating"] is not None
        assert result["avg_sentiment"] is not None
        assert result["negative_rate"] is not None
        assert result["top_theme"] is not None

    def test_has_category_comparison(self):
        """Product has category average comparison."""
        from api.services.tools import get_product_detail
        result = get_product_detail("B01G8JO5F2")
        assert "category_avg_rating" in result
        assert "rating_vs_category" in result

    def test_has_theme_breakdown(self):
        """Product has theme breakdown."""
        from api.services.tools import get_product_detail
        result = get_product_detail("B01G8JO5F2")
        assert len(result["themes"]) > 0
        theme = result["themes"][0]
        assert "theme" in theme
        assert "review_count" in theme
        assert "avg_sentiment" in theme
        assert "negative_count" in theme

    def test_metadata_flag(self):
        """has_real_metadata flag is set correctly."""
        from api.services.tools import get_product_detail
        result = get_product_detail("B01G8JO5F2")
        # This product has real metadata from McAuley dataset
        assert isinstance(result["has_real_metadata"], bool)

    def test_product_with_features(self):
        """Product with metadata has features."""
        from api.services.tools import get_product_detail
        result = get_product_detail("B01G8JO5F2")
        if result["has_real_metadata"]:
            assert result["features"] is not None


class TestSearchProducts:
    """Tests for search_products tool."""

    def test_basic_search(self):
        """Search with no filters returns products."""
        from api.services.tools import search_products
        result = search_products()
        assert result["result_count"] > 0
        assert len(result["products"]) > 0
        p = result["products"][0]
        assert "asin" in p
        assert "product_name" in p
        assert "review_count" in p

    def test_filter_by_category(self):
        """Category filter returns only that category."""
        from api.services.tools import search_products
        result = search_products(category="headphones_earbuds")
        assert result["result_count"] > 0
        for p in result["products"]:
            assert p["category"] == "headphones_earbuds"

    def test_filter_by_brand(self):
        """Brand filter does case-insensitive partial match."""
        from api.services.tools import search_products
        result = search_products(brand="logitech")
        assert result["result_count"] > 0
        for p in result["products"]:
            assert "logitech" in (p["brand"] or "").lower()

    def test_filter_by_price_range(self):
        """Price filter returns products within range."""
        from api.services.tools import search_products
        result = search_products(min_price=10, max_price=50, min_reviews=20)
        assert result["result_count"] > 0
        for p in result["products"]:
            if p["price"] is not None:
                assert 10 <= p["price"] <= 50

    def test_filter_by_features(self):
        """Feature keyword search finds matching products."""
        from api.services.tools import search_products
        result = search_products(features_contain="bluetooth", min_reviews=10)
        assert result["result_count"] > 0
        # At least one product should have bluetooth in features
        has_match = any(
            p["features"] and "bluetooth" in p["features"].lower()
            for p in result["products"]
        )
        assert has_match

    def test_filter_by_min_rating(self):
        """Min rating filter works."""
        from api.services.tools import search_products
        result = search_products(min_rating=4.5, min_reviews=20)
        for p in result["products"]:
            if p["avg_rating"] is not None:
                assert p["avg_rating"] >= 4.5

    def test_sort_by_price(self):
        """Sort by price returns ascending price order."""
        from api.services.tools import search_products
        result = search_products(category="headphones_earbuds", sort_by="price", min_reviews=20)
        prices = [p["price"] for p in result["products"] if p["price"] is not None]
        if len(prices) >= 2:
            assert prices == sorted(prices)

    def test_sort_by_rating(self):
        """Sort by avg_rating returns descending order."""
        from api.services.tools import search_products
        result = search_products(sort_by="avg_rating", min_reviews=50)
        ratings = [p["avg_rating"] for p in result["products"] if p["avg_rating"] is not None]
        if len(ratings) >= 2:
            assert ratings == sorted(ratings, reverse=True)

    def test_limit(self):
        """Limit controls result count."""
        from api.services.tools import search_products
        result = search_products(limit=3)
        assert len(result["products"]) <= 3

    def test_combined_filters(self):
        """Multiple filters combine correctly."""
        from api.services.tools import search_products
        result = search_products(
            category="headphones_earbuds",
            min_reviews=20,
            min_rating=4.0,
        )
        for p in result["products"]:
            assert p["category"] == "headphones_earbuds"
            assert p["review_count"] >= 20
            if p["avg_rating"]:
                assert p["avg_rating"] >= 4.0

    def test_search_criteria_tracked(self):
        """Applied criteria are recorded in response."""
        from api.services.tools import search_products
        result = search_products(
            category="speakers",
            brand="oontz",
            min_price=10,
        )
        criteria = result["search_criteria"]
        assert criteria["category"] == "speakers"
        assert criteria["brand"] == "oontz"
        assert criteria["min_price"] == 10

    def test_no_results(self):
        """Impossible criteria returns empty gracefully."""
        from api.services.tools import search_products
        result = search_products(
            category="headphones_earbuds",
            min_price=99999,
        )
        assert result["result_count"] == 0
        assert result["products"] == []


class TestCompareProducts:
    """Tests for compare_products tool."""

    def test_compare_two_products(self):
        """Compare 2 known products returns comparison data."""
        from api.services.tools import compare_products
        result = compare_products(["B01G8JO5F2", "B010OYASRG"])
        assert len(result["products"]) == 2
        assert "comparison" in result
        assert "winners" in result
        assert "overall_winner" in result

    def test_compare_has_metrics(self):
        """Comparison includes key metrics."""
        from api.services.tools import compare_products
        result = compare_products(["B01G8JO5F2", "B010OYASRG"])
        comp = result["comparison"]
        assert "avg_rating" in comp
        assert "avg_sentiment" in comp
        assert "negative_rate" in comp

    def test_compare_winners_computed(self):
        """Each metric has a winner identified."""
        from api.services.tools import compare_products
        result = compare_products(["B01G8JO5F2", "B010OYASRG"])
        for metric, data in result["comparison"].items():
            assert "best" in data
            assert "worst" in data
            assert "spread" in data
            assert data["best"]["asin"] in ["B01G8JO5F2", "B010OYASRG"]

    def test_compare_win_counts(self):
        """Win counts sum correctly."""
        from api.services.tools import compare_products
        result = compare_products(["B01G8JO5F2", "B010OYASRG"])
        total_wins = sum(result["win_counts"].values())
        total_metrics = len(result["comparison"])
        assert total_wins == total_metrics

    def test_compare_theme_comparison(self):
        """Theme comparison shows top themes per product."""
        from api.services.tools import compare_products
        result = compare_products(["B01G8JO5F2", "B010OYASRG"])
        assert "B01G8JO5F2" in result["theme_comparison"]
        assert len(result["theme_comparison"]["B01G8JO5F2"]) > 0

    def test_compare_single_product_error(self):
        """Single product returns error."""
        from api.services.tools import compare_products
        result = compare_products(["B01G8JO5F2"])
        assert "error" in result

    def test_compare_unknown_product(self):
        """Unknown ASIN handled gracefully."""
        from api.services.tools import compare_products
        result = compare_products(["B01G8JO5F2", "NONEXISTENT999"])
        # Should still work with 1 found product but return error (need 2)
        assert "error" in result or len(result["products"]) >= 1

    def test_compare_max_five(self):
        """More than 5 ASINs truncated to 5."""
        from api.services.tools import compare_products
        asins = ["B01G8JO5F2", "B010OYASRG", "B00ZV9RDKK", "B079QHML21",
                 "B01DFKC2SO", "B0791TX5P5", "B07FZ8S74R"]
        result = compare_products(asins)
        assert len(result["products"]) <= 5


class TestVerifyClaims:
    """Tests for verify_claims tool."""

    def test_verify_known_product(self):
        from api.services.tools import verify_claims
        result = verify_claims("B01G8JO5F2")
        assert "claims" in result
        assert "trust_score" in result
        assert "summary" in result

    def test_verify_unknown_product(self):
        from api.services.tools import verify_claims
        result = verify_claims("NONEXISTENT999")
        assert "error" in result

    def test_verify_has_verdicts(self):
        from api.services.tools import verify_claims
        result = verify_claims("B01G8JO5F2")
        if result.get("claims"):
            for claim in result["claims"]:
                assert claim["verdict"] in ["CONFIRMED", "DISPUTED", "MIXED", "INSUFFICIENT_DATA", "UNKNOWN", "ERROR"]


class TestBrandAnalysis:
    """Tests for get_brand_analysis and compare_brands tools."""

    def test_known_brand(self):
        from api.services.tools import get_brand_analysis
        result = get_brand_analysis("Logitech")
        assert result is not None
        assert result["total_reviews"] > 0
        assert result["avg_rating"] is not None

    def test_unknown_brand(self):
        from api.services.tools import get_brand_analysis
        result = get_brand_analysis("NonExistentBrand12345")
        assert result is None

    def test_brand_has_categories(self):
        from api.services.tools import get_brand_analysis
        result = get_brand_analysis("Sony")
        assert result is not None
        assert len(result["categories"]) > 0

    def test_brand_has_complaints(self):
        from api.services.tools import get_brand_analysis
        result = get_brand_analysis("Logitech")
        assert result is not None
        assert "top_complaints" in result

    def test_compare_two_brands(self):
        from api.services.tools import compare_brands
        result = compare_brands(["Logitech", "Sony"])
        assert len(result["brands"]) == 2
        assert "comparison" in result

    def test_compare_single_brand_error(self):
        from api.services.tools import compare_brands
        result = compare_brands(["Logitech"])
        assert "error" in result


class TestFindSimilarProducts:
    """Tests for find_similar_products tool."""

    def test_known_product(self):
        from api.services.tools import find_similar_products
        result = find_similar_products("B01G8JO5F2")
        assert "similar_products" in result
        assert "total_also_buy" in result

    def test_unknown_product(self):
        from api.services.tools import find_similar_products
        result = find_similar_products("NONEXISTENT999")
        assert result["similar_products"] == [] or "note" in result


class TestPriceValueAnalysis:
    """Tests for price_value_analysis tool."""

    def test_category_with_prices(self):
        from api.services.tools import price_value_analysis
        result = price_value_analysis("headphones_earbuds")
        assert "price_brackets" in result
        assert "category" in result
        assert result["category"] == "headphones_earbuds"

    def test_has_brackets(self):
        from api.services.tools import price_value_analysis
        result = price_value_analysis("headphones_earbuds")
        if result["price_brackets"]:
            bracket = result["price_brackets"][0]
            assert "bracket" in bracket
            assert "avg_rating" in bracket
            assert "product_count" in bracket

    def test_unknown_category(self):
        from api.services.tools import price_value_analysis
        result = price_value_analysis("nonexistent_category")
        assert result["price_brackets"] == []


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
