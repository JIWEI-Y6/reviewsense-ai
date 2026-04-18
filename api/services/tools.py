"""Custom agentic RAG tools.

Each tool is a self-contained function that queries Snowflake data.
Tools are called by the agent loop based on the user's question.
All tools return structured dicts — the agent synthesizes the final answer.
"""

import json
import re
from api.db import get_cursor
from api.config import settings


_BRACKET_TAG_RE = re.compile(r'\[[^\]]{1,40}\]')


def clean_product_name(raw: str | None, max_len: int = 80) -> str | None:
    """Strip Amazon title markup (e.g. '[2-Pack]', '[Apple MFi Certified]') and truncate."""
    if not raw:
        return raw
    s = _BRACKET_TAG_RE.sub('', raw)
    s = re.sub(r'\s{2,}', ' ', s).strip(' ,-')
    if len(s) > max_len:
        s = s[:max_len].rsplit(' ', 1)[0] + '…'
    return s


BRAND_ALIASES = {
    "amazon basics": "amazonbasics",
    "amazon-basics": "amazonbasics",
    "amazonbasic": "amazonbasics",
    "amazon-basic": "amazonbasics",
}


def _normalize_brand(brand: str) -> str:
    """Map common brand-name variants to the canonical form stored in metadata."""
    key = brand.strip().lower()
    return BRAND_ALIASES.get(key, key)


# ============================================
# TOOL 1: search_reviews (enhanced)
# ============================================

def search_reviews(
    query: str,
    asin: str | None = None,
    category: str | None = None,
    theme: str | None = None,
    min_rating: int | None = None,
    max_rating: int | None = None,
    verified_only: bool = False,
    quality: str | None = None,
    limit: int = 5,
) -> dict:
    """Search reviews with optional filters on ASIN, category, theme, rating, quality.

    Uses GOLD.ENRICHED_REVIEW_SEARCH (Cortex Search Service) with structured filters.
    Returns review excerpts with metadata.

    Args:
        query: Natural language search query
        asin: Filter to a specific product ASIN
        category: Filter to a derived category (e.g., 'headphones_earbuds')
        theme: Filter to a review theme (e.g., 'battery_life')
        min_rating: Minimum star rating (1-5)
        max_rating: Maximum star rating (1-5)
        verified_only: Only return verified purchase reviews
        quality: Review quality tier ('high', 'medium', 'low')
        limit: Max results (default 5)

    Returns:
        dict with 'results' (list of review dicts) and 'query_info' (filter summary).
        When asin+theme are both set, also returns 'theme_stats' with aggregate
        numbers (total reviews, avg sentiment, avg rating, positive/negative %)
        so synthesis answers stay consistent with prior aggregate turns.
    """
    # A 5-review sample is unrepresentative of a large themed population
    # (e.g. 412 reviews tagged battery_life). Bump default when theme is set.
    if theme and limit == 5:
        limit = 15

    # Build filter object for Cortex Search
    filters = {}
    if asin:
        filters["@eq"] = {"ASIN": asin}
    if category:
        if "@eq" in filters:
            # Multiple eq filters need @and
            filters = {"@and": [{"@eq": {"ASIN": asin}}, {"@eq": {"DERIVED_CATEGORY": category}}]}
        else:
            filters["@eq"] = {"DERIVED_CATEGORY": category}
    if theme:
        eq_filter = {"@eq": {"REVIEW_THEME": theme}}
        if "@and" in filters:
            filters["@and"].append(eq_filter)
        elif "@eq" in filters:
            filters = {"@and": [{"@eq": filters["@eq"]}, eq_filter]}
        else:
            filters["@eq"] = {"REVIEW_THEME": theme}
    if max_rating and max_rating <= 2:
        # Filter for negative reviews (Cortex Search needs numeric, not string)
        lte_filter = {"@lte": {"RATING": max_rating}}
        if "@and" in filters:
            filters["@and"].append(lte_filter)
        elif "@eq" in filters:
            filters = {"@and": [{"@eq": filters["@eq"]}, lte_filter]}
        else:
            filters = lte_filter
    if min_rating and min_rating >= 4:
        # Filter for positive reviews (Cortex Search needs numeric, not string)
        gte_filter = {"@gte": {"RATING": min_rating}}
        if "@and" in filters:
            filters["@and"].append(gte_filter)
        elif "@eq" in filters:
            filters = {"@and": [{"@eq": filters["@eq"]}, gte_filter]}
        else:
            filters = gte_filter
    if verified_only:
        eq_filter = {"@eq": {"VERIFIED_PURCHASE": True}}
        if "@and" in filters:
            filters["@and"].append(eq_filter)
        elif "@eq" in filters:
            filters = {"@and": [{"@eq": filters["@eq"]}, eq_filter]}
        else:
            filters = eq_filter
    if quality:
        eq_filter = {"@eq": {"REVIEW_QUALITY": quality}}
        if "@and" in filters:
            filters["@and"].append(eq_filter)
        elif "@eq" in filters:
            filters = {"@and": [{"@eq": filters["@eq"]}, eq_filter]}
        else:
            filters = eq_filter

    # Build search query JSON
    search_params = {
        "query": query,
        "columns": ["REVIEW_TEXT_CLEAN", "RATING", "ASIN", "DERIVED_CATEGORY",
                     "REVIEW_THEME", "REVIEW_QUALITY"],
        "limit": limit,
    }
    if filters:
        search_params["filter"] = filters

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    '{settings.search_service}',
                    %s
                )
            ) AS results
            """,
            (json.dumps(search_params),)
        )
        row = cur.fetchone()

        if not row or not row[0]:
            return {
                "results": [],
                "query_info": {
                    "query": query,
                    "filters_applied": {k: v for k, v in {
                        "asin": asin, "category": category, "theme": theme,
                        "min_rating": min_rating, "max_rating": max_rating,
                        "verified_only": verified_only, "quality": quality,
                    }.items() if v},
                    "result_count": 0,
                },
            }

        search_results = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        results = []
        for r in search_results.get("results", []):
            results.append({
                "text": r.get("REVIEW_TEXT_CLEAN", "")[:500],
                "rating": r.get("RATING", ""),
                "asin": r.get("ASIN", ""),
                "category": r.get("DERIVED_CATEGORY", ""),
                "theme": r.get("REVIEW_THEME", ""),
                "quality": r.get("REVIEW_QUALITY", ""),
            })

        # Enrich results with product names from PRODUCT_LOOKUP
        unique_asins = list({r["asin"] for r in results if r.get("asin")})
        product_names = {}
        if unique_asins:
            placeholders = ", ".join(["%s"] * len(unique_asins))
            cur.execute(f"""
                SELECT ASIN,
                       COALESCE(METADATA_TITLE, PRODUCT_NAME) AS PRODUCT_NAME,
                       COALESCE(METADATA_BRAND, BRAND) AS BRAND
                FROM GOLD.PRODUCT_LOOKUP
                WHERE ASIN IN ({placeholders})
            """, unique_asins)
            for row in cur.fetchall():
                product_names[row[0]] = {"product_name": row[1], "brand": row[2]}

        # Add product name/brand to each result
        for r in results:
            info = product_names.get(r["asin"], {})
            r["product_name"] = info.get("product_name")
            r["brand"] = info.get("brand")

        # Aggregate by ASIN — shows which products appear most in results
        from collections import Counter
        asin_counts = Counter(r["asin"] for r in results if r.get("asin"))
        product_mentions = []
        for asin_val, count in asin_counts.most_common(5):
            info = product_names.get(asin_val, {})
            product_mentions.append({
                "asin": asin_val,
                "product_name": info.get("product_name"),
                "brand": info.get("brand"),
                "mention_count": count,
            })

        response = {
            "results": results,
            "product_mentions": product_mentions,
            "query_info": {
                "query": query,
                "filters_applied": {k: v for k, v in {
                    "asin": asin, "category": category, "theme": theme,
                    "min_rating": min_rating, "max_rating": max_rating,
                    "verified_only": verified_only, "quality": quality,
                }.items() if v},
                "result_count": len(results),
            },
        }

        # Theme aggregate — keeps follow-up answers consistent with prior turns
        # when the user asks about a specific theme on a specific product.
        if asin and theme:
            cur.execute("""
                SELECT
                    COUNT(*)                     AS total,
                    AVG(SENTIMENT_SCORE)         AS avg_sentiment,
                    AVG(RATING)                  AS avg_rating,
                    SUM(CASE WHEN SENTIMENT_SCORE > 0.2  THEN 1 ELSE 0 END) AS positive,
                    SUM(CASE WHEN SENTIMENT_SCORE < -0.2 THEN 1 ELSE 0 END) AS negative
                FROM GOLD.ENRICHED_REVIEWS
                WHERE ASIN = %s AND REVIEW_THEME = %s
            """, (asin, theme))
            stats = cur.fetchone()
            if stats and stats[0] and stats[0] > 0:
                total = stats[0]
                response["theme_stats"] = {
                    "asin": asin,
                    "theme": theme,
                    "total_reviews": total,
                    "avg_sentiment": round(float(stats[1] or 0), 3),
                    "avg_rating": round(float(stats[2] or 0), 2),
                    "positive_pct": round(float(stats[3] or 0) / total, 3),
                    "negative_pct": round(float(stats[4] or 0) / total, 3),
                }

        return response


# ============================================
# TOOL 2: get_product_detail
# ============================================

def get_product_detail(asin: str) -> dict | None:
    """Get complete product profile: metadata + review stats + theme breakdown.

    Combines data from PRODUCT_LOOKUP (review stats + metadata),
    PRODUCT_SENTIMENT_SUMMARY (aggregations), and ENRICHED_REVIEWS (theme breakdown).

    Args:
        asin: Amazon product ASIN

    Returns:
        dict with product info, review stats, themes, or None if not found
    """
    with get_cursor() as cur:
        # Product lookup with metadata
        cur.execute("""
            SELECT
                p.ASIN,
                p.DERIVED_CATEGORY,
                p.REVIEW_COUNT,
                p.DERIVATION_CONFIDENCE,
                COALESCE(p.METADATA_TITLE, p.PRODUCT_NAME) AS PRODUCT_NAME,
                COALESCE(p.METADATA_BRAND, p.BRAND) AS BRAND,
                p.METADATA_PRICE AS PRICE,
                p.METADATA_FEATURES AS FEATURES,
                p.METADATA_CATEGORY_PATH AS CATEGORY_PATH,
                p.HAS_METADATA
            FROM GOLD.PRODUCT_LOOKUP p
            WHERE p.ASIN = %s
        """, (asin,))
        lookup = cur.fetchone()

        if not lookup:
            return None

        result = {
            "asin": lookup[0],
            "category": lookup[1],
            "review_count": lookup[2],
            "product_name": lookup[4],
            "brand": lookup[5],
            "price": float(lookup[6]) if lookup[6] else None,
            "features": lookup[7],
            "category_path": lookup[8],
            "has_real_metadata": bool(lookup[9]),
        }

        # Product sentiment summary (only for 20+ review products)
        cur.execute("""
            SELECT AVG_RATING, AVG_SENTIMENT, NEGATIVE_RATE, TOP_THEME
            FROM GOLD.PRODUCT_SENTIMENT_SUMMARY
            WHERE ASIN = %s
        """, (asin,))
        stats = cur.fetchone()

        if stats:
            result["avg_rating"] = float(stats[0])
            result["avg_sentiment"] = float(stats[1])
            result["negative_rate"] = float(stats[2])
            result["top_theme"] = stats[3]
        else:
            result["avg_rating"] = None
            result["avg_sentiment"] = None
            result["negative_rate"] = None
            result["top_theme"] = None

        # Category averages for comparison
        if lookup[1]:  # has derived_category
            cur.execute("""
                SELECT AVG_RATING, AVG_SENTIMENT, NEGATIVE_RATE
                FROM GOLD.CATEGORY_SENTIMENT_SUMMARY
                WHERE DERIVED_CATEGORY = %s
            """, (lookup[1],))
            cat_avg = cur.fetchone()
            if cat_avg:
                result["category_avg_rating"] = float(cat_avg[0])
                result["category_avg_sentiment"] = float(cat_avg[1])
                result["category_avg_negative_rate"] = float(cat_avg[2])

                # Delta vs category
                if result["avg_rating"]:
                    result["rating_vs_category"] = round(result["avg_rating"] - float(cat_avg[0]), 2)
                    result["sentiment_vs_category"] = round(result["avg_sentiment"] - float(cat_avg[1]), 4)

        # Theme breakdown for this product
        cur.execute("""
            SELECT REVIEW_THEME, COUNT(*) AS cnt,
                   ROUND(AVG(SENTIMENT_SCORE), 3) AS avg_sent,
                   ROUND(AVG(RATING), 2) AS avg_rat,
                   COUNT(CASE WHEN RATING <= 2 THEN 1 END) AS neg_cnt
            FROM GOLD.ENRICHED_REVIEWS
            WHERE ASIN = %s
            GROUP BY REVIEW_THEME
            ORDER BY cnt DESC
        """, (asin,))
        themes = [
            {
                "theme": r[0],
                "review_count": r[1],
                "avg_sentiment": float(r[2]),
                "avg_rating": float(r[3]),
                "negative_count": r[4],
            }
            for r in cur.fetchall()
        ]
        result["themes"] = themes

        return result


# ============================================
# TOOL 3: search_products
# ============================================

def search_products(
    category: str | None = None,
    brand: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    features_contain: str | None = None,
    min_rating: float | None = None,
    min_reviews: int = 5,
    sort_by: str = "review_count",
    limit: int = 10,
    review_theme: str | None = None,
) -> dict:
    """Search products by metadata criteria: price, features, brand, category, rating, theme.

    Queries CURATED.PRODUCT_METADATA joined with GOLD.PRODUCT_SENTIMENT_SUMMARY
    for review stats. When review_theme is specified, joins ENRICHED_REVIEWS to find
    products with positive reviews about that theme. Pure SQL — no Cortex functions.

    Args:
        category: Derived category filter (e.g., 'headphones_earbuds')
        brand: Brand name filter (case-insensitive partial match)
        min_price: Minimum price
        max_price: Maximum price
        features_contain: Keyword to search in product features (e.g., 'waterproof', 'noise cancelling')
        min_rating: Minimum average rating
        min_reviews: Minimum review count (default 5)
        sort_by: Sort field — 'review_count', 'avg_rating', 'price', 'avg_sentiment', 'theme_sentiment'
        limit: Max results (default 10)
        review_theme: Filter to products with reviews about this theme (e.g., 'comfort', 'battery_life').
                       Ranked by positive sentiment on that theme.

    Returns:
        dict with 'products' list and 'search_criteria' summary
    """
    conditions = ["p.REVIEW_COUNT >= %s"]
    params = [min_reviews]

    if category:
        conditions.append("p.DERIVED_CATEGORY = %s")
        params.append(category)
    if brand:
        conditions.append("LOWER(COALESCE(m.BRAND, '')) LIKE %s")
        params.append(f"%{brand.lower()}%")
    if min_price is not None:
        conditions.append("m.PRICE >= %s")
        params.append(min_price)
    if max_price is not None:
        conditions.append("m.PRICE <= %s")
        params.append(max_price)
    if features_contain:
        conditions.append("LOWER(COALESCE(m.FEATURES_TEXT, '')) LIKE %s")
        params.append(f"%{features_contain.lower()}%")
    if min_rating:
        conditions.append("s.AVG_RATING >= %s")
        params.append(min_rating)

    # Theme-based filtering: join ENRICHED_REVIEWS to find products with reviews about a theme
    theme_join = ""
    theme_select = ""
    if review_theme:
        theme_join = """
            INNER JOIN (
                SELECT ASIN,
                       COUNT(*) AS theme_review_count,
                       ROUND(AVG(SENTIMENT_SCORE), 3) AS theme_avg_sentiment,
                       ROUND(AVG(RATING), 2) AS theme_avg_rating
                FROM GOLD.ENRICHED_REVIEWS
                WHERE REVIEW_THEME = %s AND SENTIMENT_SCORE > 0
                GROUP BY ASIN
                HAVING COUNT(*) >= 2
            ) t ON p.ASIN = t.ASIN
        """
        theme_select = ", t.theme_review_count, t.theme_avg_sentiment, t.theme_avg_rating"
        params.append(review_theme)

    where_clause = " AND ".join(conditions)

    sort_map = {
        "review_count": "p.REVIEW_COUNT DESC",
        "avg_rating": "s.AVG_RATING DESC NULLS LAST",
        "price": "m.PRICE ASC NULLS LAST",
        "avg_sentiment": "s.AVG_SENTIMENT DESC NULLS LAST",
        "theme_sentiment": "t.theme_avg_sentiment DESC NULLS LAST" if review_theme else "s.AVG_SENTIMENT DESC NULLS LAST",
    }
    # Default to theme_sentiment when filtering by theme
    if review_theme and sort_by == "review_count":
        sort_by = "theme_sentiment"
    order_by = sort_map.get(sort_by, "p.REVIEW_COUNT DESC")

    with get_cursor() as cur:
        cur.execute(f"""
            SELECT
                p.ASIN,
                COALESCE(p.METADATA_TITLE, p.PRODUCT_NAME) AS PRODUCT_NAME,
                COALESCE(p.METADATA_BRAND, p.BRAND) AS BRAND,
                m.PRICE,
                p.DERIVED_CATEGORY,
                p.REVIEW_COUNT,
                s.AVG_RATING,
                s.AVG_SENTIMENT,
                s.NEGATIVE_RATE,
                s.TOP_THEME,
                m.FEATURES_TEXT AS FEATURES_STR
                {theme_select}
            FROM GOLD.PRODUCT_LOOKUP p
            LEFT JOIN CURATED.PRODUCT_METADATA m ON p.ASIN = m.ASIN
            LEFT JOIN GOLD.PRODUCT_SENTIMENT_SUMMARY s ON p.ASIN = s.ASIN
            {theme_join}
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT {limit}
        """, params)

        products = []
        for r in cur.fetchall():
            product = {
                "asin": r[0],
                "product_name": r[1],
                "brand": r[2],
                "price": float(r[3]) if r[3] else None,
                "category": r[4],
                "review_count": r[5],
                "avg_rating": float(r[6]) if r[6] else None,
                "avg_sentiment": float(r[7]) if r[7] else None,
                "negative_rate": float(r[8]) if r[8] else None,
                "top_theme": r[9],
                "features": r[10][:300] if r[10] else None,
            }
            if review_theme:
                product["theme_review_count"] = r[11]
                product["theme_avg_sentiment"] = float(r[12]) if r[12] else None
                product["theme_avg_rating"] = float(r[13]) if r[13] else None
            products.append(product)

        return {
            "products": products,
            "search_criteria": {k: v for k, v in {
                "category": category, "brand": brand,
                "min_price": min_price, "max_price": max_price,
                "features_contain": features_contain,
                "min_rating": min_rating, "min_reviews": min_reviews,
                "sort_by": sort_by, "review_theme": review_theme,
            }.items() if v is not None},
            "result_count": len(products),
        }


# ============================================
# TOOL 4: compare_products
# ============================================

def compare_products(asins: list[str]) -> dict:
    """Side-by-side comparison of 2-5 products.

    Calls get_product_detail for each ASIN, then computes deltas
    (which product is better on each metric).

    Args:
        asins: List of 2-5 Amazon ASINs to compare

    Returns:
        dict with 'products' (list of detail dicts), 'comparison' (metric deltas),
        and 'winner' (best on each metric)
    """
    if len(asins) < 2:
        return {"error": "Need at least 2 ASINs to compare", "products": [], "comparison": {}}
    if len(asins) > 5:
        asins = asins[:5]

    # Get detail for each product
    products = []
    for asin in asins:
        detail = get_product_detail(asin)
        if detail:
            products.append(detail)

    if len(products) < 2:
        return {
            "error": f"Only {len(products)} of {len(asins)} products found. Need at least 2.",
            "products": products,
            "comparison": {},
        }

    # Compute comparison metrics
    comparison = {}
    metrics = [
        ("avg_rating", "higher is better", True),
        ("avg_sentiment", "higher is better", True),
        ("negative_rate", "lower is better", False),
        ("review_count", "more reviews = more data", True),
        ("price", "lower is better", False),
    ]

    winners = {}
    for metric, description, higher_is_better in metrics:
        values = []
        for p in products:
            val = p.get(metric)
            if val is not None:
                values.append({"asin": p["asin"], "name": p.get("product_name", p["asin"]), "value": val})

        if len(values) >= 2:
            sorted_vals = sorted(values, key=lambda x: x["value"], reverse=higher_is_better)
            best = sorted_vals[0]
            worst = sorted_vals[-1]
            comparison[metric] = {
                "description": description,
                "values": {v["asin"]: v["value"] for v in values},
                "best": {"asin": best["asin"], "name": best["name"], "value": best["value"]},
                "worst": {"asin": worst["asin"], "name": worst["name"], "value": worst["value"]},
                "spread": round(abs(best["value"] - worst["value"]), 4),
            }
            winners[metric] = best["asin"]

    # Count wins per product
    win_counts = {}
    for asin in asins:
        win_counts[asin] = sum(1 for w in winners.values() if w == asin)

    overall_winner = max(win_counts, key=win_counts.get) if win_counts else None

    # Theme comparison — what themes dominate each product
    theme_comparison = {}
    for p in products:
        top_themes = sorted(p.get("themes", []), key=lambda t: t["review_count"], reverse=True)[:3]
        theme_comparison[p["asin"]] = [t["theme"] for t in top_themes]

    return {
        "products": products,
        "comparison": comparison,
        "winners": winners,
        "win_counts": win_counts,
        "overall_winner": overall_winner,
        "theme_comparison": theme_comparison,
    }


# ============================================
# TOOL 5: verify_claims
# ============================================

def verify_claims(asin: str, model: str | None = None) -> dict:
    """Compare product metadata feature claims vs actual review evidence.

    For each feature claim in the metadata, searches reviews for evidence
    and uses CORTEX.COMPLETE to judge if the claim is supported.

    Args:
        asin: Amazon product ASIN
        model: optional Cortex LLM override (e.g., 'llama3.1-70b'). Falls back
               to settings.llm_model. Threaded in from the agent at execute time
               so the candidate model is used end-to-end during bake-off runs.

    Returns:
        dict with 'product' info, 'claims' list with verdicts, 'overall_trust_score'
    """
    active_model = model or settings.llm_model
    # Get product detail (includes features from metadata)
    product = get_product_detail(asin)
    if not product or not product.get("features"):
        return {
            "error": f"No feature data available for {asin}. Claim verification requires product metadata.",
            "product": product,
            "claims": [],
        }

    # Parse features into individual claims
    features_raw = product["features"]
    if isinstance(features_raw, str):
        claims = [f.strip() for f in features_raw.split("|") if len(f.strip()) > 10][:5]
    elif isinstance(features_raw, list):
        claims = [str(f).strip() for f in features_raw if len(str(f).strip()) > 10][:5]
    else:
        claims = []

    if not claims:
        return {"error": "No parseable feature claims found.", "product": product, "claims": []}

    results = []
    with get_cursor() as cur:
        for claim in claims:
            # Search reviews for evidence related to this claim
            review_results = search_reviews(
                query=claim[:200],
                asin=asin,
                limit=5,
            )

            review_texts = []
            for r in review_results.get("results", []):
                review_texts.append(f"[Rating: {r['rating']}/5] {r['text'][:200]}")

            if not review_texts:
                results.append({
                    "claim": claim[:200],
                    "verdict": "INSUFFICIENT_DATA",
                    "confidence": 0.0,
                    "evidence_count": 0,
                    "summary": "No relevant reviews found to verify this claim.",
                })
                continue

            evidence = "\n".join(review_texts)

            # Use COMPLETE to judge the claim
            prompt = (
                "You are a claim verification analyst. Compare the manufacturer's claim "
                "against actual customer reviews.\n\n"
                f"CLAIM: {claim[:200]}\n\n"
                f"CUSTOMER REVIEWS:\n{evidence}\n\n"
                "Respond with ONLY valid JSON (no other text):\n"
                '{"verdict": "CONFIRMED|DISPUTED|MIXED", '
                '"confidence": 0.0-1.0, '
                '"summary": "one sentence explanation"}'
            )

            try:
                cur.execute(
                    "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)",
                    (active_model, prompt)
                )
                response = cur.fetchone()[0].strip()
                # Try to parse JSON from response
                import re as _re
                json_match = _re.search(r'\{.*\}', response, _re.DOTALL)
                if json_match:
                    verdict_data = json.loads(json_match.group(0))
                    results.append({
                        "claim": claim[:200],
                        "verdict": verdict_data.get("verdict", "UNKNOWN"),
                        "confidence": verdict_data.get("confidence", 0.5),
                        "evidence_count": len(review_texts),
                        "summary": verdict_data.get("summary", ""),
                    })
                else:
                    results.append({
                        "claim": claim[:200],
                        "verdict": "UNKNOWN",
                        "confidence": 0.5,
                        "evidence_count": len(review_texts),
                        "summary": response[:200],
                    })
            except Exception:
                results.append({
                    "claim": claim[:200],
                    "verdict": "ERROR",
                    "confidence": 0.0,
                    "evidence_count": len(review_texts),
                    "summary": "Error during verification.",
                })

    # Overall trust score
    verdicts = [r["verdict"] for r in results]
    confirmed = verdicts.count("CONFIRMED")
    disputed = verdicts.count("DISPUTED")
    total = len(verdicts)
    trust_score = confirmed / total if total > 0 else 0

    return {
        "product": {
            "asin": product["asin"],
            "name": product.get("product_name"),
            "brand": product.get("brand"),
        },
        "claims": results,
        "trust_score": round(trust_score, 2),
        "summary": {
            "total_claims": total,
            "confirmed": confirmed,
            "disputed": disputed,
            "mixed": verdicts.count("MIXED"),
            "insufficient_data": verdicts.count("INSUFFICIENT_DATA"),
        },
    }


# ============================================
# TOOL 6: get_brand_analysis + compare_brands
# ============================================

def get_brand_analysis(brand: str) -> dict | None:
    """Get brand-level stats: product count, avg rating, sentiment, categories, top products.

    Queries CURATED.PRODUCT_METADATA + GOLD.ENRICHED_REVIEWS aggregated by brand.

    Args:
        brand: Brand name (case-insensitive partial match)

    Returns:
        dict with brand stats, top products, category breakdown, or None if not found
    """
    with get_cursor() as cur:
        # Brand overview from metadata + reviews
        cur.execute("""
            SELECT
                m.BRAND,
                COUNT(DISTINCT m.ASIN) AS total_products,
                COUNT(DISTINCT r.REVIEW_ID) AS total_reviews,
                ROUND(AVG(r.RATING), 2) AS avg_rating,
                ROUND(AVG(r.SENTIMENT_SCORE), 4) AS avg_sentiment,
                ROUND(COUNT(CASE WHEN r.RATING <= 2 THEN 1 END)::FLOAT /
                    NULLIF(COUNT(r.REVIEW_ID), 0), 4) AS negative_rate,
                ROUND(AVG(m.PRICE), 2) AS avg_price,
                MIN(m.PRICE) AS min_price,
                MAX(m.PRICE) AS max_price
            FROM CURATED.PRODUCT_METADATA m
            LEFT JOIN GOLD.ENRICHED_REVIEWS r ON m.ASIN = r.ASIN
            WHERE LOWER(m.BRAND) LIKE %s
            GROUP BY m.BRAND
            ORDER BY total_reviews DESC
            LIMIT 1
        """, (f"%{brand.lower()}%",))
        overview = cur.fetchone()

        if not overview or not overview[2]:  # no reviews
            return None

        result = {
            "brand": overview[0],
            "total_products": overview[1],
            "total_reviews": overview[2],
            "avg_rating": float(overview[3]) if overview[3] else None,
            "avg_sentiment": float(overview[4]) if overview[4] else None,
            "negative_rate": float(overview[5]) if overview[5] else None,
            "price_range": {
                "avg": float(overview[6]) if overview[6] else None,
                "min": float(overview[7]) if overview[7] else None,
                "max": float(overview[8]) if overview[8] else None,
            },
        }

        # Category breakdown
        cur.execute("""
            SELECT p.DERIVED_CATEGORY, COUNT(DISTINCT m.ASIN) AS products,
                   COUNT(r.REVIEW_ID) AS reviews
            FROM CURATED.PRODUCT_METADATA m
            JOIN GOLD.PRODUCT_LOOKUP p ON m.ASIN = p.ASIN
            LEFT JOIN GOLD.ENRICHED_REVIEWS r ON m.ASIN = r.ASIN
            WHERE LOWER(m.BRAND) LIKE %s AND p.DERIVED_CATEGORY IS NOT NULL
            GROUP BY p.DERIVED_CATEGORY
            ORDER BY reviews DESC
        """, (f"%{brand.lower()}%",))
        result["categories"] = [
            {"category": r[0], "products": r[1], "reviews": r[2]}
            for r in cur.fetchall()
        ]

        # Top products by review count
        cur.execute("""
            SELECT m.ASIN, m.TITLE, s.REVIEW_COUNT, s.AVG_RATING, s.AVG_SENTIMENT
            FROM CURATED.PRODUCT_METADATA m
            JOIN GOLD.PRODUCT_SENTIMENT_SUMMARY s ON m.ASIN = s.ASIN
            WHERE LOWER(m.BRAND) LIKE %s
            ORDER BY s.REVIEW_COUNT DESC
            LIMIT 5
        """, (f"%{brand.lower()}%",))
        result["top_products"] = [
            {"asin": r[0], "title": r[1][:100] if r[1] else None,
             "review_count": r[2], "avg_rating": float(r[3]), "avg_sentiment": float(r[4])}
            for r in cur.fetchall()
        ]

        # Top complaint themes
        cur.execute("""
            SELECT r.REVIEW_THEME, COUNT(*) AS cnt,
                   ROUND(AVG(r.SENTIMENT_SCORE), 3) AS avg_sent
            FROM GOLD.ENRICHED_REVIEWS r
            JOIN CURATED.PRODUCT_METADATA m ON r.ASIN = m.ASIN
            WHERE LOWER(m.BRAND) LIKE %s AND r.RATING <= 2
            GROUP BY r.REVIEW_THEME
            ORDER BY cnt DESC
            LIMIT 5
        """, (f"%{brand.lower()}%",))
        result["top_complaints"] = [
            {"theme": r[0], "count": r[1], "avg_sentiment": float(r[2])}
            for r in cur.fetchall()
        ]

        return result


def compare_brands(brands: list[str]) -> dict:
    """Compare 2-4 brands side by side.

    Args:
        brands: List of brand names to compare

    Returns:
        dict with brand profiles, comparison metrics, and winner per metric
    """
    if len(brands) < 2:
        return {"error": "Need at least 2 brands to compare", "brands": []}
    if len(brands) > 4:
        brands = brands[:4]

    profiles = []
    for brand in brands:
        analysis = get_brand_analysis(brand)
        if analysis:
            profiles.append(analysis)

    if len(profiles) < 2:
        return {"error": f"Only {len(profiles)} brands found. Need at least 2.", "brands": profiles}

    # Compare key metrics
    comparison = {}
    metrics = [
        ("avg_rating", "higher is better", True),
        ("avg_sentiment", "higher is better", True),
        ("negative_rate", "lower is better", False),
        ("total_reviews", "more data", True),
        ("total_products", "more products", True),
    ]

    for metric, desc, higher_better in metrics:
        values = [{"brand": p["brand"], "value": p.get(metric)} for p in profiles if p.get(metric) is not None]
        if len(values) >= 2:
            sorted_v = sorted(values, key=lambda x: x["value"], reverse=higher_better)
            comparison[metric] = {
                "description": desc,
                "best": sorted_v[0],
                "worst": sorted_v[-1],
                "values": {v["brand"]: v["value"] for v in values},
            }

    return {
        "brands": profiles,
        "comparison": comparison,
        "brand_count": len(profiles),
    }


# ============================================
# TOOL 7: find_similar_products
# ============================================

def find_similar_products(asin: str, limit: int = 5) -> dict:
    """Find similar products using also_buy metadata cross-references.

    Args:
        asin: Source product ASIN
        limit: Max similar products to return

    Returns:
        dict with source product info and list of similar products with stats
    """
    with get_cursor() as cur:
        # Get also_buy list from metadata
        cur.execute("""
            SELECT ALSO_BUY
            FROM CURATED.PRODUCT_METADATA
            WHERE ASIN = %s
        """, (asin,))
        row = cur.fetchone()

        if not row or not row[0]:
            return {
                "source_asin": asin,
                "similar_products": [],
                "note": "No also_buy data available for this product.",
            }

        also_buy = row[0]  # VARIANT array
        if isinstance(also_buy, str):
            also_buy = json.loads(also_buy)

        if not also_buy or not isinstance(also_buy, list):
            return {"source_asin": asin, "similar_products": [], "note": "No also_buy data."}

        # Source category — filter same-category matches first so we don't surface
        # cross-category co-purchases (e.g. Lightning adapter for a headphones query).
        source = get_product_detail(asin)
        source_category = source.get("category") if source else None

        params = also_buy[:20]
        placeholders = ", ".join(["%s"] * len(params))

        def _row_to_dict(r, related=False):
            return {
                "asin": r[0],
                "product_name": clean_product_name(r[1]),
                "full_title": r[1],
                "brand": r[2],
                "price": float(r[3]) if r[3] else None,
                "category": r[4],
                "review_count": r[5],
                "avg_rating": float(r[6]) if r[6] else None,
                "avg_sentiment": float(r[7]) if r[7] else None,
                "related_category": related,
            }

        similar: list[dict] = []

        # Pass 1: same-category matches
        if source_category:
            cur.execute(f"""
                SELECT
                    p.ASIN,
                    COALESCE(p.METADATA_TITLE, p.PRODUCT_NAME) AS PRODUCT_NAME,
                    COALESCE(p.METADATA_BRAND, p.BRAND) AS BRAND,
                    m.PRICE,
                    p.DERIVED_CATEGORY,
                    s.REVIEW_COUNT,
                    s.AVG_RATING,
                    s.AVG_SENTIMENT
                FROM GOLD.PRODUCT_LOOKUP p
                LEFT JOIN CURATED.PRODUCT_METADATA m ON p.ASIN = m.ASIN
                LEFT JOIN GOLD.PRODUCT_SENTIMENT_SUMMARY s ON p.ASIN = s.ASIN
                WHERE p.ASIN IN ({placeholders})
                  AND p.DERIVED_CATEGORY = %s
                ORDER BY s.REVIEW_COUNT DESC NULLS LAST
                LIMIT {limit}
            """, params + [source_category])
            similar = [_row_to_dict(r, related=False) for r in cur.fetchall()]

        # Pass 2: top up with cross-category co-purchases if we have empty slots.
        # These get flagged related_category=True so the agent can label them.
        if len(similar) < limit:
            remaining = limit - len(similar)
            exclude_asins = [s["asin"] for s in similar]
            exclude_placeholders = ", ".join(["%s"] * len(exclude_asins)) if exclude_asins else "''"
            where_category = "p.DERIVED_CATEGORY != %s" if source_category else "1=1"
            cat_params = [source_category] if source_category else []

            cur.execute(f"""
                SELECT
                    p.ASIN,
                    COALESCE(p.METADATA_TITLE, p.PRODUCT_NAME) AS PRODUCT_NAME,
                    COALESCE(p.METADATA_BRAND, p.BRAND) AS BRAND,
                    m.PRICE,
                    p.DERIVED_CATEGORY,
                    s.REVIEW_COUNT,
                    s.AVG_RATING,
                    s.AVG_SENTIMENT
                FROM GOLD.PRODUCT_LOOKUP p
                LEFT JOIN CURATED.PRODUCT_METADATA m ON p.ASIN = m.ASIN
                LEFT JOIN GOLD.PRODUCT_SENTIMENT_SUMMARY s ON p.ASIN = s.ASIN
                WHERE p.ASIN IN ({placeholders})
                  AND {where_category}
                  AND p.ASIN NOT IN ({exclude_placeholders})
                ORDER BY s.REVIEW_COUNT DESC NULLS LAST
                LIMIT {remaining}
            """, params + cat_params + (exclude_asins if exclude_asins else ['']))
            similar.extend(_row_to_dict(r, related=True) for r in cur.fetchall())

        return {
            "source": {
                "asin": asin,
                "name": clean_product_name(source.get("product_name")) if source else None,
                "brand": source.get("brand") if source else None,
                "category": source_category,
            },
            "similar_products": similar,
            "total_also_buy": len(also_buy),
            "matched_in_dataset": len(similar),
            "same_category_count": sum(1 for s in similar if not s["related_category"]),
            "related_category_count": sum(1 for s in similar if s["related_category"]),
        }


# ============================================
# TOOL 8: price_value_analysis
# ============================================

def price_value_analysis(category: str) -> dict:
    """Analyze price vs quality within a category.

    Correlates price brackets with review ratings and sentiment.

    Args:
        category: Derived category name

    Returns:
        dict with price brackets, each showing avg rating/sentiment/negative rate
    """
    with get_cursor() as cur:
        cur.execute("""
            SELECT
                CASE
                    WHEN m.PRICE < 15 THEN 'budget (under $15)'
                    WHEN m.PRICE < 30 THEN 'mid-range ($15-$30)'
                    WHEN m.PRICE < 60 THEN 'premium ($30-$60)'
                    WHEN m.PRICE >= 60 THEN 'high-end ($60+)'
                END AS price_bracket,
                COUNT(DISTINCT m.ASIN) AS product_count,
                COUNT(r.REVIEW_ID) AS review_count,
                ROUND(AVG(r.RATING), 2) AS avg_rating,
                ROUND(AVG(r.SENTIMENT_SCORE), 4) AS avg_sentiment,
                ROUND(COUNT(CASE WHEN r.RATING <= 2 THEN 1 END)::FLOAT /
                    NULLIF(COUNT(r.REVIEW_ID), 0), 4) AS negative_rate,
                ROUND(MIN(m.PRICE), 2) AS min_price,
                ROUND(MAX(m.PRICE), 2) AS max_price,
                ROUND(AVG(m.PRICE), 2) AS avg_price
            FROM CURATED.PRODUCT_METADATA m
            JOIN GOLD.PRODUCT_LOOKUP p ON m.ASIN = p.ASIN
            JOIN GOLD.ENRICHED_REVIEWS r ON m.ASIN = r.ASIN
            WHERE p.DERIVED_CATEGORY = %s
              AND m.PRICE IS NOT NULL
              AND m.PRICE > 0
            GROUP BY price_bracket
            ORDER BY avg_price
        """, (category,))

        brackets = [
            {
                "bracket": r[0],
                "product_count": r[1],
                "review_count": r[2],
                "avg_rating": float(r[3]) if r[3] else None,
                "avg_sentiment": float(r[4]) if r[4] else None,
                "negative_rate": float(r[5]) if r[5] else None,
                "price_range": {"min": float(r[6]), "max": float(r[7]), "avg": float(r[8])},
            }
            for r in cur.fetchall()
        ]

        # Best value = highest rating at lowest price bracket
        best_value = None
        if brackets:
            best_value = max(brackets, key=lambda b: (b["avg_rating"] or 0))

        return {
            "category": category,
            "price_brackets": brackets,
            "best_value_bracket": best_value["bracket"] if best_value else None,
            "total_products_with_price": sum(b["product_count"] for b in brackets),
            "note": "Only includes products with known prices (38.7% of products have price data).",
        }
