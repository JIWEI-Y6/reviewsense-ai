-- Data quality monitoring: pipeline health checks
-- Validates freshness, row counts, NULL rates, schema, and enrichment coverage
-- Each row = one check with PASS/WARN/FAIL status

{{ config(materialized='table') }}

-- Source freshness
SELECT
    'SOURCE_FRESHNESS' AS CHECK_NAME,
    'ANALYTICS.REVIEWS_FOR_GENAI' AS TABLE_NAME,
    DATEDIFF('day', MAX(REVIEW_TS), CURRENT_TIMESTAMP()) AS CURRENT_VALUE,
    30 AS EXPECTED_VALUE,
    CASE
        WHEN MAX(REVIEW_TS) > CURRENT_TIMESTAMP() THEN 'WARN'
        WHEN DATEDIFF('day', MAX(REVIEW_TS), CURRENT_TIMESTAMP()) > 90 THEN 'FAIL'
        WHEN DATEDIFF('day', MAX(REVIEW_TS), CURRENT_TIMESTAMP()) > 30 THEN 'WARN'
        ELSE 'PASS'
    END AS STATUS,
    'Days since latest review (negative = future timestamps in data)' AS DESCRIPTION,
    CURRENT_TIMESTAMP() AS CHECKED_AT
FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI
WHERE YEAR(REVIEW_TS) <= 2026

UNION ALL

-- Source row count
SELECT
    'SOURCE_ROW_COUNT',
    'ANALYTICS.REVIEWS_FOR_GENAI',
    COUNT(*),
    183457,
    CASE
        WHEN COUNT(*) < 180000 OR COUNT(*) > 200000 THEN 'WARN'
        ELSE 'PASS'
    END,
    'Expected ~183K rows',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI

UNION ALL

-- Enrichment row count vs source
SELECT
    'ENRICHMENT_ROW_COUNT',
    'SILVER.INT_ENRICHED_REVIEWS',
    e.cnt,
    s.cnt,
    CASE
        WHEN ABS(e.cnt - s.cnt) * 100.0 / NULLIF(s.cnt, 0) > 1 THEN 'FAIL'
        ELSE 'PASS'
    END,
    'Enriched vs source row count (should match within 1%)',
    CURRENT_TIMESTAMP()
FROM (SELECT COUNT(*) AS cnt FROM REVIEWSENSE_DB.SILVER.INT_ENRICHED_REVIEWS) e,
     (SELECT COUNT(*) AS cnt FROM REVIEWSENSE_DB.SILVER.STG_REVIEWS) s

UNION ALL

-- Gold row count vs enrichment
SELECT
    'GOLD_ROW_COUNT',
    'GOLD.ENRICHED_REVIEWS',
    g.cnt,
    e.cnt,
    CASE
        WHEN ABS(g.cnt - e.cnt) * 100.0 / NULLIF(e.cnt, 0) > 1 THEN 'FAIL'
        ELSE 'PASS'
    END,
    'Gold vs enriched row count (should match within 1%)',
    CURRENT_TIMESTAMP()
FROM (SELECT COUNT(*) AS cnt FROM REVIEWSENSE_DB.GOLD.ENRICHED_REVIEWS) g,
     (SELECT COUNT(*) AS cnt FROM REVIEWSENSE_DB.SILVER.INT_ENRICHED_REVIEWS) e

UNION ALL

-- Sentiment NULL rate
SELECT
    'SENTIMENT_NULL_RATE',
    'GOLD.ENRICHED_REVIEWS',
    SUM(CASE WHEN SENTIMENT_SCORE IS NULL THEN 1 ELSE 0 END),
    0,
    CASE
        WHEN SUM(CASE WHEN SENTIMENT_SCORE IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
        ELSE 'PASS'
    END,
    'NULL sentiment scores (should be 0)',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.GOLD.ENRICHED_REVIEWS

UNION ALL

-- Theme NULL rate
SELECT
    'THEME_NULL_RATE',
    'GOLD.ENRICHED_REVIEWS',
    SUM(CASE WHEN REVIEW_THEME IS NULL THEN 1 ELSE 0 END),
    0,
    CASE
        WHEN SUM(CASE WHEN REVIEW_THEME IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
        ELSE 'PASS'
    END,
    'NULL review themes (should be 0)',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.GOLD.ENRICHED_REVIEWS

UNION ALL

-- Category coverage
SELECT
    'CATEGORY_COVERAGE',
    'GOLD.ENRICHED_REVIEWS',
    ROUND(SUM(CASE WHEN DERIVED_CATEGORY IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    60,
    CASE
        WHEN SUM(CASE WHEN DERIVED_CATEGORY IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) < 60 THEN 'WARN'
        ELSE 'PASS'
    END,
    'Percentage of reviews with derived category (expect >60%)',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.GOLD.ENRICHED_REVIEWS

UNION ALL

-- Category count check
SELECT
    'CATEGORY_COUNT',
    'GOLD.CATEGORY_SENTIMENT_SUMMARY',
    COUNT(*),
    14,
    CASE
        WHEN COUNT(*) != 14 THEN 'WARN'
        ELSE 'PASS'
    END,
    'Number of categories (expect exactly 14)',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.GOLD.CATEGORY_SENTIMENT_SUMMARY

UNION ALL

-- Schema column count for enriched_reviews
SELECT
    'SCHEMA_ENRICHED_REVIEWS',
    'GOLD.ENRICHED_REVIEWS',
    COUNT(*),
    19,
    CASE
        WHEN COUNT(*) != 19 THEN 'FAIL'
        ELSE 'PASS'
    END,
    'Column count for enriched_reviews (expect 17)',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = 'ENRICHED_REVIEWS'
