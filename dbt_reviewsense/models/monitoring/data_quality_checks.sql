-- Data quality monitoring: pipeline health checks
-- Uses DYNAMIC baselines (not hardcoded row counts)
-- Each row = one check with PASS/WARN/FAIL status

{{ config(materialized='table') }}

-- Source freshness (filter bad timestamps)
SELECT
    'SOURCE_FRESHNESS' AS CHECK_NAME,
    'RAW.REVIEWS_RAW_V2' AS TABLE_NAME,
    DATEDIFF('day', MAX(V:review_ts::TIMESTAMP_NTZ), CURRENT_TIMESTAMP()) AS CURRENT_VALUE,
    30 AS EXPECTED_VALUE,
    CASE
        WHEN MAX(V:review_ts::TIMESTAMP_NTZ) > CURRENT_TIMESTAMP() THEN 'WARN'
        WHEN DATEDIFF('day', MAX(V:review_ts::TIMESTAMP_NTZ), CURRENT_TIMESTAMP()) > 90 THEN 'FAIL'
        WHEN DATEDIFF('day', MAX(V:review_ts::TIMESTAMP_NTZ), CURRENT_TIMESTAMP()) > 30 THEN 'WARN'
        ELSE 'PASS'
    END AS STATUS,
    'Days since latest review' AS DESCRIPTION,
    CURRENT_TIMESTAMP() AS CHECKED_AT
FROM REVIEWSENSE_DB.RAW.REVIEWS_RAW_V2
WHERE YEAR(V:review_ts::TIMESTAMP_NTZ) <= 2026

UNION ALL

-- Source row count: should be > 0 (no hardcoded expected value)
SELECT
    'SOURCE_ROW_COUNT',
    'RAW.REVIEWS_RAW_V2',
    COUNT(*),
    0,
    CASE WHEN COUNT(*) = 0 THEN 'FAIL' ELSE 'PASS' END,
    'Total reviews in V2 source (should be > 0)',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.RAW.REVIEWS_RAW_V2

UNION ALL

-- Enrichment row count vs staging (relative check, not absolute)
SELECT
    'ENRICHMENT_ROW_COUNT',
    'SILVER.INT_ENRICHED_REVIEWS',
    e.cnt,
    s.cnt,
    CASE
        WHEN s.cnt = 0 THEN 'FAIL'
        WHEN ABS(e.cnt - s.cnt) * 100.0 / NULLIF(s.cnt, 0) > 1 THEN 'FAIL'
        ELSE 'PASS'
    END,
    'Enriched vs staging row count (should match within 1%)',
    CURRENT_TIMESTAMP()
FROM (SELECT COUNT(*) AS cnt FROM REVIEWSENSE_DB.SILVER.INT_ENRICHED_REVIEWS) e,
     (SELECT COUNT(*) AS cnt FROM REVIEWSENSE_DB.SILVER.STG_REVIEWS) s

UNION ALL

-- Gold row count vs enrichment (relative check)
SELECT
    'GOLD_ROW_COUNT',
    'GOLD.ENRICHED_REVIEWS',
    g.cnt,
    e.cnt,
    CASE
        WHEN e.cnt = 0 THEN 'FAIL'
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

-- Category coverage (60% threshold — documented: long-tail ASINs with <3 reviews don't get categories)
SELECT
    'CATEGORY_COVERAGE',
    'GOLD.ENRICHED_REVIEWS',
    ROUND(SUM(CASE WHEN DERIVED_CATEGORY IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1),
    60,
    CASE
        WHEN SUM(CASE WHEN DERIVED_CATEGORY IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) < 60 THEN 'WARN'
        ELSE 'PASS'
    END,
    'Pct reviews with derived category (>60%; long-tail ASINs excluded by design)',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.GOLD.ENRICHED_REVIEWS

UNION ALL

-- Category count: should match between enriched_reviews and category_sentiment_summary
SELECT
    'CATEGORY_CONSISTENCY',
    'GOLD.CATEGORY_SENTIMENT_SUMMARY',
    css.cnt,
    er.cnt,
    CASE WHEN css.cnt != er.cnt THEN 'WARN' ELSE 'PASS' END,
    'Category count in summary vs enriched_reviews should match',
    CURRENT_TIMESTAMP()
FROM (SELECT COUNT(*) AS cnt FROM REVIEWSENSE_DB.GOLD.CATEGORY_SENTIMENT_SUMMARY) css,
     (SELECT COUNT(DISTINCT DERIVED_CATEGORY) AS cnt FROM REVIEWSENSE_DB.GOLD.ENRICHED_REVIEWS WHERE DERIVED_CATEGORY IS NOT NULL) er

UNION ALL

-- Per-category source check: each SOURCE_CATEGORY should have reviews
SELECT
    'PER_CATEGORY_HEALTH',
    SOURCE_CATEGORY,
    COUNT(*),
    0,
    CASE WHEN COUNT(*) = 0 THEN 'FAIL' ELSE 'PASS' END,
    'Reviews per source category (should be > 0)',
    CURRENT_TIMESTAMP()
FROM REVIEWSENSE_DB.GOLD.ENRICHED_REVIEWS
WHERE SOURCE_CATEGORY IS NOT NULL
GROUP BY SOURCE_CATEGORY
