-- Product-level anomaly detection for top ASINs (100+ reviews)
-- Detects products hitting their worst month or significant rating drops
-- Joins with product_lookup for product name context

{{ config(materialized='table') }}

WITH current_month AS (
    SELECT MAX(REVIEW_MONTH) AS latest_month
    FROM (
        SELECT DATE_TRUNC('MONTH', REVIEW_TS) AS REVIEW_MONTH
        FROM {{ ref('enriched_reviews') }}
    )
),

-- Only top ASINs with 100+ reviews
top_asins AS (
    SELECT ASIN
    FROM {{ ref('enriched_reviews') }}
    GROUP BY ASIN
    HAVING COUNT(*) >= 100
),

-- Monthly stats per product
product_monthly AS (
    SELECT
        e.ASIN,
        DATE_TRUNC('MONTH', e.REVIEW_TS) AS REVIEW_MONTH,
        COUNT(*) AS REVIEW_COUNT,
        ROUND(AVG(e.RATING), 2) AS AVG_RATING,
        ROUND(AVG(e.SENTIMENT_SCORE), 4) AS AVG_SENTIMENT
    FROM {{ ref('enriched_reviews') }} e
    INNER JOIN top_asins a ON e.ASIN = a.ASIN
    GROUP BY e.ASIN, DATE_TRUNC('MONTH', e.REVIEW_TS)
    HAVING COUNT(*) >= 5
),

-- Historical baseline per product (3-month rolling)
product_baseline AS (
    SELECT
        pm.ASIN,
        AVG(pm.AVG_RATING) AS baseline_rating,
        STDDEV(pm.AVG_RATING) AS stddev_rating,
        AVG(pm.AVG_SENTIMENT) AS baseline_sentiment,
        MIN(pm.AVG_RATING) AS historical_min_rating
    FROM product_monthly pm
    CROSS JOIN current_month cm
    WHERE pm.REVIEW_MONTH >= DATEADD('MONTH', -4, cm.latest_month)
      AND pm.REVIEW_MONTH < cm.latest_month
    GROUP BY pm.ASIN
),

-- Detect product rating drops
product_rating_drops AS (
    SELECT
        'PRODUCT_RATING_DROP' AS ANOMALY_TYPE,
        pm.ASIN,
        p.PRODUCT_NAME,
        p.BRAND,
        p.DERIVED_CATEGORY,
        pm.REVIEW_MONTH AS DETECTION_PERIOD,
        pm.AVG_RATING AS CURRENT_VALUE,
        b.baseline_rating AS BASELINE_VALUE,
        ROUND((b.baseline_rating - pm.AVG_RATING) / NULLIF(GREATEST(b.stddev_rating, 0.1), 0), 2) AS DEVIATION_SCORE,
        pm.REVIEW_COUNT AS AFFECTED_REVIEWS,
        CASE
            WHEN pm.AVG_RATING < b.baseline_rating - 2 * GREATEST(b.stddev_rating, 0.2) THEN 'HIGH'
            WHEN pm.AVG_RATING < b.baseline_rating - 1 * GREATEST(b.stddev_rating, 0.2) THEN 'MEDIUM'
            ELSE 'LOW'
        END AS SEVERITY,
        CURRENT_TIMESTAMP() AS DETECTED_AT
    FROM product_monthly pm
    CROSS JOIN current_month cm
    JOIN product_baseline b ON pm.ASIN = b.ASIN
    LEFT JOIN {{ ref('product_lookup') }} p ON pm.ASIN = p.ASIN
    WHERE pm.REVIEW_MONTH = cm.latest_month
      AND pm.AVG_RATING < b.baseline_rating - 0.3
),

-- Detect products hitting their worst month ever
all_time_min AS (
    SELECT
        ASIN,
        MIN(AVG_RATING) AS all_time_min_rating
    FROM product_monthly
    GROUP BY ASIN
),

worst_month_ever AS (
    SELECT
        'PRODUCT_WORST_MONTH' AS ANOMALY_TYPE,
        pm.ASIN,
        p.PRODUCT_NAME,
        p.BRAND,
        p.DERIVED_CATEGORY,
        pm.REVIEW_MONTH AS DETECTION_PERIOD,
        pm.AVG_RATING AS CURRENT_VALUE,
        b.baseline_rating AS BASELINE_VALUE,
        ROUND(b.baseline_rating - pm.AVG_RATING, 2) AS DEVIATION_SCORE,
        pm.REVIEW_COUNT AS AFFECTED_REVIEWS,
        'HIGH' AS SEVERITY,
        CURRENT_TIMESTAMP() AS DETECTED_AT
    FROM product_monthly pm
    CROSS JOIN current_month cm
    JOIN all_time_min atm ON pm.ASIN = atm.ASIN
    JOIN product_baseline b ON pm.ASIN = b.ASIN
    LEFT JOIN {{ ref('product_lookup') }} p ON pm.ASIN = p.ASIN
    WHERE pm.REVIEW_MONTH = cm.latest_month
      AND pm.AVG_RATING <= atm.all_time_min_rating
      AND pm.AVG_RATING < b.baseline_rating - 0.1
)

SELECT
    ANOMALY_TYPE, ASIN, PRODUCT_NAME, BRAND, DERIVED_CATEGORY,
    DETECTION_PERIOD, CURRENT_VALUE, BASELINE_VALUE, DEVIATION_SCORE,
    AFFECTED_REVIEWS, SEVERITY, DETECTED_AT
FROM product_rating_drops
UNION ALL
SELECT
    ANOMALY_TYPE, ASIN, PRODUCT_NAME, BRAND, DERIVED_CATEGORY,
    DETECTION_PERIOD, CURRENT_VALUE, BASELINE_VALUE, DEVIATION_SCORE,
    AFFECTED_REVIEWS, SEVERITY, DETECTED_AT
FROM worst_month_ever
