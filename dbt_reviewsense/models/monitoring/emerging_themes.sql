-- Emerging theme detection: identifies themes growing disproportionately
-- Compares recent 3-month theme share vs historical 12-month share
-- Flags themes where recent share is 2x+ the historical share

{{ config(materialized='table') }}

WITH current_month AS (
    SELECT MAX(REVIEW_MONTH) AS latest_month
    FROM {{ ref('category_monthly_trends') }}
),

-- Recent period: last 3 months
recent_themes AS (
    SELECT
        e.DERIVED_CATEGORY,
        e.REVIEW_THEME,
        COUNT(*) AS recent_count
    FROM {{ ref('enriched_reviews') }} e
    CROSS JOIN current_month cm
    WHERE e.DERIVED_CATEGORY IS NOT NULL
      AND e.REVIEW_THEME IS NOT NULL
      AND DATE_TRUNC('MONTH', e.REVIEW_TS) >= DATEADD('MONTH', -3, cm.latest_month)
    GROUP BY e.DERIVED_CATEGORY, e.REVIEW_THEME
),

recent_totals AS (
    SELECT
        DERIVED_CATEGORY,
        SUM(recent_count) AS total_recent
    FROM recent_themes
    GROUP BY DERIVED_CATEGORY
),

-- Historical period: 12 months before the recent 3
historical_themes AS (
    SELECT
        e.DERIVED_CATEGORY,
        e.REVIEW_THEME,
        COUNT(*) AS historical_count
    FROM {{ ref('enriched_reviews') }} e
    CROSS JOIN current_month cm
    WHERE e.DERIVED_CATEGORY IS NOT NULL
      AND e.REVIEW_THEME IS NOT NULL
      AND DATE_TRUNC('MONTH', e.REVIEW_TS) >= DATEADD('MONTH', -15, cm.latest_month)
      AND DATE_TRUNC('MONTH', e.REVIEW_TS) < DATEADD('MONTH', -3, cm.latest_month)
    GROUP BY e.DERIVED_CATEGORY, e.REVIEW_THEME
),

historical_totals AS (
    SELECT
        DERIVED_CATEGORY,
        SUM(historical_count) AS total_historical
    FROM historical_themes
    GROUP BY DERIVED_CATEGORY
),

-- Compare shares
theme_shifts AS (
    SELECT
        r.DERIVED_CATEGORY,
        r.REVIEW_THEME,
        r.recent_count,
        COALESCE(h.historical_count, 0) AS historical_count,
        rt.total_recent,
        COALESCE(ht.total_historical, 1) AS total_historical,
        ROUND(r.recent_count * 100.0 / NULLIF(rt.total_recent, 0), 2) AS RECENT_SHARE,
        ROUND(COALESCE(h.historical_count, 0) * 100.0 / NULLIF(ht.total_historical, 0), 2) AS HISTORICAL_SHARE,
        ROUND(
            (r.recent_count * 100.0 / NULLIF(rt.total_recent, 0)) /
            NULLIF(COALESCE(h.historical_count, 0) * 100.0 / NULLIF(ht.total_historical, 0), 0),
        2) AS GROWTH_FACTOR
    FROM recent_themes r
    JOIN recent_totals rt ON r.DERIVED_CATEGORY = rt.DERIVED_CATEGORY
    LEFT JOIN historical_themes h ON r.DERIVED_CATEGORY = h.DERIVED_CATEGORY
        AND r.REVIEW_THEME = h.REVIEW_THEME
    LEFT JOIN historical_totals ht ON r.DERIVED_CATEGORY = ht.DERIVED_CATEGORY
    WHERE r.recent_count >= 5
)

SELECT
    DERIVED_CATEGORY,
    REVIEW_THEME,
    RECENT_SHARE,
    HISTORICAL_SHARE,
    GROWTH_FACTOR,
    recent_count AS RECENT_REVIEW_COUNT,
    historical_count AS HISTORICAL_REVIEW_COUNT,
    CASE
        WHEN GROWTH_FACTOR >= 3 THEN 'HIGH'
        WHEN GROWTH_FACTOR >= 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS SEVERITY,
    CURRENT_TIMESTAMP() AS DETECTED_AT
FROM theme_shifts
WHERE GROWTH_FACTOR >= 1.5
  AND RECENT_SHARE >= 5
ORDER BY GROWTH_FACTOR DESC
