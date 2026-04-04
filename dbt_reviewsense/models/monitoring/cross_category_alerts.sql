-- Cross-category pattern detection
-- Fires when a complaint theme spikes across 3+ categories simultaneously
-- Indicates a platform-wide trend (e.g., "connectivity issues everywhere")

{{ config(materialized='table') }}

SELECT
    REVIEW_THEME,
    COUNT(DISTINCT DERIVED_CATEGORY) AS AFFECTED_CATEGORIES,
    ARRAY_AGG(DISTINCT DERIVED_CATEGORY) AS CATEGORY_LIST,
    ROUND(AVG(DEVIATION_SCORE), 2) AS AVG_DEVIATION,
    MAX(SEVERITY) AS MAX_SEVERITY,
    SUM(AFFECTED_REVIEWS) AS TOTAL_AFFECTED_REVIEWS,
    CURRENT_TIMESTAMP() AS DETECTED_AT
FROM {{ ref('review_anomalies') }}
WHERE ANOMALY_TYPE = 'COMPLAINT_SPIKE'
  AND REVIEW_THEME IS NOT NULL
GROUP BY REVIEW_THEME
HAVING COUNT(DISTINCT DERIVED_CATEGORY) >= 3
