-- Anomaly detection: compares current period metrics against rolling baselines
-- Uses z-scores (STDDEV-based) to adapt to each category's natural variance
-- 5 anomaly types: RATING_DROP, SENTIMENT_SHIFT, COMPLAINT_SPIKE, DECLINING_TREND, RANK_DROP
-- Reads from gold marts (no LLM calls, pure SQL)

{{ config(materialized='table') }}

WITH current_month AS (
    SELECT MAX(REVIEW_MONTH) AS latest_month
    FROM {{ ref('category_monthly_trends') }}
),

-- Baseline: 3-month rolling average BEFORE the current month
rating_baseline AS (
    SELECT
        t.DERIVED_CATEGORY,
        AVG(t.AVG_RATING) AS baseline_avg_rating,
        STDDEV(t.AVG_RATING) AS stddev_rating,
        AVG(t.AVG_SENTIMENT) AS baseline_avg_sentiment,
        STDDEV(t.AVG_SENTIMENT) AS stddev_sentiment,
        AVG(t.NEGATIVE_RATE) AS baseline_negative_rate,
        STDDEV(t.NEGATIVE_RATE) AS stddev_negative_rate
    FROM {{ ref('category_monthly_trends') }} t
    CROSS JOIN current_month cm
    WHERE t.REVIEW_MONTH >= DATEADD('MONTH', -4, cm.latest_month)
      AND t.REVIEW_MONTH < cm.latest_month
    GROUP BY t.DERIVED_CATEGORY
),

-- Detect rating drops
rating_drops AS (
    SELECT
        'RATING_DROP' AS ANOMALY_TYPE,
        t.DERIVED_CATEGORY,
        NULL AS REVIEW_THEME,
        t.REVIEW_MONTH AS DETECTION_PERIOD,
        t.AVG_RATING AS CURRENT_VALUE,
        b.baseline_avg_rating AS BASELINE_VALUE,
        ROUND((b.baseline_avg_rating - t.AVG_RATING) / NULLIF(GREATEST(b.stddev_rating, 0.1), 0), 2) AS DEVIATION_SCORE,
        t.REVIEW_COUNT AS AFFECTED_REVIEWS,
        t.NEGATIVE_RATE AS CURRENT_NEGATIVE_RATE,
        b.baseline_negative_rate AS BASELINE_NEGATIVE_RATE,
        CASE
            WHEN t.AVG_RATING < b.baseline_avg_rating - 2 * GREATEST(b.stddev_rating, 0.1) THEN 'HIGH'
            WHEN t.AVG_RATING < b.baseline_avg_rating - 1 * GREATEST(b.stddev_rating, 0.1) THEN 'MEDIUM'
            ELSE 'LOW'
        END AS SEVERITY,
        CURRENT_TIMESTAMP() AS DETECTED_AT
    FROM {{ ref('category_monthly_trends') }} t
    CROSS JOIN current_month cm
    JOIN rating_baseline b ON t.DERIVED_CATEGORY = b.DERIVED_CATEGORY
    WHERE t.REVIEW_MONTH = cm.latest_month
      AND t.AVG_RATING < b.baseline_avg_rating - 0.5 * GREATEST(b.stddev_rating, 0.1)
      AND t.REVIEW_COUNT >= 10
),

-- Detect sentiment shifts
sentiment_shifts AS (
    SELECT
        'SENTIMENT_SHIFT' AS ANOMALY_TYPE,
        t.DERIVED_CATEGORY,
        NULL AS REVIEW_THEME,
        t.REVIEW_MONTH AS DETECTION_PERIOD,
        t.AVG_SENTIMENT AS CURRENT_VALUE,
        b.baseline_avg_sentiment AS BASELINE_VALUE,
        ROUND((b.baseline_avg_sentiment - t.AVG_SENTIMENT) / NULLIF(GREATEST(b.stddev_sentiment, 0.05), 0), 2) AS DEVIATION_SCORE,
        t.REVIEW_COUNT AS AFFECTED_REVIEWS,
        t.NEGATIVE_RATE AS CURRENT_NEGATIVE_RATE,
        b.baseline_negative_rate AS BASELINE_NEGATIVE_RATE,
        CASE
            WHEN t.AVG_SENTIMENT < b.baseline_avg_sentiment - 2 * GREATEST(b.stddev_sentiment, 0.05) THEN 'HIGH'
            WHEN t.AVG_SENTIMENT < b.baseline_avg_sentiment - 1 * GREATEST(b.stddev_sentiment, 0.05) THEN 'MEDIUM'
            ELSE 'LOW'
        END AS SEVERITY,
        CURRENT_TIMESTAMP() AS DETECTED_AT
    FROM {{ ref('category_monthly_trends') }} t
    CROSS JOIN current_month cm
    JOIN rating_baseline b ON t.DERIVED_CATEGORY = b.DERIVED_CATEGORY
    WHERE t.REVIEW_MONTH = cm.latest_month
      AND t.AVG_SENTIMENT < b.baseline_avg_sentiment - 0.5 * GREATEST(b.stddev_sentiment, 0.05)
      AND t.REVIEW_COUNT >= 10
),

-- Detect complaint spikes (cross-category comparison per theme)
theme_baselines AS (
    SELECT
        REVIEW_THEME,
        AVG(COMPLAINT_COUNT) AS avg_complaints,
        STDDEV(COMPLAINT_COUNT) AS stddev_complaints
    FROM {{ ref('complaint_analysis') }}
    GROUP BY REVIEW_THEME
),

complaint_spikes AS (
    SELECT
        'COMPLAINT_SPIKE' AS ANOMALY_TYPE,
        c.DERIVED_CATEGORY,
        c.REVIEW_THEME,
        NULL AS DETECTION_PERIOD,
        c.COMPLAINT_COUNT AS CURRENT_VALUE,
        b.avg_complaints AS BASELINE_VALUE,
        ROUND((c.COMPLAINT_COUNT - b.avg_complaints) / NULLIF(GREATEST(b.stddev_complaints, 3), 0), 2) AS DEVIATION_SCORE,
        c.COMPLAINT_COUNT AS AFFECTED_REVIEWS,
        NULL AS CURRENT_NEGATIVE_RATE,
        NULL AS BASELINE_NEGATIVE_RATE,
        CASE
            WHEN c.COMPLAINT_COUNT > b.avg_complaints + 2 * GREATEST(b.stddev_complaints, 3) THEN 'HIGH'
            WHEN c.COMPLAINT_COUNT > b.avg_complaints + 1 * GREATEST(b.stddev_complaints, 3) THEN 'MEDIUM'
            ELSE 'LOW'
        END AS SEVERITY,
        CURRENT_TIMESTAMP() AS DETECTED_AT
    FROM {{ ref('complaint_analysis') }} c
    JOIN theme_baselines b ON c.REVIEW_THEME = b.REVIEW_THEME
    WHERE c.COMPLAINT_COUNT > b.avg_complaints + 0.5 * GREATEST(b.stddev_complaints, 3)
),

-- Detect declining trends: 3+ consecutive months of declining rating or sentiment
monthly_changes AS (
    SELECT
        t.DERIVED_CATEGORY,
        t.REVIEW_MONTH,
        t.AVG_RATING,
        t.AVG_SENTIMENT,
        t.REVIEW_COUNT,
        t.NEGATIVE_RATE,
        LAG(t.AVG_RATING) OVER (PARTITION BY t.DERIVED_CATEGORY ORDER BY t.REVIEW_MONTH) AS prev_rating,
        LAG(t.AVG_SENTIMENT) OVER (PARTITION BY t.DERIVED_CATEGORY ORDER BY t.REVIEW_MONTH) AS prev_sentiment
    FROM {{ ref('category_monthly_trends') }} t
    WHERE t.REVIEW_COUNT >= 10
),

consecutive_declines AS (
    SELECT
        DERIVED_CATEGORY,
        REVIEW_MONTH,
        AVG_RATING,
        AVG_SENTIMENT,
        REVIEW_COUNT,
        NEGATIVE_RATE,
        CASE WHEN AVG_RATING < prev_rating THEN 1 ELSE 0 END AS rating_declined,
        SUM(CASE WHEN AVG_RATING < prev_rating THEN 0 ELSE 1 END)
            OVER (PARTITION BY DERIVED_CATEGORY ORDER BY REVIEW_MONTH) AS rating_reset_group
    FROM monthly_changes
    WHERE prev_rating IS NOT NULL
),

declining_streaks AS (
    SELECT
        DERIVED_CATEGORY,
        MAX(REVIEW_MONTH) AS latest_month,
        MIN(AVG_RATING) AS lowest_rating,
        COUNT(*) AS consecutive_months
    FROM consecutive_declines
    WHERE rating_declined = 1
    GROUP BY DERIVED_CATEGORY, rating_reset_group
    HAVING COUNT(*) >= 3
),

declining_trends AS (
    SELECT
        'DECLINING_TREND' AS ANOMALY_TYPE,
        d.DERIVED_CATEGORY,
        NULL AS REVIEW_THEME,
        d.latest_month AS DETECTION_PERIOD,
        d.lowest_rating AS CURRENT_VALUE,
        b.baseline_avg_rating AS BASELINE_VALUE,
        d.consecutive_months AS DEVIATION_SCORE,
        NULL AS AFFECTED_REVIEWS,
        NULL AS CURRENT_NEGATIVE_RATE,
        NULL AS BASELINE_NEGATIVE_RATE,
        CASE
            WHEN d.consecutive_months >= 5 THEN 'HIGH'
            WHEN d.consecutive_months >= 4 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS SEVERITY,
        CURRENT_TIMESTAMP() AS DETECTED_AT
    FROM declining_streaks d
    JOIN rating_baseline b ON d.DERIVED_CATEGORY = b.DERIVED_CATEGORY
),

-- Detect rank drops: category dropped 3+ positions in rating rankings
current_ranks AS (
    SELECT
        t.DERIVED_CATEGORY,
        t.AVG_RATING,
        RANK() OVER (ORDER BY t.AVG_RATING DESC) AS current_rank
    FROM {{ ref('category_monthly_trends') }} t
    CROSS JOIN current_month cm
    WHERE t.REVIEW_MONTH = cm.latest_month
      AND t.REVIEW_COUNT >= 10
),

baseline_ranks AS (
    SELECT
        t.DERIVED_CATEGORY,
        AVG(t.AVG_RATING) AS avg_rating,
        RANK() OVER (ORDER BY AVG(t.AVG_RATING) DESC) AS baseline_rank
    FROM {{ ref('category_monthly_trends') }} t
    CROSS JOIN current_month cm
    WHERE t.REVIEW_MONTH >= DATEADD('MONTH', -4, cm.latest_month)
      AND t.REVIEW_MONTH < cm.latest_month
      AND t.REVIEW_COUNT >= 10
    GROUP BY t.DERIVED_CATEGORY
),

rank_drops AS (
    SELECT
        'RANK_DROP' AS ANOMALY_TYPE,
        c.DERIVED_CATEGORY,
        NULL AS REVIEW_THEME,
        cm.latest_month AS DETECTION_PERIOD,
        c.current_rank AS CURRENT_VALUE,
        b.baseline_rank AS BASELINE_VALUE,
        (c.current_rank - b.baseline_rank) AS DEVIATION_SCORE,
        NULL AS AFFECTED_REVIEWS,
        NULL AS CURRENT_NEGATIVE_RATE,
        NULL AS BASELINE_NEGATIVE_RATE,
        CASE
            WHEN c.current_rank - b.baseline_rank >= 5 THEN 'HIGH'
            WHEN c.current_rank - b.baseline_rank >= 3 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS SEVERITY,
        CURRENT_TIMESTAMP() AS DETECTED_AT
    FROM current_ranks c
    JOIN baseline_ranks b ON c.DERIVED_CATEGORY = b.DERIVED_CATEGORY
    CROSS JOIN current_month cm
    WHERE c.current_rank - b.baseline_rank >= 3
)

SELECT * FROM rating_drops
UNION ALL
SELECT * FROM sentiment_shifts
UNION ALL
SELECT * FROM complaint_spikes
UNION ALL
SELECT * FROM declining_trends
UNION ALL
SELECT * FROM rank_drops
