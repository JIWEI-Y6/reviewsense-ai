USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

SELECT
    ASIN,
    COUNT(*) AS total_reviews,
    ROUND(AVG(RATING), 2) AS avg_rating
FROM ANALYTICS.REVIEWS_SENTIMENT
GROUP BY ASIN
HAVING COUNT(*) >= 500
ORDER BY total_reviews DESC
LIMIT 20;

SELECT
    ASIN,
    COUNT(*) AS negative_reviews
FROM ANALYTICS.REVIEWS_SENTIMENT
WHERE sentiment_label = 'Negative'
GROUP BY ASIN
HAVING COUNT(*) >= 50
ORDER BY negative_reviews DESC
LIMIT 20;

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Summarize the main customer complaints for this product in 3 short bullet points: ' ||
    LISTAGG(REVIEW_TEXT, ' ') WITHIN GROUP (ORDER BY REVIEW_TS)
) AS complaint_summary
FROM (
    SELECT REVIEW_TEXT, REVIEW_TS
    FROM ANALYTICS.REVIEWS_SENTIMENT
    WHERE ASIN = 'B00ZV9RDKK'
      AND sentiment_label = 'Negative'
    LIMIT 30
);

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Summarize the main product strengths customers appreciate in 3 short bullet points: ' ||
    LISTAGG(REVIEW_TEXT, ' ') WITHIN GROUP (ORDER BY REVIEW_TS)
) AS strength_summary
FROM (
    SELECT REVIEW_TEXT, REVIEW_TS
    FROM ANALYTICS.REVIEWS_SENTIMENT
    WHERE ASIN = 'B00ZV9RDKK'
      AND sentiment_label = 'Positive'
    LIMIT 30
);

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Based on these customer reviews, give 3 short improvement recommendations for the product: ' ||
    LISTAGG(REVIEW_TEXT, ' ') WITHIN GROUP (ORDER BY REVIEW_TS)
) AS improvement_recommendations
FROM (
    SELECT REVIEW_TEXT, REVIEW_TS
    FROM ANALYTICS.REVIEWS_SENTIMENT
    WHERE ASIN = 'B00ZV9RDKK'
    LIMIT 40
);

INSERT INTO ANALYTICS.DEMO_PRODUCT_SUMMARY
WITH product_metrics AS (
    SELECT
        ASIN,
        COUNT(*) AS total_reviews,
        ROUND(AVG(RATING), 2) AS avg_rating,
        SUM(CASE WHEN sentiment_label = 'Positive' THEN 1 ELSE 0 END) AS positive_reviews,
        SUM(CASE WHEN sentiment_label = 'Neutral' THEN 1 ELSE 0 END) AS neutral_reviews,
        SUM(CASE WHEN sentiment_label = 'Negative' THEN 1 ELSE 0 END) AS negative_reviews,
        ROUND(SUM(CASE WHEN sentiment_label = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS positive_pct,
        ROUND(SUM(CASE WHEN sentiment_label = 'Negative' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS negative_pct
    FROM ANALYTICS.REVIEWS_SENTIMENT
    WHERE ASIN = 'B00ZV9RDKK'
    GROUP BY ASIN
),

complaints AS (
    SELECT
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'Summarize the main customer complaints for this product in 3 short bullet points: ' ||
            LISTAGG(REVIEW_TEXT, ' ') WITHIN GROUP (ORDER BY REVIEW_TS)
        ) AS complaint_summary
    FROM (
        SELECT REVIEW_TEXT, REVIEW_TS
        FROM ANALYTICS.REVIEWS_SENTIMENT
        WHERE ASIN = 'B00ZV9RDKK'
          AND sentiment_label = 'Negative'
        LIMIT 30
    )
),

strengths AS (
    SELECT
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'Summarize the main product strengths customers appreciate in 3 short bullet points: ' ||
            LISTAGG(REVIEW_TEXT, ' ') WITHIN GROUP (ORDER BY REVIEW_TS)
        ) AS strength_summary
    FROM (
        SELECT REVIEW_TEXT, REVIEW_TS
        FROM ANALYTICS.REVIEWS_SENTIMENT
        WHERE ASIN = 'B00ZV9RDKK'
          AND sentiment_label = 'Positive'
        LIMIT 30
    )
),

recommendations AS (
    SELECT
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'Based on these customer reviews, give 3 short improvement recommendations for the product: ' ||
            LISTAGG(REVIEW_TEXT, ' ') WITHIN GROUP (ORDER BY REVIEW_TS)
        ) AS improvement_recommendations
    FROM (
        SELECT REVIEW_TEXT, REVIEW_TS
        FROM ANALYTICS.REVIEWS_SENTIMENT
        WHERE ASIN = 'B00ZV9RDKK'
        LIMIT 40
    )
)

SELECT
    m.ASIN,
    m.total_reviews,
    m.avg_rating,
    m.positive_reviews,
    m.neutral_reviews,
    m.negative_reviews,
    m.positive_pct,
    m.negative_pct,
    c.complaint_summary,
    s.strength_summary,
    r.improvement_recommendations
FROM product_metrics m
CROSS JOIN complaints c
CROSS JOIN strengths s
CROSS JOIN recommendations r;

SELECT ASIN, total_reviews, avg_rating
FROM ANALYTICS.DEMO_PRODUCT_SUMMARY
ORDER BY total_reviews DESC;