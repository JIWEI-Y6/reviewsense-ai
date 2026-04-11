-- Derive product categories from review text using Cortex COMPLETE
-- Only for ASINs with 3+ reviews (meaningful sample)
-- INCREMENTAL: only categorizes NEW ASINs (MERGE on ASIN)

{{ config(
    materialized='incremental',
    unique_key='ASIN',
    incremental_strategy='merge'
) }}

WITH asin_review_counts AS (
    SELECT
        ASIN,
        COUNT(*) AS REVIEW_COUNT
    FROM {{ ref('stg_reviews') }}
    {% if is_incremental() %}
    WHERE ASIN NOT IN (SELECT ASIN FROM {{ this }})
    {% endif %}
    GROUP BY ASIN
    HAVING COUNT(*) >= 3
),

-- Pick top 3 longest reviews per ASIN for category derivation
top_reviews AS (
    SELECT
        s.ASIN,
        s.REVIEW_TEXT_CLEAN,
        s.TEXT_LEN,
        ROW_NUMBER() OVER (PARTITION BY s.ASIN ORDER BY s.TEXT_LEN DESC) AS rn
    FROM {{ ref('stg_reviews') }} s
    INNER JOIN asin_review_counts a ON s.ASIN = a.ASIN
),

-- Concat top 3 reviews per ASIN
sample_reviews AS (
    SELECT
        t.ASIN,
        a.REVIEW_COUNT,
        LISTAGG(t.REVIEW_TEXT_CLEAN, ' | ') WITHIN GROUP (ORDER BY t.TEXT_LEN DESC) AS SAMPLE_TEXT
    FROM top_reviews t
    INNER JOIN asin_review_counts a ON t.ASIN = a.ASIN
    WHERE t.rn <= 3
    GROUP BY t.ASIN, a.REVIEW_COUNT
),

-- Derive category via Cortex COMPLETE
categorized AS (
    SELECT
        ASIN,
        REVIEW_COUNT,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'Based on these product reviews, classify this product into exactly one category. '
            || 'Categories: headphones_earbuds, speakers, streaming_devices, smart_home, '
            || 'cables_adapters, chargers_batteries, phone_accessories, computer_peripherals, '
            || 'storage_media, cameras_accessories, tv_accessories, gaming_accessories, '
            || 'wearables, other_electronics. '
            || 'Respond with ONLY the category name, nothing else. '
            || 'Reviews: ' || LEFT(SAMPLE_TEXT, 2000)
        ) AS RAW_CATEGORY,

        CASE
            WHEN REVIEW_COUNT >= 50 THEN 'high'
            WHEN REVIEW_COUNT >= 10 THEN 'medium'
            ELSE 'low'
        END AS DERIVATION_CONFIDENCE

    FROM sample_reviews
)

SELECT
    ASIN,
    REVIEW_COUNT,
    TRIM(LOWER(RAW_CATEGORY)) AS DERIVED_CATEGORY,
    DERIVATION_CONFIDENCE
FROM categorized
