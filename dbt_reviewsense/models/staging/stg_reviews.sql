-- Staging view: extract from VARIANT, clean, filter, assign quality tiers
-- Source: RAW.REVIEWS_RAW_V2 (multi-category, VARIANT)
-- Backward compatible: existing Electronics data produces identical output

WITH source AS (
    SELECT
        V:review_id::VARCHAR AS REVIEW_ID,
        V:asin::VARCHAR AS ASIN,
        V:user_id::VARCHAR AS USER_ID,
        V:rating::NUMBER AS RATING,
        V:title::VARCHAR AS TITLE,
        V:review_text::VARCHAR AS REVIEW_TEXT,
        V:verified_purchase::BOOLEAN AS VERIFIED_PURCHASE,
        V:helpful_vote::NUMBER AS HELPFUL_VOTE,
        V:review_ts::TIMESTAMP_NTZ AS REVIEW_TS,
        V:text_len::NUMBER AS TEXT_LEN,
        SOURCE_CATEGORY
    FROM {{ source('raw', 'REVIEWS_RAW_V2') }}
),

cleaned AS (
    SELECT
        REVIEW_ID,
        ASIN,
        USER_ID,
        RATING,
        TITLE,
        REVIEW_TEXT,
        VERIFIED_PURCHASE,
        HELPFUL_VOTE,
        REVIEW_TS,
        TEXT_LEN,
        SOURCE_CATEGORY,

        -- Concat title + text for enrichment
        COALESCE(TITLE, '') || ' ' || COALESCE(REVIEW_TEXT, '') AS REVIEW_TEXT_FULL,

        -- Regex cleaning: HTML tags, URLs, excessive whitespace
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    COALESCE(TITLE, '') || ' ' || COALESCE(REVIEW_TEXT, ''),
                    '<[^>]+>', ' '
                ),
                'https?://\\S+', ' '
            ),
            '\\s+', ' '
        ) AS REVIEW_TEXT_CLEAN,

        -- Review quality tier
        CASE
            WHEN TEXT_LEN >= 500 THEN 'high'
            WHEN TEXT_LEN >= 150 THEN 'medium'
            ELSE 'low'
        END AS REVIEW_QUALITY

    FROM source
    WHERE REVIEW_TEXT IS NOT NULL
      AND LENGTH(TRIM(REVIEW_TEXT)) >= 20
      AND RATING BETWEEN 1 AND 5
      AND REVIEW_TS IS NOT NULL
      AND YEAR(REVIEW_TS) <= 2026
)

SELECT * FROM cleaned
