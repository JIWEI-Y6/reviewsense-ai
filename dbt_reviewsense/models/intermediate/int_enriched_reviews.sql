-- Intermediate: Cortex AI enrichment on staged reviews
-- INCREMENTAL: only enriches NEW reviews (MERGE on REVIEW_ID)
-- Existing enriched rows are NOT re-processed — saves Cortex credits
-- Use --full-refresh to rebuild from scratch if needed

{{ config(
    materialized='incremental',
    unique_key='REVIEW_ID',
    incremental_strategy='merge'
) }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_reviews') }}
    {% if is_incremental() %}
    WHERE REVIEW_ID NOT IN (SELECT REVIEW_ID FROM {{ this }})
    {% endif %}
)

SELECT
    REVIEW_ID,
    ASIN,
    USER_ID,
    RATING,
    TITLE,
    REVIEW_TEXT,
    REVIEW_TEXT_CLEAN,
    VERIFIED_PURCHASE,
    HELPFUL_VOTE,
    REVIEW_TS,
    TEXT_LEN,
    REVIEW_QUALITY,
    SOURCE_CATEGORY,

    -- Sentiment score (cheap, run on all rows)
    SNOWFLAKE.CORTEX.SENTIMENT(REVIEW_TEXT_CLEAN) AS SENTIMENT_SCORE,

    -- Theme classification
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        REVIEW_TEXT_CLEAN,
        ['battery_life', 'build_quality', 'sound_quality', 'connectivity',
         'comfort', 'value_for_money', 'customer_service', 'durability',
         'ease_of_use', 'other']
    ):label::STRING AS REVIEW_THEME,

    -- Conditional summarize (only reviews worth summarizing)
    CASE
        WHEN TEXT_LEN > 500 THEN SNOWFLAKE.CORTEX.SUMMARIZE(REVIEW_TEXT_CLEAN)
        ELSE REVIEW_TEXT_CLEAN
    END AS REVIEW_SUMMARY

FROM staged
