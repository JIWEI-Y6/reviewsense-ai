-- Enriched reviews: join AI enrichment with product category lookup
-- This is the main fact table for downstream analytics

{{ config(materialized='table') }}

SELECT
    e.REVIEW_ID,
    e.ASIN,
    e.USER_ID,
    e.RATING,
    e.TITLE,
    e.REVIEW_TEXT,
    e.REVIEW_TEXT_CLEAN,
    e.VERIFIED_PURCHASE,
    e.HELPFUL_VOTE,
    e.REVIEW_TS,
    e.TEXT_LEN,
    e.REVIEW_QUALITY,
    e.SENTIMENT_SCORE,
    e.REVIEW_THEME,
    e.REVIEW_SUMMARY,
    e.SOURCE_CATEGORY,
    p.DERIVED_CATEGORY,
    p.DERIVATION_CONFIDENCE,
    p.BRAND,
    p.PRODUCT_NAME
FROM {{ ref('int_enriched_reviews') }} e
LEFT JOIN {{ ref('product_lookup') }} p ON e.ASIN = p.ASIN
