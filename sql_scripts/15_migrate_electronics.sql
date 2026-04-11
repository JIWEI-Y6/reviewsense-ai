-- ============================================
-- MIGRATE EXISTING ELECTRONICS DATA TO V2 TABLE
-- Run this once after 14_multi_category_setup.sql
-- Copies existing Electronics reviews into the new unified table
-- ============================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE REVIEWSENSE_WH;
USE DATABASE REVIEWSENSE_DB;

-- Check: V2 table should be empty before migration
SELECT COUNT(*) AS v2_before FROM RAW.REVIEWS_RAW_V2;

-- Migrate Electronics reviews (183K rows)
INSERT INTO RAW.REVIEWS_RAW_V2 (V, SOURCE_CATEGORY, LOADED_AT)
SELECT
    OBJECT_CONSTRUCT(
        'review_id', REVIEW_ID,
        'asin', ASIN,
        'user_id', USER_ID,
        'rating', RATING,
        'title', TITLE,
        'review_text', REVIEW_TEXT,
        'verified_purchase', VERIFIED_PURCHASE,
        'helpful_vote', HELPFUL_VOTE,
        'review_ts', REVIEW_TS,
        'text_len', TEXT_LEN
    ) AS V,
    'Electronics' AS SOURCE_CATEGORY,
    CURRENT_TIMESTAMP() AS LOADED_AT
FROM ANALYTICS.REVIEWS_FOR_GENAI;

-- Verify: row counts should match
SELECT
    (SELECT COUNT(*) FROM ANALYTICS.REVIEWS_FOR_GENAI) AS original_count,
    (SELECT COUNT(*) FROM RAW.REVIEWS_RAW_V2 WHERE SOURCE_CATEGORY = 'Electronics') AS migrated_count;

-- Migrate existing metadata (786K rows)
INSERT INTO RAW.METADATA_RAW_V2 (V, SOURCE_CATEGORY, LOADED_AT)
SELECT
    V,
    'Electronics' AS SOURCE_CATEGORY,
    CURRENT_TIMESTAMP() AS LOADED_AT
FROM RAW.PRODUCT_METADATA_RAW;

-- Verify metadata
SELECT
    (SELECT COUNT(*) FROM RAW.PRODUCT_METADATA_RAW) AS original_count,
    (SELECT COUNT(*) FROM RAW.METADATA_RAW_V2 WHERE SOURCE_CATEGORY = 'Electronics') AS migrated_count;

-- Check category distribution
SELECT SOURCE_CATEGORY, COUNT(*) AS row_count
FROM RAW.REVIEWS_RAW_V2
GROUP BY SOURCE_CATEGORY;
