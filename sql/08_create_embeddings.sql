-- ============================================
-- REVIEW EMBEDDINGS
-- Model: snowflake-arctic-embed-m-v1.5 (768-dim)
-- Source: REVIEWS_CLEAN_FOR_EMBEDDINGS 
-- Function: EMBED_TEXT_768 
-- ============================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

-- Step 1: Create embeddings table
CREATE OR REPLACE TABLE REVIEW_EMBEDDINGS (
    REVIEW_ID           VARCHAR,
    ASIN                VARCHAR,
    USER_ID             VARCHAR,
    RATING              NUMBER,
    VERIFIED_PURCHASE   BOOLEAN,
    HELPFUL_VOTE        NUMBER,
    REVIEW_TS           TIMESTAMP_NTZ,
    CATEGORY            VARCHAR,
    TEXT_LEN            NUMBER,
    REVIEW_TEXT_CLEAN   VARCHAR,
    EMBEDDING           VECTOR(FLOAT, 768)
);

-- Step 2: Generate and insert embeddings
INSERT INTO REVIEWSENSE_DB.ANALYTICS.REVIEW_EMBEDDINGS
SELECT
    REVIEW_ID,
    ASIN,
    USER_ID,
    RATING,
    VERIFIED_PURCHASE,
    HELPFUL_VOTE,
    REVIEW_TS,
    CATEGORY,
    TEXT_LEN,
    REVIEW_TEXT_CLEAN,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'snowflake-arctic-embed-m-v1.5',
        REVIEW_TEXT_CLEAN
    ) AS EMBEDDING
FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_CLEAN_FOR_EMBEDDINGS;

-- Step 3: Validate after INSERT completes
SELECT COUNT(*) AS embedded_rows 
FROM REVIEWSENSE_DB.ANALYTICS.REVIEW_EMBEDDINGS;