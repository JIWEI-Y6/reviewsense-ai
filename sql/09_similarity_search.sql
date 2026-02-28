-- ============================================
-- PHASE 2: SIMILARITY SEARCH
-- Takes a user question, embeds it, finds top-k 
-- most relevant reviews using cosine similarity
-- ============================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

-- Test query: change this to any question you want
SET SEARCH_QUERY = 'What is the best noise cancelling headphones?';

-- Similarity search: embed the query and find top 5 most similar reviews
SELECT
    REVIEW_ID,
    ASIN,
    RATING,
    REVIEW_TS,
    REVIEW_TEXT_CLEAN,
    VECTOR_COSINE_SIMILARITY(
        EMBEDDING,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m-v1.5',
            $SEARCH_QUERY
        )
    ) AS SIMILARITY_SCORE
FROM REVIEWSENSE_DB.ANALYTICS.REVIEW_EMBEDDINGS
ORDER BY SIMILARITY_SCORE DESC
LIMIT 5;