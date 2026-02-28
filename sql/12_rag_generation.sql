-- ============================================
-- PHASE 3: RAG GENERATION
-- Model: mistral-large
-- Two modes: Consumer Q&A + Business Analyst
-- ============================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

-- ============================================
-- MODE 1: CONSUMER Q&A
-- Uncomment this block to run consumer mode
-- Change the query string to test questions
-- ============================================

-- SET SEARCH_QUERY = 'What do people think about noise cancelling headphones?';
-- WITH retrieved_reviews AS (
--     SELECT
--         value:REVIEW_ID::VARCHAR         AS review_id,
--         value:ASIN::VARCHAR              AS asin,
--         value:RATING::NUMBER             AS rating,
--         value:REVIEW_TEXT_CLEAN::VARCHAR AS review_text
--     FROM (
--         SELECT PARSE_JSON(
--             SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
--                 'REVIEWSENSE_DB.ANALYTICS.REVIEW_SEARCH',
--                 '{
--                     "query": "What do people think about noise cancelling headphones?",
--                     "columns": ["REVIEW_ID", "ASIN", "RATING", "REVIEW_TEXT_CLEAN"],
--                     "limit": 5
--                 }'
--             )
--         ) AS results
--     ),
--     LATERAL FLATTEN(input => results:results)
-- ),
-- prompt_context AS (
--     SELECT LISTAGG(
--         '- Review (Rating: ' || rating || '/5): ' || review_text,
--         '\n'
--     ) AS context
--     FROM retrieved_reviews
-- )
-- SELECT SNOWFLAKE.CORTEX.COMPLETE(
--     'mistral-large',
--     CONCAT(
--         'You are ReviewSense AI, a customer review analysis assistant. ',
--         'Answer the user''s question based ONLY on the following Amazon customer reviews. ',
--         'Be concise, factual, and highlight both positives and negatives. ',
--         'Keep your answer to 4-5 sentences maximum. ',
--         'If the reviews do not contain enough information, say so honestly.\n\n',
--         'CUSTOMER REVIEWS:\n',
--         context,
--         '\n\nUSER QUESTION: ', $SEARCH_QUERY,
--         '\n\nANSWER:'
--     )
-- ) AS rag_answer
-- FROM prompt_context;

-- ============================================
-- MODE 2: BUSINESS ANALYST
-- Uncomment this block to run business mode
-- Change ASIN to analyze different products
-- ============================================

WITH product_info AS (
    SELECT 
        ASIN,
        COUNT(*) AS total_reviews,
        ROUND(AVG(RATING), 2) AS avg_rating,
        COUNT(CASE WHEN RATING <= 2 THEN 1 END) AS negative_reviews,
        COUNT(CASE WHEN RATING >= 4 THEN 1 END) AS positive_reviews,
        ROUND(COUNT(CASE WHEN RATING <= 2 THEN 1 END) * 100.0 / COUNT(*), 1) AS negative_pct,
        ROUND(COUNT(CASE WHEN RATING >= 4 THEN 1 END) * 100.0 / COUNT(*), 1) AS positive_pct
    FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI
    WHERE ASIN = 'B01G8JO5F2'
    GROUP BY ASIN
),
retrieved_reviews AS (
    SELECT
        value:REVIEW_ID::VARCHAR         AS review_id,
        value:ASIN::VARCHAR              AS asin,
        value:RATING::NUMBER             AS rating,
        value:REVIEW_TEXT_CLEAN::VARCHAR AS review_text
    FROM (
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'REVIEWSENSE_DB.ANALYTICS.REVIEW_SEARCH',
                '{
                    "query": "product quality complaints issues",
                    "columns": ["REVIEW_ID", "ASIN", "RATING", "REVIEW_TEXT_CLEAN"],
                    "filter": {"@eq": {"ASIN": "B01G8JO5F2"}},
                    "limit": 10
                }'
            )
        ) AS results
    ),
    LATERAL FLATTEN(input => results:results)
),
prompt_context AS (
    SELECT 
        COUNT(*) AS review_count,
        LISTAGG(
            '- Review (Rating: ' || rating || '/5): ' || review_text,
            '\n'
        ) AS context
    FROM retrieved_reviews
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    CONCAT(
        'You are ReviewSense AI, a product intelligence analyst.\n\n',
        'IMPORTANT: Use the following database statistics as the authoritative source. ',
        'DO NOT calculate your own statistics from the sample reviews.\n\n',
        '=== AUTHORITATIVE PRODUCT STATISTICS FROM DATABASE ===\n',
        'ASIN: ', p.ASIN, '\n',
        'TOTAL REVIEWS IN DATABASE: ', p.total_reviews::VARCHAR, '\n',
        'OFFICIAL AVERAGE RATING: ', p.avg_rating::VARCHAR, ' out of 5\n',
        'POSITIVE REVIEWS (4-5 stars): ', p.positive_reviews::VARCHAR, ' (', p.positive_pct::VARCHAR, '%)\n',
        'NEGATIVE REVIEWS (1-2 stars): ', p.negative_reviews::VARCHAR, ' (', p.negative_pct::VARCHAR, '%)\n',
        'SAMPLE REVIEWS ANALYZED: ', c.review_count::VARCHAR, ' (complaint-focused sample)\n',
        '======================================================\n\n',
        'First identify the product type from the sample reviews, then generate the report.\n\n',
        'Your response MUST follow this EXACT format:\n\n',
        '=====================================\n',
        'PRODUCT ANALYSIS REPORT\n',
        'ASIN: ', p.ASIN, '\n',
        'PRODUCT TYPE: [Identify from reviews]\n',
        'OFFICIAL AVG RATING: ', p.avg_rating::VARCHAR, '/5\n',
        'TOTAL REVIEWS: ', p.total_reviews::VARCHAR,
        ' (', p.positive_pct::VARCHAR, '% positive | ', p.negative_pct::VARCHAR, '% negative)\n',
        'SAMPLE ANALYZED: ', c.review_count::VARCHAR, ' complaint-focused reviews\n',
        '=====================================\n\n',
        'SENTIMENT: [Based on full database stats, not just sample]\n\n',
        'TOP PRAISED FEATURES:\n',
        '1. [Feature]: [Brief explanation]\n',
        '2. [Feature]: [Brief explanation]\n',
        '3. [Feature]: [Brief explanation]\n\n',
        'TOP COMPLAINTS:\n',
        '1. [Issue]: [Brief explanation]\n',
        '2. [Issue]: [Brief explanation]\n',
        '3. [Issue]: [Brief explanation]\n\n',
        'RECOMMENDATION: [One actionable suggestion for the product team]\n\n',
        'Base feature/complaint analysis on these sample reviews:\n\n',
        'CUSTOMER REVIEWS:\n',
        c.context,
        '\n\nANALYSIS:'
    )
) AS business_analysis
FROM prompt_context c
CROSS JOIN product_info p;