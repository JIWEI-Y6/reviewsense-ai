USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

WITH search_results AS (
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'REVIEWSENSE_DB.ANALYTICS.REVIEW_SEARCH',
            '{
                "query": "What do people think about noise cancelling headphones?",
                "columns": ["REVIEW_ID", "ASIN", "RATING", "REVIEW_TEXT_CLEAN"],
                "limit": 5
            }'
        )
    ) AS results
)
SELECT
    value:REVIEW_ID::VARCHAR     AS review_id,
    value:ASIN::VARCHAR          AS asin,
    value:RATING::NUMBER         AS rating,
    value:REVIEW_TEXT_CLEAN::VARCHAR AS review_text,
    value['@scores']['cosine_similarity']::FLOAT AS cosine_similarity,
    value['@scores']['reranker_score']::FLOAT    AS reranker_score
FROM search_results,
LATERAL FLATTEN(input => results:results);