USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'llama3.1-8b',
  'Summarize this in one sentence: These headphones have good sound quality, but many customers complain that they stop working after a few months.'
) AS response;


WITH review_sample AS (
  SELECT LISTAGG(REVIEW_TEXT, ' || ') AS reviews_text
  FROM (
    SELECT REVIEW_TEXT
    FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_SENTIMENT
    WHERE ASIN = 'B01G8JO5F2'
      AND SENTIMENT_LABEL = 'Negative'
    ORDER BY HELPFUL_VOTE DESC NULLS LAST, REVIEW_TS DESC
    LIMIT 8
  )
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'llama3.1-8b',
  'You are analyzing Amazon product reviews. Based only on the following negative reviews, write 3 short bullet points summarizing the main customer complaints. Reviews: ' || reviews_text
) AS complaint_summary
FROM review_sample;


WITH review_sample AS (
  SELECT LISTAGG(REVIEW_TEXT, ' || ') AS reviews_text
  FROM (
    SELECT REVIEW_TEXT
    FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_SENTIMENT
    WHERE ASIN = 'B01G8JO5F2'
      AND SENTIMENT_LABEL = 'Positive'
    ORDER BY HELPFUL_VOTE DESC NULLS LAST, REVIEW_TS DESC
    LIMIT 8
  )
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'llama3.1-8b',
  'You are analyzing Amazon product reviews. Based only on the following positive reviews, write 3 short bullet points summarizing the main strengths customers appreciate. Reviews: ' || reviews_text
) AS strength_summary
FROM review_sample;


demo_step_5_final_summary