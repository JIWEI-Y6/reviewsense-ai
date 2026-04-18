USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;

SHOW FUNCTIONS LIKE '%CORTEX%' IN ACCOUNT;

SHOW FUNCTIONS IN SCHEMA SNOWFLAKE.CORTEX;

SHOW FUNCTIONS LIKE '%COMPLETE%' IN ACCOUNT;

USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

SELECT *
FROM ANALYTICS.REVIEWS_FOR_GENAI
LIMIT 5;

--SELECT
  --review_id,
  --SNOWFLAKE.CORTEX.COMPLETE(
    --'mistral-large',
    --'Classify the sentiment of this review as Positive, Neutral, or Negative: ' || review_text
  --) AS sentiment_test
--FROM ANALYTICS.REVIEWS_FOR_GENAI
--LIMIT 3;

SELECT
    REVIEW_ID,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'Return only one word: Positive, Neutral, or Negative. Review: '
        || REVIEW_TEXT
    ) AS sentiment_test
FROM ANALYTICS.REVIEWS_FOR_GENAI
LIMIT 3;

CREATE OR REPLACE TABLE ANALYTICS.REVIEW_INSIGHTS AS
SELECT
  review_id,
  asin,
  rating,
  review_ts,
  review_text,

  SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Classify the sentiment of this review as Positive, Neutral, or Negative: ' || review_text
  ) AS sentiment_label,

  SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Summarize this customer review in one short sentence: ' || review_text
  ) AS review_summary

FROM ANALYTICS.REVIEWS_FOR_GENAI
LIMIT 200;

SELECT *
FROM ANALYTICS.REVIEW_INSIGHTS
LIMIT 5;

SELECT sentiment_label, COUNT(*) AS review_count
FROM ANALYTICS.REVIEW_INSIGHTS
GROUP BY sentiment_label
ORDER BY review_count DESC;

CREATE OR REPLACE TABLE ANALYTICS.REVIEW_INSIGHTS AS
SELECT
    REVIEW_ID,
    ASIN,
    RATING,
    REVIEW_TS,
    REVIEW_TEXT,

    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'Return only one word: Positive, Neutral, or Negative. Review: '
        || REVIEW_TEXT
    ) AS sentiment_label,

    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'Summarize this review in one short sentence: '
        || REVIEW_TEXT
    ) AS review_summary

FROM ANALYTICS.REVIEWS_FOR_GENAI
LIMIT 200;

CREATE OR REPLACE TABLE ANALYTICS.REVIEW_INSIGHTS AS
SELECT
    REVIEW_ID,
    ASIN,
    RATING,
    REVIEW_TS,
    REVIEW_TEXT,

    TRIM(
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'You must answer with ONLY one word: Positive, Neutral, or Negative. No explanation. Review: '
            || REVIEW_TEXT
        )
    ) AS sentiment_label,

    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'Summarize this review in one short sentence: '
        || REVIEW_TEXT
    ) AS review_summary

FROM ANALYTICS.REVIEWS_FOR_GENAI
LIMIT 200;



--final version
CREATE OR REPLACE TABLE ANALYTICS.REVIEW_INSIGHTS AS
SELECT
    REVIEW_ID,
    ASIN,
    RATING,
    REVIEW_TS,
    REVIEW_TEXT,

    UPPER(
        REGEXP_SUBSTR(
            SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large',
                'Return only one word: Positive, Neutral, or Negative. Review: '
                || REVIEW_TEXT
            ),
            'Positive|Neutral|Negative'
        )
    ) AS sentiment_label,

    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large',
        'Summarize this review in one short sentence: '
        || REVIEW_TEXT
    ) AS review_summary

FROM ANALYTICS.REVIEWS_FOR_GENAI
LIMIT 200;

SELECT sentiment_label, COUNT(*) AS review_count
FROM ANALYTICS.REVIEW_INSIGHTS
GROUP BY sentiment_label
ORDER BY review_count DESC;