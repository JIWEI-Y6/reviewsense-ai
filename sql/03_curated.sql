USE DATABASE REVIEWSENSE_DB;
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE ELECTRONICS_REVIEWS AS
SELECT
    v:review_id::STRING          AS review_id,
    v:asin::STRING               AS asin,
    v:user_id::STRING            AS user_id,
    TRY_TO_NUMBER(v:rating::STRING)        AS rating,
    v:title::STRING              AS title,
    v:text::STRING               AS review_text,
    TRY_TO_BOOLEAN(v:verified_purchase::STRING) AS verified_purchase,
    TRY_TO_NUMBER(v:helpful_vote::STRING)  AS helpful_vote,

    CASE
      WHEN v:timestamp IS NULL THEN NULL
      WHEN TRY_TO_NUMBER(v:timestamp::STRING) IS NULL
        THEN TRY_TO_TIMESTAMP_NTZ(v:timestamp::STRING)
      WHEN TRY_TO_NUMBER(v:timestamp::STRING) >= 1000000000000
        THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(v:timestamp::STRING) / 1000)  -- milliseconds
      ELSE
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(v:timestamp::STRING))              -- seconds
    END AS review_ts,

    'Electronics' AS category
FROM REVIEWSENSE_DB.RAW.ELECTRONICS_REVIEWS_RAW
WHERE v:text IS NOT NULL;
