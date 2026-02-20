USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;


-- RAW Views
USE SCHEMA RAW;

CREATE OR REPLACE VIEW V_ELECTRONICS_REVIEWS_RAW AS
SELECT v
FROM ELECTRONICS_REVIEWS_RAW;

CREATE OR REPLACE VIEW V_ELECTRONICS_RAW_KEYS AS
SELECT
  v:review_id::STRING AS review_id,
  v:asin::STRING      AS asin,
  v:user_id::STRING   AS user_id,
  v:rating::STRING    AS rating_raw,
  v:timestamp::STRING AS timestamp_raw
FROM ELECTRONICS_REVIEWS_RAW;

-- CURATED Views

USE SCHEMA CURATED;

CREATE OR REPLACE VIEW V_ELECTRONICS_REVIEWS AS
SELECT
  review_id,
  asin,
  user_id,
  rating,
  title,
  review_text,
  verified_purchase,
  helpful_vote,
  review_ts,
  category
FROM ELECTRONICS_REVIEWS;

CREATE OR REPLACE VIEW V_ELECTRONICS_REVIEWS_TEXT AS
SELECT
  review_id,
  asin,
  rating,
  review_ts,
  helpful_vote,
  TRIM(
    CONCAT(
      COALESCE(title, ''),
      CASE WHEN title IS NULL OR title = '' THEN '' ELSE ' — ' END,
      COALESCE(review_text, '')
    )
  ) AS review_text_full
FROM ELECTRONICS_REVIEWS
WHERE review_text IS NOT NULL;

CREATE OR REPLACE VIEW V_ELECTRONICS_NEGATIVE_REVIEWS AS
SELECT
  review_id,
  asin,
  rating,
  review_ts,
  helpful_vote,
  title,
  review_text
FROM ELECTRONICS_REVIEWS
WHERE rating <= 2;

SELECT
  review_id,
  asin,
  user_id,
  rating,
  title,
  LEFT(review_text, 200) AS review_text_preview,
  verified_purchase,
  helpful_vote,
  review_ts,
  category
FROM REVIEWSENSE_DB.CURATED.ELECTRONICS_REVIEWS
ORDER BY review_ts DESC NULLS LAST
LIMIT 20;

