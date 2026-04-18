USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;

USE SCHEMA RAW;
SHOW TABLES;

USE SCHEMA CURATED;
SHOW TABLES;

SELECT COUNT(*) FROM RAW.ELECTRONICS_REVIEWS_RAW;
SELECT COUNT(*) FROM CURATED.ELECTRONICS_REVIEWS;



SELECT COUNT(*) AS raw_cnt
FROM RAW.ELECTRONICS_REVIEWS_RAW;

SELECT COUNT(*) AS curated_cnt
FROM CURATED.ELECTRONICS_REVIEWS;

SELECT *
FROM RAW.ELECTRONICS_REVIEWS_RAW
LIMIT 3;

SELECT review_id, asin, user_id, rating, title, review_text, review_ts
FROM CURATED.ELECTRONICS_REVIEWS
LIMIT 10;

SELECT review_id, COUNT(*) AS cnt
FROM CURATED.ELECTRONICS_REVIEWS
GROUP BY review_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;

CREATE OR REPLACE VIEW CURATED.V_ELECTRONICS_REVIEWS_DEDUP AS
SELECT *
FROM CURATED.ELECTRONICS_REVIEWS
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY review_id
  ORDER BY review_ts DESC NULLS LAST,
           LENGTH(COALESCE(title,'')) + LENGTH(COALESCE(review_text,'')) DESC
) = 1;

SELECT review_id, COUNT(*) AS cnt
FROM CURATED.V_ELECTRONICS_REVIEWS_DEDUP
GROUP BY review_id
HAVING COUNT(*) > 1;

SELECT
  (SELECT COUNT(*) FROM CURATED.ELECTRONICS_REVIEWS) AS before_cnt,
  (SELECT COUNT(*) FROM CURATED.V_ELECTRONICS_REVIEWS_DEDUP) AS after_cnt;

SELECT review_id, asin, rating, review_ts, LEFT(review_text, 200) AS preview
FROM CURATED.V_ELECTRONICS_REVIEWS_DEDUP
ORDER BY review_ts DESC NULLS LAST
LIMIT 20;



-- Add a duplicate-prevention insertion rule to the CURATED table
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA CURATED;
SHOW TABLES LIKE 'ELECTRONICS_REVIEWS' IN SCHEMA CURATED;
SELECT COUNT(*) FROM CURATED.ELECTRONICS_REVIEWS;

USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA CURATED;
MERGE INTO CURATED.ELECTRONICS_REVIEWS tgt
USING (
  SELECT
    v:review_id::STRING AS review_id,
    v:asin::STRING AS asin,
    v:user_id::STRING AS user_id,
    TRY_TO_NUMBER(v:rating::STRING) AS rating,
    v:title::STRING AS title,
    v:text::STRING AS review_text,
    TRY_TO_BOOLEAN(v:verified_purchase::STRING) AS verified_purchase,
    TRY_TO_NUMBER(v:helpful_vote::STRING) AS helpful_vote,
    CASE
      WHEN v:timestamp IS NULL THEN NULL
      WHEN TRY_TO_NUMBER(v:timestamp::STRING) IS NULL
        THEN TRY_TO_TIMESTAMP_NTZ(v:timestamp::STRING)
      WHEN TRY_TO_NUMBER(v:timestamp::STRING) >= 1000000000000
        THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(v:timestamp::STRING) / 1000)
      ELSE
        TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(v:timestamp::STRING))
    END AS review_ts,
    'Electronics' AS category
  FROM RAW.ELECTRONICS_REVIEWS_RAW
  WHERE v:text IS NOT NULL
) src
ON tgt.review_id = src.review_id
WHEN NOT MATCHED THEN
  INSERT (
    review_id, asin, user_id, rating, title, review_text,
    verified_purchase, helpful_vote, review_ts, category
  )
  VALUES (
    src.review_id, src.asin, src.user_id, src.rating, src.title, src.review_text,
    src.verified_purchase, src.helpful_vote, src.review_ts, src.category
  );
