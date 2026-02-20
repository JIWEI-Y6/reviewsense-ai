USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;

CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Simple normalization for downstream LLM/RAG
CREATE OR REPLACE VIEW ANALYTICS.V_REVIEWS_BASE AS
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
  category,
  -- one field to use everywhere
  TRIM(
    CONCAT(
      COALESCE(title, ''), 
      CASE WHEN title IS NULL OR title = '' THEN '' ELSE ' — ' END,
      COALESCE(review_text, '')
    )
  ) AS review_text_full
FROM CURATED.ELECTRONICS_REVIEWS
WHERE review_text IS NOT NULL;
