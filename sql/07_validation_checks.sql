-- ============================================
-- DATA QUALITY VALIDATION - V_REVIEWS_CLEAN
-- ============================================
USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

-- Check 1: Row count (expected: 183,447)
SELECT COUNT(*) FROM REVIEWSENSE_DB.ANALYTICS.V_REVIEWS_CLEAN;

-- Check 2: No br tags remaining
SELECT COUNT(*) AS should_be_zero
FROM REVIEWSENSE_DB.ANALYTICS.V_REVIEWS_CLEAN
WHERE review_text_clean LIKE '%<br%';

-- Check 3: No ASIN refs remaining  
SELECT COUNT(*) AS should_be_zero
FROM REVIEWSENSE_DB.ANALYTICS.V_REVIEWS_CLEAN
WHERE review_text_clean LIKE '%[[ASIN%';

-- Check 4: No VIDEOID refs remaining
SELECT COUNT(*) AS should_be_zero
FROM REVIEWSENSE_DB.ANALYTICS.V_REVIEWS_CLEAN
WHERE review_text_clean LIKE '%[[VIDEOID%';