-- ============================================
-- MONITORING SETUP: ALERT_LOG, STREAM, PROCEDURE, TASK
-- Run this in Snowflake after dbt monitoring models are built
-- ============================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE REVIEWSENSE_WH;
USE DATABASE REVIEWSENSE_DB;

-- ============================================
-- 1. ALERT_LOG TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS GOLD.ALERT_LOG (
    ALERT_ID          VARCHAR DEFAULT UUID_STRING(),
    ALERT_SOURCE      VARCHAR NOT NULL,       -- 'anomaly', 'cross_category', 'emerging_theme', 'product', 'data_quality'
    ANOMALY_TYPE      VARCHAR NOT NULL,
    DERIVED_CATEGORY  VARCHAR,
    REVIEW_THEME      VARCHAR,
    ASIN              VARCHAR,
    PRODUCT_NAME      VARCHAR,
    DETECTION_PERIOD  TIMESTAMP_NTZ,
    CURRENT_VALUE     FLOAT,
    BASELINE_VALUE    FLOAT,
    DEVIATION_SCORE   FLOAT,
    AFFECTED_REVIEWS  NUMBER,
    SEVERITY          VARCHAR NOT NULL,       -- 'HIGH', 'MEDIUM', 'LOW'
    AI_SUMMARY        VARCHAR,
    ACKNOWLEDGED      BOOLEAN DEFAULT FALSE,
    ACKNOWLEDGED_AT   TIMESTAMP_NTZ,
    CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- 2. STREAM ON ANOMALIES TABLE
-- ============================================
CREATE OR REPLACE STREAM GOLD.REVIEW_ANOMALIES_STREAM
    ON TABLE GOLD.REVIEW_ANOMALIES
    SHOW_INITIAL_ROWS = TRUE;

-- ============================================
-- 3. STORED PROCEDURE: GENERATE ALERTS WITH AI SUMMARIES
-- ============================================
CREATE OR REPLACE PROCEDURE GOLD.GENERATE_ALERTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Process category-level anomalies from stream
    INSERT INTO GOLD.ALERT_LOG
        (ALERT_SOURCE, ANOMALY_TYPE, DERIVED_CATEGORY, REVIEW_THEME,
         DETECTION_PERIOD, CURRENT_VALUE, BASELINE_VALUE, DEVIATION_SCORE,
         AFFECTED_REVIEWS, SEVERITY, AI_SUMMARY, CREATED_AT)
    SELECT
        'anomaly',
        s.ANOMALY_TYPE,
        s.DERIVED_CATEGORY,
        s.REVIEW_THEME,
        s.DETECTION_PERIOD,
        s.CURRENT_VALUE,
        s.BASELINE_VALUE,
        s.DEVIATION_SCORE,
        s.AFFECTED_REVIEWS,
        s.SEVERITY,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'You are a product intelligence analyst. Generate a concise 2-3 sentence alert. '
            || 'Anomaly: ' || s.ANOMALY_TYPE
            || '. Category: ' || s.DERIVED_CATEGORY
            || COALESCE('. Theme: ' || s.REVIEW_THEME, '')
            || '. Current value: ' || s.CURRENT_VALUE::VARCHAR
            || '. Baseline: ' || COALESCE(s.BASELINE_VALUE::VARCHAR, 'N/A')
            || '. Deviation (z-score): ' || COALESCE(s.DEVIATION_SCORE::VARCHAR, 'N/A')
            || '. Severity: ' || s.SEVERITY
            || '. Explain what this means and suggest one action.'
        ),
        CURRENT_TIMESTAMP()
    FROM GOLD.REVIEW_ANOMALIES_STREAM s
    WHERE s.METADATA$ACTION = 'INSERT';

    -- Process cross-category alerts
    INSERT INTO GOLD.ALERT_LOG
        (ALERT_SOURCE, ANOMALY_TYPE, DERIVED_CATEGORY, REVIEW_THEME,
         CURRENT_VALUE, DEVIATION_SCORE, AFFECTED_REVIEWS,
         SEVERITY, AI_SUMMARY, CREATED_AT)
    SELECT
        'cross_category',
        'CROSS_CATEGORY_SPIKE',
        'MULTIPLE (' || AFFECTED_CATEGORIES || ' categories)',
        REVIEW_THEME,
        TOTAL_AFFECTED_REVIEWS,
        AVG_DEVIATION,
        TOTAL_AFFECTED_REVIEWS,
        MAX_SEVERITY,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'A complaint theme is spiking across multiple product categories simultaneously. '
            || 'Theme: ' || REVIEW_THEME
            || '. Affected categories: ' || AFFECTED_CATEGORIES::VARCHAR
            || '. Total reviews: ' || TOTAL_AFFECTED_REVIEWS::VARCHAR
            || '. Avg deviation: ' || AVG_DEVIATION::VARCHAR
            || '. Generate a 2-3 sentence alert explaining the pattern and recommend an action.'
        ),
        CURRENT_TIMESTAMP()
    FROM GOLD.CROSS_CATEGORY_ALERTS;

    -- Process emerging themes
    INSERT INTO GOLD.ALERT_LOG
        (ALERT_SOURCE, ANOMALY_TYPE, DERIVED_CATEGORY, REVIEW_THEME,
         CURRENT_VALUE, BASELINE_VALUE, DEVIATION_SCORE,
         AFFECTED_REVIEWS, SEVERITY, AI_SUMMARY, CREATED_AT)
    SELECT
        'emerging_theme',
        'EMERGING_THEME',
        DERIVED_CATEGORY,
        REVIEW_THEME,
        RECENT_SHARE,
        HISTORICAL_SHARE,
        GROWTH_FACTOR,
        RECENT_REVIEW_COUNT,
        SEVERITY,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'An emerging complaint pattern detected. '
            || 'Category: ' || DERIVED_CATEGORY
            || '. Theme: ' || REVIEW_THEME
            || '. Recent share: ' || RECENT_SHARE::VARCHAR || '%'
            || '. Historical share: ' || HISTORICAL_SHARE::VARCHAR || '%'
            || '. Growth: ' || GROWTH_FACTOR::VARCHAR || 'x'
            || '. Generate a 2-sentence alert explaining why this matters.'
        ),
        CURRENT_TIMESTAMP()
    FROM GOLD.EMERGING_THEMES
    WHERE SEVERITY IN ('HIGH', 'MEDIUM');

    -- Process product anomalies
    INSERT INTO GOLD.ALERT_LOG
        (ALERT_SOURCE, ANOMALY_TYPE, DERIVED_CATEGORY, ASIN, PRODUCT_NAME,
         DETECTION_PERIOD, CURRENT_VALUE, BASELINE_VALUE, DEVIATION_SCORE,
         AFFECTED_REVIEWS, SEVERITY, AI_SUMMARY, CREATED_AT)
    SELECT
        'product',
        ANOMALY_TYPE,
        DERIVED_CATEGORY,
        ASIN,
        COALESCE(BRAND, '') || ' - ' || COALESCE(PRODUCT_NAME, ASIN),
        DETECTION_PERIOD,
        CURRENT_VALUE,
        BASELINE_VALUE,
        DEVIATION_SCORE,
        AFFECTED_REVIEWS,
        SEVERITY,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'Product alert detected. '
            || 'Product: ' || COALESCE(BRAND, '') || ' ' || COALESCE(PRODUCT_NAME, ASIN)
            || '. ASIN: ' || ASIN
            || '. Category: ' || COALESCE(DERIVED_CATEGORY, 'Unknown')
            || '. Type: ' || ANOMALY_TYPE
            || '. Current rating: ' || CURRENT_VALUE::VARCHAR
            || '. Baseline: ' || COALESCE(BASELINE_VALUE::VARCHAR, 'N/A')
            || '. Generate a 2-sentence alert.'
        ),
        CURRENT_TIMESTAMP()
    FROM GOLD.PRODUCT_ANOMALIES;

    -- Process data quality failures (WARN and FAIL only)
    INSERT INTO GOLD.ALERT_LOG
        (ALERT_SOURCE, ANOMALY_TYPE, DERIVED_CATEGORY,
         CURRENT_VALUE, BASELINE_VALUE,
         SEVERITY, AI_SUMMARY, CREATED_AT)
    SELECT
        'data_quality',
        'DQ_' || CHECK_NAME,
        TABLE_NAME,
        CURRENT_VALUE,
        EXPECTED_VALUE,
        CASE WHEN STATUS = 'FAIL' THEN 'HIGH' ELSE 'MEDIUM' END,
        'Data quality check ' || CHECK_NAME || ' on ' || TABLE_NAME
        || ': ' || STATUS || '. Current: ' || CURRENT_VALUE::VARCHAR
        || ', Expected: ' || EXPECTED_VALUE::VARCHAR
        || '. ' || DESCRIPTION,
        CURRENT_TIMESTAMP()
    FROM GOLD.DATA_QUALITY_CHECKS
    WHERE STATUS IN ('WARN', 'FAIL');

    RETURN 'Alerts generated successfully';
END;
$$;

-- ============================================
-- 4. SNOWFLAKE TASK (weekly schedule)
-- ============================================
CREATE OR REPLACE TASK GOLD.GENERATE_ALERTS_TASK
    WAREHOUSE = REVIEWSENSE_WH
    SCHEDULE = 'USING CRON 0 8 * * 1 America/New_York'
    WHEN SYSTEM$STREAM_HAS_DATA('REVIEWSENSE_DB.GOLD.REVIEW_ANOMALIES_STREAM')
AS
    CALL GOLD.GENERATE_ALERTS();

-- Resume the task (must be explicitly activated)
ALTER TASK GOLD.GENERATE_ALERTS_TASK RESUME;

-- ============================================
-- 5. VERIFY SETUP
-- ============================================
SHOW TABLES LIKE 'ALERT_LOG' IN SCHEMA GOLD;
SHOW STREAMS LIKE 'REVIEW_ANOMALIES_STREAM' IN SCHEMA GOLD;
SHOW PROCEDURES LIKE 'GENERATE_ALERTS' IN SCHEMA GOLD;
SHOW TASKS LIKE 'GENERATE_ALERTS_TASK' IN SCHEMA GOLD;
