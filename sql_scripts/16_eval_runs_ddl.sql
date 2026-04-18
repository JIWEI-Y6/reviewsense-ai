-- ============================================================
-- 16_eval_runs_ddl.sql
-- Evaluation run persistence for the Cortex LLM bake-off.
--
-- One row per (run_id, model, question_id). All 3 candidates
-- in a single bake-off invocation share the same RUN_ID so we
-- can correlate results across models.
-- ============================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE REVIEWSENSE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

CREATE TABLE IF NOT EXISTS ANALYTICS.EVAL_RUNS (
    RUN_ID                   VARCHAR(64)     NOT NULL,
    RUN_TS                   TIMESTAMP_NTZ   NOT NULL,
    MODEL                    VARCHAR(64)     NOT NULL,
    JUDGE_MODEL              VARCHAR(64)     NOT NULL,
    QUESTION_ID              VARCHAR(32)     NOT NULL,
    QUESTION_TYPE            VARCHAR(16),
    QUESTION_TEXT            VARCHAR(2000),
    EXPECTED_INTENT          VARCHAR(32),
    ACTUAL_INTENT            VARCHAR(64),
    INTENT_CORRECT           BOOLEAN,
    DATA_CORRECT             BOOLEAN,
    JUDGE_FACTUALITY         NUMBER(2,0),
    JUDGE_COMPLETENESS       NUMBER(2,0),
    JUDGE_CITATION           NUMBER(2,0),
    JUDGE_CONTEXT            NUMBER(2,0),
    JUDGE_REASONING          VARCHAR(2000),
    IS_HALLUCINATION         BOOLEAN,
    LATENCY_MS               NUMBER(10,1),
    TOOLS_USED               ARRAY,
    FALLBACK_USED            BOOLEAN,
    ANSWER_PREVIEW           VARCHAR(2000),
    MATCH_DETAIL             VARCHAR(500),
    LLM_CALLS                NUMBER(3,0),
    ESTIMATED_COST           NUMBER(10,4)
);

-- Helpful aggregation view for quick model comparison
CREATE OR REPLACE VIEW ANALYTICS.V_EVAL_MODEL_SUMMARY AS
SELECT
    RUN_ID,
    MODEL,
    JUDGE_MODEL,
    MIN(RUN_TS)                                            AS RUN_TS,
    COUNT(*)                                               AS QUESTIONS,
    SUM(IFF(INTENT_CORRECT, 1, 0)) / COUNT(*)              AS INTENT_ACCURACY,
    SUM(IFF(DATA_CORRECT,   1, 0)) / COUNT(*)              AS DATA_CORRECTNESS,
    AVG(JUDGE_FACTUALITY)                                  AS AVG_FACTUALITY,
    AVG(JUDGE_COMPLETENESS)                                AS AVG_COMPLETENESS,
    AVG(JUDGE_CITATION)                                    AS AVG_CITATION,
    AVG(JUDGE_CONTEXT)                                     AS AVG_CONTEXT,
    SUM(IFF(IS_HALLUCINATION, 1, 0)) / COUNT(*)            AS HALLUCINATION_RATE,
    SUM(IFF(FALLBACK_USED, 1, 0))    / COUNT(*)            AS FALLBACK_RATE,
    AVG(LATENCY_MS)                                        AS AVG_LATENCY_MS,
    SUM(ESTIMATED_COST)                                    AS TOTAL_COST
FROM ANALYTICS.EVAL_RUNS
GROUP BY RUN_ID, MODEL, JUDGE_MODEL;
