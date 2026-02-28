CREATE OR REPLACE VIEW V_REVIEWS_CLEAN AS
SELECT
    REVIEW_ID,
    ASIN,
    USER_ID,
    RATING,
    VERIFIED_PURCHASE,
    HELPFUL_VOTE,
    REVIEW_TS,
    CATEGORY,
    TEXT_LEN,
    LEFT(
        TRIM(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
                TRIM(CONCAT(
                    COALESCE(TITLE, ''),
                    CASE WHEN TITLE IS NULL OR TITLE = '' THEN '' ELSE '. ' END,
                    COALESCE(REVIEW_TEXT, '')
                )),
                '<br\\s*/?>',                  ' '),
                '&#[0-9]+',                    ''),
                '\\[\\[VIDEOID:[^\\]]*\\]\\]', ''),
                '\\[\\[ASIN:[^\\]]*\\]\\]',    ''),
                'https?://\\S+',               ''),
                'www\\.\\S+',                  ''),
            '\\s{2,}',                         ' ')
        ),
    8000) AS review_text_clean
FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI
WHERE YEAR(REVIEW_TS) <= 2026;