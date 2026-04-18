USE ROLE TRAINING_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE REVIEWSENSE_DB;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE TABLE REVIEWSENSE_DB.ANALYTICS.DEMO_PRODUCT_SUMMARY (
    ASIN STRING,
    TOTAL_REVIEWS NUMBER,
    AVG_RATING NUMBER(10,2),
    POSITIVE_REVIEWS NUMBER,
    NEUTRAL_REVIEWS NUMBER,
    NEGATIVE_REVIEWS NUMBER,
    POSITIVE_PCT NUMBER(10,2),
    NEGATIVE_PCT NUMBER(10,2),
    COMPLAINT_SUMMARY STRING,
    STRENGTH_SUMMARY STRING,
    IMPROVEMENT_RECOMMENDATIONS STRING
);


INSERT INTO REVIEWSENSE_DB.ANALYTICS.DEMO_PRODUCT_SUMMARY
SELECT
    'B01G8JO5F2' AS ASIN,
    4526 AS TOTAL_REVIEWS,
    3.78 AS AVG_RATING,
    3037 AS POSITIVE_REVIEWS,
    372 AS NEUTRAL_REVIEWS,
    1117 AS NEGATIVE_REVIEWS,
    67.10 AS POSITIVE_PCT,
    24.68 AS NEGATIVE_PCT,
    'PASTE_COMPLAINT_SUMMARY_HERE' AS COMPLAINT_SUMMARY,
    'PASTE_STRENGTH_SUMMARY_HERE' AS STRENGTH_SUMMARY,
    'PASTE_IMPROVEMENT_RECOMMENDATIONS_HERE' AS IMPROVEMENT_RECOMMENDATIONS;

SELECT *
FROM REVIEWSENSE_DB.ANALYTICS.DEMO_PRODUCT_SUMMARY;


UPDATE REVIEWSENSE_DB.ANALYTICS.DEMO_PRODUCT_SUMMARY
SET
    COMPLAINT_SUMMARY = '在这里粘贴你刚才生成的 complaint summary',
    STRENGTH_SUMMARY = '在这里粘贴你刚才生成的 strength summary',
    IMPROVEMENT_RECOMMENDATIONS = '在这里粘贴你刚才生成的 improvement recommendations'
WHERE ASIN = 'B01G8JO5F2';

SELECT *
FROM REVIEWSENSE_DB.ANALYTICS.DEMO_PRODUCT_SUMMARY
WHERE ASIN = 'B01G8JO5F2';


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


WITH review_sample AS (
  SELECT LISTAGG(REVIEW_TEXT, ' || ') AS reviews_text
  FROM (
    SELECT REVIEW_TEXT
    FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_SENTIMENT
    WHERE ASIN = 'B01G8JO5F2'
    ORDER BY HELPFUL_VOTE DESC NULLS LAST, REVIEW_TS DESC
    LIMIT 12
  )
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'llama3.1-8b',
  'You are a product analyst. Based only on the following Amazon product reviews, provide 3 short business recommendations to improve this product. Focus on the most important product issues and customer experience improvements. Reviews: ' || reviews_text
) AS improvement_recommendations
FROM review_sample;


UPDATE REVIEWSENSE_DB.ANALYTICS.DEMO_PRODUCT_SUMMARY
SET
    COMPLAINT_SUMMARY = 'Here are three short bullet points summarizing the main customer complaints:

• **Poor Bluetooth connectivity and range**: Many customers experienced issues with Bluetooth connectivity, including dropped connections, stuttering music, and weak signals, especially when outdoors or in windy conditions.
• **Quality control and durability issues**: Several customers reported that their headphones stopped working after a short period of time, often due to water exposure, sweat, or general wear and tear. Some also mentioned that the ear hooks and earbuds themselves became loose or broke easily.
• **Decreased quality over time**: Some customers who purchased multiple pairs of these headphones noticed a significant decline in quality over time, including cheaper materials, looser ear hooks, and reduced sound quality.',

    STRENGTH_SUMMARY = 'Here are three short bullet points summarizing the main strengths customers appreciate about the Senso earbuds:

• **Excellent sound quality**: Customers rave about the earbuds'' sound quality, with many mentioning that it rivals that of more expensive headphones and earbuds. They praise the clear and rich sound, as well as the strong bass.
• **Comfort and fit**: The earbuds are designed to be comfortable and secure, with many customers mentioning that they fit well and don''t slip out of their ears. Some even mention that they can wear them for extended periods without noticing they''re there.
• **Long battery life and quick charging**: Customers appreciate the earbuds'' battery life, which is often reported to be around 8 hours or more, and the quick charging time, which can fully recharge the earbuds in under 2 hours.',

    IMPROVEMENT_RECOMMENDATIONS = 'Based on the Amazon product reviews, here are three business recommendations to improve the product:

1. **Improve the durability and quality of the ear hooks**: Several reviewers have mentioned that the ear hooks on the newer versions of the product are flimsy and prone to breaking or not staying in place. This is a major issue for a product that is designed for physical activity and is meant to be worn for extended periods of time. Consider redesigning the ear hooks to be more durable and adjustable, or using a different material that is more resistant to wear and tear.
2. **Address the Bluetooth connectivity issues**: Many reviewers have reported issues with the Bluetooth connectivity, including dropped connections, stuttering, and poor sound quality. This is a major issue for a product that relies on Bluetooth connectivity. Consider improving the Bluetooth technology used in the product or providing a more robust solution to address these issues.
3. **Improve the battery life and charging system**: Some reviewers have reported issues with the battery life, including the battery dying suddenly or not charging properly. Consider improving the battery life and charging system to ensure that the product can be used for extended periods of time without needing to be recharged. Additionally, consider providing a more reliable charging system that can handle different types of charging cables and power sources.

These recommendations are based on the most common issues reported by reviewers and can help to improve the overall quality and user experience of the product.'
WHERE ASIN = 'B01G8JO5F2';


SELECT *
FROM REVIEWSENSE_DB.ANALYTICS.DEMO_PRODUCT_SUMMARY
WHERE ASIN = 'B01G8JO5F2';