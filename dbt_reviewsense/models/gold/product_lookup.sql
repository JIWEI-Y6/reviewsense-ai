-- Product lookup: ASIN → derived category + confidence
-- Cleans LLM output: strips whitespace, extracts category from verbose responses

{{ config(materialized='table') }}

WITH raw_categories AS (
    SELECT
        ASIN,
        REVIEW_COUNT,
        DERIVATION_CONFIDENCE,
        -- Clean: lowercase, strip whitespace/newlines, remove quotes
        TRIM(REPLACE(REPLACE(LOWER(DERIVED_CATEGORY), '\n', ''), '"', '')) AS CLEANED
    FROM {{ ref('int_product_categories') }}
),

extracted AS (
    SELECT
        ASIN,
        REVIEW_COUNT,
        DERIVATION_CONFIDENCE,
        CLEANED,
        -- Extract valid category name from verbose LLM responses
        CASE
            WHEN CLEANED IN ('headphones_earbuds', 'speakers', 'streaming_devices',
                'smart_home', 'cables_adapters', 'chargers_batteries', 'phone_accessories',
                'computer_peripherals', 'storage_media', 'cameras_accessories',
                'tv_accessories', 'gaming_accessories', 'wearables', 'other_electronics')
                THEN CLEANED
            WHEN CLEANED LIKE '%headphones_earbuds%' THEN 'headphones_earbuds'
            WHEN CLEANED LIKE '%speakers%' THEN 'speakers'
            WHEN CLEANED LIKE '%streaming_devices%' THEN 'streaming_devices'
            WHEN CLEANED LIKE '%smart_home%' THEN 'smart_home'
            WHEN CLEANED LIKE '%cables_adapters%' THEN 'cables_adapters'
            WHEN CLEANED LIKE '%chargers_batteries%' THEN 'chargers_batteries'
            WHEN CLEANED LIKE '%phone_accessories%' THEN 'phone_accessories'
            WHEN CLEANED LIKE '%computer_peripherals%' THEN 'computer_peripherals'
            WHEN CLEANED LIKE '%storage_media%' THEN 'storage_media'
            WHEN CLEANED LIKE '%cameras_accessories%' OR CLEANED LIKE '%camera_accessories%' THEN 'cameras_accessories'
            WHEN CLEANED LIKE '%tv_accessories%' THEN 'tv_accessories'
            WHEN CLEANED LIKE '%gaming_accessories%' THEN 'gaming_accessories'
            WHEN CLEANED LIKE '%wearables%' THEN 'wearables'
            WHEN CLEANED LIKE '%other_electronics%' THEN 'other_electronics'
            -- Common misclassifications
            WHEN CLEANED LIKE '%tablet%' THEN 'computer_peripherals'
            WHEN CLEANED LIKE '%networking%' THEN 'computer_peripherals'
            ELSE 'other_electronics'
        END AS DERIVED_CATEGORY
    FROM raw_categories
)

-- Clean product names: strip "Brand - " prefix, newlines, verbose text
, cleaned_names AS (
    SELECT
        ASIN,
        -- Clean the raw name: strip newlines, trim
        TRIM(REPLACE(RAW_PRODUCT_NAME, '\n', '')) AS CLEAN_RAW,
        -- Extract brand and product name from "Brand - Product Name" format
        -- Handle cases like "Brand - Amazon - Echo Dot" by removing "Brand - " prefix first
        CASE
            WHEN TRIM(REPLACE(RAW_PRODUCT_NAME, '\n', '')) LIKE 'Brand -%'
            THEN TRIM(REPLACE(RAW_PRODUCT_NAME, '\n', ''))
            ELSE TRIM(REPLACE(RAW_PRODUCT_NAME, '\n', ''))
        END AS NORMALIZED
    FROM {{ ref('int_product_names') }}
),

final_names AS (
    SELECT
        ASIN,
        -- Remove "Brand - " and "Product Name: " prefixes
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(NORMALIZED, '^Brand\\s*-\\s*', ''),
                'Product Name:\\s*', ''
            ),
            '^Based on.*$', ''
        ) AS CLEANED_NAME
    FROM cleaned_names
),

parsed_names AS (
    SELECT
        ASIN,
        CASE
            WHEN CLEANED_NAME LIKE '%-%' AND LENGTH(CLEANED_NAME) < 100
            THEN TRIM(SPLIT_PART(CLEANED_NAME, '-', 1))
            ELSE 'Unknown'
        END AS BRAND,
        CASE
            WHEN CLEANED_NAME LIKE '%-%' AND LENGTH(CLEANED_NAME) < 100
            THEN TRIM(SUBSTR(CLEANED_NAME, POSITION('-' IN CLEANED_NAME) + 1))
            WHEN LENGTH(CLEANED_NAME) < 100
            THEN TRIM(CLEANED_NAME)
            ELSE NULL
        END AS PRODUCT_NAME
    FROM final_names
)

-- Deduplicate metadata (some ASINs appear twice in the source)
, deduped_metadata AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY PRICE DESC NULLS LAST) AS rn
        FROM {{ source('curated', 'PRODUCT_METADATA') }}
    )
    WHERE rn = 1
)

SELECT
    e.ASIN,
    e.DERIVED_CATEGORY,
    e.REVIEW_COUNT,
    e.DERIVATION_CONFIDENCE,
    COALESCE(m.BRAND, p.BRAND) AS BRAND,
    COALESCE(m.TITLE, p.PRODUCT_NAME) AS PRODUCT_NAME,
    m.TITLE AS METADATA_TITLE,
    m.BRAND AS METADATA_BRAND,
    m.PRICE AS METADATA_PRICE,
    m.FEATURES_TEXT AS METADATA_FEATURES,
    m.CATEGORY_PATH AS METADATA_CATEGORY_PATH,
    CASE WHEN m.ASIN IS NOT NULL THEN TRUE ELSE FALSE END AS HAS_METADATA
FROM extracted e
LEFT JOIN parsed_names p ON e.ASIN = p.ASIN
LEFT JOIN deduped_metadata m ON e.ASIN = m.ASIN
