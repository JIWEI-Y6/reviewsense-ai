# CLAUDE.md — ReviewSense AI

## What This Project Is

ReviewSense AI is a Product Intelligence Platform that analyzes 200K+ Amazon Electronics reviews using Snowflake and Cortex AI. It combines structured analytics (Cortex Analyst) with semantic search (Cortex Search Service) through an intelligent query router to answer product research questions.

This is a resume-grade project, not a class demo. Every architectural decision should be defensible, auditable, and production-minded.

## Snowflake Environment

```
Account:    pgb87192
User:       FINCH
Role:       TRAINING_ROLE
Warehouse:  REVIEWSENSE_WH (dedicated, created for this project)
Database:   REVIEWSENSE_DB
Auth:       RSA key-pair (rsa_key.p8 / rsa_key.pub)
```

COMPUTE_WH is a shared warehouse — do NOT alter its size. Use REVIEWSENSE_WH for all project work.

Credentials are in `.env` (never commit this). Connection uses **key-pair auth** (no password/MFA):

```python
import snowflake.connector
from dotenv import load_dotenv
import os
load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),       # pgb87192
    user=os.getenv("SNOWFLAKE_USER"),             # FINCH
    private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
    role=os.getenv("SNOWFLAKE_ROLE"),             # TRAINING_ROLE
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),   # REVIEWSENSE_WH
    database=os.getenv("SNOWFLAKE_DATABASE"),     # REVIEWSENSE_DB
)
```

## Architecture Overview

Three retrieval paths, one orchestrator:

- **Structured Path** → Cortex Analyst (natural language → SQL over aggregate marts)
- **Semantic Path** → Cortex Search Service (hybrid vector + keyword + structured filters)
- **Synthesis Path** → Both paths combined, merged by Cortex COMPLETE

The orchestrator classifies user intent and routes to the correct path(s).

The system is **category-and-theme-centric**, not product-centric. With 102K ASINs averaging <2 reviews each, product-level analysis is only meaningful for the ~300 ASINs with 20+ reviews. Category-level and theme-level analysis is always statistically valid.

## Data Profile

```
Total reviews:           183,457 (after dedup from 200K)
Unique ASINs:            102,583
Unique users:            32,858
Date range:              2001-10-26 to present (some bad timestamps, year 33579+)
Avg rating:              4.21 / 5
Rating distribution:     ★1: 16,040 · ★2: 8,826 · ★3: 13,536 · ★4: 27,680 · ★5: 117,375
                         (64% are 5-star, ~13.5% are 1-2 star)
Verified purchases:      151,390 (82.5%)
Reviews with votes:      47,153 (25.7%), avg 6.02 votes when present
Text length:             min 20, median 189, avg 365, p75 418, max 29,809 chars
```

### Critical Data Constraints

**No product metadata.** Only ASINs — no product names, categories, brands, or prices.
- Phase 1 derives product categories + names from review text using Cortex COMPLETE
- Two-level derivation: Category (all ASINs with 3+ reviews) + Name/Brand (ASINs with 20+ reviews)
- Derived categories are the primary dimension for all aggregation and filtering

**Extreme long-tail.** 102K ASINs, avg <2 reviews/product.
- Product-level aggregation only for ASINs with 20+ reviews (~300 products)
- Category-level aggregation is always meaningful (14 categories, thousands of reviews each)
- Anomaly detection only viable at category/theme level + top ~20 ASINs with 100+ reviews

**Short reviews.** Median 189 chars (~2 sentences).
- SUMMARIZE only for TEXT_LEN > 500 (reviews worth summarizing)
- Review quality tier: high (500+), medium (150-499), low (<150)
- CLASSIFY_TEXT runs on all but low-quality classifications are flagged
- RAG retrieval prefers high-quality reviews for context

**Bad timestamps.** Some review_ts values are far-future (year 33579+).
- dbt staging model MUST filter: WHERE YEAR(review_ts) <= 2026

**Rating skew.** 64% are 5-star.
- Negative review analysis works with ~25K reviews (rating 1-2)
- Document the skew, don't treat it as a bug

### Top Products by Volume (Good for Testing/Demos)

| ASIN | Reviews | Avg Rating |
|------|---------|------------|
| B01G8JO5F2 | 4,526 | 3.78 |
| B00ZV9RDKK | 551 | 4.33 |
| B079QHML21 | 465 | 4.57 |
| B01DFKC2SO | 370 | 4.24 |
| B0791TX5P5 | 317 | 4.53 |

### Derived Product Category Taxonomy

```
headphones_earbuds, speakers, streaming_devices, smart_home, cables_adapters,
chargers_batteries, phone_accessories, computer_peripherals, storage_media,
cameras_accessories, tv_accessories, gaming_accessories, wearables, other_electronics
```

## Complete Data Lineage

```
RAW.ELECTRONICS_REVIEWS_RAW (200K rows, VARIANT/Parquet)
  └─► CURATED.ELECTRONICS_REVIEWS (200K rows, typed + cleaned)
        └─► CURATED.V_ELECTRONICS_REVIEWS_DEDUP (deduped by REVIEW_ID)
              └─► ANALYTICS.V_REVIEWS_GENAI_BASE (explicit type casting)
                    └─► ANALYTICS.REVIEWS_FOR_GENAI (183,457 rows, +TEXT_LEN)
                          ├─► ANALYTICS.V_REVIEWS_CLEAN (regex, WHERE YEAR <= 2026)
                          │     └─► ANALYTICS.REVIEWS_CLEAN_FOR_EMBEDDINGS (183,447 rows)
                          │           ├─► ANALYTICS.REVIEW_EMBEDDINGS (183,447 + VECTOR(FLOAT,768))
                          │           └─► ANALYTICS.REVIEW_SEARCH (Cortex Search Service, ACTIVE)
                          │                 └─► RAG Generation (Consumer Q&A + Biz Analyst)
                          ├─► ANALYTICS.REVIEW_INSIGHTS (200 rows — POC) [Jiwei]
                          │
                          └─► [NEW] dbt pipeline
                                ├─► SILVER.stg_reviews (staging view)
                                ├─► SILVER.int_enriched_reviews (Cortex enrichment)
                                ├─► SILVER.int_product_categories (derive categories via COMPLETE)
                                ├─► GOLD.product_lookup (ASIN → category + name + confidence)
                                ├─► GOLD.enriched_reviews (enriched + product lookup joined)
                                ├─► GOLD.category_sentiment_summary (Tier 1 aggregation)
                                ├─► GOLD.category_monthly_trends (Tier 1 time-series)
                                ├─► GOLD.theme_category_analysis (Tier 3 cross-cutting)
                                ├─► GOLD.product_sentiment_summary (Tier 2, 20+ reviews only)
                                └─► GOLD.complaint_analysis (category-level complaints)
```

## Database Layout — All Objects

```
REVIEWSENSE_DB
├── RAW                                     ← IMMUTABLE
│   ├── ELECTRONICS_REVIEWS_RAW             (Table — VARIANT, 200K rows)
│   ├── ELECTRONICS_STAGE                   (Internal Stage — Parquet source)
│   ├── V_ELECTRONICS_REVIEWS_RAW           (View — raw passthrough)
│   └── V_ELECTRONICS_RAW_KEYS             (View — key extraction)
│
├── CURATED                                 ← IMMUTABLE
│   ├── ELECTRONICS_REVIEWS                 (Table — typed, 200K rows)
│   │   Columns: REVIEW_ID, ASIN, USER_ID, RATING, TITLE, TEXT,
│   │            VERIFIED_PURCHASE, HELPFUL_VOTE, REVIEW_TS, CATEGORY
│   ├── V_ELECTRONICS_REVIEWS_DEDUP         (View — deduped, ROW_NUMBER) [Jiwei]
│   ├── V_ELECTRONICS_REVIEWS               (View — clean select)
│   ├── V_ELECTRONICS_REVIEWS_TEXT          (View — review_id + concat text)
│   └── V_ELECTRONICS_NEGATIVE_REVIEWS      (View — rating <= 2)
│
├── ANALYTICS                               ← Existing AI/ML layer
│   ├── V_REVIEWS_GENAI_BASE               (View — type casting) [Jiwei]
│   ├── REVIEWS_FOR_GENAI                   (Table — 183,457 rows, +TEXT_LEN) [Jiwei]
│   │   *** CENTRAL TABLE — all downstream flows from here ***
│   ├── V_REVIEWS_BASE                      (View — ORPHAN, unused downstream)
│   ├── V_REVIEWS_CLEAN                     (View — regex cleaning + WHERE YEAR <= 2026)
│   ├── REVIEWS_CLEAN_FOR_EMBEDDINGS        (Table — 183,447 rows)
│   ├── REVIEW_EMBEDDINGS                   (Table — 183,447 + VECTOR(FLOAT,768))
│   ├── REVIEW_SEARCH                       (Cortex Search Service — ACTIVE)
│   │   Search: REVIEW_TEXT_CLEAN
│   │   Filterable: ASIN, RATING, VERIFIED_PURCHASE, HELPFUL_VOTE, REVIEW_TS
│   │   TARGET_LAG: 1 hour
│   └── REVIEW_INSIGHTS                    (Table — 200 rows, POC) [Jiwei]
│       Columns: REVIEW_ID, ASIN, RATING, REVIEW_TS, REVIEW_TEXT,
│                SENTIMENT_LABEL, REVIEW_SUMMARY
│
├── SILVER                                  ← dbt staging + intermediate (BUILT)
│   ├── STG_REVIEWS                         (View — cleaned, filtered, quality-tiered)
│   ├── INT_ENRICHED_REVIEWS                (Table — 183,447 rows, SENTIMENT + CLASSIFY_TEXT + SUMMARIZE)
│   ├── INT_ENRICHED_REVIEWS_SAMPLE         (Table — 100-row validation sample)
│   └── INT_PRODUCT_CATEGORIES              (Table — 12,028 ASINs categorized via COMPLETE)
│
└── GOLD                                    ← dbt marts + Cortex Analyst (BUILT)
    ├── PRODUCT_LOOKUP                      (Table — 12,028 ASINs → 14 categories)
    ├── ENRICHED_REVIEWS                    (Table — 183,447 rows, main fact table)
    ├── CATEGORY_SENTIMENT_SUMMARY          (Table — 14 rows, Tier 1)
    ├── CATEGORY_MONTHLY_TRENDS             (Table — 1,715 rows, Tier 1 time-series)
    ├── PRODUCT_SENTIMENT_SUMMARY           (Table — 407 rows, Tier 2, 20+ reviews only)
    ├── THEME_CATEGORY_ANALYSIS             (Table — 138 rows, Tier 3)
    ├── COMPLAINT_ANALYSIS                  (Table — 118 rows)
    ├── REVIEWSENSE_ANALYTICS               (Semantic View — for Cortex Analyst)
    └── SEMANTIC_STAGE                      (Stage — YAML backup)
```

## What's Already Built (Do NOT Rebuild)

- Data ingestion: 200K reviews → Parquet → RAW
- Curated layer: Typed, cleaned, timestamp handling, NULL filtering
- Deduplication: ROW_NUMBER dedup keeping most recent + longest text [Jiwei]
- Type-safe GenAI base: Explicit casting view [Jiwei]
- Materialized GenAI table: 183,457 rows with TEXT_LEN [Jiwei]
- Text cleaning: Regex pipeline (HTML, markup, URLs, whitespace, 8K truncation)
- Embeddings: 183,447 rows with snowflake-arctic-embed-m-v1.5 (768-dim)
- Cortex Search Service: ANALYTICS.REVIEW_SEARCH — hybrid search, TARGET_LAG 1 hour
- Manual vector search: 8.2s → 670ms = 12x improvement with Cortex Search
- RAG generation: Consumer Q&A + Business Analyst modes with mistral-large
- AI Insights POC: 200 rows with SENTIMENT_LABEL + REVIEW_SUMMARY [Jiwei]
- Validation: Row counts (~183,447), zero-contamination checks

## Implementation Phases

### Phase 1: Data Enrichment via dbt ✅ COMPLETE
- 183,447 reviews enriched with SENTIMENT, CLASSIFY_TEXT, conditional SUMMARIZE (~11.5 min on MEDIUM warehouse)
- 12,028 ASINs categorized into 14 categories via COMPLETE(mistral-large) (~15 min)
- 100-row sample validated first: sentiment correlates with rating, theme distribution diverse (only 2% "other")
- 7 gold mart tables built (seconds, pure SQL aggregations)
- LLM category output cleaned: stripped newlines, extracted category names from verbose responses via CASE/LIKE

### Phase 2: Cortex Analyst + Search Upgrade ✅ COMPLETE
- Semantic model YAML with 4 tables, 3 relationships, 5 verified queries
- Deployed as Semantic View via SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML
- Stage-based YAML also uploaded as backup to GOLD.SEMANTIC_STAGE
- Cortex Analyst generates verified SQL from natural language (tested: category rankings, complaint themes, sentiment trends)

### Phase 3: FastAPI Backend ✅ COMPLETE
- API with intent-based orchestrator: classifies → routes to structured/semantic/synthesis path
- Structured path: Cortex Analyst REST API → generates SQL → executes → returns data
- Semantic path: Cortex Search (SEARCH_PREVIEW) → RAG with COMPLETE → returns answer + source reviews
- Synthesis path: runs both, merges via COMPLETE
- Endpoints: POST /query, GET /categories, GET /categories/{category}, GET /products/{asin}, POST /compare, GET /health
- Swagger docs at /docs, Pydantic request/response schemas
- Agentic RAG upgrade (IN PROGRESS):
  - Cortex Agent API (`/api/v2/cortex/agent:run`) integrated with claude-4-sonnet as orchestrator
  - 7 tools registered: cortex_analyst, cortex_search, + 5 gold mart UDFs
  - New enriched search service: GOLD.ENRICHED_REVIEW_SEARCH (filterable by category, theme, quality)
  - Product names derived for 407 ASINs via COMPLETE (BRAND + PRODUCT_NAME in PRODUCT_LOOKUP)
  - Agent API returns 400 on tool config (error 392700 — semantic model validation mismatch)
  - **WORKAROUND**: Legacy orchestrator fallback works end-to-end (Analyst + Search + Synthesis)
  - **TODO**: Debug Agent API tool_resources config — likely needs semantic_view instead of semantic_model_file, or YAML format mismatch between stage file and agent expectations
  - Streaming endpoint `/query/stream` built but blocked until agent API works
  - Guardrails: input validation (injection, off-topic, ASIN detection), output PII stripping

### Phase 4: Evaluation Framework ✅ COMPLETE
- 100 Q&A pairs: 30 category-level, 40 high-volume product, 30 theme-level
- Ground truth: SQL for structured, manual annotation for semantic
- Metrics: intent accuracy, retrieval precision@10, answer faithfulness, numerical accuracy
- Results: ~92% intent accuracy, ~67% data correctness (before agent upgrade)
- 38 errors were caused by COMPLETE temperature param (not available on account) — fixed
- 2 errors from guardrail false positives on ASIN-only questions — fixed

### Phase 5: Monitoring Agent ✅ COMPLETE
- 5 dbt monitoring models: review_anomalies (94 anomalies), cross_category_alerts (6), emerging_themes (18), product_anomalies (1), data_quality_checks (9)
- Anomaly types: RATING_DROP, SENTIMENT_SHIFT, COMPLAINT_SPIKE, DECLINING_TREND, RANK_DROP
- Z-score based thresholds (adapts to each category's variance)
- Snowflake objects: ALERT_LOG table, stream, GENERATE_ALERTS() stored procedure, weekly scheduled task
- AI-generated alert summaries via CORTEX.COMPLETE
- API endpoints: GET /alerts, POST /alerts/analyze, PATCH /alerts/{id}/acknowledge
- 105 alerts generated: 20 HIGH, 23 MEDIUM, 62 LOW severity

### Phase 5b: Report Generation & Business Intelligence ✅ COMPLETE
- Category + product report generation via gold marts + Cortex Search evidence + COMPLETE narrative
- RED/YELLOW/GREEN business signals from pre-computed negative_rate + avg_rating
- API: GET /report/category/{category}, GET /report/product/{asin}
- Stats comparison vs overall/category average with delta indicators

### Phase 6: Streamlit Frontend ✅ COMPLETE
- 5 pages: Intelligence Chat, Category Explorer, Product Analysis, Business Intelligence, Monitoring & Alerts
- Dark theme with purple/cyan/rose Plotly charts
- Chat with markdown rendering, review cards with color-coded ratings
- BI reports with signal badges, theme/complaint charts, customer evidence
- Alert dashboard with severity filtering, on-demand analysis

### Phase 7: Product Metadata Enrichment ✅ COMPLETE
- McAuley Electronics metadata loaded: 786,445 products into RAW.PRODUCT_METADATA_RAW (VARIANT)
- CURATED.PRODUCT_METADATA view extracts: ASIN, title, brand, price, category, features, description, rank
- ASIN overlap: 25,408 of 102K review ASINs (24.8%) have real metadata
- PRODUCT_LOOKUP updated: COALESCE(real metadata, LLM-derived) fallback pattern
- 3,323 of 12K categorized ASINs enriched with real title, brand, price, features
- CURATED.V_BRAND_SUMMARY view for brand-level analysis
- Scraping script for 282 missing top-product ASINs (Amazon product pages)

### Phase 8: Custom Agentic RAG (IN PROGRESS)
Custom agent loop with purpose-built tools — NOT a wrapper over Snowflake's Agent API.
COMPLETE plans which tools to call, our code executes them, COMPLETE synthesizes the answer.

**Architecture: Hybrid Plan-then-ReAct**
- Step 1 (PLAN): COMPLETE sees question + 10 tool descriptions → outputs JSON plan (1 LLM call)
- Step 2 (EXECUTE): Python runs tools from plan, adapts if results empty (0 LLM calls — pure SQL/Search)
- Step 3 (SYNTHESIZE): COMPLETE generates grounded answer from all tool results (1 LLM call)
- Total: 2 LLM calls per query. Safety: max 5 steps, 45s timeout, citation required.

**Tools built (with tests):**
1. `search_reviews` ✅ — Cortex Search with 7 filters (ASIN, rating, category, theme, quality, verified, limit). 12/12 tests passing.
2. `get_product_detail` ✅ — 4-table join: product_lookup + product_sentiment + category_sentiment + enriched_reviews. Category comparison deltas. 7/7 tests passing.
3. `search_products` ✅ — SQL on metadata + gold marts. Filter by price, brand, features, category, rating. Sort by review_count/rating/price/sentiment. 12/12 tests passing.
4. `compare_products` ✅ — Calls get_product_detail per ASIN. Computes metric deltas, identifies winners, aggregates win counts, theme comparison. 8 tests.

**Tools planned:**
5. `verify_claims` — EXTRACT_ANSWER + Search + COMPLETE verdict. Compare metadata feature claims vs review reality.
6. `get_brand_analysis` + `compare_brands` — brand-level competitive intelligence from metadata + reviews.
7. `find_similar_products` — uses also_buy metadata cross-references.
8. `price_value_analysis` — price brackets vs sentiment correlation within categories.

**Agent loop (planned):**
9. Plan-Execute-Synthesize loop in `api/services/agent_custom.py`
10. Evaluation: unit tests per tool + agent integration tests + 20-30 new eval questions

**Additional features built:**
- Conversation memory: follow-up detection via keyword signals, session context tracking (products/brands/categories)
- Question resolution: "Should I get them?" → "Regarding product B01G8JO5F2: Should I get them?"
- ASIN auto-detection in search queries for product-specific filtering
- Analyst refusal → semantic search fallback
- Tool trace display in Streamlit (shows which tools were called, what they returned)
- Guardrails adapt: skip off-topic check for mid-conversation follow-ups

**What makes this original work:**
- Tool implementations (SQL + Python logic) — ours
- Agent planning prompt + loop — ours
- Claim verification algorithm — ours
- Product recommendation matching — ours
- Brand competitive analysis — ours
- Conversation memory + session context — ours
- Snowflake provides: COMPLETE (LLM), Cortex Search, Cortex Analyst

## Tech Stack

- **Data Warehouse**: Snowflake (all compute runs here)
- **AI/ML**: Snowflake Cortex (SENTIMENT, CLASSIFY_TEXT, SUMMARIZE, COMPLETE, EMBED_TEXT_768)
- **Embedding Model**: snowflake-arctic-embed-m-v1.5 (768-dim, selected via MTEB benchmark)
- **LLM**: mistral-large (RAG generation, intent classification)
- **Transformations**: dbt-snowflake
- **API**: FastAPI (REST endpoints, Swagger docs, Pydantic schemas)
- **Orchestration**: dbt Cloud scheduler + Snowflake Tasks (no Airflow)
- **Frontend**: Streamlit (calling FastAPI backend)
- **Version Control**: Git/GitHub

## dbt Configuration

### Source Definition
```yaml
# models/staging/sources.yml
sources:
  - name: analytics
    database: REVIEWSENSE_DB
    schema: ANALYTICS
    tables:
      - name: REVIEWS_FOR_GENAI
        description: >
          Primary source. 183,457 deduplicated, typed reviews with TEXT_LEN.
          Created by teammate (Jiwei). Do NOT modify.
        columns:
          - name: REVIEW_ID
          - name: ASIN
          - name: USER_ID
          - name: RATING
          - name: TITLE
          - name: REVIEW_TEXT
          - name: VERIFIED_PURCHASE
          - name: HELPFUL_VOTE
          - name: REVIEW_TS
          - name: CATEGORY
          - name: TEXT_LEN
```

### dbt Conventions
- **Profile name**: `reviewsense`
- **profiles.yml**: uses `env_var()` from `.env` — never hardcode credentials
- **Target database**: `REVIEWSENSE_DB`
- **Source**: `REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI` — the 183K-row table
- **Staging models** (`stg_`): Views in SILVER. Column renaming, text concat, regex cleaning, timestamp filter (YEAR <= 2026).
- **Intermediate models** (`int_`): Tables in SILVER. Cortex enrichment + product category derivation. Tables because LLM functions are expensive.
- **Gold models** (no prefix): Tables in GOLD. 3-tier aggregations, product lookup, marts for Cortex Analyst.
- **Tests**: Every enriched column needs schema.yml with not_null and accepted_values tests.

### Cortex Function Patterns
```sql
-- Sentiment (cheap, run on all rows)
SNOWFLAKE.CORTEX.SENTIMENT(review_text_clean) AS sentiment_score

-- Theme classification (run on all, flag low-quality)
SNOWFLAKE.CORTEX.CLASSIFY_TEXT(review_text_clean,
    ['battery_life', 'build_quality', 'sound_quality', 'connectivity',
     'comfort', 'value_for_money', 'customer_service', 'durability',
     'ease_of_use', 'other']) AS review_theme

-- Conditional summarize (only reviews worth summarizing)
CASE
    WHEN TEXT_LEN > 500 THEN SNOWFLAKE.CORTEX.SUMMARIZE(review_text_clean)
    ELSE review_text_clean
END AS review_summary

-- Review quality tier
CASE
    WHEN TEXT_LEN >= 500 THEN 'high'
    WHEN TEXT_LEN >= 150 THEN 'medium'
    ELSE 'low'
END AS review_quality

-- Always test on LIMIT 100 first before full 183K run
```

### Gold Mart Aggregation Tiers

**Tier 1 — Category-Level** (always statistically valid, 14 categories):
- category_sentiment_summary
- category_monthly_trends

**Tier 2 — Product-Level** (only ASINs with 20+ reviews, ~300 products):
- product_sentiment_summary (with HAVING COUNT(*) >= 20)

**Tier 3 — Theme × Category Cross-Analysis** (unique insight):
- theme_category_analysis ("biggest pain points in headphones?")

### Cortex Analyst Scope
Semantic model covers Tier 1 + Tier 3 marts ONLY.
Product-level lookups go through direct SQL in the orchestrator, NOT through Analyst.
This keeps the semantic model dense and reliable.

### Product Category Derivation
```sql
-- For each ASIN with 3+ reviews, use COMPLETE on 3 sample reviews to derive category
-- Validate on top 100 ASINs first (manual spot-check 20)
-- Add derivation_confidence: high (50+ reviews), medium (10-49), low (3-9)
-- Only include high+medium in Cortex Analyst semantic model
```

## Review Theme Categories

Standard categories for ALL CLASSIFY_TEXT calls, Search filters, and Analyst semantic model:

```
battery_life, build_quality, sound_quality, connectivity, comfort,
value_for_money, customer_service, durability, ease_of_use, other
```

## Existing SQL Scripts (Reference Only)

| Script | Purpose |
|--------|---------|
| 01_setup.sql | Created DB, schemas, file format, stage |
| 03_curated.sql | CTAS from raw → typed CURATED.ELECTRONICS_REVIEWS |
| 04_analytics_prep.sql | Created ANALYTICS schema + V_REVIEWS_BASE |
| 05_views.sql | Utility views across RAW, CURATED, ANALYTICS |
| 06_cleaning_embeddings_prep.sql | V_REVIEWS_CLEAN regex cleaning view |
| 07_validation_checks.sql | Row counts, contamination checks |
| 08_create_embeddings.sql | Materialized clean text + 183K embeddings |
| 09_similarity_search.sql | Manual VECTOR_COSINE_SIMILARITY demo |
| 10_cortex_search_service.sql | Created ANALYTICS.REVIEW_SEARCH |
| 11_cortex_search_test.sql | Tested with SEARCH_PREVIEW() |
| 12_rag_generation.sql | Consumer Q&A + Business Analyst RAG modes |

Teammate-created objects (not in scripts):
- CURATED.V_ELECTRONICS_REVIEWS_DEDUP [Jiwei]
- ANALYTICS.V_REVIEWS_GENAI_BASE [Jiwei]
- ANALYTICS.REVIEWS_FOR_GENAI (183,457 rows) [Jiwei]
- ANALYTICS.REVIEW_INSIGHTS (200 rows, POC) [Jiwei]

## Key Design Decisions (Defensible in Interviews)

1. **Why snowflake-arctic-embed-m-v1.5?** — Benchmarked all 9 Cortex embedding models against MTEB. Best English retrieval in 768-dim class.
2. **Why Cortex Search over manual vector search?** — 12x performance (8.2s → 670ms). Managed, auto-refresh, hybrid semantic+keyword.
3. **Why Cortex Analyst for stats, not RAG?** — LLM fabricated statistics. Analyst generates verified SQL.
4. **Why pre-compute enrichment in dbt?** — LLM functions expensive. Compute once, query instantly. Enables aggregation + filtering.
5. **Why regex cleaning, not LLM?** — Deterministic, auditable, zero per-query cost at 183K rows.
6. **Why source from REVIEWS_FOR_GENAI?** — Dedup + type casting already done upstream. Don't redo validated work.
7. **Why conditional SUMMARIZE (500+ chars)?** — Median review is 189 chars. Summarizing 2 sentences wastes credits and adds no value.
8. **Why derive product categories via LLM?** — No metadata in dataset. Extract from review text. Validate before trusting.
9. **Why category-centric, not product-centric?** — 102K ASINs, avg <2 reviews. Product-level stats are noise for 99.5%. Category-level is always meaningful.
10. **Why 3-tier aggregation?** — Category (always valid), product (20+ reviews only), theme×category (unique cross-cutting insight).
11. **Why FastAPI as the core, not Streamlit?** — API-first architecture. The product is the intelligence service, not a UI. Swagger docs, testable, any frontend can consume it.
12. **Why Analyst only on category/theme marts?** — Product mart is too sparse (102K rows, 99% with <5 reviews). Analyst needs dense, reliable tables.

## Issues Encountered & Fixes

### MFA / Authentication
- Snowflake account uses Google Authenticator (TOTP) for MFA
- `username_password_mfa` authenticator sends push, doesn't accept inline TOTP
- `externalbrowser` authenticator failed — Snowflake account has no SAML/SSO configured
- OAuth token approach failed — session tokens are not OAuth tokens
- **Fix**: RSA key-pair auth. Generated rsa_key.p8/rsa_key.pub, ran `ALTER USER FINCH SET RSA_PUBLIC_KEY=...`. No MFA needed ever again.
- TRAINING_ROLE has OWNERSHIP on user FINCH, enabling the ALTER USER

### dbt Schema Naming
- dbt appends `+schema` to the default schema, producing `SILVER_SILVER`
- **Fix**: Custom `generate_schema_name` macro that uses the custom schema name directly

### Source Column Names
- CLAUDE.md originally documented column as `TEXT`, but actual table has `REVIEW_TEXT`
- `TEXT` is also a reserved word in Snowflake
- **Fix**: Queried `DESCRIBE TABLE ANALYTICS.REVIEWS_FOR_GENAI` to get real column names, updated staging model

### CLASSIFY_TEXT Output Format
- Expected JSON with `label` + `score` fields
- Actual output: `{"label": "..."}` only — no confidence score
- **Fix**: Removed THEME_CONFIDENCE column from all models (intermediate, gold, schema.yml)

### LLM Category Derivation Cleanup
- COMPLETE returned categories with leading newlines, quotes, and verbose text like "the category for this product is: cables_adapters"
- **Fix**: product_lookup.sql cleans with TRIM/REPLACE/LOWER, then CASE/LIKE extraction to map to canonical 14 categories. Also maps edge cases (e.g., "tablets" → computer_peripherals, "networking_device" → computer_peripherals)

### Cortex Analyst Semantic View Relationships
- `COMPLAINTS_TO_THEMES` relationship on composite key (DERIVED_CATEGORY + REVIEW_THEME) rejected — Snowflake requires referenced key to be primary/unique
- **Fix**: Changed to `COMPLAINTS_TO_SENTIMENT` joining only on DERIVED_CATEGORY to CATEGORY_SENTIMENT (which has unique categories)
- Dimension names in YAML must match the `name` field, not the raw column — renamed all to `DERIVED_CATEGORY` for consistency

### Cortex Search SEARCH_PREVIEW API
- Called with 3 args (service, query, limit) — function only accepts 2
- **Fix**: Moved `limit` and `columns` into the JSON query parameter

### Shared Warehouse
- COMPUTE_WH is shared across teams — should not ALTER its size
- **Fix**: Created dedicated REVIEWSENSE_WH for this project. Used MEDIUM for LLM runs, can scale to XSMALL for SQL-only work

## What NOT to Do

- Don't hardcode Snowflake credentials — always use .env
- Don't modify anything in RAW, CURATED, or existing ANALYTICS objects — immutable
- Don't re-implement deduplication or type casting — already done by Jiwei
- Don't delete REVIEW_INSIGHTS (200-row POC) — keep for validation comparison
- Don't rebuild Cortex Search Service until Phase 1 is complete
- Don't run LLM functions at query time for pre-computable things
- Don't use RAG for quantitative questions — route to Cortex Analyst
- Don't add Airflow — use dbt scheduler + Snowflake Tasks
- Don't skip dbt tests on enriched columns
- Don't aggregate products with <20 reviews — statistics meaningless at low volume
- Don't SUMMARIZE reviews under 500 chars — the review IS the summary
- Don't assume product_category or product_title exist — only ASIN is available, categories are DERIVED
- Don't ignore bad timestamps — filter WHERE YEAR(review_ts) <= 2026 in staging
- Don't put product-level mart in Cortex Analyst — too sparse, use direct SQL
- Don't trust derived categories without validation — spot-check top 100 ASINs first
- Don't start frontend before FastAPI backend is working — API-first, always

## Teammate

Jiwei Yang handles data validation, quality assessment, and shares presentation responsibilities. Built: dedup view, type-safe GenAI base, REVIEWS_FOR_GENAI table, 200-row REVIEW_INSIGHTS POC. System designed for live code demos, not slides.

## Project Files

### Planning Docs
- `ReviewSense_AI_Implementation_Plan.md` — Original 7-phase plan (being updated)
- `ReviewSense_Required_Changes.md` — Data-driven changes to the plan
- `ReviewSense_Updated_Architecture.md` — FastAPI + Next.js architecture (Option A reference)
- `ReviewSense_Local_Setup_Guide.md` — Step-by-step local setup

### Code
- `dbt_reviewsense/` — Full dbt project (profiles, models, macros, schema tests)
- `api/` — FastAPI backend (main, config, db, routers, services, Pydantic models)
- `tests/` — Test suite (pytest)
- `run_dbt.py` — Wrapper to run dbt commands with .env loaded
- `deploy_semantic_model.py` — Deploys semantic model YAML to Snowflake
- `.env.example` — Template for Snowflake credentials
- `requirements.txt` — Python dependencies
- `rsa_key.p8` / `rsa_key.pub` — RSA key-pair for Snowflake auth (gitignored)
