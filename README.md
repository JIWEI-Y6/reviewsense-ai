# ReviewSense AI — Product Intelligence Platform

ReviewSense AI is a comprehensive Product Intelligence Platform that turns 183,000+ Amazon Electronics reviews into actionable category, product, and theme-level insights. **Built entirely on Snowflake**, the application leverages the full power of the Data Cloud — secure storage, scalable compute, and Cortex AI — to answer product research questions with verified numbers, real review evidence, and trustworthy citations.

Ask in natural language. Get a grounded answer with the SQL, the reviews, or both.

## Authors

- **Rishik Ganta** — Architecture, Custom Agentic RAG, FastAPI Backend, Evaluation Framework
- **Jiwei Yang** — Data Engineering, Ingestion Pipeline, Dedup & Validation, Embedding Pipeline
- **Weijia Fang** — Streamlit Frontend, Monitoring & Alerts, Business Intelligence Reports, dbt Gold Marts

## Project Evolution

Over the past 3 months, ReviewSense AI has evolved from a single-dataset review-search prototype into an eight-phase production-minded intelligence platform: ingestion → enrichment → three-tier marts → semantic model → FastAPI agent → evaluation → monitoring → report generation.

> **[View Full Project Log (Google Drive)](https://docs.google.com/document/d/1PgmtL0T8xzkR1ZkozhBffg4XQEWABSMRIMTkdHpyn-U/edit?usp=sharing)** — weekly progress notes, decisions, and issues-&-fixes log.

---

## Problem

Customer reviews are highly unstructured and difficult to analyze manually at scale. Important signals such as recurring complaints, sentiment patterns, and product improvement opportunities are often buried in large volumes of raw review text.

---

## Solution

We built an end-to-end review intelligence system that combines data engineering, analytics, and AI to:

- Ingest and clean review data
- Transform raw reviews into structured analytical models
- Support analytics and insight generation
- Generate AI-powered summaries and business insights
- Deliver results through an interactive application interface

---

## System Architecture & Design

### Overall System Architecture

![System Architecture](docs/ReviewSenseAI%20System%20Architecture.png)

The application follows a three-tier architecture — Streamlit frontend calling a FastAPI backend, which orchestrates Snowflake Cortex services. All LLM inference and data storage stay inside Snowflake, so review text never leaves the platform during enrichment or generation.

### Data Flow & ELT Pipeline

Raw Parquet → `RAW.ELECTRONICS_REVIEWS_RAW` (VARIANT) → `CURATED.ELECTRONICS_REVIEWS` (typed) → dedup → `ANALYTICS.REVIEWS_FOR_GENAI` (183,457 rows) → dbt `staging` → `intermediate` (Cortex enrichment) → `gold` marts. LLM functions compute once at ELT time (not per query), enabling fast aggregation + filtering at serve time. Source Excalidraw: [docs/data_flow.excalidraw](docs/data_flow.excalidraw).

### Agentic Workflow Orchestration

![Agent Architecture](docs/ReviewsenseAgentArc.png)

The Custom Agent dispatches user questions to up to 5 tools per query, grouped into dependency waves that execute concurrently via `ThreadPoolExecutor`. Adaptive fallback: if a dependency returns empty, dependent steps are skipped. Reflection step verifies grounding against tool results before returning the answer.

### Evaluation Pipeline

![Evaluation Pipeline](docs/Reviewsense_Evaluation_pipeline.png)

Detailed flow: CLI invocation → stratified sampler → for-each-candidate-model loop → Custom Agent (plan/execute/synthesize/reflect, model-parameterized) → Scoring (SQL correctness + LLM-as-Judge on `mistral-large2`) → dual persistence to per-model JSON and `ANALYTICS.EVAL_RUNS` Snowflake table → `compare_models.py` renders the HTML report.

---

## Key Features

- **Intelligence Chat** — ask natural-language questions; the system chooses SQL, semantic search, or both, and returns a grounded answer with citations.
- **Three-Tier Aggregation** — **Category** (always valid), **Product** (20+ review ASINs only), **Theme × Category** (cross-cutting pain points).
- **Category Explorer** — 14 derived categories with sentiment charts, trend lines, top themes, and top complaints.
- **Product Analysis** — ASIN lookup with metadata, review stats, category comparison deltas, and verifiable claims.
- **Business Intelligence Reports** — category and product reports with RED / YELLOW / GREEN signals on avg_rating + negative_rate.
- **Monitoring & Alerts** — z-score-based anomaly detection across 5 dimensions (rating drops, sentiment shifts, complaint spikes, declining trends, rank drops) with weekly Snowflake Task.
- **Evaluation Dashboard** — multi-model bake-off results live in the product; winner-per-metric highlighted; KPIs tracked over time.
- **8 Custom Agentic Tools** — search reviews, product detail, search products, compare products, verify claims, brand analysis, find similar, price/value analysis.

---

## Tech Stack

- **Data Warehouse**: [Snowflake](https://www.snowflake.com/) — all compute runs here; dedicated `REVIEWSENSE_WH` warehouse.
- **AI / ML**: [Snowflake Cortex](https://docs.snowflake.com/en/guides-overview-ai-features) — `SENTIMENT`, `CLASSIFY_TEXT`, `SUMMARIZE`, `COMPLETE`, `EMBED_TEXT_768`.
- **Embedding Model**: `snowflake-arctic-embed-m-v1.5` (768-dim) — selected via MTEB benchmark against 9 Cortex embedding models.
- **LLM (default)**: `mistral-large` — chosen by **benchmarked bake-off**, not convention (see Evaluation Framework).
- **Transformations**: [dbt](https://www.getdbt.com/) with `dbt-snowflake` — 17 models across `staging`, `intermediate`, `gold`, `monitoring`.
- **Backend**: [FastAPI](https://fastapi.tiangolo.com/) — 12 REST endpoints, Pydantic schemas, Swagger at `/docs`.
- **Frontend**: [Streamlit](https://streamlit.io/) — 6-page responsive UI (Chat, Category Explorer, Product Analysis, BI, Monitoring, Evaluation Dashboard).
- **Authentication**: RSA key-pair (no passwords, no MFA friction).
- **Orchestration**: dbt scheduler + Snowflake Tasks (no Airflow — keeps compute inside Snowflake).
- **Version Control**: Git / GitHub.

---

## AI Models & Snowflake Cortex

This project leverages **Snowflake Cortex** to access state-of-the-art Large Language Models (LLMs) securely and efficiently within the Data Cloud. By using Cortex we ensure zero data exfiltration and low-latency inference — every LLM call runs against data already inside Snowflake.

**Models Used:**

| Model | Role |
|---|---|
| **Mistral Large** | Agent planning, answer synthesis, reflection (production default — benchmarked winner) |
| **Mistral Large 2** | **LLM-as-Judge** for evaluation — deliberately outside the candidate set to avoid self-preference bias |
| **Llama 3.1 70B** | Bake-off candidate — tied on quality, edges out on citation (viable swap) |
| **Snowflake Arctic** | Bake-off candidate — 2× faster but weaker on quality; kept as the latency-budget option |
| **snowflake-arctic-embed-m-v1.5** | 768-dim embedding model, powers Cortex Search Service |

Model choice is **config-flippable**: [api/config.py](api/config.py) `llm_model` — one line to switch production without code changes.

---

## Advanced Snowflake Integration

ReviewSense AI uses the full Snowflake ecosystem, not just the warehouse:

- **Snowflake Cortex Search** — hybrid semantic + keyword search over 183,447 reviews. `ENRICHED_REVIEW_SEARCH` service indexes cleaned review text with 7 filterable dimensions (ASIN, rating, category, theme, quality, verified, helpful-votes). Delivered a **12× latency improvement** over manual `VECTOR_COSINE_SIMILARITY` (8.2 s → 670 ms).
- **Snowflake Cortex Analyst** — natural language → verified SQL via a curated **semantic view** (`GOLD.REVIEWSENSE_ANALYTICS`). YAML semantic model covers 4 tables, 3 relationships, 5 verified queries. Prevents LLM fabrication of statistics.
- **Snowflake Cortex COMPLETE** — used at three controlled points in the agent loop: planning (tool selection), synthesis (final answer), and reflection (grounding verification). Every call is model-parameterized so the bake-off can swap LLMs per-request.
- **Snowflake Semantic View** — deployed via `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML`, with a stage-based YAML backup at `GOLD.SEMANTIC_STAGE`.
- **Snowflake Tasks** — weekly `GENERATE_ALERTS()` task runs the monitoring pipeline automatically; no Airflow needed.
- **Snowflake Streams + Stored Procedures** — power the alerting system: `ALERT_LOG` table, stream for change-tracking, stored procedure that calls `CORTEX.COMPLETE` for AI-generated alert summaries.

---

## Security, Privacy & Guardrails

ReviewSense AI prioritizes safety and data privacy through a multi-layered approach:

- **Input Guardrails** — block prompt injection signatures, detect off-topic queries, validate ASIN format, allow conversational follow-ups without false positives.
- **Output Guardrails** — PII stripping (email, phone, card numbers), HTML escape on review text before inclusion in RAG prompts (mitigates prompt injection via untrusted review content).
- **Rate Limiting** — `slowapi` on `/query` at 10 requests per minute per IP.
- **Request Timeouts** — 30 s on all Cortex Analyst API calls and every tool execution (ThreadPoolExecutor).
- **NULL Safety** — defensive casts in all router responses (`float(None) → 0.0`) to prevent schema-validation crashes.
- **Structured Error Handling** — global FastAPI exception handler with typed error responses; logging on every exception path.
- **Auth** — Snowflake connection via **RSA key-pair** (no passwords, no MFA friction). Private key is `.gitignore`d.
- **No Data Exfiltration** — all Cortex calls run inside Snowflake; review text never leaves the platform during enrichment or generation.

---

## Data Governance

The project follows strict governance standards enabled by Snowflake's platform:

- **Schema Contracts via dbt** — every model has a `schema.yml` with `not_null` and `accepted_values` tests on every enriched column. Run via `dbt test`.
- **Deterministic Transformations** — regex cleaning (HTML, URLs, whitespace), timestamp filtering (`WHERE YEAR(review_ts) <= 2026` — handles bad data), explicit type casts. No black-box LLM in the core ELT.
- **Immutable Source Layer** — `RAW` and `CURATED` schemas are append-only; downstream models read, never mutate.
- **Lineage** — 17-node dbt DAG with clear `staging → intermediate → gold → monitoring` boundaries. See [docs/data_flow.excalidraw](docs/data_flow.excalidraw).
- **Query History + Time Travel** — all Snowflake queries logged; 90-day table rollback available via `AT(OFFSET => -N)`.
- **Derived Column Provenance** — `product_lookup` uses `COALESCE(real_metadata, LLM-derived) + derivation_confidence` so every categorical attribute is traceable to its source (metadata vs. Cortex COMPLETE).

---

## Data Engineering

The backbone of ReviewSense AI is a robust ELT architecture built for reliability and scale:

- **Source**: 200K raw Amazon Electronics reviews in `RAW.ELECTRONICS_REVIEWS_RAW` (VARIANT, Parquet-backed). Plus 786K McAuley metadata records in `RAW.PRODUCT_METADATA_RAW`.
- **dbt Pipeline** — separation of concerns across 4 layers:
  - `staging/` — clean + type-cast + timestamp filter, materialized as views.
  - `intermediate/` — Cortex enrichment (sentiment, themes, summaries, product categorization via COMPLETE), materialized as tables because LLM calls are expensive.
  - `gold/` — analytics marts (category, product, theme-category, complaint analysis), product lookup joining real metadata + LLM-derived fallback.
  - `monitoring/` — 5 anomaly-detection models producing 105 alerts (20 HIGH, 23 MEDIUM, 62 LOW).
- **Cortex Enrichment (one-time, ~11 min on MEDIUM warehouse)** — 183,447 rows annotated with `SENTIMENT`, `CLASSIFY_TEXT` (10 themes), conditional `SUMMARIZE` (only `TEXT_LEN > 500` — saves credits where the review IS already the summary).
- **LLM Category Derivation** — `COMPLETE(mistral-large)` on 3 sample reviews per ASIN for 12,028 ASINs (~15 min); cleaned via `TRIM/LOWER/REPLACE/CASE` to canonical 14-category taxonomy.
- **Vector Search** — `snowflake-arctic-embed-m-v1.5` produces 768-dim embeddings for all 183,447 cleaned reviews; indexed by Cortex Search Service `ENRICHED_REVIEW_SEARCH` with 1-hour TARGET_LAG.
- **Orchestration** — dbt Cloud scheduler for periodic rebuilds; Snowflake Tasks for the weekly monitoring job. No Airflow — keeps orchestration inside the data platform.

---

## Project Structure

```
ReviewSenseAI/
├── api/                          FastAPI backend
│   ├── main.py                   App factory + CORS + error handlers
│   ├── config.py                 Pydantic settings (env-driven)
│   ├── db.py                     Snowflake connection manager
│   ├── models/                   Pydantic request/response schemas
│   ├── routers/                  Endpoint handlers (query, categories,
│   │                             products, reports, alerts, compare, health)
│   └── services/                 Business logic
│       ├── agent_custom.py       Plan → Execute → Synthesize → Reflect
│       ├── tools.py              8 custom agentic tools
│       ├── orchestrator.py       Query routing with legacy fallback
│       ├── analyst.py            Cortex Analyst client
│       ├── search.py             Cortex Search client
│       ├── synthesis.py          Legacy fallback synthesis path
│       ├── guardrails.py         Input/output safety
│       ├── monitoring.py         Alert generation + dbt integration
│       └── report.py             BI report generator
│
├── dbt_reviewsense/              dbt project
│   ├── profiles.yml              Snowflake connection (env-driven)
│   ├── dbt_project.yml           Project config
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/              stg_reviews (clean + filter)
│       ├── intermediate/         int_enriched_reviews, int_product_categories
│       ├── gold/                 product_lookup, enriched_reviews,
│       │                         7 aggregation marts
│       └── monitoring/           review_anomalies, cross_category_alerts,
│                                 emerging_themes, product_anomalies,
│                                 data_quality_checks
│
├── eval/                         Evaluation framework
│   ├── test_questions.py         100 stratified Q&A pairs
│   ├── run_eval.py               Bake-off harness (--models, --sample)
│   ├── compare_models.py         Comparison report generator
│   ├── eval_results_<model>.json Per-model run results
│   └── comparison_report.html    Visual bake-off report
│
├── sql_scripts/                  One-off DDL
│   ├── 01_setup.sql              → 12_rag_generation.sql (original pipeline)
│   ├── 13_monitoring_setup.sql
│   ├── 14_multi_category_setup.sql
│   ├── 15_migrate_electronics.sql
│   └── 16_eval_runs_ddl.sql      ANALYTICS.EVAL_RUNS + V_EVAL_MODEL_SUMMARY
│
├── scripts/                      Data-loading helpers
│   ├── ingest_category.py
│   ├── prep_metadata.py
│   └── scrape_metadata.py
│
├── docs/                         Architecture diagrams (PNG + source)
│   ├── ReviewSenseAI System Architecture.png
│   ├── ReviewsenseAgentArc.png
│   ├── Reviewsense_Evaluation_pipeline.png
│   └── data_flow.excalidraw      (source — no PNG rendered yet)
│
├── tests/                        pytest test suite (73 tests)
│   ├── test_agent_custom.py
│   ├── test_tools_*.py
│   ├── test_guardrails.py
│   └── test_smoke.py
│
├── streamlit_app.py              6-page frontend
├── run_dbt.py                    dbt wrapper with .env loading
├── deploy_semantic_model.py      Semantic view deployment
├── requirements.txt
├── .env.example                  Credential template
├── rsa_key.p8 / rsa_key.pub      Snowflake key-pair auth (gitignored)
└── CLAUDE.md                     Project context + design decisions
```

---

## Custom Agentic RAG

ReviewSense AI uses a **custom agent loop** — purpose-built for review-analysis queries, not a wrapper over an off-the-shelf framework. Cortex COMPLETE plans which tools to call, our Python code executes them in parallel waves, then COMPLETE synthesizes the final grounded answer.

### Agent Loop — Plan → Execute → Synthesize → Reflect

1. **Plan** *(1 LLM call)* — COMPLETE sees the question + 10 tool descriptions → outputs a JSON plan with up to 5 steps and dependency graph.
2. **Execute** *(0 LLM calls)* — Python runs tools from the plan in **dependency waves** via `ThreadPoolExecutor`. Pure SQL + Cortex Search; no LLM in the hot path. Adaptive: if a dependency returned empty, dependent steps are skipped.
3. **Synthesize** *(1 LLM call)* — COMPLETE generates the final answer grounded in all tool results, with citation rules for statistics and review quotes.
4. **Reflect** *(1 LLM call, advisory)* — grounding-verification check: "are all claims in the answer supported by tool outputs?" Returns a `reflection` object but never blocks the response.

**Total: 2–3 LLM calls per query.** Safety: max 5 plan steps, 30 s timeout per tool, 45 s total budget, citation required.

### Tiered Routing

- **Tier 1 — Fast Path** *(0 LLM calls)*: rule-based for simple queries (ASIN lookup, stat questions). Ultra-low latency.
- **Tier 2 — LLM Planning**: the full 4-step loop. Used for complex, multi-tool queries.

### Tool Registry (8 Custom Tools)

| Tool | Purpose |
|---|---|
| `search_reviews` | Cortex Search with 7 filters (ASIN, rating, category, theme, quality, verified, limit) |
| `get_product_detail` | 4-table join: product_lookup + product_sentiment + category_sentiment + enriched_reviews; includes category comparison deltas |
| `search_products` | SQL over metadata + gold marts; filter by price, brand, features, category, rating |
| `compare_products` | Per-ASIN detail + metric deltas + winner identification + theme comparison |
| `verify_claims` | For each feature claim in metadata, search reviews + COMPLETE verdict (`CONFIRMED` / `DISPUTED` / `MIXED`). Trust-score output. **Model-parameterized** for bake-off end-to-end fairness |
| `get_brand_analysis` / `compare_brands` | Brand stats from metadata + reviews; category breakdown, top products per brand |
| `find_similar_products` | Uses McAuley `also_buy` metadata cross-references |
| `price_value_analysis` | Price-bracket vs sentiment correlation within a category |

All tools have integration tests (73 passing).

### Conversation Memory

Session-level context tracking: follow-up detection via keyword signals (*"it"*, *"them"*, *"how about"*), resolution of pronouns into product/brand/category context from prior turns. Guardrails adapt — off-topic check skipped for mid-conversation follow-ups.

### What Makes This Original Work

- Tool implementations (SQL + Python logic) — **ours**
- Agent planning prompt + plan-execute-synthesize loop — **ours**
- Claim verification algorithm — **ours**
- Product recommendation matching logic — **ours**
- Brand competitive analysis SQL — **ours**
- Conversation memory + session context — **ours**
- Snowflake provides: `COMPLETE` (LLM inference), Cortex Search (retrieval), Cortex Analyst (NL → SQL)

---

## Evaluation Framework — Cortex LLM Bake-Off

Rigorous multi-model evaluation with **zero tolerance for self-graded results**.

### What We Measure

| Category | Metric | Source |
|---|---|---|
| **Correctness** | Intent accuracy, Data correctness (5% numeric tolerance), Hallucination rate | Ground-truth SQL comparison |
| **Answer Quality** | Factuality, Completeness, Citation quality, Context utilization (1–5 each) | LLM-as-Judge |
| **Operational** | P50 / P95 / P99 latency, Cost per query, Fallback rate | Runtime telemetry |
| **Agent Behavior** | Tool utilization histogram, Parallel waves per query | Agent trace |

### Bias Control — Non-Candidate Judge

The LLM-as-Judge is **`mistral-large2`**, deliberately outside the candidate set (`mistral-large`, `llama3.1-70b`, `snowflake-arctic`). This eliminates the 3–10% self-preference inflation that happens when a model judges its own output. Using `mistral-large` as both candidate and judge would have rigged the bake-off in its favor — we specifically built the infrastructure to prevent that.

### Bake-Off Pipeline

See [docs/eval_pipeline.excalidraw](docs/eval_pipeline.excalidraw) for the detailed diagram.

1. **Eval Runner** — stratified sampler (preserves 30 / 40 / 30 ratio of category / product / theme questions).
2. **For each candidate model** — POST `/query` with `model` override; agent runs end-to-end with the candidate, including `verify_claims` (the only model-aware tool).
3. **Scoring** — dual-axis: SQL correctness check (5% numeric tolerance + category/ASIN overlap + answer-text fallback match) plus LLM-as-Judge across 4 dimensions.
4. **Persistence** — results land in both per-model JSON files *and* `ANALYTICS.EVAL_RUNS` (Snowflake), enabling regression tracking across commits.
5. **Comparison Report** — `compare_models.py` renders `comparison_report.html` with summary table (winner bold per column) and per-case tool trace.

### Bake-Off Results (20-Q stratified smoke)

| Model | Intent % | Data % | Factuality | Citation | Hallu % | P95 s |
|---|---|---|---|---|---|---|
| **mistral-large** *(default)* | **75.0** | 90.0 | **5.00** | 4.85 | 0 | 47.6 |
| llama3.1-70b | 70.0 | 90.0 | **5.00** | **4.95** | 0 | 41.4 |
| snowflake-arctic | 65.0 | 90.0 | 4.80 | 4.30 | 5 | **21.1** |

**Verdict:** mistral-large wins intent accuracy by 5 pp and ties on factuality. Llama3.1-70b is a viable swap (slightly better citation). Arctic is the budget / latency option. Production default unchanged — now benchmark-backed.

Full results in the **Evaluation Dashboard** page of the Streamlit app.

---

## Getting Started

### Prerequisites

- Python 3.11 or newer
- Snowflake account with Cortex enabled (`mistral-large`, `mistral-large2`, `llama3.1-70b`, `snowflake-arctic`)
- Role with `OWNERSHIP` on the user (for RSA key setup — one-time)
- ~8 GB disk (for dataset + dbt compiled targets)

### Installation

1. **Clone**
   ```bash
   git clone https://github.com/RishzG/ReviewSenseV.git
   cd ReviewSenseV
   ```

2. **Virtual environment**
   ```bash
   python -m venv venv
   source venv/Scripts/activate   # Windows (git bash)
   # source venv/bin/activate     # macOS / Linux
   pip install -r requirements.txt
   ```

3. **Snowflake credentials — RSA key-pair** *(no password, no MFA friction)*
   ```bash
   openssl genrsa -out rsa_key.p8 2048
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   # Then in Snowflake: ALTER USER <you> SET RSA_PUBLIC_KEY='<contents of rsa_key.pub without BEGIN/END lines>';
   ```

4. **Environment variables**
   ```bash
   cp .env.example .env
   # Edit .env:
   #   SNOWFLAKE_ACCOUNT=your-account
   #   SNOWFLAKE_USER=your-user
   #   SNOWFLAKE_PRIVATE_KEY_PATH=./rsa_key.p8
   #   SNOWFLAKE_ROLE=TRAINING_ROLE
   #   SNOWFLAKE_WAREHOUSE=REVIEWSENSE_WH
   #   SNOWFLAKE_DATABASE=REVIEWSENSE_DB
   ```

5. **Run dbt pipeline** *(Cortex enrichment takes ~11 min on MEDIUM)*
   ```bash
   python run_dbt.py run
   python run_dbt.py test    # optional — run schema tests
   ```

### Running the App

Start the API (terminal 1):
```bash
python -m uvicorn api.main:app --reload --port 8000
```

Start Streamlit (terminal 2):
```bash
streamlit run streamlit_app.py
```

Open the app at **http://localhost:8501**. Swagger docs at **http://localhost:8000/docs**.

### Running the Bake-Off

```bash
# Smoke (20 questions × 3 models, ~15 min)
python -m eval.run_eval --models mistral-large llama3.1-70b snowflake-arctic --sample 20

# Full run (100 questions × 3 models, ~60-90 min)
python -m eval.run_eval --models mistral-large llama3.1-70b snowflake-arctic

# Generate comparison report
python -m eval.compare_models
# Opens eval/comparison_report.html
```

Results are persisted to `ANALYTICS.EVAL_RUNS`. Query via:
```sql
SELECT * FROM ANALYTICS.V_EVAL_MODEL_SUMMARY ORDER BY AVG_FACTUALITY DESC;
```

---

## Usage Guide

1. **Intelligence Chat** — ask questions like *"Which product categories have the worst reviews?"* or *"What do people say about battery life in wireless earbuds?"*. The agent picks SQL, semantic search, or both.
2. **Category Explorer** — pick a category → see sentiment, top themes, top complaints, 12-month trend.
3. **Product Analysis** — enter an ASIN → product stats, category comparison deltas, review evidence. Works best for ASINs with 20+ reviews.
4. **Business Intelligence** — generate category or product reports with RED / YELLOW / GREEN business signals.
5. **Monitoring & Alerts** — view alerts by severity, filter by source (anomaly, emerging theme, product, data quality), acknowledge alerts.
6. **Evaluation Dashboard** — inspect the latest bake-off run: KPI scorecard, per-model comparison table with winner highlighting, grouped bar charts, key takeaways.

### Demo Queries

| Question | What it demonstrates |
|---|---|
| *"Which product categories have the worst reviews?"* | Structured → Cortex Analyst SQL |
| *"What do people say about battery life in wireless earbuds?"* | Semantic → Cortex Search RAG |
| *"Compare Logitech vs Sony"* | Agentic → brand analysis tools |
| *"Is the SENSO headphone battery really 8 hours?"* | Agentic → `verify_claims` tool |
| *"Find waterproof headphones under $50"* | Agentic → `search_products` + `search_reviews` |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/query` | Natural language question (main endpoint; accepts optional `model` override for bake-off) |
| `POST` | `/query/stream` | SSE streaming variant |
| `GET` | `/categories` | List all 14 derived categories |
| `GET` | `/categories/{cat}` | Category detail — themes, complaints, 12-month trend |
| `GET` | `/products/{asin}` | Product stats (20+ review ASINs only) |
| `POST` | `/compare` | Cross-category comparison |
| `GET` | `/report/category/{cat}` | BI report (narrative + signals + evidence) |
| `GET` | `/report/product/{asin}` | Product BI report |
| `GET` | `/alerts` | Monitoring alerts (filterable by severity, source, ack status) |
| `POST` | `/alerts/analyze` | On-demand anomaly scan |
| `PATCH` | `/alerts/{id}/acknowledge` | Acknowledge an alert |
| `GET` | `/health` | System health check |

Full schemas in [api/models/requests.py](api/models/requests.py) and [api/models/responses.py](api/models/responses.py).

---

## Data Profile

- **183,447 reviews** (after dedup from 200K raw)
- **102,583 unique ASINs** (avg < 2 reviews / ASIN — extreme long-tail; product-level aggregation gated to 20+ review ASINs)
- **786,445 McAuley metadata records** (24.8% overlap with review ASINs = 25,408 enriched)
- **14 derived categories**, **10 review themes**, **12,028 categorized ASINs**, **407 ASINs with derived product names**
- **Date range**: 2001-10-26 to present (bad timestamps filtered in staging)
- **Rating distribution**: 64% five-star, ~13.5% one or two stars — long-tail positivity skew documented
- **Review quality tiers**: 500+ chars → summarize; 150–499 → medium; <150 → low-quality flag

---

## Testing

```bash
# Full suite (73 tests)
pytest tests/

# Excluding HTTP smoke (if API not running)
pytest tests/ --ignore=tests/test_smoke.py
```

Covers: agent planning, every custom tool, guardrails, orchestration, numeric parsing, SQL injection sinks.

---

## Issues Encountered & Fixes

A full log of challenges and fixes — authentication (MFA / SAML / OAuth all failed → RSA key-pair), dbt schema naming (`SILVER_SILVER` bug → custom macro), `CLASSIFY_TEXT` output format (no confidence score → dropped column), LLM category cleanup (verbose COMPLETE responses → regex + CASE/LIKE normalization), Cortex Analyst semantic view relationships (composite-key rejection → refactored join), `SEARCH_PREVIEW` API signature changes, shared warehouse (COMPUTE_WH locked → dedicated `REVIEWSENSE_WH`). See [CLAUDE.md](CLAUDE.md) "Issues Encountered & Fixes" for root-cause details.

---

## License

[MIT License](LICENSE) — free to use, modify, and distribute with attribution.

---

## Contributing

This is a Northeastern MSIS capstone-grade project. Issues and pull requests welcome. For major changes, open an issue first to discuss the change.

---

**ReviewSense AI** — 183K reviews, 14 categories, 8 custom tools, 2 LLM calls per query, 0% fabricated statistics.
