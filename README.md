# ReviewSense AI

**Product Intelligence Platform** — analyzes 200K+ Amazon Electronics reviews using Snowflake Cortex AI.

Combines structured analytics (Cortex Analyst), semantic search (Cortex Search), and custom agentic RAG tools to answer product research questions with verified data and real review evidence.

## Architecture

```
Streamlit Frontend → FastAPI Backend → Snowflake + Cortex AI
                                         ├── Cortex Analyst (NL → SQL)
                                         ├── Cortex Search (183K reviews indexed)
                                         ├── 8 Custom Agentic Tools
                                         ├── dbt Pipeline (17 models)
                                         └── Monitoring (105 alerts)
```

See [docs/architecture_diagram.md](docs/architecture_diagram.md) for the full system diagram.

## Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/RishzG/ReviewSenseV.git
cd ReviewSenseV
python -m venv venv
source venv/Scripts/activate  # Windows
pip install -r requirements.txt

# 2. Configure credentials
cp .env.example .env
# Edit .env with your Snowflake account, user, and private key path

# 3. Run dbt pipeline (if first time)
python run_dbt.py run

# 4. Start API
python -m uvicorn api.main:app --reload

# 5. Start Streamlit (separate terminal)
streamlit run streamlit_app.py
```

## Key Features

- **Intelligence Chat** — ask natural language questions, get answers from SQL data or review evidence
- **Category Explorer** — browse 14 product categories with sentiment charts and trend lines
- **Product Analysis** — lookup by ASIN with metadata, review stats, and category comparison
- **Business Intelligence** — structured reports with RED/YELLOW/GREEN signals
- **Monitoring & Alerts** — anomaly detection (rating drops, sentiment shifts, complaint spikes)
- **Custom Agentic Tools** — 8 tools: search reviews, product detail, search products, compare products, verify claims, brand analysis, find similar, price-value analysis

## Demo Questions

| Question | What it demonstrates |
|----------|---------------------|
| "Which product categories have the worst reviews?" | Structured → Cortex Analyst SQL |
| "What do people say about battery life in wireless earbuds?" | Semantic → Cortex Search RAG |
| "Compare Logitech vs Sony" | Agentic → brand analysis tools |
| "Is the SENSO headphone battery really 8 hours?" | Agentic → verify_claims tool |
| "Find waterproof headphones under $50" | Agentic → search_products + search_reviews |

## Tech Stack

- **Snowflake** — data warehouse, Cortex AI (SENTIMENT, CLASSIFY_TEXT, SUMMARIZE, COMPLETE)
- **dbt** — 17 models across staging, intermediate, gold, and monitoring layers
- **FastAPI** — 12 REST endpoints with Swagger docs
- **Streamlit** — 5-page frontend
- **Cortex Search** — hybrid vector + keyword search on 183K reviews
- **Cortex Analyst** — semantic model for natural language → SQL

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /query | Natural language question (main endpoint) |
| GET | /categories | List all 14 categories |
| GET | /categories/{cat} | Category detail with themes, complaints, trends |
| GET | /products/{asin} | Product stats (20+ reviews only) |
| POST | /compare | Compare categories |
| GET | /alerts | Monitoring alerts (filterable) |
| POST | /alerts/analyze | On-demand anomaly scan |
| GET | /report/category/{cat} | Category BI report |
| GET | /report/product/{asin} | Product BI report |
| GET | /health | System health check |

## Data

- **183,447 reviews** enriched with sentiment, themes, summaries
- **786,445 product metadata** records (McAuley dataset)
- **14 derived categories**, 10 review themes
- **12,028 ASINs** categorized, 407 with product names
- **25,408 ASINs** with real metadata (title, brand, price, features)

## Project Structure

```
api/                    FastAPI backend
  routers/              Endpoint handlers
  services/             Business logic
    tools.py            8 custom agentic tools
    agent_custom.py     Plan-Execute-Synthesize loop
    orchestrator.py     Query routing + fallbacks
    guardrails.py       Input/output safety
dbt_reviewsense/        dbt project
  models/
    staging/            Clean + filter
    intermediate/       Cortex AI enrichment
    gold/               Analytics marts
    monitoring/         Anomaly detection
docs/                   Architecture diagrams
eval/                   100 evaluation questions
tests/                  Unit + integration tests
streamlit_app.py        Frontend
```
