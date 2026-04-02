# Project 5 – Natural Language Data Explorer
### Technical Documentation

---

## 1. Overview

A multi-agent Text-to-SQL system that allows business users to query Synthea healthcare data using plain English. Built on Azure Databricks with a Streamlit chat UI deployed as a Databricks App.

---

## 2. Tech Stack

| Layer | Technology |
|---|---|
| Cloud | Microsoft Azure |
| Data Platform | Azure Databricks (Unity Catalog) |
| Storage | Azure Blob Storage (ADLS Gen2) |
| Data Format | Delta Tables |
| Orchestration | LangGraph |
| LLM Framework | LangChain + OpenAI GPT-4o-mini |
| UI Framework | Streamlit (Databricks App) |
| Data Validation | Pydantic v2 |
| SQL Execution | Databricks SQL Connector |

---

## 3. Architecture

```
User (Natural Language Query)
        ↓
Databricks App (Streamlit UI)
        ↓
[Multi-Agent Pipeline — core/pipeline.py]
        ↓
Data Intent Classifier
        ↓
Analyzer Agent          → Parses query intent, filters, group-by
        ↓
Table Picker Agent      → Selects relevant tables from available list
        ↓
Schema Fetcher Agent    → Runs DESCRIBE TABLE on selected tables (live schema)
        ↓
Schema Mapper Agent     → Picks relevant columns from live schema
        ↓
SQL Generator Agent     → Generates Spark SQL
        ↓
Executor                → Runs SQL via Databricks SQL Connector
        ↓ (on error)
SQL Fix Agent           → Auto-corrects SQL on failure (1 retry)
        ↓
Summary Agent           → Generates natural language answer
        ↓
Streamlit UI            → Displays result (table / summary / chart)
```

---

## 4. Agent Details

| Agent | Input | Output |
|---|---|---|
| Data Intent Classifier | User query | `data` or `chat` |
| Analyzer | User query | Intent type, filters, group-by, complexity |
| Table Picker | Intent | List of table names needed |
| Schema Fetcher | Table names | Live column names + data types via `DESCRIBE TABLE` |
| Schema Mapper | Intent + live schema | Relevant columns + join keys |
| SQL Generator | Intent + schema + sampled values | Valid Spark SQL |
| Executor | SQL | Pandas DataFrame |
| SQL Fix | Failed SQL + error | Corrected SQL (1 retry) |
| Summary | Query + SQL + result | One-sentence natural language answer |

---

## 5. Data

- **Source:** Synthea synthetic healthcare dataset
- **Format:** CSV files → ingested into Delta tables
- **Catalog:** `deepcatalog`
- **Schema:** `delta_tables`
- **Tables:** `patients`, `conditions`, `medications`, `observations`, `encounters`, `procedures`, `careplans`, `allergies`, `immunizations`
- **Scale:** ~8M+ total records across all tables

---

## 6. Data Pipeline (Ingestion)

- CSV files stored in Azure Blob Storage container
- Accessed via Unity Catalog External Volume
- Ingested using PySpark with:
  - Column name normalization (lowercase, no special chars)
  - Date column casting via `try_cast`
  - Delta format write with `overwrite` mode
- Data quality issues fixed pre-ingestion:
  - Extra field rows removed (patients.csv — 152 rows)
  - Invalid UUID reformatting (patients.csv — 90 IDs)

---

## 7. App Structure

```
streamlit-data-app/
├── app.py               # Entry point, navbar routing
├── app.yaml             # Databricks App config + env vars
├── requirements.txt     # Python dependencies
├── core/
│   └── pipeline.py      # Full multi-agent pipeline
└── pages/
    ├── chatbot.py        # Chat UI page
    ├── dashboard.py      # Analytics dashboard (PDF embed)
    └── ai_advisor.py     # AI Advisor page (WIP)
```

---

## 8. Key Design Decisions

- **Live schema fetching** — `DESCRIBE TABLE` runs at query time instead of hardcoded schema, so column changes in Databricks are automatically reflected
- **Join keys hardcoded** — Databricks Unity Catalog does not store foreign key constraints, so join relationships are maintained as a static mapping
- **Sampled values hardcoded** — key categorical columns (`gender`, `race`) are pre-defined to avoid per-query Spark jobs
- **No retry loop** — single SQL fix attempt on failure; avoids infinite loops and latency
- **Safety gate** — destructive SQL keywords and natural language destructive intent both blocked before any agent runs
- **Auto LIMIT** — all queries capped at 50 rows unless user specifies a number

---

## 9. Security

- OpenAI API key stored as Databricks App environment variable
- Databricks Warehouse ID injected via `app.yaml` resource binding
- App service principal granted `SELECT` on `deepcatalog.delta_tables`
- Destructive command blocking at input level (regex + NL pattern match)

---

## 10. Dependencies

```
databricks-sql-connector~=3.4.0
databricks-sdk~=0.33.0
streamlit~=1.38.0
pandas~=2.2.3
langchain
langchain-openai
pydantic
streamlit-option-menu
```

---

## 11. Limitations

- Schema Fetcher adds 1–2 SQL calls per query (one `DESCRIBE TABLE` per needed table)
- Complex multi-table joins may produce incorrect SQL on first attempt — handled by SQL Fix Agent
- LLM may misclassify ambiguous queries as `chat` vs `data`
- No authentication on the Streamlit UI beyond Databricks App access controls
