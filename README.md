# Natural Language Data Explorer & Self-Healing Pipeline

## Overview

An end-to-end cloud data pipeline and AI-driven data exploration system built on **Azure Databricks**, designed around three core aims:

1. **AI Chatbot for Analysts & Doctors** — Users such as analysts, doctors, and other healthcare professionals interact with a conversational chatbot. Behind the scenes, a multi-agent Text-to-SQL pipeline (Pipeline 2) converts their plain-English questions into Spark SQL, fetches results from Delta tables in Databricks, and applies **role-based PHI masking** — doctors can see patient identifiers in targeted queries, while analysts and other non-authorized users only see redacted/masked data.

2. **Fully Automated Ingestion & Self-Healing** — Pipeline 1 (Ingestion) and Pipeline 3 (AI Schema Advisor) operate automatically. When new Synthea CSV data arrives in Azure Storage (ADLS Gen2), Pipeline 1 detects it, normalizes columns, checks for schema drift, and writes Delta tables. If critical drift is detected (e.g. new or missing columns), Pipeline 3 is triggered automatically — it uses GPT-4o to recommend and apply schema fixes, then updates the canonical `master_schema.json`, all without manual intervention.

3. **Streamlit Data Explorer App** — A Streamlit web application deployed as a Databricks App serves as the user-facing interface. Users open the app, type natural language questions in the chatbot page, and receive data tables, summaries, and charts. The app also provides a dashboard page and an AI Advisor page for manual schema management.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        PIPELINE 1 — Ingestion                   │
│  Synthea CSVs (ADLS /raw/)                                      │
│     → Pre-flight schema check (master_schema.json)              │
│     → Normalize column names + cast types                       │
│     → Drift detection (NONE / INFO / WARNING / CRITICAL)        │
│     → Write Delta tables (Unity Catalog)                        │
│     → Log run audit + null stats                                │
│     → [CRITICAL drift] → trigger Pipeline 3 (blocking)          │
└────────────────────────────┬────────────────────────────────────┘
                             │ CRITICAL drift event (JSON)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PIPELINE 3 — AI Schema Advisor                 │
│  Receive drift event                                            │
│     → Call GPT-4o for remediation recommendation                │
│     → Validate proposed DDL (safety gate)                       │
│     → Autonomous: execute ALTER TABLE ADD COLUMNS               │
│     → Interactive: show HTML card → human approve/decline       │
│     → Update master_schema.json                                 │
│     → Return status to Pipeline 1 (fix_applied / advisory /     │
│       fix_declined / no_fix_needed / error)                     │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              PIPELINE 2 — Text-to-SQL Multi-Agent               │
│  User Question (natural language)                               │
│     → [Analyzer Agent]      parse intent + filters              │
│     → [Schema Mapper Agent] map to tables / join keys           │
│     → [SQL Generator Agent] generate Spark SQL + value sampling │
│     → [Executor Agent]      run via spark.sql() (safety gate)   │
│     → [Evaluator Agent]     sanity-check result credibility     │
│     → [Diagnostician Agent] root-cause analysis on failure      │
│     → [Human Agent]         final fallback after 3 retries      │
│     → Apply role-based PHI masking                              │
│     → Return results via Streamlit UI                           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Features

### Pipeline 1 — Self-Healing Ingestion
- Reads 9 Synthea CSV tables from ADLS Gen2 (`/raw/`) into Delta tables
- **inferSchema=False** for known tables — preserves drift columns before detection
- Normalizes column names, casts known date/type columns, computes null stats
- Drift detection severity levels: `NONE` → `INFO` → `WARNING` → `CRITICAL`
- On `CRITICAL` drift: blocks ingestion, calls Pipeline 3 synchronously (30-min timeout)
- On `WARNING` with new columns: calls Pipeline 3 for PHI classification before committing
- Handles subtractive drift with `mergeSchema` fallback (keeps missing column in Delta as NULL)
- Supports `dry_run` mode (detect drift without writing) and `table_filter` parameter
- Per-table and summary run reports with row counts, column counts, drift severity

### Pipeline 3 — AI Schema Advisor
- Triggered by Pipeline 1 or run manually in interactive mode
- Calls **GPT-4o** with a structured drift report; receives `SQL_FIX` + `NEW_JSON` + `REASONING`
- **DDL Safety Gate**: validates proposed SQL before execution
  - Autonomous mode: only `ALTER TABLE ... ADD COLUMNS (...)` is permitted
  - Interactive mode: allows `ALTER TABLE` or `CREATE OR REPLACE TABLE`
  - Blocks: `DROP`, `DELETE`, `UPDATE`, `INSERT`, `TRUNCATE`, `GRANT`, `REVOKE`, `MERGE`, subqueries
- Merges new column definitions into `master_schema.json` with correct PHI flags
- Flags missing columns as `source_status: "missing_from_upstream"` in the schema
- Interactive mode renders an HTML recommendation card (drift tables, DDL, reasoning) before executing
- Always exits cleanly via a single `dbutils.notebook.exit()` call (never crashes P1)

### Pipeline 2 — Text-to-SQL Multi-Agent System
- **LangGraph**-orchestrated pipeline with typed `AgentState` shared across all agents
- **Analyzer Agent** — classifies intent (`count` / `select` / `aggregate`) and extracts filters
- **Schema Mapper Agent** — maps intent to Synthea table names, columns, and join keys
- **SQL Generator Agent** — generates Spark SQL; samples actual column values to avoid wrong-value bugs
- **Executor Agent** — runs SQL via `spark.sql()` with a destructive-keyword safety regex
- **Evaluator Agent** — checks result credibility (e.g. COUNT returning multiple rows, empty results)
- **Diagnostician Agent** — classifies root cause (`schema` / `syntax` / `logic` / `unknown`), injects fix into retry context
- **Human Agent** — final fallback after 3 failed retries; surfaces query + SQL + errors for manual review
- Temperature 0 on first attempt, 0.3 on retries (deterministic first pass, variation on retry)

### PHI Masking System (`utils/02_phi_masking.py`)
Role-based masking applied **after** SQL execution, before results are returned to the user. Data is stored raw in Delta tables.

| Role | Access |
|---|---|
| `doctor` | Direct identifiers **visible** for ≤10-row targeted queries; masked for population queries |
| `analyst` | All `DIRECT_IDENTIFIER` + `QUASI_IDENTIFIER` columns masked |
| `sysadmin` | Same as analyst (structural access, no clinical PHI) |

Masking strategies defined per column in `master_schema.json`:

| Strategy | Description |
|---|---|
| `REDACT` | Replace with `***REDACTED***` |
| `FPE_UUID` | Deterministic SHA-256 UUID pseudonymisation — joins remain valid after masking |
| `FPE_DATE_SHIFT` | Patient-specific ±365-day date shift — temporal relationships preserved per patient |
| `GENERALIZE` | Reduce geographic granularity (`Springfield, MA` → `MA`) |
| `TRUNCATE_3DIG` | Keep first 3 characters (ZIP: `02134` → `021`) |
| `ROUND_2DP` | Round to 2 decimal places (lat/lon coordinates) |

### Streamlit Data Explorer (`ai-data-explorer/`)

A full-featured web app deployed as a **Databricks App**, with three pages accessible via a top navigation bar:

| Page | Description |
|---|---|
| **💬 Chatbot** | Natural language chat interface — type a question, get a data table or summary answer + expandable SQL |
| **📊 Dashboard** | Embedded analytics dashboard (PDF report rendered inline) |
| **🤖 AI Advisor** | Entry point for manually triggering the AI Schema Advisor (Pipeline 3) |

#### Chatbot Agent Pipeline (`core/pipeline.py`)

The app runs its own self-contained agent pipeline (separate from the notebook version) using **GPT-4o-mini** and the **Databricks SQL Connector** for live query execution:

1. **Intent Classifier** — determines if the message is a data query or casual chat; routes chit-chat to a friendly response directly
2. **Analyzer Agent** — parses structured intent (`count` / `select` / `aggregate`) + filters + `group_by`
3. **Table Picker Agent** — selects which of the 9 Synthea tables are needed from the intent
4. **Schema Fetcher Agent** — runs `DESCRIBE TABLE` live against Unity Catalog to get actual column names and types (not hardcoded)
5. **Schema Mapper Agent** — maps intent to specific columns and join keys from the live schema
6. **SQL Generator Agent** — generates fully-qualified Databricks SQL (`deepcatalog.delta_tables.<table>`); uses hardcoded sampled values for gender/race filters; extracts `LIMIT` from natural language ("top 10", "first 5")
7. **SQL Fix Agent** — on execution error, calls GPT-4o-mini with the original SQL + error message and retries once with the corrected SQL
8. **Summary Agent** — for count/aggregate queries, distills the result into a single plain-English sentence

Additional features:
- Dual destructive-keyword guard: blocks both natural language ("delete", "drop", "wipe") and SQL-level keywords before any query runs
- Auto-detects graphable results (≥2 columns with a numeric column) and offers a **📊 Show Chart** button
- Expandable **🔍 View SQL Query** panel on every assistant response
- Results capped at 50 rows by default; explicit user limits ("top 20") override the cap

#### App Requirements
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

#### Required Environment Variable
```
DATABRICKS_WAREHOUSE_ID   # Set in app.yaml for the Databricks App deployment
OPENAI_API_KEY            # OpenAI API key
```

---

## Project Structure

```
Group-5/
├── notebooks/                          # Production Databricks pipelines
│   ├── pipeline_1_ingestion.py         # Pipeline 1 — full ingestion script (single file)
│   ├── pipeline_3_advisor.py           # Pipeline 3 — full advisor script (single file)
│   ├── pipeline_1/                     # Pipeline 1 split into Databricks notebook cells
│   │   ├── 01_preflight_ingestion.py
│   │   ├── 02_write_tables.py
│   │   └── 03_finalize_ingestion.py
│   ├── pipeline_3/                     # Pipeline 3 split into Databricks notebook cells
│   │   ├── 01_intake_drift_event.py
│   │   ├── 02_generate_recommendation.py
│   │   ├── 03_validate_execution_plan.py
│   │   ├── 04_apply_ddl.py
│   │   ├── 05_update_metadata.py
│   │   ├── 06_finalize.py
│   │   └── 90_manual_review_apply.py
│   └── utils/                          # Shared utility modules (%run'd by all pipelines)
│       ├── 00_config.py                # Central config: paths, constants, catalog helpers
│       ├── 01_schema_utils.py          # Schema drift detection, column normalisation
│       ├── 02_phi_masking.py           # Role-based PHI masking (6 strategies, 3 roles)
│       ├── 03_openai_client.py         # GPT-4o client with retry + structured output
│       ├── 04_metadata_manager.py      # master_schema.json CRUD + cache management
│       ├── 05_pipeline1_state_manager.py
│       └── 06_advisor_policy.py        # Pure-Python advisor decision logic (unit-testable)
│
├── Text-to-SQL LLM/                    # Research & prototyping notebooks
│   ├── Multi-Agent Pipeline.ipynb      # Full LangGraph pipeline prototype
│   ├── Multi-Agent_Pipeline.ipynb
│   └── single agent trial.ipynb
│
├── ai-data-explorer/                   # Streamlit frontend (Databricks App)
│   └── healthcare-explorer_2026_04_01-07_07/
│       └── streamlit-data-app/
│           ├── app.py                  # App entry point
│           ├── core/                   # Backend: SQL execution, agent integration
│           └── pages/                  # Streamlit multi-page UI
│
├── tests/
│   └── test_pipeline3.py               # 25+ unit tests for Pipeline 3 pure-Python logic
│
├── data/
│   └── csv/                            # Synthea source CSV files (local copy)
│       ├── patients.csv
│       ├── encounters.csv
│       ├── conditions.csv
│       ├── medications.csv
│       ├── procedures.csv
│       ├── allergies.csv
│       ├── immunizations.csv
│       ├── observations.csv
│       ├── careplans.csv
│       └── all_csv_columns.json        # Column inventory for all 9 tables
│
├── docs/
│   ├── text-to-sql.md                  # Detailed implementation guide for Pipeline 2 agents
│   ├── initial plan.md                 # Original 3-phase project plan
│   └── Project 5.docx                  # Project brief
│
├── master_schema.json                  # Canonical schema: types, PHI flags, masking rules
├── csv_to_delta.py                     # Standalone local ingestion script (dev/testing)
├── csv_to_delta_databricks_azure.py    # Simplified Databricks ingestion script
├── DATABRICKS_SETUP.md                 # Full Azure + Databricks provisioning guide
└── requirements.txt                    # Python dependencies
```

---

## Data — Synthea Healthcare Tables

9 tables ingested from the [Synthea](https://synthea.mitre.org/) synthetic patient generator:

| Table | Description | Key Columns |
|---|---|---|
| `patients` | Patient demographics (FK anchor) | `Id`, `BIRTHDATE`, `DEATHDATE`, `GENDER`, `RACE`, `CITY`, `STATE` |
| `encounters` | Clinical visits | `Id`, `START`, `STOP`, `PATIENT`, `CODE`, `DESCRIPTION` |
| `conditions` | Diagnoses | `START`, `STOP`, `PATIENT`, `ENCOUNTER`, `CODE`, `DESCRIPTION` |
| `medications` | Prescriptions | `START`, `STOP`, `PATIENT`, `ENCOUNTER`, `CODE`, `DESCRIPTION` |
| `procedures` | Procedures performed | `DATE`, `PATIENT`, `ENCOUNTER`, `CODE`, `DESCRIPTION` |
| `allergies` | Allergy records | `START`, `STOP`, `PATIENT`, `ENCOUNTER`, `CODE`, `DESCRIPTION` |
| `immunizations` | Vaccination records | `DATE`, `PATIENT`, `ENCOUNTER`, `CODE`, `DESCRIPTION` |
| `observations` | Lab results & vitals | `DATE`, `PATIENT`, `ENCOUNTER`, `CODE`, `VALUE`, `UNITS`, `TYPE` |
| `careplans` | Care plan records | `START`, `STOP`, `PATIENT`, `ENCOUNTER`, `CODE`, `DESCRIPTION` |

Primary join key: `patients.Id = <table>.PATIENT` across all tables.

---

## `master_schema.json`

The single source of truth for the entire project. It drives:
- **Drift detection** — expected column names and types per table
- **PHI classification** — `phi: true/false`, `phi_type: DIRECT_IDENTIFIER / QUASI_IDENTIFIER`
- **Masking assignment** — which `masking` strategy to apply per column
- **Role-based access policies** — which `phi_type`s are masked per role
- **Missing column tracking** — `source_status: "missing_from_upstream"` set by Pipeline 3 when a column disappears from the source CSV

---

## Getting Started

### 1. Prerequisites
- Azure subscription with an ADLS Gen2 storage account
- Azure Databricks workspace (Unity Catalog enabled)
- OpenAI API key

### 2. Infrastructure Setup
Follow **[DATABRICKS_SETUP.md](DATABRICKS_SETUP.md)** for step-by-step provisioning:
1. Create Azure Resource Group and ADLS Gen2 storage account
2. Configure Databricks workspace (compute cluster, libraries, Unity Catalog)
3. Set up External Location (`abfss://` — no mounting required)
4. Create Secret Scope (`llm-scope`) with `openai-key` and `fpe-encryption-key`
5. Upload Synthea CSVs to ADLS `/raw/` container

### 3. Run the Ingestion Pipeline
Upload the `notebooks/` directory to your Databricks workspace and run:

```
pipeline_1_ingestion  (or pipeline_1/ notebooks in order: 01 → 02 → 03)
```

**Widget parameters:**

| Widget | Default | Description |
|---|---|---|
| `table_filter` | `all` | Comma-separated table names, or `all` |
| `dry_run` | `false` | `true` to detect drift without writing Delta tables |
| `on_critical_drift` | `halt` | `halt` (trigger P3) or `fallback` (proceed without enforcement) |

### 4. Run the Text-to-SQL UI
Deploy `ai-data-explorer/` as a Databricks App and open it in your browser.

### 5. Run Tests Locally

```bash
cd notebooks/utils
python -m pytest ../../tests/test_pipeline3.py -v
```

> Tests for Pipeline 3 run entirely locally — no Spark, Databricks, or Azure required.

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Cloud Platform** | Microsoft Azure |
| **Data Platform** | Azure Databricks, Unity Catalog |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake |
| **AI / LLM** | OpenAI GPT-4o |
| **Agent Framework** | LangGraph, LangChain |
| **Data Validation** | Pydantic v2 |
| **Data Processing** | PySpark |
| **Frontend UI** | Streamlit (Databricks Apps) |
| **Testing** | pytest |

---

## Security Notes

- Data is stored **raw** in Delta tables. PHI masking is applied at query time, not at rest.
- The FPE encryption key (`fpe-encryption-key`) must be stored in the Databricks Secret Scope before production use. Without it, the system falls back to a deterministic dev key with a warning — **do not use the dev key with real patient data**.
- The SQL executor blocks all destructive operations (`DROP`, `DELETE`, `UPDATE`, etc.) via a regex safety gate before any `spark.sql()` call.
- Pipeline 3 in autonomous mode is restricted to `ALTER TABLE ... ADD COLUMNS (...)` only — it cannot drop, rename, or truncate tables.