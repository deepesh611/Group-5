# Project 5 – Natural Language Data Explorer & Self-Healing Pipeline

## Tech Stack

| Component         | Choice                          |
|-------------------|---------------------------------|
| Platform          | Azure Databricks                |
| Storage           | Delta Lake                      |
| Language          | PySpark + Python                |
| LLM               | OpenAI GPT-4 (API)             |
| Data Source        | Synthea Healthcare (CSV)       |
| Self-Healing Mode | AI Advisor (human approval)     |

---

## What We're Building

A system where business users can ask healthcare data questions in plain English and get results back — no SQL knowledge needed. On top of that, a monitoring layer that detects when source data changes (schema drift) and uses AI to suggest fixes, keeping the pipeline alive without manual intervention.

**Architecture:**
```
User Question → LLM → SQL → spark.sql() → Results
                                    ↓
              Schema Monitor → Drift Detected → LLM Advisor → Human Approval → Fix Applied
```


## Phase 1 — Data Engineering

Get the foundation right first. Everything else depends on clean, queryable data.

1. Download Synthea CSV data (patients, encounters, conditions, medications, procedures, observations, immunizations, allergies, careplans)
2. Upload to Azure Blob Storage / DBFS
3. Read with PySpark — clean column names, standardize types, handle nulls
4. Write each dataset as a **Delta table**
5. Validate with sample queries to confirm everything loaded correctly

**Output:** A set of production-ready Delta tables we can query against.

---

## Phase 2 — AI Data Explorer (Text-to-SQL)

Now that data is in place, build the natural language query engine.

1. **Extract schema** — pull table names, column names, and data types from the Delta tables using `spark.catalog` / `DESCRIBE TABLE`
2. **Build the prompt** — design a system prompt that injects the full schema context so GPT-4 knows exactly what's available
3. **Generate SQL** — send the user's natural language question + schema to GPT-4, get back a SQL query
4. **Guardrails** — validate the generated SQL (block destructive operations like `DROP`, `DELETE`, `UPDATE`)
5. **Execute** — run the SQL via `spark.sql()`, return results as a formatted DataFrame
6. **Error handling** — catch invalid SQL, empty results, timeouts gracefully

**Output:** A working pipeline where a user types "How many patients were diagnosed with diabetes?" and gets a table of results.

---

## Phase 3 — Schema Monitoring & AI Advisor (Self-Healing)

The hardest and most interesting part. Make the pipeline resilient to change.

### Schema Monitoring
1. **Store a baseline** — snapshot the current schema of every Delta table (column names + types) as a reference
2. **Simulate drift** — manually alter source data (add a column, rename one, change a type) to test detection
3. **Compare on load** — every time new data comes in, compare incoming schema vs. baseline using Python sets
4. **Generate a drift report** — flag exactly what changed (new columns, missing columns, type mismatches)

### AI Advisor
5. **Construct a remediation prompt** — when drift is detected, send GPT-4 the old schema, new schema, and the nature of the drift
6. **Generate a fix** — GPT-4 returns actionable code (ALTER TABLE, PySpark transformation, etc.)
7. **Human approval** — display the suggested fix to the user; only execute on approval
8. **Verify recovery** — re-run the pipeline to confirm the fix worked

**Output:** A monitoring system that catches schema changes and an AI advisor that proposes fixes — with human-in-the-loop approval before anything runs.