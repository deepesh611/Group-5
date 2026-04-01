# Databricks notebook source
# 03_openai_client.py — OpenAI / LangChain Wrapper + Prompt Builders
#
# Loads the OpenAI API key from Databricks secret scope, provides a reusable
# ChatOpenAI factory, builds prompts for:
#   - SQL generation with full schema context (Pipeline 2)
#   - AI Advisor remediation plans (Pipeline 3)
# Includes response parsers, SQL safety guardrails, and retry logic.
#
# Depends on: 00_config, 04_metadata_manager

# COMMAND ----------

import os
import re
import json
import time

# ─── Load OpenAI API Key before importing LangChain
# LangChain reads OPENAI_API_KEY from os.environ on first import.

def _init_openai():
    os.environ["OPENAI_API_KEY"] = dbutils.secrets.get(scope="llm-scope", key="openai-key")

_init_openai()

# COMMAND ----------

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

# COMMAND ----------

# ─── LLM Factory

def get_llm(temperature: float = None, model: str = None, max_tokens: int = None) -> ChatOpenAI:
    """
    Return a ChatOpenAI instance.

    Use temperature=0.0 for deterministic SQL / advisor output.
    Use temperature=0.2 on retry attempts (adds variation to break stuck outputs).
    """
    return ChatOpenAI(
        model=model or OPENAI_MODEL,
        temperature=temperature if temperature is not None else OPENAI_TEMPERATURE,
        max_tokens=max_tokens or OPENAI_MAX_TOKENS,
    )

# COMMAND ----------

# ─── Retry Wrapper

def call_with_retry(chain, inputs: dict, max_retries: int = None):
    """
    Invoke a LangChain chain with exponential-backoff retry on transient errors.

    Returns the AIMessage on success.
    Raises the last exception after exhausting all retries.
    """
    retries    = max_retries if max_retries is not None else MAX_RETRIES
    last_error = None

    for attempt in range(retries):
        try:
            return chain.invoke(inputs)
        except Exception as e:
            last_error = e
            if attempt < retries - 1:
                wait = RETRY_DELAY_SECONDS * (2 ** attempt)
                log.warning(
                    f"LLM call attempt {attempt + 1}/{retries} failed: {e}. "
                    f"Retrying in {wait}s…"
                )
                time.sleep(wait)

    log.error(f"All {retries} LLM attempts exhausted.")
    raise last_error

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
#  PROMPT BUILDERS — SQL Generation  (Pipeline 2)
# ─────────────────────────────────────────────────────────────────────────────

def build_sql_system_prompt(schema_json: dict) -> str:
    """
    Build the complete system prompt for NL→SQL generation.
    Injects: table definitions, canonical join paths, and few-shot examples
    from master_schema.json — the LLM never infers schema from column names.
    """
    # ── Table definitions block ──────────────────────────────────────────
    table_blocks = []
    for tname, tdata in schema_json.get("tables", {}).items():
        full_name = get_full_table_name(tname)
        col_lines = [
            f"      {cname} ({cdef.get('type','string')}) — {cdef.get('description','')}"
            for cname, cdef in tdata.get("columns", {}).items()
        ]
        table_blocks.append(
            f"  TABLE: {full_name}\n"
            f"  Description: {tdata.get('description','')}\n"
            + "\n".join(col_lines)
        )
    tables_str = "\n\n".join(table_blocks)

    # ── Canonical join paths ─────────────────────────────────────────────
    joins_str = "\n".join(
        f"  • {j['name']}: {j['sql']}"
        for j in schema_json.get("relationships", {}).get("paths", [])
    )

    # ── Few-shot examples ────────────────────────────────────────────────
    examples_str = "\n\n".join(
        f"  Q: {ex['question']}\n"
        f"  Reasoning: {ex['reasoning']}\n"
        f"  SQL:\n  {ex['sql']}"
        for ex in schema_json.get("few_shot_examples", [])
    )

    return f"""You are a Spark SQL expert for a healthcare analytics platform on Azure Databricks (Unity Catalog).
Your ONLY job is to convert natural language questions into valid Spark SQL queries.

═══ STRICT RULES ═══
1. Return ONLY the SQL query — no markdown, no backticks, no explanation.
2. Use fully qualified names: {CATALOG}.{SCHEMA}.<table>
3. Always use LOWER(column) LIKE '%keyword%' for text matching — NEVER exact equality on description/name fields.
4. For observations.value numeric operations: always CAST(value AS DOUBLE).
5. Always add LIMIT {MAX_QUERY_ROWS} to SELECT queries unless the user explicitly requests all rows.
6. NEVER emit: DROP, DELETE, UPDATE, INSERT, ALTER, TRUNCATE, CREATE, GRANT, REVOKE, MERGE.
7. Always qualify column names with table aliases when joining multiple tables.
8. Do NOT alias patient/encounter ID columns — the masking layer identifies them by name.
9. Date arithmetic: use Spark SQL functions — CURRENT_DATE(), DATE_SUB(), YEAR(), DATEDIFF(), ADD_MONTHS().
10. When a question is genuinely ambiguous, choose the simpler interpretation.

═══ AVAILABLE TABLES ═══
{tables_str}

═══ CANONICAL JOIN PATHS (follow these exactly — never infer joins from column name similarity) ═══
{joins_str}

═══ FEW-SHOT EXAMPLES ═══
{examples_str}"""


def build_sql_prompt_template() -> ChatPromptTemplate:
    """Return the ChatPromptTemplate for SQL generation (used by Pipeline 2 LangGraph nodes)."""
    return ChatPromptTemplate.from_messages([
        ("system", "{system_prompt}"),
        ("human", "{question}"),
    ])

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
#  PROMPT BUILDERS — AI Advisor  (Pipeline 3)
# ─────────────────────────────────────────────────────────────────────────────

def build_advisor_prompt(
    drift_report: dict,
    table_name: str,
    table_schema_snippet: dict,
) -> str:
    """
    Build the full prompt for the AI Schema Advisor.

    The LLM must return a JSON object with four keys:
        SQL_FIX, NEW_JSON, SEVERITY, REASONING
    — matching the drift_handling.response_format in master_schema.json.
    """
    drift_json  = json.dumps(drift_report, indent=2)
    schema_json = json.dumps(table_schema_snippet, indent=2)
    full_table  = get_full_table_name(table_name)

    return f"""You are an AI Schema Advisor for a healthcare Delta Lake pipeline on Azure Databricks.

═══ CONTEXT ═══
Fully qualified table: {full_table}
Data source: Synthea synthetic EHR data.
Compliance: HIPAA Safe Harbor — 18 HIPAA identifier categories.

═══ CURRENT TABLE SCHEMA ═══
{schema_json}

═══ DETECTED DRIFT ═══
{drift_json}

═══ YOUR TASK ═══
Analyze the detected drift and produce a remediation plan.

STRICT RULES — YOU MUST FOLLOW THESE EXACTLY:

1. ADDITIVE DRIFT (new_columns in drift): a column exists in the CSV but NOT in the Delta table.
   Action: SQL_FIX = ALTER TABLE {full_table} ADD COLUMNS (col_name TYPE).
           NEW_JSON = schema definition for ONLY the new columns.

2. SUBTRACTIVE DRIFT (missing_columns in drift): a column expected by master_schema.json is ABSENT from the CSV.
   This means the UPSTREAM DATA SOURCE lost the column. We CANNOT restore it via DDL.
   Action: SQL_FIX = "" (EXACTLY empty string — DO NOT write any ALTER TABLE statement).
           NEW_JSON = {{}} (EXACTLY empty dict — DO NOT define any column here).
           SEVERITY = "CRITICAL".
   REASON: Adding the column back via ALTER TABLE would create a NULL column. The pipeline
   will instead use a schema-merge fallback (INSERT OVERWRITE) to preserve the Delta table
   column with NULLs for new rows. Your job is ONLY to flag it as CRITICAL and advise.

3. TYPE CHANGE DRIFT (type_changes in drift): a column exists in both but types differ.
   If safe widening (e.g. int→long): SQL_FIX = CAST statement, SEVERITY = "WARNING".
   If PHI column or dangerous change: SQL_FIX = "", SEVERITY = "CRITICAL", advise reingestion.

4. PHI RULE: If any new column name or description matches: names, geographic data, dates,
   phone, email, SSN, MRN, account numbers, or any unique identifiers → set phi=true, masking="REDACT".

═══ RESPOND WITH EXACTLY THIS JSON — no markdown, no backticks, no extra text ═══
{{
  "SQL_FIX": "ALTER TABLE DDL string, OR empty string \"\" if no DDL is appropriate (required for subtractive drift)",
  "NEW_JSON": {{
    "only_for_new_columns": {{
      "type": "spark_sql_type",
      "phi": true,
      "phi_type": "DIRECT_IDENTIFIER",
      "masking": "REDACT",
      "description": "What this column contains."
    }}
  }},
  "SEVERITY": "INFO or WARNING or CRITICAL",
  "REASONING": "Plain English explanation: what drifted, which rule above you applied, and why."
}}"""


def build_advisor_prompt_template() -> ChatPromptTemplate:
    """Return the ChatPromptTemplate for AI Advisor calls (used by Pipeline 3)."""
    return ChatPromptTemplate.from_messages([
        ("system", (
            "You are an AI Schema Advisor specializing in healthcare data governance on Azure Databricks. "
            "You always output valid JSON with no surrounding text."
        )),
        ("human", "{advisor_prompt}"),
    ])

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
#  RESPONSE PARSERS + GUARDRAILS
# ─────────────────────────────────────────────────────────────────────────────

def parse_sql_response(response_text: str) -> str:
    """Strip markdown code fences and excess whitespace from LLM SQL output."""
    sql = response_text.strip()
    sql = re.sub(r"^```[\w]*\n?", "", sql)
    sql = re.sub(r"\n?```$", "", sql)
    return sql.strip()


def validate_sql_safety(sql: str) -> tuple:
    """
    Scan generated SQL for blocked destructive keywords.

    Returns:
        (is_safe: bool, violation_message: str | None)
    """
    sql_upper = sql.upper()
    for kw in BLOCKED_SQL_PATTERNS:
        if re.search(r"\b" + kw + r"\b", sql_upper):
            return False, f"Blocked keyword detected: '{kw}'"
    return True, None


def parse_advisor_response(response_text: str, drift_report: dict = None) -> dict:
    """
    Parse and validate the AI Advisor's JSON response, enforcing strict type coercion
    to protect downstream consumers.

    drift_report is optional but strongly recommended: when provided, an additional
    server-side rule is applied to prevent GPT-4o from generating DDL for subtractive
    drift (missing columns), which would cause the downstream INSERT OVERWRITE to fail.
    """
    text = response_text.strip()
    text = re.sub(r"^```[\w]*\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    text = text.strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        log.error(f"Advisor JSON parse failed: {e}")
        log.error(f"Raw response (first 600 chars): {text[:600]}")
        raise ValueError(f"Advisor response is not valid JSON: {e}") from e

    if not isinstance(parsed, dict):
        raise ValueError("Advisor response root must be a JSON object")

    # 1. Enforce SQL_FIX as string
    sql_fix = parsed.get("SQL_FIX")
    if sql_fix is None:
        parsed["SQL_FIX"] = ""
    else:
        parsed["SQL_FIX"] = str(sql_fix).strip()

    # 2. Enforce NEW_JSON strictly as a dictionary to prevent AttributeError
    new_json = parsed.get("NEW_JSON")
    if not isinstance(new_json, dict):
        parsed["NEW_JSON"] = {}

    # 3. Enforce SEVERITY as string mapping to INFO, WARNING, CRITICAL
    severity = str(parsed.get("SEVERITY", "WARNING")).strip().upper()
    if severity not in ("INFO", "WARNING", "CRITICAL"):
        severity = "WARNING"
    parsed["SEVERITY"] = severity

    # 4. Enforce REASONING as string
    reasoning = parsed.get("REASONING")
    parsed["REASONING"] = str(reasoning) if reasoning is not None else ""

    # 5. SUBTRACTIVE DRIFT GUARDRAIL (server-side, deterministic, ignores LLM output)
    # If the drift has missing columns (subtractive), the LLM must NOT generate any
    # DDL or NEW_JSON. Delta cannot restore data that was never in the CSV.
    # If LLM still returned DDL here (prompt hallucination), we forcibly discard it.
    if drift_report is not None:
        has_missing   = bool(drift_report.get("missing_columns"))
        has_new       = bool(drift_report.get("new_columns"))
        is_pure_subtractive = has_missing and not has_new

        if is_pure_subtractive and parsed["SQL_FIX"]:
            log.warning(
                "[parse_advisor_response] LLM generated SQL_FIX '%s' for SUBTRACTIVE drift — "
                "discarding (cannot restore missing column data via DDL). "
                "Pipeline will use mergeSchema fallback.",
                parsed["SQL_FIX"][:120],
            )
            parsed["SQL_FIX"]  = ""
            parsed["NEW_JSON"] = {}
            if parsed["SEVERITY"] != "CRITICAL":
                parsed["SEVERITY"] = "CRITICAL"

    return parsed

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
#  CONVENIENCE — End-to-End One-Shot Calls Used by Pipelines
# ─────────────────────────────────────────────────────────────────────────────

def generate_sql_from_question(question: str) -> str:
    """
    End-to-end SQL generation: load schema → build prompt → call LLM → parse → validate.

    Returns clean SQL string on success.
    Raises ValueError if the generated SQL fails the safety guardrail.

    Note: PHI masking is applied AFTER this call in Pipeline 2.
    """
    schema        = load_master_schema()
    system_prompt = build_sql_system_prompt(schema)

    template = build_sql_prompt_template()
    llm      = get_llm()
    chain    = template | llm

    response = call_with_retry(chain, {
        "system_prompt": system_prompt,
        "question":      question,
    })

    sql = parse_sql_response(response.content)

    is_safe, violation = validate_sql_safety(sql)
    if not is_safe:
        raise ValueError(
            f"Generated SQL blocked by safety guardrail: {violation}\nSQL: {sql}"
        )

    log.info(f"SQL generated: '{question[:80]}{'…' if len(question) > 80 else ''}'")
    return sql


def get_advisor_recommendation(drift_report: dict, table_name: str) -> dict:
    """
    End-to-end AI Advisor call: build prompt → call LLM → parse and validate.

    Returns the parsed advisor dict: {SQL_FIX, NEW_JSON, SEVERITY, REASONING}.
    Raises ValueError if the response cannot be parsed.

    drift_report is passed through to parse_advisor_response to enable the
    server-side subtractive-drift guardrail: if the LLM hallucinates a DDL fix
    for a missing column (despite prompt instructions), the guardrail discards
    it deterministically before it reaches Pipeline 3's execution logic.
    """
    table_meta   = get_table_metadata(table_name)
    advisor_text = build_advisor_prompt(drift_report, table_name, table_meta)

    template = build_advisor_prompt_template()
    llm      = get_llm(temperature=0.0)
    chain    = template | llm

    response = call_with_retry(chain, {"advisor_prompt": advisor_text})
    return parse_advisor_response(response.content, drift_report=drift_report)
