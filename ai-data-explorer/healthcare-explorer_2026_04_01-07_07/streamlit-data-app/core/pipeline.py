import os
import re
import json
import pandas as pd

from databricks import sql
from pydantic import BaseModel, Field
from databricks.sdk.core import Config
from langchain_openai import ChatOpenAI
from typing import Literal, List, Dict, Optional, Union
from langchain_core.prompts import ChatPromptTemplate

# ── CONFIG ────────────────────────────────────────────────────────────────────
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

cfg      = Config()
llm      = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=os.environ["OPENAI_API_KEY"])
max_rows = 50

CATALOG = "deepcatalog"
SCHEMA  = "delta_tables"

# ── JOIN KEYS (hardcoded — Databricks has no FK constraints) ──────────────────
JOIN_KEYS = {
    "conditions":    ["patients.id", "conditions.patient"],
    "medications":   ["patients.id", "medications.patient"],
    "observations":  ["patients.id", "observations.patient"],
    "encounters":    ["patients.id", "encounters.patient"],
    "procedures":    ["patients.id", "procedures.patient"],
    "careplans":     ["patients.id", "careplans.patient"],
    "allergies":     ["patients.id", "allergies.patient"],
    "immunizations": ["patients.id", "immunizations.patient"]
}

SAMPLED_VALUES = {
    "patients.gender": ["M", "F"],
    "patients.race":   ["white", "black", "asian", "hispanic", "native"],
    "conditions.description":  ["use LIKE '%<actual_keyword>%' with the keyword from the user query"],
    "medications.description": ["use LIKE '%<actual_keyword>%' with the keyword from the user query"],
    "observations.type":       ["numeric", "text"]
}

AVAILABLE_TABLES = [
    "patients", "conditions", "medications", "observations",
    "encounters", "procedures", "careplans", "allergies", "immunizations"
]

# ── DB CONNECTION ─────────────────────────────────────────────────────────────
def run_sql(query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# ── SCHEMA FETCHER AGENT ──────────────────────────────────────────────────────
def schema_fetcher_agent(tables: List[str]) -> dict:
    live_schema = {"tables": {}, "join_keys": {}}
    for table in tables:
        try:
            df   = run_sql(f"DESCRIBE TABLE {CATALOG}.{SCHEMA}.{table}")
            cols = df[df["col_name"].str.strip() != ""][["col_name", "data_type"]]
            cols = cols[~cols["col_name"].str.startswith("#")]
            live_schema["tables"][table] = {
                row["col_name"].strip(): row["data_type"].strip()
                for _, row in cols.iterrows()
            }
        except Exception as e:
            live_schema["tables"][table] = {"error": str(e)}
        if table in JOIN_KEYS:
            live_schema["join_keys"][table] = JOIN_KEYS[table]
    return live_schema

# ── PYDANTIC MODELS ───────────────────────────────────────────────────────────
class IntentFilter(BaseModel):
    column:   str
    operator: str
    value:    Union[str, List[Union[str, int]]] = ""

    def model_post_init(self, __context):
        if isinstance(self.value, list):
            self.value = ", ".join(map(str, self.value))
        else:
            self.value = str(self.value)

class IntentResponse(BaseModel):
    intent_type: Literal["count", "select", "aggregate", "other"] = "select"
    targets:     List[str] = Field(default_factory=list)
    filters:     List[IntentFilter] = Field(default_factory=list)
    group_by:    List[str] = Field(default_factory=list)
    complexity:  Literal["low", "medium", "high"] = "low"

class SchemaMapResponse(BaseModel):
    tables:    List[str] = Field(default_factory=list)
    join_keys: List[List[str]] = Field(default_factory=list)
    columns:   Dict[str, List[str]] = Field(default_factory=dict)

# ── PROMPTS ───────────────────────────────────────────────────────────────────
ANALYZER_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a healthcare data query analyzer working with Synthea data.
Parse the user query and return a JSON object with these exact fields:
- intent_type: one of count, select, aggregate, other
- targets: list of ALL entity types involved. Always include related entities
- filters: list of objects with column, operator, value (value must always be a string)
- group_by: list of grouping fields if any
- complexity: low, medium, or high
Return ONLY valid JSON. No explanation, no markdown, no backticks."""),
    ("human", "{query}")
])

TABLE_PICKER_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a healthcare database expert.
Given the user intent, return a JSON list of ONLY the table names needed from this list:
{available_tables}

Table descriptions:
- patients: patient demographics
- conditions: medical conditions/diagnoses
- medications: prescribed medications
- observations: lab results and vitals
- encounters: hospital/clinic visits
- procedures: medical procedures performed
- careplans: patient care plans
- allergies: patient allergies
- immunizations: patient immunizations

Return ONLY a JSON array of table names. Example: ["patients", "conditions"]
No explanation, no markdown, no backticks."""),
    ("human", "Intent: {intent}")
])

SCHEMA_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a healthcare database schema expert working with Synthea data.
Given the user intent and the LIVE schema of the relevant tables, select the needed columns.
Return a JSON object with:
- tables: list of table names needed
- join_keys: list of [left_key, right_key] pairs needed for joins
- columns: dict of table -> list of ONLY the columns needed for this query

Live schema (table -> column: datatype):
{live_schema}

Join keys available:
{join_keys}

Return ONLY valid JSON. No explanation, no markdown, no backticks."""),
    ("human", "Intent: {intent}")
])

SQL_GEN_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a SQL expert working with Synthea healthcare data on Azure Databricks.
Generate a valid SQL query based on the intent and schema provided.
Rules:
- Always use fully qualified table names: deepcatalog.delta_tables.tablename
- All column names are lowercase
- Use table aliases: p=patients, c=conditions, m=medications, o=observations, e=encounters, pr=procedures, cp=careplans, a=allergies, im=immunizations
- Use ONLY the actual sampled column values provided for filters
- For partial text matches use LOWER(column) LIKE LOWER('%keyword%')
- For conditions.description and medications.description, extract the actual keyword from the user query
- Always qualify column names with table alias
- NEVER use SELECT * — always explicitly list columns
- If columns share names across tables, alias them (e.g., c.description AS condition_description)
- When user asks for medications/conditions/procedures, SELECT from THAT table only
- Only include patient columns if explicitly asked
- Never add a LIMIT unless the user explicitly asks for a specific number of records
- Never add extra columns beyond what the user asked for
- Return ONLY the SQL query. No explanation, no markdown, no backticks.
- When joining conditions and medications, use SELECT DISTINCT to avoid duplicate rows
- If the result is a list of medications per condition, group or deduplicate appropriately"""),
    ("human", """Intent: {intent}
Schema: {schema}
Sampled column values: {sampled_values}""")
])

SUMMARY_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a helpful healthcare data assistant.
Answer the user's question directly in one sentence.
Rules:
- Never start with "The query returned" or "Based on the data" or "The result shows"
- Start directly with the answer e.g. "There are 18,574 female patients with diabetes."
- Never imply any data was deleted or modified."""),
    ("human", """Question: {query}
SQL: {sql}
Result: {result}""")
])

CHITCHAT_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a friendly healthcare data assistant for Synthea data.
Answer casual greetings warmly.
If someone asks for a SQL query, write it and explain briefly.
Keep responses short and friendly."""),
    ("human", "{query}")
])

DATA_INTENT_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """Classify if this message is a data query that needs SQL, or casual conversation.
Return ONLY one word: 'data' or 'chat'.
Return ONLY one word. No explanation, no markdown, no backticks."""),
    ("human", "{query}")
])

SQL_FIX_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a SQL debugging expert for Azure Databricks.
Fix the SQL query based on the error.
All column names must be lowercase.
Always use fully qualified table names: deepcatalog.delta_tables.tablename
Never add a LIMIT unless the user explicitly asked for one.
Return ONLY the corrected SQL. No explanation, no markdown, no backticks."""),
    ("human", """SQL: {sql}
Error: {error}""")
])

# ── REGEX ─────────────────────────────────────────────────────────────────────
DESTRUCTIVE_NL = re.compile(
    r'\b(remove|delete|drop|truncate|clear|erase|wipe|destroy|eliminate)\b',
    re.IGNORECASE
)
DESTRUCTIVE = re.compile(
    r'\b(DROP|DELETE|TRUNCATE|INSERT|UPDATE|ALTER|CREATE|REPLACE)\b',
    re.IGNORECASE
)
TABLE_INTENT = re.compile(
    r'\b(show|list|display|fetch|retrieve|show me the records|show records|show patients|list patients|find all|get all)\b',
    re.IGNORECASE
)

def extract_limit(query: str):
    match = re.search(r'\b(top|first|any|last)\s+(\d+)', query.lower())
    if match:
        return int(match.group(2))
    return None

# ── PIPELINE ──────────────────────────────────────────────────────────────────
def run_query(question: str):
    if DESTRUCTIVE_NL.search(question) or DESTRUCTIVE.search(question):
        return "⛔ Access Denied: You do not have authority to run destructive commands.", None, None

    state = {
        "query":       question,
        "intent":      None,
        "tables":      None,
        "live_schema": None,
        "schema":      None,
        "sql":         None,
        "errors":      []
    }

    # Classify intent
    try:
        classification = (DATA_INTENT_PROMPT | llm).invoke({"query": question}).content.strip().lower()
        if classification == "chat":
            response = (CHITCHAT_PROMPT | llm).invoke({"query": question}).content.strip()
            return response, None, None
    except:
        pass

    # 1 — Analyzer
    try:
        resp   = (ANALYZER_PROMPT | llm).invoke({"query": question})
        intent = IntentResponse.model_validate_json(resp.content)
        state["intent"] = intent.model_dump()
    except Exception as e:
        return f"❌ Could not understand the query: {str(e)}", None, None

    # 2 — Table Picker
    try:
        resp   = (TABLE_PICKER_PROMPT | llm).invoke({
            "intent":           str(state["intent"]),
            "available_tables": str(AVAILABLE_TABLES)
        })
        tables = json.loads(resp.content.strip())
        state["tables"] = tables
    except Exception as e:
        return f"❌ Could not identify tables: {str(e)}", None, None

    # 3 — Schema Fetcher Agent
    try:
        live_schema = schema_fetcher_agent(state["tables"])
        state["live_schema"] = live_schema
    except Exception as e:
        return f"❌ Could not fetch schema: {str(e)}", None, None

    # 4 — Schema Mapper
    try:
        resp   = (SCHEMA_PROMPT | llm).invoke({
            "intent":      str(state["intent"]),
            "live_schema": str(state["live_schema"]["tables"]),
            "join_keys":   str(state["live_schema"]["join_keys"])
        })
        schema = SchemaMapResponse.model_validate_json(resp.content)
        state["schema"] = schema.model_dump()
    except Exception as e:
        return f"❌ Could not map schema: {str(e)}", None, None

    # 5 — SQL Generator
    try:
        sampled  = {k: v for k, v in SAMPLED_VALUES.items()
                    if any(k.startswith(t) for t in state["tables"])}
        resp     = (SQL_GEN_PROMPT | llm).invoke({
            "intent":         str(state["intent"]),
            "schema":         str(state["schema"]),
            "sampled_values": str(sampled)
        })
        sql_query = resp.content.strip()

        # Apply explicit LIMIT only if user asked for one
        limit = extract_limit(question)
        if limit and "limit" not in sql_query.lower():
            sql_query = sql_query.rstrip(";") + f" LIMIT {limit}"
        elif not limit and "limit" not in sql_query.lower():
            sql_query = sql_query.rstrip(";") + f" LIMIT {max_rows}"

        state["sql"] = sql_query
    except Exception as e:
        return f"❌ Could not generate SQL: {str(e)}", None, None

    # 6 — Executor with one retry
    try:
        result_df = run_sql(state["sql"])
    except Exception as e:
        try:
            fix_resp  = (SQL_FIX_PROMPT | llm).invoke({"sql": state["sql"], "error": str(e)})
            fixed_sql = fix_resp.content.strip()
            result_df = run_sql(fixed_sql)
            state["sql"] = fixed_sql
        except Exception as e2:
            return f"❌ Query failed: {str(e2)}", None, None

    # Show table or summary
    show_table = (
        bool(TABLE_INTENT.search(question)) or
        state["intent"]["intent_type"] in ["select", "aggregate", "count"]
    )

    if show_table:
        row_count = len(result_df)
        if row_count >= max_rows and not extract_limit(question):
            return f"Showing first {max_rows} records (results are limited).", result_df, state["sql"]
        return "Here are the results for your query:", result_df, state["sql"]

    try:
        summary = (SUMMARY_PROMPT | llm).invoke({
            "query":  question,
            "sql":    state["sql"],
            "result": result_df.to_string(index=False)[:500]
        }).content.strip()
        return summary, None, state["sql"]
    except Exception as e:
        return "Result ready.", None, state["sql"]