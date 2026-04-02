import os
import re
import pandas as pd

from databricks import sql
from pydantic import BaseModel, Field
from databricks.sdk.core import Config
from langchain_openai import ChatOpenAI
from typing import Literal, List, Dict, Optional, Union
from langchain_core.prompts import ChatPromptTemplate

# ── CONFIG ────────────────────────────────────────────────────────────────────
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

cfg = Config()
llm = ChatOpenAI(model="gpt-5-mini", temperature=0, api_key=os.environ["OPENAI_API_KEY"])
max_rows = 50

# ── DB CONNECTION ─────────────────────────────────────────────────────────────
def run_sql(query: str) -> list:
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# ── PYDANTIC MODELS ───────────────────────────────────────────────────────────
class IntentFilter(BaseModel):
    column:   str
    operator: str
    value: Union[str, List[Union[str, int]]] = ""

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

# ── SCHEMA ────────────────────────────────────────────────────────────────────
SYNTHEA_SCHEMA = {
    "tables": {
        "patients":    ["id", "birthdate", "deathdate", "gender", "race", "ethnicity", "first", "last", "prefix", "suffix", "maiden", "marital", "ssn", "drivers", "passport", "birthplace", "address"],
        "conditions":  ["start", "stop", "patient", "encounter", "code", "description"],
        "medications": ["start", "stop", "patient", "encounter", "code", "description", "reasoncode"],
        "observations":["date", "patient", "encounter", "code", "description", "value", "units", "type"],
        "encounters":  ["id", "start", "stop", "patient", "code", "description", "reasoncode"],
        "procedures":  ["date", "patient", "encounter", "code", "description"],
        "careplans": ["id","start","stop","patient","encounter","code","description","reasoncode","reasondescription"],
        "allergies": ["start","stop","patient","encounter","code","description"],
        "immunizations": ["date","patient","encounter","code","description"]
    },

    "join_keys": {
        "conditions":  ["patients.id", "conditions.patient"],
        "medications": ["patients.id", "medications.patient"],
        "observations":["patients.id", "observations.patient"],
        "encounters":  ["patients.id", "encounters.patient"],
        "procedures":  ["patients.id", "procedures.patient"],
        "careplans": ["patients.id", "careplans.patient"],
        "allergies": ["patients.id", "allergies.patient"],
        "immunizations.csv": ["patients.id", "immunizations.csv.patient"]
    }
}

SYNTHEA_SCHEMA_REFERENCE = {
    "tables": {
        "patients":    "patient demographics",
        "conditions":  "patient medical conditions",
        "medications": "patient medications",
        "observations": ["patient observations"],
        "encounters":  "patient encounters",
        "procedures":  "patient procedures",
        "careplans": "patient care plans",
        "allergies": "patient allergies",
        "immunizations.csv": "patient immunizations",
    },
}

SAMPLED_VALUES = {
    "patients.gender": ["M", "F"],
    "patients.race":   ["white", "black", "asian", "hispanic", "native"],
    "conditions.description":  ["use LIKE '%<actual_keyword>%' with the keyword from the user query"],
    "medications.description": ["use LIKE '%<actual_keyword>%' with the keyword from the user query"],
    "observations.type":       ["numeric", "text"]
}

# ── PROMPTS ───────────────────────────────────────────────────────────────────
ANALYZER_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a healthcare data query analyzer working with Synthea data.
Parse the user query and return a JSON object with these exact fields:
- intent_type: one of count, select, aggregate, other
- targets: list of ALL entity types involved. Always include related entities
- filters: list of objects with column, operator, value
- group_by: list of grouping fields if any
- complexity: low, medium, or high
Return ONLY valid JSON. No explanation, no markdown, no backticks."""),
    ("human", "{query}")
])

SCHEMA_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a healthcare database schema expert working with Synthea data.
Given the user intent, SELECT ONLY the relevant tables and columns needed.
Return a JSON object with:
- tables: list of ONLY the table names needed
- join_keys: list of [left_key, right_key] pairs needed
- columns: dict of table -> list of ONLY the columns needed
Available schema: {schema_reference}
Return ONLY valid JSON. No explanation, no markdown, no backticks."""),
    ("human", "Intent: {intent}")
])

SQL_GEN_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a SQL expert working with Synthea healthcare data on Azure Databricks.
Generate a valid SQL query based on the intent and schema provided.
Rules:
- Always use fully qualified table names: deepcatalog.delta_tables.tablename
- All column names are lowercase — never use uppercase column names
- Use table aliases: p=patients, c=conditions, m=medications, o=observations, e=encounters
- Use ONLY the actual sampled column values provided for filters
- For partial text matches use LOWER(column) LIKE LOWER('%keyword%')
- For conditions.DESCRIPTION and medications.DESCRIPTION, extract the actual keyword from the user query
- Always qualify column names with table alias
- Return ONLY the SQL query. No explanation, no markdown, no backticks
- When user asks for medications/conditions/procedures, SELECT columns from THAT table, not from patients
- Only include patient columns if the user explicitly asks for patient details
- Always respect LIMIT in the query — if user says "first 3" or "5 patients", use LIMIT 3 or LIMIT 5
- Never add extra columns beyond what the user asked for
- If the user specifies a number (e.g., "first 3", "any 5", "top 10"), always add LIMIT <number> to the query.
- NEVER use SELECT *. Always explicitly select columns with unique aliases.
- If columns have same names across tables, rename them using AS 
  (e.g., c.description AS condition_description)
"""),
    ("human", """Intent: {intent}
Schema: {schema}
Sampled column values: {sampled_values}""")
])

SUMMARY_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a helpful healthcare data assistant.
Answer the user's question directly in one sentence.
Rules:
- Never start with "The query returned" or "Based on the data" or "The result shows" or any such prefix
- Start directly with the answer e.g. "There are 18,574 female patients with diabetes."
- Never imply any data was deleted or modified."""),
    ("human", """Question: {query}
SQL: {sql}
Result: {result}""")
])

CHITCHAT_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a friendly healthcare data assistant for Synthea data.
You can answer casual greetings and simple conversational messages warmly.
If someone asks for a SQL query or how to query the data, write the SQL for them and explain it briefly.
Keep responses short and friendly."""),
    ("human", "{query}")
])

DATA_INTENT_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """Classify if this message is a data query that needs SQL, or just casual conversation.
Return ONLY one word: 'data' or 'chat'. If the users is asking for an SQL query, return 'data'.
Return ONLY one word. No explanation, no markdown, no backticks."""),
    ("human", "{query}")
])

SQL_FIX_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a SQL debugging expert.
Fix the SQL query based on the error.

Return ONLY corrected SQL."""),
    ("human", """SQL: {sql}
Error: {error}""")
])


DESTRUCTIVE_NL = re.compile(
    r'\b(remove|delete|drop|truncate|clear|erase|wipe|destroy|eliminate)\b',
    re.IGNORECASE
)

DESTRUCTIVE = re.compile(
    r'\b(DROP|DELETE|TRUNCATE|INSERT|UPDATE|ALTER|CREATE|REPLACE)\b',
    re.IGNORECASE
)

TABLE_INTENT = re.compile(
    r'\b(show|list|display|give me|fetch|get|retrieve|table|all|top|first|sample)\b',
    re.IGNORECASE
)

# HELPER FUNCTIONS

def extract_limit(query: str):
    match = re.search(r'\b(top|first|any|last)\s+(\d+)', query.lower())
    if match:
        return int(match.group(2))
    return None

# ── PIPELINE ──────────────────────────────────────────────────────────────────
def run_query(question: str):
    if DESTRUCTIVE_NL.search(question) or DESTRUCTIVE.search(question):
        return "⛔ Access Denied: You do not have authority to run destructive commands.", None

    state = {"query": question, "intent": None, "schema": None, "sql": None, "errors": []}

    # Classify intent
    try:
        classification = (DATA_INTENT_PROMPT | llm).invoke({"query": question}).content.strip().lower()
        if classification == "chat":
            response = (CHITCHAT_PROMPT | llm).invoke({"query": question}).content.strip()
            return response, None
    except:
        pass

    # Analyzer
    try:
        resp   = (ANALYZER_PROMPT | llm).invoke({"query": question})
        intent = IntentResponse.model_validate_json(resp.content)
        state["intent"] = intent.model_dump()
    except Exception as e:
        return f"❌ Could not understand the query: {str(e)}", None

    # Schema Mapper
    try:
        resp   = (SCHEMA_PROMPT | llm).invoke({
            "intent":           str(state["intent"]),
            "schema_reference": str(SYNTHEA_SCHEMA)
        })
        schema = SchemaMapResponse.model_validate_json(resp.content)
        state["schema"] = schema.model_dump()
    except Exception as e:
        return f"❌ Could not map schema: {str(e)}", None

    # SQL Generator
    try:
        sampled = {k: v for k, v in SAMPLED_VALUES.items()
                   if any(k.startswith(t) for t in state["schema"].get("tables", []))}
        resp    = (SQL_GEN_PROMPT | llm).invoke({
            "intent":         str(state["intent"]),
            "schema":         str(state["schema"]),
            "sampled_values": str(sampled)
        })
        sql_query = resp.content.strip()

        # 🔥 FORCE LIMIT
        limit = extract_limit(question)
        if limit:
            if "limit" not in sql_query.lower():
                sql_query = sql_query.rstrip(";") + f" LIMIT {limit}"
        else:
            if "limit" not in sql_query.lower():
                sql_query = sql_query.rstrip(";") + f" LIMIT {max_rows}"

        state["sql"] = sql_query
    except Exception as e:
        return f"❌ Could not generate SQL: {str(e)}", None

    # Executor with one retry
    try:
        result_df = run_sql(state["sql"])
    except Exception as e:
        try:
            fix_resp  = (SQL_FIX_PROMPT | llm).invoke({"sql": state["sql"], "error": str(e)})
            fixed_sql = fix_resp.content.strip()
            result_df = run_sql(fixed_sql)
            state["sql"] = fixed_sql
        except Exception as e2:
            return f"❌ Query failed: {str(e2)}", None

    # Show table or summary
    show_table = (
        bool(TABLE_INTENT.search(question)) or 
        state["intent"]["intent_type"] in ["aggregate", "count"]
    )

    if show_table:
        row_count = len(result_df)

        if row_count >= max_rows and not extract_limit(question):
            return f"Showing first {max_rows} records (results are limited).", result_df

        return "Here are the results for your query:", result_df

    try:
        summary = (SUMMARY_PROMPT | llm).invoke({
            "query":  question,
            "sql":    state["sql"],
            "result": result_df.to_string(index=False)[:500]
        }).content.strip()
        return summary, None
    except Exception as e:
        return f"Result ready.", None



