# 🧠 Text-to-SQL Multi-Agent System
### Natural Language Data Explorer — Implementation Guide
**Stack:** LangGraph · LangChain · ChatGPT API · PySpark · Delta Tables (Azure Databricks)

---

## 1. Overview

This system converts natural language questions into SQL queries and executes them against Synthea healthcare Delta tables on Azure Databricks. It uses a LangGraph-orchestrated multi-agent pipeline where each agent handles one responsibility, passing typed state forward.

**Infrastructure assumed ready:**
- Azure Databricks workspace
- Delta tables loaded from Synthea dataset
- Azure Blob Storage linked as external storage in Databricks

---

## 2. Architecture

```
User Query
   ↓
[Analyzer Agent]       → Parses intent (count / select / aggregate)
   ↓
[Schema Mapper Agent]  → Maps intent to tables, columns, join keys
   ↓
[SQL Generator Agent]  → Generates SQL with value sampling
   ↓
[Executor Agent]       → Runs SQL via PySpark on Delta tables
   ↓
[Evaluator Agent]      → Sanity-checks result (credibility check)
   ↓ (if issue)
[Diagnostician Agent]  → Root cause analysis + fix suggestion
   ↓ (retry or escalate)
[Human Agent]          → Final fallback after 3 failed retries
   ↓
Result
```

---

## 3. LangGraph State Schema

All agents read from and write to a single shared state dict. Define it at the top of your notebook.

```python
from typing import TypedDict, Optional, Any

class AgentState(TypedDict):
    query: str                        # Original user question
    intent: Optional[dict]            # Output of Analyzer
    schema: Optional[dict]            # Output of Schema Mapper
    sql: Optional[str]                # Generated SQL
    result: Optional[Any]             # Query result (DataFrame or value)
    eval_result: Optional[dict]       # Output of Evaluator
    diag_result: Optional[dict]       # Output of Diagnostician
    errors: list[str]                 # Accumulated error messages
    retry_count: int                  # Retry counter (max 3)
    final: bool                       # Flag to end the graph
    needs_human: bool                 # Escalation flag
```

---

## 4. Pydantic Output Models

Each agent returns a structured Pydantic object. This ensures reliable state passing and prevents raw string parsing errors.

```python
from pydantic import BaseModel, Field
from typing import Literal, List, Dict, Optional

# --- Analyzer Agent Output ---
class IntentFilter(BaseModel):
    column: str
    operator: str   # e.g., "=", "LIKE", ">"
    value: str

class IntentResponse(BaseModel):
    intent_type: Literal["count", "select", "aggregate", "other"] = "select"
    targets: List[str] = Field(default_factory=list)       # e.g., ["patients", "conditions"]
    filters: List[IntentFilter] = Field(default_factory=list)
    group_by: List[str] = Field(default_factory=list)
    complexity: Literal["low", "medium", "high"] = "low"

# --- Schema Mapper Agent Output ---
class SchemaMapResponse(BaseModel):
    tables: List[str] = Field(default_factory=list)
    join_keys: List[List[str]] = Field(default_factory=list)   # e.g., [["patients.Id", "conditions.PATIENT"]]
    columns: Dict[str, List[str]] = Field(default_factory=dict)

# --- Evaluator Agent Output ---
class EvalResponse(BaseModel):
    credible: bool
    reason: str
    suggested_fix: Optional[str] = None

# --- Diagnostician Agent Output ---
class DiagResponse(BaseModel):
    error_type: Literal["schema", "syntax", "logic", "unknown"] = "unknown"
    fix_suggestion: str = ""
    needs_human: bool = False
```

---

## 5. Agent Implementations

### 5.1 Analyzer Agent

Parses the user's natural language query into structured intent.

```python
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

llm = ChatOpenAI(model="gpt-4o", temperature=0)

ANALYZER_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a healthcare data query analyzer.
Parse the user query and return a JSON object with:
- intent_type: one of count, select, aggregate, other
- targets: list of entity types involved (e.g., patients, conditions, medications)
- filters: list of {{column, operator, value}} objects
- group_by: list of grouping fields if any
- complexity: low, medium, or high

Return ONLY valid JSON. No explanation."""),
    ("human", "{query}")
])

def analyzer_node(state: AgentState) -> AgentState:
    chain = ANALYZER_PROMPT | llm
    response = chain.invoke({"query": state["query"]})
    intent = IntentResponse.model_validate_json(response.content)
    return {**state, "intent": intent.model_dump()}
```

**Example:**
- Input: `"How many female patients have diabetes?"`
- Output:
```json
{
  "intent_type": "count",
  "targets": ["patients", "conditions"],
  "filters": [{"column": "GENDER", "operator": "=", "value": "F"},
               {"column": "DESCRIPTION", "operator": "LIKE", "value": "%diabetes%"}],
  "group_by": [],
  "complexity": "low"
}
```

---

### 5.2 Schema Mapper Agent

Maps the analyzed intent to actual Synthea table names, column names, and join keys.

```python
# Synthea schema reference — embed this into the prompt
SYNTHEA_SCHEMA = """
Tables and key columns:
- patients: Id, BIRTHDATE, DEATHDATE, GENDER, RACE, CITY, STATE
- conditions: START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION
- medications: START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION, REASONCODE
- observations: DATE, PATIENT, ENCOUNTER, CODE, DESCRIPTION, VALUE, UNITS, TYPE
- encounters: Id, START, STOP, PATIENT, CODE, DESCRIPTION, REASONCODE
- procedures: DATE, PATIENT, ENCOUNTER, CODE, DESCRIPTION

Join keys:
- patients.Id = conditions.PATIENT
- patients.Id = medications.PATIENT
- patients.Id = observations.PATIENT
- patients.Id = encounters.PATIENT
- encounters.Id = conditions.ENCOUNTER
"""

SCHEMA_PROMPT = ChatPromptTemplate.from_messages([
    ("system", f"""You are a healthcare database schema expert.
Given the user intent and the schema below, identify:
- tables: list of table names needed
- join_keys: list of [left_key, right_key] pairs needed
- columns: dict of table -> list of columns needed

Schema:
{SYNTHEA_SCHEMA}

Return ONLY valid JSON. No explanation."""),
    ("human", "Intent: {intent}")
])

def schema_mapper_node(state: AgentState) -> AgentState:
    chain = SCHEMA_PROMPT | llm
    response = chain.invoke({"intent": str(state["intent"])})
    schema = SchemaMapResponse.model_validate_json(response.content)
    return {**state, "schema": schema.model_dump()}
```

---

### 5.3 SQL Generator Agent

Generates SQL from intent + schema. Uses value sampling to ensure correct filter values.

```python
def sample_column_values(table: str, column: str, spark, n: int = 5) -> list:
    """Sample actual values from a Delta table column to guide the LLM."""
    rows = spark.sql(f"SELECT DISTINCT {column} FROM {table} LIMIT {n}").collect()
    return [row[0] for row in rows]

SQL_GEN_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a Spark SQL expert working with healthcare data.
Generate a valid Spark SQL query using:
- Table aliases: p=patients, c=conditions, m=medications, o=observations, e=encounters
- Always use actual sampled column values provided (not assumed values)
- For LIKE filters, use %keyword% pattern
- Return ONLY the SQL query. No explanation, no markdown."""),
    ("human", """Intent: {intent}
Schema: {schema}
Sampled values: {sampled_values}
Previous errors (if any): {errors}""")
])

def sql_generator_node(state: AgentState) -> AgentState:
    schema = state["schema"]
    
    # Sample values for filter columns
    sampled = {}
    for table in schema.get("tables", []):
        for col in schema.get("columns", {}).get(table, []):
            try:
                sampled[f"{table}.{col}"] = sample_column_values(table, col, spark)
            except:
                pass

    # Increase temperature slightly on retries for variation
    temp = 0.0 if state["retry_count"] == 0 else 0.3
    gen_llm = ChatOpenAI(model="gpt-4o", temperature=temp)
    
    chain = SQL_GEN_PROMPT | gen_llm
    response = chain.invoke({
        "intent": str(state["intent"]),
        "schema": str(schema),
        "sampled_values": str(sampled),
        "errors": state.get("errors", [])
    })
    return {**state, "sql": response.content.strip()}
```

---

### 5.4 Executor Agent

Executes the generated SQL against Delta tables via PySpark. Includes a safety filter.

```python
import re

DESTRUCTIVE_PATTERNS = re.compile(
    r'\b(DROP|DELETE|TRUNCATE|INSERT|UPDATE|ALTER|CREATE|REPLACE)\b',
    re.IGNORECASE
)

def executor_node(state: AgentState) -> AgentState:
    sql = state["sql"]
    
    # Safety gate — reject destructive SQL
    if DESTRUCTIVE_PATTERNS.search(sql):
        return {
            **state,
            "errors": state["errors"] + ["Blocked: destructive SQL detected."],
            "final": False
        }
    
    try:
        result_df = spark.sql(sql)
        result = result_df.collect()
        return {**state, "result": result, "errors": state["errors"]}
    except Exception as e:
        return {
            **state,
            "errors": state["errors"] + [f"Execution error: {str(e)}"],
            "result": None
        }
```

---

### 5.5 Evaluator Agent

Sanity-checks the result for logical credibility (not just technical correctness).

```python
EVAL_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a healthcare data quality evaluator.
Check if the query result is credible given the original question.
Flag issues like:
- COUNT queries returning multiple rows
- Percentages > 100%
- Empty results where data should exist
- Results that contradict medical logic

Return JSON: {{"credible": bool, "reason": str, "suggested_fix": str or null}}"""),
    ("human", """Question: {query}
SQL: {sql}
Result (sample): {result}""")
])

def evaluator_node(state: AgentState) -> AgentState:
    result_sample = str(state["result"])[:500] if state["result"] else "None"
    chain = EVAL_PROMPT | llm
    response = chain.invoke({
        "query": state["query"],
        "sql": state["sql"],
        "result": result_sample
    })
    eval_result = EvalResponse.model_validate_json(response.content)
    
    is_final = eval_result.credible and not state["errors"]
    return {**state, "eval_result": eval_result.model_dump(), "final": is_final}
```

---

### 5.6 Diagnostician Agent

Called when the Evaluator flags an issue or execution fails. Identifies root cause and suggests a fix.

```python
DIAG_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a SQL debugging expert for healthcare data.
Diagnose the issue and classify it:
- schema: wrong table/column name
- syntax: invalid SQL syntax
- logic: correct SQL but wrong answer
- unknown: cannot determine

Return JSON: {{"error_type": str, "fix_suggestion": str, "needs_human": bool}}
Set needs_human=true only if the fix requires domain expertise beyond SQL."""),
    ("human", """Question: {query}
SQL: {sql}
Errors: {errors}
Eval feedback: {eval_feedback}""")
])

def diagnostician_node(state: AgentState) -> AgentState:
    chain = DIAG_PROMPT | llm
    response = chain.invoke({
        "query": state["query"],
        "sql": state["sql"],
        "errors": state["errors"],
        "eval_feedback": str(state.get("eval_result", {}))
    })
    diag = DiagResponse.model_validate_json(response.content)
    return {
        **state,
        "diag_result": diag.model_dump(),
        "needs_human": diag.needs_human,
        "retry_count": state["retry_count"] + 1,
        # Inject fix suggestion into errors so SQL Generator sees it on retry
        "errors": state["errors"] + [f"Fix suggestion: {diag.fix_suggestion}"]
    }
```

---

### 5.7 Human Agent

Final fallback. Surfaces the query, SQL, and all errors for manual review.

```python
def human_node(state: AgentState) -> AgentState:
    print("=" * 60)
    print("⚠️  HUMAN REVIEW REQUIRED")
    print(f"Query     : {state['query']}")
    print(f"Last SQL  : {state['sql']}")
    print(f"Errors    : {state['errors']}")
    print(f"Diag      : {state.get('diag_result', {})}")
    print("=" * 60)
    return {**state, "final": True, "needs_human": True}
```

---

## 6. LangGraph Orchestration

### Routing Logic

```python
from langgraph.graph import END

def after_executor(state: AgentState) -> str:
    """Route after execution: evaluate if result exists, diagnose if error."""
    if state["errors"] and not state["result"]:
        return "diagnose"
    return "evaluate"

def after_eval(state: AgentState) -> str:
    """Route after evaluation: end if credible, diagnose if not."""
    if state.get("final"):
        return END
    if state["retry_count"] >= 3:
        return "human"
    return "diagnose"

def after_diag(state: AgentState) -> str:
    """Route after diagnosis: escalate to human or retry SQL generation."""
    if state.get("needs_human") or state["retry_count"] >= 3:
        return "human"
    return "generate_sql"
```

### Graph Definition

```python
from langgraph.graph import StateGraph

def build_graph():
    graph = StateGraph(AgentState)

    # Register nodes
    graph.add_node("analyze",       analyzer_node)
    graph.add_node("map_schema",    schema_mapper_node)
    graph.add_node("generate_sql",  sql_generator_node)
    graph.add_node("execute",       executor_node)
    graph.add_node("evaluate",      evaluator_node)
    graph.add_node("diagnose",      diagnostician_node)
    graph.add_node("human",         human_node)

    # Linear flow
    graph.set_entry_point("analyze")
    graph.add_edge("analyze",      "map_schema")
    graph.add_edge("map_schema",   "generate_sql")
    graph.add_edge("generate_sql", "execute")

    # Conditional routing
    graph.add_conditional_edges("execute",  after_executor, {"evaluate": "evaluate", "diagnose": "diagnose"})
    graph.add_conditional_edges("evaluate", after_eval,     {END: END, "diagnose": "diagnose", "human": "human"})
    graph.add_conditional_edges("diagnose", after_diag,     {"generate_sql": "generate_sql", "human": "human"})

    graph.add_edge("human", END)

    return graph.compile()

app = build_graph()
```

---

## 7. Running a Query

```python
def run_query(user_question: str):
    initial_state: AgentState = {
        "query": user_question,
        "intent": None,
        "schema": None,
        "sql": None,
        "result": None,
        "eval_result": None,
        "diag_result": None,
        "errors": [],
        "retry_count": 0,
        "final": False,
        "needs_human": False
    }

    final_state = app.invoke(initial_state)

    if final_state["needs_human"]:
        print("Query escalated to human review.")
    elif final_state["result"]:
        print("✅ Result:")
        print(final_state["result"])
        print("\nSQL Used:")
        print(final_state["sql"])
    else:
        print("❌ Query failed. Check errors:")
        print(final_state["errors"])

    return final_state

# Example usage
run_query("How many female patients were diagnosed with diabetes in 2020?")
run_query("What are the top 5 most common conditions in patients over 60?")
```

---

## 8. Notebook Structure (Databricks)

Organize your Databricks notebook in this cell order:

| Cell | Content |
|------|---------|
| 1 | `%pip install langgraph langchain langchain-openai pydantic` |
| 2 | Imports and OpenAI API key setup |
| 3 | Pydantic models (Section 4) |
| 4 | LangGraph state schema (Section 3) |
| 5 | Agent node functions (Section 5) |
| 6 | Routing functions + graph definition (Section 6) |
| 7 | `run_query()` function (Section 7) |
| 8 | Test queries |

---

## 9. Environment Setup

```python
# Cell 1 — Install dependencies
%pip install langgraph langchain langchain-openai pydantic

# Cell 2 — Imports and config
import os
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import StateGraph, END
from pydantic import BaseModel, Field
from typing import TypedDict, Optional, Any, Literal, List, Dict

# Set your OpenAI API key (use Databricks secrets in production)
os.environ["OPENAI_API_KEY"] = dbutils.secrets.get(scope="your-scope", key="openai-api-key")

# Spark is already available in Databricks as `spark`
```

---

## 10. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Separate Evaluator + Diagnostician | Evaluator checks *if* result is wrong; Diagnostician finds *why* — different LLM tasks |
| Value sampling before SQL generation | Prevents silent wrong-result bugs (e.g., `'Female'` vs `'F'`) |
| Temperature 0 on first attempt, 0.3 on retry | Deterministic first pass; variation on retry avoids repeating the same broken SQL |
| Retry cap at 3 → Human Agent | Prevents infinite loops; gives a clean escape hatch |
| Fix suggestion injected into errors list | SQL Generator sees prior mistakes as context in subsequent attempts |
| Destructive SQL regex gate | Non-negotiable safety layer before any `spark.sql()` call |