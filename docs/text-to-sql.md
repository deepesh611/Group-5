# 🧠 Phase 2: Text → SQL (with LangGraph)

## 🎯 Goal

Convert **natural language → SQL → execute on Delta tables**

---

## 🧠 Full Agent List (Text → SQL)

1. 🧭 Analyzer Agent
Understand user query
Output: intent
2. 🧠 Planner Agent (sometimes merged, but better to keep)
Break query into steps
Helps with complex queries
3. 📚 Schema Agent
Select tables, columns, joins
4. 🧾 SQL Generator Agent
Generate SQL
5. ⚡ Executor Agent
Run SQL (Spark)
6. 🔍 Validator / Diagnoser Agent
Check:
SQL errors
wrong results
7. 🔁 Fix Agent
Correct SQL using feedback
8. 👨‍💻 Human Agent (optional but you’re including)
Final fallback

*(Validator + Fix come later)*

---

# 🔗 Flow (LangGraph)

```text id="h3ynf8"
User Query
   ↓
Analyzer Node
   ↓
Schema Node
   ↓
SQL Node
   ↓
Executor Node
   ↓
Result
```

---

# ⚙️ How LangGraph fits

* Each **agent = node**
* Data passed as **state (dict/JSON)**

Example state:

```json id="fb08ic"
{
  "query": "...",
  "intent": {...},
  "schema": {...},
  "sql": "...",
  "result": ...
}
```

---

# 🧠 Execution logic

* LangGraph handles:

  * flow between agents
  * state passing

👉 No manual pipeline needed

---

# 🔧 Tech stack

* **LangGraph** → orchestration
* **LangChain** → LLM calls
* **ChatGPT API** → reasoning
* **PySpark** → SQL execution
* **Delta (local)** → data
