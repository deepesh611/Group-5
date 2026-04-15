# Project Structure: Natural Language Data Explorer & Self-Healing Pipeline

This document defines the recommended "production-grade" structure for the Natural Language Data Explorer project. It separates core business logic from orchestration and presentation layers to ensure testability, scalability, and maintainability.

## 📂 Project Directory Map

```text
Group-5/
├── .github/                # CI/CD workflows (GitHub Actions)
├── apps/                   # Frontend and user-facing applications
│   └── data_explorer/      # Streamlit app (Databricks App)
│       ├── .streamlit/     # App-specific configuration
│       ├── core/           # Frontend backend logic (SQL, agents)
│       ├── pages/          # Streamlit multi-page UI files
│       ├── requirements.txt# Frontend-specific dependencies
│       └── app.py          # Streamlit entry point
├── config/                 # Static configuration and metadata
│   └── master_schema.json  # Canonical metadata source of truth
├── notebooks/              # Databricks Orchestration (the "glue")
│   ├── ingestion/          # Calls src.ingestion for pipeline 1
│   ├── text_to_sql/        # Calls src.agents for pipeline 2
│   └── advisor/            # Calls src.advisor for pipeline 3
├── src/                    # CORE LOGIC (Importable Python Package)
│   ├── __init__.py
│   ├── common/             # Config, Logging, Secrets, ADLS Utils
│   ├── ingestion/          # Normalization, Drift Detection logic
│   ├── agents/             # Text-to-SQL nodes, prompts, LangGraph
│   ├── advisor/            # Policy logic, DDL validation
│   └── security/           # PHI Masking, FPE, Role checks
├── tests/                  # Automated test suites
│   ├── unit/               # Fast, pure-Python tests (no Spark)
│   ├── integration/        # Tests requiring Spark/Databricks
│   └── e2e/                # Full pipeline/application tests
├── docs/                   # Documentation (Architecture, Guides, ADRs)
├── data/                   # Local sample/test data (ignored by git)
├── scripts/                # Local CLI tools or setup scripts
├── .gitignore              # Git ignore rules
├── requirements.txt        # Shared project-wide dependencies
├── pyproject.toml          # Build system and linting configuration
└── README.md               # Project overview and quickstart
```

---

## 💡 Key Structural Principles

### 1. Separation of Logic and Orchestration
Core business logic (e.g., PHI masking rules, drift detection algorithms) is moved from Databricks notebooks into a standard Python package (`src/`). Databricks notebooks should only serve as "orchestrators" that import and call this logic.

### 2. Flattened Application Architecture
The UI code is moved out of deeply nested research folders into a top-level `apps/` directory. This improves discoverability and simplifies deployment configurations.

### 3. Isolated Configuration
Metadata like `master_schema.json` is stored in a dedicated `config/` directory. This distinguishes static configuration from executable code.

### 4. Comprehensive Testing Strategy
The `tests/` directory mirrors the `src/` directory. This encourages writing unit tests for core logic that can run locally (without Spark) in milliseconds.

### 5. Standardized Environment Management
Dependencies are managed via a root `requirements.txt` for the core package and app-specific `requirements.txt` files for frontends, ensuring minimal and reproducible environments.

### 6. Professional Documentation
The `docs/` folder is used to maintain **Architecture Decision Records (ADRs)**, documenting *why* technical choices (like LangGraph or FPE) were made.
