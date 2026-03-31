# Azure Databricks Setup Guide
## Project 5 — Natural Language Data Explorer & Self-Healing Pipeline

---

## OVERVIEW

This document covers everything you need to set up before writing a single line of pipeline code.
Complete these steps in order — each section is a dependency for the next.

---

## SECTION 1 — Azure Prerequisites (Do this first, outside Databricks)

### 1.1 — Resource Group
Create a dedicated resource group for this project in Azure Portal:
- Name suggestion: `rg-project5-healthcare`
- Region: Choose one and stick to it (e.g. East US 2)

### 1.2 — Azure Data Lake Storage Gen2 (ADLS)
This is where your CSVs, Delta tables, and master_schema.json will live.

Steps:
1. Create a Storage Account (Azure Portal → Storage Accounts → Create)
   - Name: `project5storage` (must be globally unique, lowercase, no hyphens)
   - Performance: Standard
   - Redundancy: LRS (sufficient for a project)
   - **CRITICAL: Enable "Hierarchical namespace"** — this makes it ADLS Gen2

2. Inside the storage account, create these containers:
   ```
   raw/                         ← Synthea CSVs land here
   delta/                       ← Delta tables written here
   metadata/                    ← master_schema.json lives here
   logs/                        ← Pipeline run logs, drift event logs
   ```

3. Note down:
   - Storage account name: `<your_storage_account_name>`
   - Container name (use one container with folders, or separate containers — your choice)
   - Storage account key (Portal → Storage Account → Access Keys → key1)

### 1.3 — Azure Databricks Workspace
1. Portal → Create a resource → Azure Databricks
2. Workspace name: `project5-databricks`
3. Pricing tier: **Premium** (required for Unity Catalog and Secret Scopes)
4. Attach to the same resource group and region as your storage account

---

## SECTION 2 — Databricks Workspace Setup

### 2.1 — Unity Catalog Setup
Unity Catalog is the governance layer that manages your Delta tables.

Steps (inside Databricks workspace):
1. Go to **Data** → **Catalog** in the left sidebar
2. If Unity Catalog is not yet enabled, follow the Databricks prompt to enable it
   (requires an Account Admin — this may need your Azure AD admin)
3. Create a **Catalog**:
   - Name: `project5`
4. Inside `project5`, create a **Schema**:
   - Name: `delta_tables`

Your tables will be referenced as: `project5.delta_tables.<table_name>`

### 2.2 — Compute (Cluster Setup)
Create one cluster for the project:

1. Sidebar → **Compute** → **Create Compute**
2. Settings:
   ```
   Cluster name:     project5-cluster
   Policy:           Unrestricted
   Cluster mode:     Single Node (sufficient for dev/testing)
   Databricks Runtime: 14.3 LTS (includes Apache Spark 3.5, Scala 2.12)
                       — LTS = Long Term Support, stable for production
   Node type:        Standard_DS3_v2 (14GB RAM, 4 cores — good balance)
   Auto-termination: 30 minutes (saves cost)
   ```

3. Under **Advanced Options → Spark Config**, add:
   ```
   spark.databricks.delta.preview.enabled true
   spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
   spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
   ```

4. Under **Advanced Options → Environment Variables**, add:
   ```
   OPENAI_API_KEY={{secrets/project5-scope/openai-api-key}}
   ```
   (We will create this secret scope in Section 3)

### 2.3 — Install Required Libraries on Cluster
After creating the cluster, go to the cluster → **Libraries** tab → **Install New**:

Install these via PyPI:
```
openai>=1.0.0
langchain>=0.2.0
langchain-openai>=0.1.0
azure-storage-blob>=12.19.0
azure-identity>=1.15.0
ff3                          ← Format-Preserving Encryption library
```

Install via Maven (for Delta):
```
io.delta:delta-spark_2.12:3.1.0
```
(Usually pre-installed on Databricks Runtime 14.3+, but verify)

---

## SECTION 3 — Secret Scope Setup (Security-Critical)

Never hardcode credentials. Databricks Secret Scopes store them encrypted.

### 3.1 — Create a Secret Scope
In your browser, navigate to:
```
https://<your-databricks-workspace-url>#secrets/createScope
```

Fill in:
```
Scope Name:    project5-scope
Manage Principal: All Users (or restrict to your user for tighter security)
```

If using Azure Key Vault-backed scope (recommended for production):
- Create an Azure Key Vault first
- Link it during scope creation
- This lets Azure manage key rotation automatically

### 3.2 — Add Secrets via Databricks CLI
Install the CLI locally:
```bash
pip install databricks-cli
databricks configure --token
# Enter your workspace URL and a personal access token
```

Add your secrets:
```bash
# OpenAI API Key
databricks secrets put --scope project5-scope --key openai-api-key
# (paste your OpenAI key when prompted)

# ADLS Storage Account Key
databricks secrets put --scope project5-scope --key adls-account-key
# (paste your storage account key when prompted)

# ADLS Account Name (not sensitive but good practice to centralise)
databricks secrets put --scope project5-scope --key adls-account-name
# (paste your storage account name e.g. project5storage)

# ADLS Container Name
databricks secrets put --scope project5-scope --key adls-container-name
# (paste e.g. project5data)
```

### 3.3 — Verify Secrets Work in a Notebook
Run this in a Databricks notebook to verify (output will be [REDACTED] — that's correct):
```python
import os
openai_key = dbutils.secrets.get(scope="project5-scope", key="openai-api-key")
print(f"Key loaded: {'[OK]' if openai_key else '[FAILED]'}")
# Never print the actual key value
```

---

## SECTION 4 — ADLS Mount Point Setup

Mount your ADLS storage to Databricks so all notebooks access it via a clean path.

Create a notebook called `00_mount_adls` and run it once:

```python
# 00_mount_adls.py
# Run ONCE to mount ADLS. After mounting, the path /mnt/project5/ is available
# to ALL notebooks on this workspace.

storage_account = dbutils.secrets.get("project5-scope", "adls-account-name")
storage_key     = dbutils.secrets.get("project5-scope", "adls-account-key")
container       = dbutils.secrets.get("project5-scope", "adls-container-name")

mount_point = "/mnt/project5"

# Unmount if already mounted (safe to re-run)
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

dbutils.fs.mount(
    source=f"wasbs://{container}@{storage_account}.blob.core.windows.net/",
    mount_point=mount_point,
    extra_configs={
        f"fs.azure.account.key.{storage_account}.blob.core.windows.net": storage_key
    }
)

# Verify
display(dbutils.fs.ls("/mnt/project5/"))
```

After mounting, your paths in all notebooks become:
```
/mnt/project5/raw/            ← Synthea CSVs
/mnt/project5/delta/          ← Delta tables
/mnt/project5/metadata/       ← master_schema.json
/mnt/project5/logs/           ← Pipeline logs
```

---

## SECTION 5 — Repo & Notebook Structure in Databricks

### 5.1 — Connect Your GitHub Repo to Databricks
1. Sidebar → **Repos** → **Add Repo**
2. Connect via GitHub (OAuth or PAT token)
3. Enter your repo URL: `https://github.com/<your-org>/project_genie_ai`

This lets you commit/pull directly from Databricks notebooks.

### 5.2 — Final Folder Structure Inside Databricks Repos

```
project_genie_ai/
│
├── notebooks/
│   ├── utils/
│   │   ├── 00_config.py              ← Central config: catalog, paths, scope name
│   │   ├── 01_schema_utils.py        ← Drift detection, schema diff logic
│   │   ├── 02_phi_masking.py         ← FPE + dynamic masking by role
│   │   ├── 03_openai_client.py       ← OpenAI API wrapper, prompt builders
│   │   └── 04_metadata_manager.py    ← ADLS read/write for master_schema.json
│   │
│   ├── pipeline_1_ingestion.py       ← CSV → Bronze Delta → Silver Delta
│   │                                    Calls 01_schema_utils for pre-flight check
│   │                                    Triggers pipeline_3_advisor on drift
│   │
│   ├── pipeline_2_extractor.py       ← NL input → 03_openai_client → SQL
│   │                                    → Spark execute → 02_phi_masking → output
│   │
│   └── pipeline_3_advisor.py         ← Drift event input → AI Advisor prompt
│                                        → Human approval widget → ALTER TABLE
│                                        → 04_metadata_manager rewrites JSON
│
├── schema/
│   └── master_schema.json            ← Canonical copy in repo (source of truth
│                                        is the ADLS copy at /mnt/project5/metadata/)
│
├── data/
│   └── sample/                       ← Small sample Synthea CSVs for local testing
│
└── README.md
```

### 5.3 — How Notebooks Call Each Other
In Databricks, utility notebooks are loaded with `%run`:

```python
# At the top of pipeline_1_ingestion.py:
%run ./utils/00_config
%run ./utils/01_schema_utils
%run ./utils/02_phi_masking
```

This executes the utility notebook inline, making all its functions and variables
available in the calling notebook's scope — equivalent to an import.

---

## SECTION 6 — Databricks Jobs Setup (Automation)

### Job 1 — Ingestion Pipeline (Scheduled)
```
Job name:     Pipeline1_Ingestion
Task:         pipeline_1_ingestion.py
Cluster:      project5-cluster
Schedule:     Daily at 2:00 AM (or file-arrival trigger if using Event Grid)
On failure:   Email notification + auto-trigger Pipeline3_Advisor job
```

### Job 2 — AI Extractor (On-Demand)
```
Job name:     Pipeline2_Extractor
Task:         pipeline_2_extractor.py
Cluster:      project5-cluster
Trigger:      Manual / REST API call (user submits a question)
Parameters:   user_query (string), user_role (doctor|analyst|sysadmin)
```

### Job 3 — AI Advisor / Drift Monitor (Event-Driven)
```
Job name:     Pipeline3_Advisor
Task:         pipeline_3_advisor.py
Cluster:      project5-cluster
Trigger:      Called by Pipeline1 on drift detection OR run on schedule as always-active monitor
Parameters:   drift_event (JSON string describing the detected change)
```

To set up jobs: Sidebar → **Workflows** → **Create Job** → configure as above.

---

## SECTION 7 — Upload master_schema.json to ADLS

Once your mount is set up, upload the master_schema.json from the repo to ADLS
so both Pipeline 2 and Pipeline 3 can access it at runtime:

```python
# Run once in a notebook after mounting
import json

# Read from repo location
with open("/Workspace/Repos/<your-username>/project_genie_ai/schema/master_schema.json") as f:
    schema = json.load(f)

# Write to ADLS mount
with open("/dbfs/mnt/project5/metadata/master_schema.json", "w") as f:
    json.dump(schema, f, indent=2)

print("master_schema.json uploaded to ADLS successfully.")
```

---

## SECTION 8 — Prerequisites Checklist

Before starting notebook development, verify each item:

### Azure
- [ ] Resource group created
- [ ] ADLS Gen2 storage account created with hierarchical namespace enabled
- [ ] Containers created: raw/, delta/, metadata/, logs/
- [ ] Azure Databricks workspace created (Premium tier)

### Databricks Workspace
- [ ] Unity Catalog enabled
- [ ] Catalog `project5` created
- [ ] Schema `project5.delta_tables` created
- [ ] Cluster created with correct runtime (14.3 LTS) and Spark config
- [ ] All PyPI libraries installed on cluster
- [ ] Secret scope `project5-scope` created
- [ ] Secrets added: openai-api-key, adls-account-key, adls-account-name, adls-container-name
- [ ] Secret verified in a test notebook (returns [REDACTED])
- [ ] ADLS mounted at /mnt/project5/ and verified with dbutils.fs.ls()

### Repo & Files
- [ ] GitHub repo connected to Databricks Repos
- [ ] Folder structure created as per Section 5.2
- [ ] master_schema.json uploaded to /mnt/project5/metadata/

### Data
- [ ] Synthea CSVs downloaded from https://synthea.mitre.org
- [ ] All 9 CSVs uploaded to /mnt/project5/raw/

Once all boxes are checked, we begin writing the utility notebooks (utils/),
starting with 00_config.py, then 01_schema_utils.py, and so on upward.

---

## QUICK REFERENCE — Key Paths

| Resource              | Path / Reference                                              |
|-----------------------|---------------------------------------------------------------|
| Raw CSVs              | /mnt/project5/raw/                                            |
| Delta tables          | project5.delta_tables.<table_name>                            |
| master_schema.json    | /mnt/project5/metadata/master_schema.json                     |
| Pipeline logs         | /mnt/project5/logs/                                           |
| Secret scope          | project5-scope                                                |
| OpenAI key secret     | dbutils.secrets.get("project5-scope", "openai-api-key")       |
| ADLS key secret       | dbutils.secrets.get("project5-scope", "adls-account-key")     |

---
*Setup guide for Project 5 — Natural Language Data Explorer & Self-Healing Pipeline*


## How to test ingestion pipeline:
Follow the 4-step progressive test plan in the walkthrough:

Dry run on patients only — validates ADLS connectivity, schema loading, drift detection (0 writes)
Real write on patients only — validates Delta write, row count check, metadata update
Drift detection test — add a column to a CSV copy and run dry-run to see the WARNING
Full run all tables — production run, ~15–30 min total

## How to Test Manually

### Step 1: Pre-test checklist
□ External Location created and accessible (run ensure_adls_dirs() in 00_config)
□ master_schema.json uploaded to {ADLS_BASE}/metadata/master_schema.json
□ At least one Synthea CSV uploaded to {ADLS_BASE}/raw/patients.csv
□ Secret scope 'llm-scope' with key 'openai-key' configured
□ Cluster running (DBR 14.3 LTS or higher, with langchain_openai library)
□ Unity Catalog enabled with catalog 'project_5' and schema 'delta_tables' created

### Step 2: Test 1 — Dry run (safe, no writes)
Open pipeline_1_ingestion in Databricks Repos
Set widget dry_run = true, table_filter = patients
Click Run All
Expected output:
[1/6] Reading CSV: abfss://...patients.csv
[2/6] Normalizing column names
[3/6] Pre-flight schema drift check
✅ patients: No schema drift detected.
[5/6] DRY RUN — would write to project_5.delta_tables.patients
⏭️  patients  rows=0  cols=17  drift=NONE  status=SKIPPED

### Step 3: Test 2 — Single table real write
Set dry_run = false, table_filter = patients, on_critical_drift = fallback
Run All
Expected output:
[1/6] Reading CSV: Mode: explicit schema (17 fields) from master_schema.json
Rows: 132,893  |  Columns: 17
[3/6] Pre-flight schema drift check
✅ patients: No schema drift detected.
[5/6] Writing Delta table → project_5.delta_tables.patients
✓ 132,893 rows written and verified
[6/6] Updating metadata ...
✅ 'patients' complete — 132,893 rows
Verify in Databricks SQL: SELECT COUNT(*) FROM project_5.delta_tables.patients

### Step 4: Test 3 — Drift detection (safe, no writes)
Upload a modified patients.csv to ADLS raw/ with an extra column (e.g., add blood_type column)
Set dry_run = true, table_filter = patients
Run All
Expected output:
[3/6] Pre-flight schema drift check
⚠️  DRIFT DETECTED — patients  (Severity: WARNING)
+ New columns (1):
    blood_type                       string

### Step 5: Test 4 — Full pipeline all tables
Set table_filter = all, dry_run = false, on_critical_drift = fallback
Run All (expect ~15-30 minutes for 10M rows total)
Verify in Databricks SQL:
sql
SELECT table_name, COUNT(*) as rows
FROM project_5.information_schema.tables
WHERE table_schema = 'delta_tables'
GROUP BY table_name
ORDER BY rows DESC

### Step 6: Verify ADLS logs
python
# Run in a Databricks notebook cell to check audit logs
display(dbutils.fs.ls("abfss://project5data@project5storage.dfs.core.windows.net/logs/ingestion/"))
display(dbutils.fs.ls("abfss://project5data@project5storage.dfs.core.windows.net/metadata/"))
