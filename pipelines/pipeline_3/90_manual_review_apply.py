# Databricks notebook source
# =============================================================================
# Pipeline 3 / Manual Utility — Review + Apply
# =============================================================================
# Use this notebook ONLY when 06_finalize returns:
#   status = error
#   action = manual_review_required
#
# Responsibilities:
#   1. Load prior recommendation + validation artifacts.
#   2. Let a human approve/decline, or override SQL/NEW_JSON.
#   3. Re-validate in interactive mode.
#   4. Apply DDL + metadata if approved.
#
# This is intentionally separate from the automated workflow.

# COMMAND ----------

# MAGIC %run ../utils/00_config

# COMMAND ----------

# MAGIC %run ../utils/04_metadata_manager

# COMMAND ----------

# MAGIC %run ../utils/05_advisor_state_manager

# COMMAND ----------

# MAGIC %run ../utils/06_advisor_policy

# COMMAND ----------

# DBTITLE 1,Load OpenAI client for live AI demo
# MAGIC %run ../utils/03_openai_client

# COMMAND ----------

import json

# COMMAND ----------

dbutils.widgets.text("table_name", "", "Table name")
dbutils.widgets.text("run_id", "", "Advisor run id")
dbutils.widgets.dropdown("approval_decision", "decline", ["approve", "decline"], "Approval")
dbutils.widgets.text("override_sql_fix", "", "Override SQL_FIX (optional)")
dbutils.widgets.text("override_new_json", "", "Override NEW_JSON JSON (optional)")

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()
DECISION = dbutils.widgets.get("approval_decision").strip().lower()
OVERRIDE_SQL = dbutils.widgets.get("override_sql_fix").strip()
OVERRIDE_NEW_JSON = dbutils.widgets.get("override_new_json").strip()

# COMMAND ----------

# DBTITLE 1,Review AI Advisor Recommendation
# ─── Call the AI Advisor LIVE and display full recommendation ─────────────────
import html as _html
import time as _time

# Re-read widgets
TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()
DECISION = dbutils.widgets.get("approval_decision").strip().lower()

assert TABLE_NAME, "table_name widget is empty"
assert RUN_ID, "run_id widget is empty"

# ── Load drift from the intake artifact ──
_intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
_drift = _intake.get("drift", {})
_kind = drift_kind(_drift)

# ── Call GPT-4o LIVE ──
print("⏳ Calling AI Advisor (GPT-4o)...")
_t0 = _time.time()

_table_meta = get_table_metadata(TABLE_NAME)
_advisor_prompt = build_advisor_prompt(_drift, TABLE_NAME, _table_meta)

_template = build_advisor_prompt_template()
_llm = get_llm(temperature=0.0)
_chain = _template | _llm
_response = call_with_retry(_chain, {"advisor_prompt": _advisor_prompt})
_raw_text = _response.content
_elapsed = round(_time.time() - _t0, 1)

print(f"✅ AI responded in {_elapsed}s")

# ── Parse with guardrails ──
_parsed = parse_advisor_response(_raw_text, drift_report=_drift)

# ── Run policy validation ──
_validation = validate_recommendation(
    drift=_drift,
    rec=_parsed,
    table_name=TABLE_NAME,
    full_table_name=get_full_table_name(TABLE_NAME),
    run_mode="interactive",
)

# ── Store for Cell 10 (Execute Approval) ──
_live_rec = _parsed  # available if user wants to apply

# ── Build display ──
_severity = _parsed.get("SEVERITY", "UNKNOWN")
_sev_colors = {"INFO": "#2196F3", "WARNING": "#FF9800", "CRITICAL": "#F44336", "UNKNOWN": "#9E9E9E"}
_sev_color = _sev_colors.get(_severity, "#9E9E9E")
_strategy = _validation.get("execution_strategy", "N/A")
_strat_colors = {
    "AUTO_APPLY_DDL": "#4CAF50", "METADATA_ONLY": "#FF9800",
    "ADVISORY_ONLY": "#2196F3", "MANUAL_REVIEW": "#F44336",
    "NO_ACTION": "#9E9E9E"
}
_strat_color = _strat_colors.get(_strategy, "#9E9E9E")
_val_ok = _validation.get("validation_ok", None)
_val_color = "#4CAF50" if _val_ok else "#F44336" if _val_ok is False else "#9E9E9E"
_val_text = "PASS" if _val_ok else "FAIL" if _val_ok is False else "N/A"

_sql_fix = _parsed.get("SQL_FIX", "") or ""
_new_json_raw = _parsed.get("NEW_JSON", {}) or {}
_new_json_str = json.dumps(_new_json_raw, indent=2) if _new_json_raw else ""
_reasoning = _parsed.get("REASONING", "N/A") or "N/A"

_new_cols = [c["column"] for c in _drift.get("new_columns", [])]
_missing_cols = [c["column"] for c in _drift.get("missing_columns", [])]
_type_changes = [c.get("column", str(c)) for c in _drift.get("type_changes", [])]

_dec_color = "#F44336" if DECISION == "decline" else "#4CAF50"

def _code_block(code_id, code_text, label):
    if not code_text:
        return f'<p style="color:#999;font-style:italic;">No {label} generated</p>'
    escaped = _html.escape(code_text)
    return f'''
    <div style="position:relative;margin:8px 0;">
      <pre id="{code_id}" style="background:#1e1e1e;color:#d4d4d4;padding:14px 50px 14px 14px;
        border-radius:6px;overflow-x:auto;font-size:13px;line-height:1.5;margin:0;
        white-space:pre-wrap;word-wrap:break-word;">{escaped}</pre>
      <button onclick="navigator.clipboard.writeText(document.getElementById('{code_id}').innerText)
        .then(()=>{{this.innerText='\u2713 Copied';setTimeout(()=>this.innerText='Copy',1500)}})"
        style="position:absolute;top:8px;right:8px;background:#444;color:#fff;border:none;
        border-radius:4px;padding:4px 10px;cursor:pointer;font-size:12px;">Copy</button>
    </div>'''

_html_content = f'''
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  max-width:860px;margin:16px auto;">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#1a237e,#283593);color:#fff;padding:20px 24px;
    border-radius:12px 12px 0 0;">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <div>
        <h2 style="margin:0;font-size:20px;">\U0001f9e0 AI Schema Advisor — Live</h2>
        <p style="margin:4px 0 0;opacity:0.8;font-size:13px;">Table: <b>{TABLE_NAME}</b>
          &nbsp;|&nbsp; Run: <code style="background:rgba(255,255,255,0.15);padding:2px 6px;
          border-radius:3px;">{RUN_ID}</code>
          &nbsp;|&nbsp; GPT-4o responded in <b>{_elapsed}s</b></p>
      </div>
      <span style="background:{_sev_color};padding:6px 16px;border-radius:20px;font-weight:600;
        font-size:13px;letter-spacing:0.5px;">{_severity}</span>
    </div>
  </div>

  <div style="border:1px solid #e0e0e0;border-top:none;border-radius:0 0 12px 12px;overflow:hidden;">

    <!-- Drift Summary -->
    <div style="padding:18px 24px;border-bottom:1px solid #e0e0e0;background:#fafafa;">
      <h3 style="margin:0 0 12px;font-size:15px;color:#333;">Detected Drift
        <span style="background:#e3f2fd;color:#1565c0;padding:2px 10px;border-radius:12px;
          font-size:12px;margin-left:8px;font-weight:500;">{_kind}</span></h3>
      <div style="display:flex;gap:24px;flex-wrap:wrap;">
        <div>{'\U0001f7e2' if _new_cols else '\u26aa'} <b>New columns:</b>
          {', '.join(f'<code>{c}</code>' for c in _new_cols) if _new_cols else '<span style="color:#999">none</span>'}</div>
        <div>{'\U0001f534' if _missing_cols else '\u26aa'} <b>Missing columns:</b>
          {', '.join(f'<code>{c}</code>' for c in _missing_cols) if _missing_cols else '<span style="color:#999">none</span>'}</div>
        <div>{'\U0001f7e1' if _type_changes else '\u26aa'} <b>Type changes:</b>
          {', '.join(f'<code>{c}</code>' for c in _type_changes) if _type_changes else '<span style="color:#999">none</span>'}</div>
      </div>
    </div>

    <!-- AI Reasoning -->
    <div style="padding:18px 24px;border-bottom:1px solid #e0e0e0;">
      <h3 style="margin:0 0 8px;font-size:15px;color:#333;">\U0001f4ac AI Reasoning</h3>
      <p style="margin:0;color:#444;line-height:1.7;font-size:14px;background:#f8f9fa;
        padding:12px 16px;border-radius:8px;border-left:4px solid {_sev_color};">{_html.escape(_reasoning)}</p>
    </div>

    <!-- SQL_FIX -->
    <div style="padding:18px 24px;border-bottom:1px solid #e0e0e0;">
      <h3 style="margin:0 0 8px;font-size:15px;color:#333;">\U0001f527 SQL_FIX
        {'<span style="background:#e8f5e9;color:#2e7d32;padding:2px 8px;border-radius:10px;font-size:11px;margin-left:8px;">Will auto-execute</span>' if _sql_fix and _strategy == 'AUTO_APPLY_DDL' else ''}</h3>
      {_code_block("sql_fix_code", _sql_fix, "SQL fix — drift type does not require DDL")}
    </div>

    <!-- NEW_JSON -->
    <div style="padding:18px 24px;border-bottom:1px solid #e0e0e0;">
      <h3 style="margin:0 0 8px;font-size:15px;color:#333;">\U0001f4cb NEW_JSON (Schema Update)</h3>
      {_code_block("new_json_code", _new_json_str, "schema update — drift type does not require new columns")}
    </div>

    <!-- Raw LLM Response -->
    <div style="padding:18px 24px;border-bottom:1px solid #e0e0e0;background:#fafafa;">
      <h3 style="margin:0 0 8px;font-size:15px;color:#333;">\U0001f916 Raw GPT-4o Response
        <span style="color:#999;font-weight:400;font-size:12px;margin-left:8px;">before guardrails</span></h3>
      {_code_block("raw_llm_code", _raw_text, "LLM response")}
    </div>

    <!-- Policy Validation -->
    <div style="padding:18px 24px;border-bottom:1px solid #e0e0e0;">
      <h3 style="margin:0 0 12px;font-size:15px;color:#333;">\U0001f6e1\ufe0f Policy Validation</h3>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
        <div style="background:#f5f5f5;padding:12px 16px;border-radius:8px;">
          <div style="font-size:12px;color:#999;margin-bottom:4px;">Strategy</div>
          <div style="font-weight:600;"><span style="display:inline-block;width:10px;height:10px;
            border-radius:50%;background:{_strat_color};margin-right:6px;"></span>{_strategy}</div>
        </div>
        <div style="background:#f5f5f5;padding:12px 16px;border-radius:8px;">
          <div style="font-size:12px;color:#999;margin-bottom:4px;">Validation</div>
          <div style="font-weight:600;color:{_val_color};">{_val_text}</div>
        </div>
        <div style="background:#f5f5f5;padding:12px 16px;border-radius:8px;">
          <div style="font-size:12px;color:#999;margin-bottom:4px;">SQL Check</div>
          <div>{_validation.get('sql_validation', {}).get('reason', 'N/A')}</div>
        </div>
        <div style="background:#f5f5f5;padding:12px 16px;border-radius:8px;">
          <div style="font-size:12px;color:#999;margin-bottom:4px;">Cross Check</div>
          <div>{_validation.get('cross_validation', {}).get('reason', 'N/A')}</div>
        </div>
      </div>
    </div>

    <!-- Decision Banner -->
    <div style="padding:18px 24px;background:{'#fff3e0' if DECISION == 'decline' else '#e8f5e9'};">
      <div style="display:flex;align-items:center;gap:12px;">
        <span style="background:{_dec_color};color:#fff;padding:6px 16px;border-radius:20px;
          font-weight:600;font-size:13px;">{DECISION.upper()}</span>
        <span style="color:#555;font-size:14px;">
          {'\u23f8\ufe0f Pipeline will <b>halt</b> here. Use <b>Repair run</b> with <code>approval_decision=approve</code> to proceed.'
           if DECISION == 'decline'
           else '\u2705 Proceeding with apply...'}
        </span>
      </div>
    </div>

  </div>
</div>
'''

displayHTML(_html_content)

# COMMAND ----------

# DBTITLE 1,Execute Approval Decision
# ─── Halt on decline, apply on approve ────────────────────────────────────────
import html as _html

TABLE_NAME = dbutils.widgets.get("table_name").strip().lower()
RUN_ID = dbutils.widgets.get("run_id").strip()
DECISION = dbutils.widgets.get("approval_decision").strip().lower()
OVERRIDE_SQL = dbutils.widgets.get("override_sql_fix").strip()
OVERRIDE_NEW_JSON = dbutils.widgets.get("override_new_json").strip()

assert TABLE_NAME, "table_name widget is empty — set it before running this cell"
assert RUN_ID, "run_id widget is empty — set it before running this cell"

try:
    intake = read_advisor_artifact(RUN_ID, "01_intake", TABLE_NAME)
    drift = intake["drift"]

    try:
        rec = dict(_live_rec)
        log.info("[P3] Using live AI recommendation from Cell 9")
    except NameError:
        rec_artifact = read_advisor_artifact(RUN_ID, "02_recommendation", TABLE_NAME)
        rec = rec_artifact.get("recommendation", {})
        log.info("[P3] Using stored recommendation from artifact")

    if OVERRIDE_SQL:
        rec["SQL_FIX"] = OVERRIDE_SQL
    if OVERRIDE_NEW_JSON:
        rec["NEW_JSON"] = json.loads(OVERRIDE_NEW_JSON)

    validation = validate_recommendation(
        drift=drift, rec=rec, table_name=TABLE_NAME,
        full_table_name=get_full_table_name(TABLE_NAME), run_mode="interactive",
    )

    if DECISION != "approve":
        decline_result = {
            "status": "fix_declined", "table": TABLE_NAME, "run_id": RUN_ID,
            "severity": rec.get("SEVERITY", drift.get("severity", "UNKNOWN")),
            "sql_fix": rec.get("SQL_FIX", ""), "ddl_executed": False,
            "schema_updated": False, "reasoning": "Awaiting human approval.",
            "missing_columns": [m["column"] for m in drift.get("missing_columns", [])],
            "action": "manual_review_required",
        }
        write_advisor_artifact(RUN_ID, "90_manual_review", decline_result, TABLE_NAME)
        raise RuntimeError(
            f"[P3] Manual review required for {TABLE_NAME} (run {RUN_ID}). "
            f"→ To approve: Repair this run and set approval_decision='approve'."
        )

    if not validation["validation_ok"]:
        raise ValueError(f"Manual plan failed validation: {validation}")

    ddl_executed = False
    schema_updated = False
    ddl_note = ""
    sql_fix = rec.get("SQL_FIX", "") or ""
    new_json = rec.get("NEW_JSON", {}) or {}

    if sql_fix:
        try:
            spark.sql(sql_fix)
            ddl_executed = True
            ddl_note = "DDL executed successfully"
        except Exception as ddl_err:
            if "already exists" in str(ddl_err).lower():
                ddl_executed = True
                ddl_note = "Column already exists — treated as idempotent success"
            else:
                raise
    if new_json:
        update_table_columns(TABLE_NAME, new_json)
        schema_updated = True

    _result = {
        "status": "fix_applied" if (ddl_executed or schema_updated) else "advisory",
        "table": TABLE_NAME, "run_id": RUN_ID,
        "severity": rec.get("SEVERITY", drift.get("severity", "UNKNOWN")),
        "sql_fix": sql_fix, "ddl_executed": ddl_executed,
        "schema_updated": schema_updated, "ddl_note": ddl_note,
        "reasoning": rec.get("REASONING", ""),
        "missing_columns": [m["column"] for m in drift.get("missing_columns", [])],
        "action": "",
    }
    write_advisor_artifact(RUN_ID, "90_manual_review", _result, TABLE_NAME)
except Exception as e:
    raise

# ─── Build Result Card ───
_st = _result["status"]
_sev = _result["severity"]
_sev_colors = {"INFO": "#2196F3", "WARNING": "#FF9800", "CRITICAL": "#F44336", "UNKNOWN": "#9E9E9E"}
_sev_c = _sev_colors.get(_sev, "#9E9E9E")

if _st == "fix_applied":
    _hdr_bg = "linear-gradient(135deg,#1b5e20,#2e7d32)"
    _st_icon, _st_label = "\u2705", "Fix Applied"
elif _st == "advisory":
    _hdr_bg = "linear-gradient(135deg,#e65100,#ef6c00)"
    _st_icon, _st_label = "\u26a0\ufe0f", "Advisory Only"
else:
    _hdr_bg = "linear-gradient(135deg,#b71c1c,#c62828)"
    _st_icon, _st_label = "\u274c", "Error"

_sf = _result["sql_fix"]
_mc = _result.get("missing_columns", [])
_reasoning = _result.get("reasoning", "") or "N/A"
_dn = _result.get("ddl_note", "")

def _mk_code(cid, code):
    if not code:
        return '<p style="color:#999;font-style:italic;">No SQL fix generated</p>'
    esc = _html.escape(code)
    return (
        '<div style="position:relative;margin:8px 0;">'
        f'<pre id="{cid}" style="background:#1e1e1e;color:#d4d4d4;padding:14px 50px 14px 14px;'
        'border-radius:6px;overflow-x:auto;font-size:13px;line-height:1.5;margin:0;'
        f'white-space:pre-wrap;word-wrap:break-word;">{esc}</pre>'
        f'<button onclick="navigator.clipboard.writeText(document.getElementById(\'{cid}\').innerText)'
        '.then(()=>{this.innerText=\'\u2713 Copied\';setTimeout(()=>this.innerText=\'Copy\',1500)})"'
        ' style="position:absolute;top:8px;right:8px;background:#444;color:#fff;border:none;'
        'border-radius:4px;padding:4px 10px;cursor:pointer;font-size:12px;">Copy</button></div>'
    )

def _mk_stat(label, content):
    return (
        '<div style="background:#fff;padding:14px 16px;border-radius:8px;'
        'border:1px solid #e0e0e0;text-align:center;">'
        '<div style="font-size:11px;color:#999;text-transform:uppercase;'
        f'letter-spacing:0.5px;margin-bottom:6px;">{label}</div>'
        f'<div style="font-size:18px;">{content}</div></div>'
    )

_warn_html = ""
if _mc:
    _mc_badges = ", ".join(
        f'<code style="background:#ffccbc;padding:2px 6px;border-radius:3px;">{c}</code>'
        for c in _mc
    )
    _warn_html = (
        '<div style="padding:18px 24px;background:#fff3e0;">'
        '<h3 style="margin:0 0 8px;font-size:15px;color:#e65100;">'
        '\u26a0\ufe0f Upstream Data Loss Detected</h3>'
        '<p style="margin:0;color:#bf360c;font-size:14px;">'
        f'Missing columns (absent from CSV): {_mc_badges}'
        '<br><span style="font-size:13px;color:#999;">'
        'These columns still exist in Delta but will contain NULLs for new rows.</span></p></div>'
    )

_ddl_title = '\U0001f527 DDL Executed' if _result['ddl_executed'] else '\U0001f527 SQL_FIX'
_ddl_note_html = f'<div style="margin-top:6px;font-size:13px;color:#666;font-style:italic;">{_html.escape(_dn)}</div>' if _dn else ""

_h = (
    '<div style="font-family:-apple-system,BlinkMacSystemFont,Roboto,sans-serif;'
    'max-width:860px;margin:16px auto;">'
    f'<div style="background:{_hdr_bg};color:#fff;padding:20px 24px;'
    'border-radius:12px 12px 0 0;">'
    '<div style="display:flex;justify-content:space-between;align-items:center;">'
    f'<div><h2 style="margin:0;font-size:20px;">{_st_icon} Execution Result</h2>'
    f'<p style="margin:4px 0 0;opacity:0.85;font-size:13px;">Table: <b>{TABLE_NAME}</b>'
    f' &nbsp;|&nbsp; Run: <code style="background:rgba(255,255,255,0.15);padding:2px 6px;'
    f'border-radius:3px;">{RUN_ID}</code></p></div>'
    '<span style="background:rgba(255,255,255,0.25);padding:6px 16px;border-radius:20px;'
    f'font-weight:600;font-size:14px;">{_st_label}</span>'
    '</div></div>'
    '<div style="border:1px solid #e0e0e0;border-top:none;border-radius:0 0 12px 12px;overflow:hidden;">'
    '<div style="padding:20px 24px;border-bottom:1px solid #e0e0e0;background:#fafafa;">'
    '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;">'
    + _mk_stat("DDL Executed", "\u2705" if _result["ddl_executed"] else "\u274c")
    + _mk_stat("Schema Updated", "\u2705" if _result["schema_updated"] else "\u274c")
    + _mk_stat("Severity", f'<span style="color:{_sev_c};font-weight:700;">{_sev}</span>')
    + _mk_stat("Status", f'<span style="font-weight:700;">{_st_label}</span>')
    + '</div></div>'
    f'<div style="padding:18px 24px;border-bottom:1px solid #e0e0e0;">'
    f'<h3 style="margin:0 0 8px;font-size:15px;color:#333;">{_ddl_title}</h3>'
    + _mk_code("exec_sql", _sf) + _ddl_note_html + '</div>'
    '<div style="padding:18px 24px;border-bottom:1px solid #e0e0e0;">'
    '<h3 style="margin:0 0 8px;font-size:15px;color:#333;">\U0001f4ac AI Reasoning</h3>'
    f'<p style="margin:0;color:#444;line-height:1.7;font-size:14px;background:#f8f9fa;'
    f'padding:12px 16px;border-radius:8px;border-left:4px solid {_sev_c};">'
    f'{_html.escape(_reasoning)}</p></div>'
    + _warn_html
    + '</div></div>'
)

displayHTML(_h)

# COMMAND ----------

# DBTITLE 1,Notebook exit (job runs only)
# This cell is only needed when running as a job task.
# During interactive demos, stop at Cell 10 to see the HTML result card.
dbutils.notebook.exit(json.dumps(_result))
