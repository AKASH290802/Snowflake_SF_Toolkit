# ⚡️ TAVANT MIGRATION APP – Complete Application Guide

> **Version:** 2.0 (June 2026)  
> **See also:** [DIAGRAMS.md](DIAGRAMS.md) — pictorial flow diagrams for every operation

## Overview

**SF Bulk App** is a high-performance Streamlit application for bidirectional data operations between **Salesforce** and **Snowflake**. It supports bulk insert, update, delete, multi-object parallel processing, data migration pipelines, automated test case generation, and record-level validation.

---

## Table of Contents

1. [Installation & Setup](#installation--setup)
2. [Launching the App](#launching-the-app)
3. [Sidebar Connections](#sidebar-connections)
4. [Tabs Overview](#tabs-overview)
   - [📥 Insert](#-insert)
   - [🔄 Update](#-update)
   - [🗑️ Delete](#️-delete)
   - [🚀 Multi-Object](#-multi-object)
   - [❄️ Snowflake](#️-snowflake)
   - [🔄❄️ SF → Snowflake](#️-sf--snowflake)
   - [🧪 Test Case Generator](#-test-case-generator)
   - [🔍 Record by Record Test](#-record-by-record-test)
5. [File Format Support](#file-format-support)
6. [Performance & Architecture](#performance--architecture)
7. [Troubleshooting](#troubleshooting)

---

## Installation & Setup

### Prerequisites
- Python 3.10+ (tested on 3.12)
- pip (Python package manager)

### Install Dependencies

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```
streamlit>=1.57.0
pandas>=2.0.0
simple-salesforce>=1.12.0
requests>=2.31.0
snowflake-connector-python[pandas]>=3.0.0
cryptography>=41.0.0
openpyxl>=3.1.0
python-dateutil>=2.8.2
```

> **Note:** Snowflake connector is optional. The app works without it — Snowflake-dependent tabs show an install prompt.

---

## Launching the App

```bash
cd sf_bulk_app
streamlit run app.py
```

Opens at `http://localhost:8501`. The active tab is remembered across reruns.

On every fresh browser load, the app briefly shows a **TAVANT DATA MIGRATION** intro for two seconds, then opens the workspace automatically. The compact sun/moon control in the upper-right switches between Light and Dark appearance and remembers the choice in the browser.

Saved credentials and configurations show an acknowledgement dialog after a successful Save, Update, or Delete. Destructive saved-item deletes require confirmation. The **STOP** controls request cooperative cancellation of the current activity; they do not roll back records already committed by Salesforce or Snowflake.

### Stop And Appearance Controls

- Every Stop button sets cancellation before the page reruns. Operation tabs use full reruns with `runner.fastReruns = true`; fragment reruns can queue behind blocked work.
- Stop prevents additional chunks, retry batches, queued validation SQL, and queued Snowflake stage uploads from starting at their next cancellation check. Bulk ingest pollers request an abort and continue checking for terminal status.
- An HTTP request or Snowflake statement already executing may need to return before its worker can stop. Stop does not roll back committed records. Check Salesforce job results before rerunning an interrupted Insert.
- Cancellation uses the application's existing process-wide flag, shared across tabs and browser sessions on that server. Stop remains set until a new Run. Wait for outstanding jobs to settle before starting another run or restarting the server.
- The sun/moon button above the application header switches light/dark colors without rerunning Python or interrupting a load. The choice is remembered in the browser. Text fields, placeholders, dropdowns, and custom panels use paired foreground/background colors.
- Restart Streamlit after active work has finished to load backend changes. The theme component requires Streamlit 1.57 or newer.

Local regression checks (no live API writes):

```bash
python -m unittest test_stop_controls test_load_pipeline test_multi_object_loading test_delete_failures test_bulk_query_export test_delete_soql -q
```

---

## Sidebar Connections

### 🔌 Salesforce Connection

| Field | Description |
|-------|-------------|
| Saved Credentials | Select a previously saved credential set |
| Username | Salesforce username (e.g. `user@org.sandbox`) |
| Password | Salesforce password |
| Security Token | From SF Settings → Reset Security Token |
| Domain | `test` (sandbox) or `login` (production) |

- Click **⚡️ Connect**
- On success: sidebar shows the SF instance URL
- Save credentials with **💾 Save** and a name (e.g. `DTNA QA`)

### ❄️ Snowflake Connection

| Field | Description |
|-------|-------------|
| Saved Credentials | Select a previously saved credential set |
| User | Snowflake username |
| Account | Account identifier (e.g. `PEB93217`) |
| Warehouse | Compute warehouse (e.g. `ssz_nextgen_adhoc_wh`) |
| Database | Target database |
| Schema | Target schema |
| Role | Optional role override |
| Auth Method | `Private Key` (PEM file) or `Password` |

- Click **❄️ Connect**
- Credentials saved in `saved_snowflake_credentials.json`

---

## Tabs Overview

---

### 📥 Insert

**Purpose:** Bulk insert new records into a Salesforce object from a file or Snowflake table.

#### Steps:
1. Enter **Salesforce Object API Name** (e.g. `Account`, `WOD_2__Warranty_Code__c`)
2. Select **Data Source**:
   - **📄 File** — select from disk or upload (CSV, XLSX, XLS, TSV, TXT, pipe-delimited)
   - **❄️ Snowflake Table** — enter fully-qualified table name
3. Review the **Data Preview** (first 5 rows)
4. Adjust **Column Mapping** (auto-mapped by name similarity)
5. Set **Chunk Size** (default 25,000) and **Parallel Jobs** (up to 32)
6. Click **🚀 Run Insert**
7. Monitor the **Live Dashboard** — records/sec, ETA, success/fail counts

#### Automatic API fallback:
```
Bulk v2  →  Bulk v1  →  REST API
  (fast)       (compat)   (safe)
```
The app switches automatically when a batch fails. Thread count scales down on rate-limit errors.

---

### 🔄 Update / Upsert

**Purpose:** Bulk update existing Salesforce records (match by Id) or upsert (match by External ID).

#### Difference from Insert:

| Mode | Key Column Required | Behaviour |
|------|---------------------|-----------|
| Update | `Id` (18-char SF Id) | Updates existing records only |
| Upsert | Custom External ID field | Updates if exists, inserts if not |

#### Steps:
1. Choose **Update** or **Upsert** operation
2. For Upsert: enter the **External ID Field** (must be marked External ID on the SF object)
3. Data source, mapping, chunk size — same as Insert
4. Click **🚀 Run Update**

---

### 🗑️ Delete

**Purpose:** Bulk delete records from Salesforce by Salesforce Record ID.

#### Steps:
1. Data source must contain an **`Id` column** with 18-character Salesforce IDs
2. Select file or Snowflake table
3. Select the **ID column** from the dropdown
4. Set chunk size and parallel jobs
5. Click **🗑️ Run Delete**

#### SOQL Source (Large Datasets)
1. Select **SOQL Query**, enter a query returning `Id`, and click **Count / Preview Records**.
2. Check the object and matching count, then click **Run Delete** and confirm.
3. The app submits a **Bulk API 2.0 Query** job and displays its job ID, Salesforce processing state, elapsed time, and downloaded ID count.
4. Results stream into an ID-only temporary CSV in chunks of at most 25,000 rows. After export completes, the existing parallel delete engine reads the CSV in chunks, starting with Bulk API 2.0.

The query is sent unchanged: filters and `LIMIT` are never removed. Bulk Query has SOQL restrictions; if Salesforce rejects the query, the app displays the error and does not start deletion or silently fall back to a full REST download. Failed, empty, or cancelled exports do not start deletion. Partial exports are removed; successful export CSVs are removed after the delete call finishes.

**STOP** is checked between export requests and CSV chunks. Requests already in progress can take up to their network timeout before control returns. Interrupting the Streamlit run attempts to abort a pending query job. Stopping the app does not guarantee cancellation of delete jobs already submitted to Salesforce. Avoid starting a second run while one is active.

Restart Streamlit after upgrading the loader module; a run already in progress keeps its old code. Bulk export reduces download overhead and memory use, but does not guarantee a deletion rate: Salesforce query selectivity, queues, record locks, automation, and API limits still apply.

> ⚠️ Deleted records go to the Salesforce Recycle Bin (recoverable for 15 days). Hard deletes are permanent.

---

### 🚀 Multi-Object

**Purpose:** Queue and run multiple Salesforce load operations simultaneously — each object gets its own thread pool.

#### Streaming Performance
Insert and Update now submit single-API uploads while later source chunks are being prepared. The multi-API loader limits outstanding tasks to twice its configured worker count, including fallback mini-batches, instead of enqueueing the entire input. This is a task-count limit, not a strict byte/RAM cap; row width, source chunks, connector buffers, failure records, and dataset count still affect memory.
Insert and Update now submit single-API uploads while later source chunks are being prepared. The multi-API loader limits outstanding tasks to twice its configured worker count, including fallback mini-batches, instead of enqueueing the entire input. This is a task-count limit, not a strict byte/RAM cap; row width, source chunks, connector buffers, failure records, and dataset count still affect memory.

Multi-Object Snowflake sources use independent cursors and bounded `fetchmany` batches instead of loading full tables into DataFrames. Three datasets can read and upload concurrently. Insert/Update/Upsert-only groups share a FIFO upload limit based on the total parallel-thread setting; unused slots become available to other datasets. Groups containing Delete retain the existing per-object allocation. Source totals and ETA may remain unknown while streaming; final results provide confirmed totals.

Multi-Object dashboard updates are coalesced per dataset and rendered by the Streamlit thread. The live feed is a recent-status display, not an exhaustive audit log; failure CSVs and final results remain authoritative. Stop requests are owned by the coordinator so one child finishing does not clear the signal for siblings. In-flight requests may still finish before cancellation takes effect.

Restart Streamlit to load backend changes. This phase retains existing row chunk limits and API routing. Byte-based job sizing, a strict shared memory budget, durable resume, and measured Salesforce tuning remain follow-up work; do not increase chunk size or threads solely on the basis of reference-project throughput claims.

Offline checks (no Salesforce writes):
```powershell
python -m unittest test_load_pipeline test_multi_object_loading test_delete_failures test_bulk_query_export test_delete_soql -q
python benchmark_load_pipeline.py --chunks 100 --rows-per-chunk 1000
```
The benchmark compares pre-buffered versus streamed synthetic inputs using the current loader and real CSV serialization with mocked uploads. It reports time to first submission and traced Python allocations, not Salesforce throughput or full process RSS.

#### Steps:
1. For each object:
   - Enter **SF Object API Name**, **Operation**, **Data Source** (file path or Snowflake table)
   - For upsert: specify **External ID Field**
   - For Snowflake: optionally add a **WHERE clause** filter
2. Click **➕ Add to Queue**
3. Repeat for all objects
4. Review queue — remove items with 🗑️ if needed
5. Set **Total Parallel Jobs** and **Chunk Size**
6. Click **🚀 Run All (Parallel)**

#### Example Queue:
| # | Object | Operation | Source |
|---|--------|-----------|--------|
| 1 | Account | insert | ❄️ `DW.STG.ACCOUNTS` |
| 2 | Contact | upsert | 📄 `contacts.csv` |
| 3 | Asset | insert | ❄️ `DW.STG.ASSETS` WHERE `STATUS='A'` |

All three run in parallel with live per-object dashboards.

---

### ❄️ Snowflake

**Purpose:** Load data files (CSV/Excel/TSV/TXT/pipe) INTO Snowflake tables.

#### Steps:
1. Connect to Snowflake (sidebar)
2. Select or upload a data file
3. Enter **Target Table Name**
4. Choose **Load Mode**:
   - `Create/Replace` — drops and recreates the table
   - `Append` — inserts into existing table
5. Optionally configure:
   - **Zero Padding** — pad numeric columns (e.g. `000123`)
   - **Date Normalization** — standardize date formats
6. Click **⬆️ Load Data**

#### Under the hood:
```
File  →  pandas DataFrame  →  gzip CSV chunks  →  Snowflake PUT  →  COPY INTO table
```
Parallel compression + PUT for maximum throughput.

---

### 🔄❄️ SF → Snowflake

**Purpose:** Extract data from Salesforce via SOQL and load directly into Snowflake. A complete single-click ETL pipeline.

#### Steps:
1. Connect **both** Salesforce AND Snowflake (sidebar)
2. Write a **SOQL Query** (e.g. `SELECT Id, Name, Email FROM Contact`)
3. Set **Target Snowflake Table** (fully-qualified)
4. Choose **Write Mode**: `Create/Replace` or `Append`
5. Choose **API Method**: `Bulk API 2.0` (fast, large) or `REST` (all field types)
6. Optionally save the configuration for reuse
7. Click **🚀 Extract & Load**

#### Supported SOQL tips:
- ✅ `SELECT Id, Name, Field1__c FROM MyObject__c WHERE CreatedDate > 2024-01-01T00:00:00Z`
- ❌ `SELECT *` — not supported by Salesforce SOQL
- ❌ `SELECT FIELDS(ALL)` — limited to `LIMIT 200`; use explicit fields for large extractions

---

### 🧪 Test Case Generator

**Purpose:** Automated validation comparing a Snowflake source table vs Salesforce target object. Tests 100% of data — not sampled.

#### Steps:
1. Connect both Salesforce and Snowflake
2. Enter **Snowflake Source Table** and **SF Target Object**
3. Enter **Key Field** (used to match records between source and target)
4. Configure **column mapping** (auto-mapped by name)
5. For lookup fields: map Snowflake reference tables
6. Select test types to run
7. Click **🚀 Generate & Run Test Cases**

#### Test Types:

| Test | What it checks |
|------|---------------|
| Record Count | Total row count: source vs target |
| Null Count | Fields that are populated in source but NULL in SF |
| Data Match | Field-by-field value comparison for ALL records |
| Picklist Validation | Values exist in SF picklist options |
| Lookup Validation | Foreign key integrity (lookup/reference fields) |
| RecordType Validation | RecordTypeId assignments are correct |
| Required Field Check | All required fields have values |

#### Output:
- On-screen PASS/FAIL per test with category grouping
- **📥 Download Excel Report** with full details, failed record IDs, and the exact queries used

---

### 🔍 Record by Record Test

**Purpose:** Row-level comparison between Snowflake and Salesforce. Shows source vs target value side-by-side for every field of every record.

#### Steps:
1. Connect both Salesforce and Snowflake
2. Enter Snowflake table, SF object, and key field
3. Map columns
4. Click **🔍 Run Record-by-Record Comparison**

#### Output:
- Per-record summary: ✅ matched / ❌ mismatched / ❌ NOT IN TARGET
- Aggregated mismatch counts per column
- **📥 Download Excel Report** with color-coded cells (green=match, red=mismatch)

---

## File Format Support

The app auto-detects the format of any uploaded or selected file:

| Format | Extension | Detection Method |
|--------|-----------|-----------------|
| CSV (comma) | `.csv` | Auto-sniffed |
| TSV (tab) | `.tsv` | Forced tab |
| Pipe-delimited | `.txt`, `.csv` | Auto-sniffed |
| Semicolon-delimited | `.txt`, `.csv` | Auto-sniffed |
| Markdown table `\|col\|col\|` | `.txt` | Border-stripped |
| SQL\*Plus / DB2 spool | `.txt` | Fixed-width fallback |
| Excel | `.xlsx`, `.xls` | pandas read_excel |
| UTF-8 BOM | any | Encoding probe |
| Windows cp1252 / latin-1 | any | Encoding probe |
| Separator rows `\|---\|---\|` | any | Auto-skipped |

---

## Performance & Architecture

### API Strategy

| Mode | Speed | Use Case |
|------|-------|----------|
| Bulk API v2 | ⚡️⚡️⚡️ Fastest | Standard objects, large volumes |
| Bulk API v1 | ⚡️⚡️ Fast | Objects not supported by v2 |
| REST API | ⚡️ Safe | Rate-limited fallback, small batches |

AUTO mode chains all three automatically.

### Typical Throughput:
- **Insert/Update**: 50,000–150,000 records/minute
- **Delete**: 100,000–200,000 records/minute
- **SF → Snowflake extract**: 500,000+ records/minute
- **Test Case Generator**: Aggregate tests < 5 seconds; full data match scales linearly

### File Structure:
```
sf_bulk_app/
├── app.py                              # Main Streamlit UI + all logic
├── sf_bulk_loader.py                   # Bulk load engine (Bulk v2/v1/REST, threading)
├── requirements.txt                    # Python dependencies
├── APP_GUIDE.md                        # This guide
├── DIAGRAMS.md                         # Pictorial flow diagrams
├── saved_credentials.json              # SF credentials (⚠️ gitignore!)
├── saved_snowflake_credentials.json    # Snowflake credentials (⚠️ gitignore!)
├── saved_sf_to_snowflake_configs.json  # SF→Snowflake pipeline configs
├── saved_testcase_configs.json         # Test case configurations
├── api_quota_state.json                # Live API quota tracking
└── failed/                             # Failed record CSVs (auto-created)
    └── insert/
        └── failed_*.csv
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| **400 Bad Request on job creation** | Check SF Object API name (exact case, `__c` suffix for custom objects). Re-run to see the actual Salesforce error code. |
| **"Connect to Salesforce first"** | Use the sidebar to connect. Check credentials and security token. |
| **"Not connected to Snowflake"** | Switch sidebar to ❄️ Snowflake mode and connect. |
| **Rate limit / 429 errors** | Auto-handled — threads reduce automatically. If persistent, lower Parallel Jobs manually. |
| **INVALID_SESSION_ID** | SF session expired. Reconnect via sidebar. |
| **Bulk API quota exceeded** | Wait 24h (SF resets daily) or use REST mode. |
| **Snowflake connector not installed** | `pip install snowflake-connector-python[pandas] cryptography` |
| **Column mapping wrong** | Ensure source names roughly match SF field API names. Adjust manually in the mapping section. |
| **File preview error / bad lines** | App skips bad lines automatically. Check that the file delimiter was detected correctly (shown in preview header). |
| **UI spinning / slow** | Hard refresh the browser (Ctrl+Shift+R). Tab state is saved in sessionStorage. |

### Credentials Security

> ⚠️ `saved_credentials.json` and `saved_snowflake_credentials.json` store credentials **in plain text**. Add both to `.gitignore`.

---

## Quick Start

```bash
# 1. Start
streamlit run app.py

# 2. Connect Salesforce (sidebar → 🔌 Salesforce → Connect)
# 3. Connect Snowflake (sidebar → ❄️ Snowflake → Connect)

# 4a. Load data into Salesforce:
#     📥 Insert tab → Object: Account → Data Source: ❄️ Snowflake Table
#     → Table: MY_DB.STG.ACCOUNTS → Map columns → 🚀 Run Insert

# 4b. Validate the load:
#     🧪 Test Case Generator → Snowflake: MY_DB.STG.ACCOUNTS
#     → SF Object: Account → Key: External_ID__c → 🚀 Generate & Run

# 4c. Debug mismatches:
#     🔍 Record by Record Test → same inputs → Run Comparison → Download Excel
```

---

## Tips & Best Practices

1. **Test with 100 rows first** before loading millions
2. **Use External IDs for upsert** — prevents duplicates on re-runs
3. **Run Test Case Generator after every load** — catches issues early
4. **Save SF→Snowflake configs** — reuse pipelines without re-entering SOQL
5. **Multi-Object tab** — best for loading related objects together (Account → Contact → Opportunity)
6. **Large loads (>5M rows)** — run during off-peak hours when API limits are freshly reset
7. **Pipe-delimited / SQL spool files** — just upload as `.txt`; format is auto-detected



