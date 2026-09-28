# ⚡️ SF Bulk App — Pictorial Flow Diagrams

> Mermaid flowcharts for every operation. See also: [APP_GUIDE.md](APP_GUIDE.md)

---

## 📥 Insert — Bulk Insert into Salesforce

### Step 1 — Setup (UI)

```mermaid
flowchart TD
    A([Open 📥 Insert Tab]) --> B{Data Source?}

    B -->|📄 File| C[Select file from disk\nor Upload\nCSV · TSV · TXT · XLSX · pipe-delimited]
    B -->|❄️ Snowflake| D[Enter table name\nDB.SCHEMA.TABLE\nClick 🔍 Preview]

    C --> E[Auto-detect delimiter\ncsv.Sniffer → pipe → semicolon → tab → comma\nStrip border rows like pipe-Markdown tables]
    D --> F[Snowflake: SELECT star FROM table LIMIT 5\nDisplay row count via COUNT-star]

    E --> G[Show Data Preview\nFirst 5 rows · total row count]
    F --> G

    G --> H[Fetch SF Object fields\nvia describe API - cached 5 min]
    H --> I[Auto-map columns\nfuzzy-match source col → SF field API name\nExclude compound fields for Bulk API]
    I --> J{All columns matched?}
    J -->|Yes - auto-confirm| K[✅ Mapping locked\nno selectboxes shown]
    J -->|Unmatched cols| L[Show column mapping UI\n2-col grid of selectboxes\nSkip = exclude column]
    L --> K

    K --> M[Enter Date Columns\noptional extra date fields]
    M --> N[Set Chunk Size default 25000\nand Parallel Jobs default 32]
    N --> O([Click 🚀 Run Insert])

    style A fill:#1e88e5,color:#fff
    style O fill:#f9a825,color:#000
    style K fill:#43a047,color:#fff
```

### Step 2 — Execution Engine (bulk_load_v2)

```mermaid
flowchart TD
    START([🚀 Run Insert triggered]) --> QUOTA{API Quota\nnear limit 85%?}
    QUOTA -->|Yes| REST_ONLY[Force REST API mode\nconserve daily quota]
    QUOTA -->|No| CHAIN[API Chain = Bulk v2 → Bulk v1 → REST\nThread Ladder = 32-24-16-12-8-4-2-1]

    REST_ONLY --> SRC
    CHAIN --> SRC

    SRC{Data Source} -->|File CSV-TSV-TXT-XLSX| FILEREAD[pd.read_csv in chunks\nchunksize=25000\nStream - no full pre-load]
    SRC -->|Snowflake Table| SFSTREAM[Snowflake get_result_batches\nParallel prefetch 4 batches\nBackground downloader thread]

    FILEREAD --> PREP
    SFSTREAM --> PREP

    PREP[Per-chunk prep\nDrop all-null rows\nApply column mapping\nFormat date columns\nExclude compound fields]

    PREP --> MULTI{threads ≥ 8\nand AUTO mode?}

    MULTI -->|Yes| SIMUL
    MULTI -->|No| SINGLE

    subgraph SIMUL [Multi-API SIMULTANEOUS mode]
        direction LR
        RR[Weighted Round-Robin distributor\nBulk v2 = 75% of chunks\nBulk v1 = 20% of chunks\nREST = 5% of chunks]
        RR --> W1[Bulk v2 thread pool\nPOST /jobs/ingest\nCSV upload + close job\nPoll for Complete]
        RR --> W2[Bulk v1 thread pool\nXML batch API\nSOAP envelope]
        RR --> W3[REST Composite thread pool\nJSON 200-record sub-requests]
    end

    subgraph SINGLE [Single-API sequential mode]
        direction LR
        API[Current API\nbulk_v2 → bulk_v1 → rest]
    end

    W1 & W2 & W3 --> DRAIN
    API --> DRAIN

    DRAIN[Drain completed futures\nFIRST_COMPLETED wait - 3s timeout\nClassify each result]

    DRAIN --> CLASS{Error class?}
    CLASS -->|✅ Success| COUNT[Add to total_success\nAppend failed records to error list\nReport chunk timing]
    CLASS -->|⚠️ RETRYABLE\n429 rate limit- timeout| REDUCE[Reduce thread count\nThread Ladder step down\nRequeue chunk for retry]
    CLASS -->|🔄 SWITCH_API\nObject not supported| SWITCH[Move to next API\nin chain - reset threads\nRequeue chunk]
    CLASS -->|❌ NON_RETRYABLE\nschema error- invalid field| FAILFILE[Write to failed_*.csv\nAbort if fatal schema error]

    REDUCE --> RETRY{Ladder\nexhausted?}
    RETRY -->|No| DRAIN
    RETRY -->|Yes| SWITCH

    SWITCH --> DRAIN
    COUNT --> MORE{More chunks\nin source?}
    MORE -->|Yes| PREP
    MORE -->|No - all submitted| REPROCESS

    REPROCESS{Retryable chunks\nremaining?}
    REPROCESS -->|Yes - up to 3 rounds| PREP
    REPROCESS -->|No| DONE

    DONE[Flush remaining failed_records to CSV\nCompute total elapsed time\nReturn result dict] --> SUMMARY

    SUMMARY([Live Dashboard + Metrics\ntotal_processed · total_success · total_failed\nelapsed time · records-per-sec\nDownload failed_*.csv if any errors])

    style START fill:#f9a825,color:#000
    style SUMMARY fill:#43a047,color:#fff
    style FAILFILE fill:#b71c1c,color:#fff
    style SIMUL fill:#1a237e,color:#fff
    style SINGLE fill:#4a148c,color:#fff
    style REDUCE fill:#e65100,color:#fff
    style SWITCH fill:#0277bd,color:#fff
```

---

## 🔄 Update / Upsert — Update Existing Salesforce Records

```mermaid
flowchart TD
    A([Start: 🔄 Update Tab]) --> B{Operation Type?}
    B -->|Update| C[Key: Salesforce Record Id\n18-char SF ID column required]
    B -->|Upsert| D[Key: External ID Field\ne.g. External_ID__c]
    C --> E[Select Data Source\nFile or Snowflake Table]
    D --> E
    E --> F[Column Mapping\nMust include key field]
    F --> G[Configure\nChunk Size · Parallel Jobs]
    G --> H{Key field found\nin data?}
    H -->|No| I([Error: missing key column])
    H -->|Yes| J[🚀 Run Update]
    J --> K[Send to Bulk API v2\nupdate or upsert operation]
    K -->|Match found| L[Update record fields]
    K -->|No match - upsert| M[Insert new record]
    L --> N{All chunks done?}
    M --> N
    N -->|No| J
    N -->|Yes| O[Results\n✅ Updated · ✅ Inserted · ❌ Failed]
    O --> P([Done: Live Dashboard])

    style A fill:#1e88e5,color:#fff
    style P fill:#43a047,color:#fff
    style I fill:#e53935,color:#fff
    style L fill:#7b1fa2,color:#fff
    style M fill:#0288d1,color:#fff
```

---

## 🗑️ Delete — Bulk Delete from Salesforce

```mermaid
flowchart TD
    A([Start: 🗑️ Delete Tab]) --> B[Select Data Source\nFile or Snowflake Table]
    B --> C[Select ID Column\n18-char Salesforce Record ID]
    C --> D{IDs valid?}
    D -->|No| E([Error: invalid ID format])
    D -->|Yes| F[Configure\nChunk Size · Parallel Jobs]
    F --> G[🗑️ Run Delete]
    G --> H[Bulk API v2 Delete Job\nPOST /jobs/ingest — delete op]
    H --> I[Upload ID list chunk\none Id per row CSV]
    I --> J[SF marks records deleted]
    J --> K{All chunks done?}
    K -->|No| G
    K -->|Yes| L[Records → Recycle Bin\nRecoverable 15 days]
    L --> M[Results\n✅ Deleted · ❌ Failed IDs]
    M --> N([Done])

    style A fill:#e53935,color:#fff
    style N fill:#43a047,color:#fff
    style E fill:#e53935,color:#fff
    style L fill:#f9a825,color:#000
```

---

## 🚀 Multi-Object — Parallel Processing of Multiple Objects

```mermaid
flowchart TD
    A([Start: 🚀 Multi-Object Tab]) --> B[Build Operation Queue]
    B --> C[For each object: specify\nSF Object · Operation · Source · External ID]
    C --> D{Add to Queue\n➕ button}
    D -->|More objects| C
    D -->|Queue ready| E[Set Total Threads\nand Chunk Size]
    E --> F[🚀 Run All Parallel]
    F --> G{Distribute threads\nacross queue items}

    G --> H1[Object 1 Thread Pool\nBulk API worker]
    G --> H2[Object 2 Thread Pool\nBulk API worker]
    G --> H3[Object N Thread Pool\nBulk API worker]

    H1 --> I1[Read source →\nchunk → send to SF]
    H2 --> I2[Read source →\nchunk → send to SF]
    H3 --> I3[Read source →\nchunk → send to SF]

    I1 --> J1[✅ Object 1 done]
    I2 --> J2[✅ Object 2 done]
    I3 --> J3[✅ Object N done]

    J1 & J2 & J3 --> K[Aggregate Results\nper-object success/fail counts]
    K --> L([Done: Live per-object dashboards])

    style A fill:#1e88e5,color:#fff
    style L fill:#43a047,color:#fff
    style H1 fill:#7b1fa2,color:#fff
    style H2 fill:#7b1fa2,color:#fff
    style H3 fill:#7b1fa2,color:#fff
```

---

## ❄️ Snowflake Load — File into Snowflake Table

```mermaid
flowchart TD
    A([Start: ❄️ Snowflake Tab]) --> B[Select File\nCSV · TSV · TXT · XLSX · pipe]
    B --> C[Auto-detect Delimiter\ncsv.Sniffer]
    C --> D[Read into DataFrame\npandas with encoding probe]
    D --> E[Infer Snowflake Schema\nVARCHAR · NUMBER · DATE · BOOLEAN]
    E --> F{Load Mode?}
    F -->|Create/Replace| G[DROP TABLE IF EXISTS\nCREATE TABLE with inferred DDL]
    F -->|Append| H[Table must already exist]
    G --> I[Compress to gzip CSV chunks]
    H --> I
    I --> J[PUT file to Snowflake Stage\nBulk PUT command]
    J --> K[COPY INTO table\nSF_FORMAT = CSV GZIP]
    K --> L{All chunks loaded?}
    L -->|No| I
    L -->|Yes| M[Verify row count]
    M --> N([Done: rows loaded ✅])

    style A fill:#29b6f6,color:#000
    style N fill:#43a047,color:#fff
    style G fill:#ef6c00,color:#fff
    style K fill:#7b1fa2,color:#fff
```

---

## 🔄❄️ SF → Snowflake — Salesforce Extract to Snowflake ETL

```mermaid
flowchart TD
    A([Start: 🔄❄️ SF→Snowflake Tab]) --> B[Enter SOQL Query\nSELECT Id, Name ... FROM Object]
    B --> C[Set Target Table\nDB.SCHEMA.TABLE]
    C --> D{Write Mode?}
    D -->|Create/Replace| E[DROP + CREATE table]
    D -->|Append| F[Table already exists]
    E --> G[Start Bulk Query Job\nPOST /jobs/query — Bulk API v2]
    F --> G
    G --> H[Poll until Complete\nGET /jobs/query/{id}]
    H --> I{Job ready?}
    I -->|No, wait| H
    I -->|Yes| J[Stream result chunks\nGET /jobs/query/{id}/results]
    J --> K[Parse CSV chunk\npandas read_csv streaming]
    K --> L[Write chunk to Snowflake\nINSERT batch via connector]
    L --> M{More result pages?}
    M -->|Yes| J
    M -->|No| N[Save config for reuse\nsaved_sf_to_snowflake_configs.json]
    N --> O([Done: SF → Snowflake complete ✅])

    style A fill:#1e88e5,color:#fff
    style O fill:#43a047,color:#fff
    style G fill:#7b1fa2,color:#fff
    style J fill:#29b6f6,color:#000
    style L fill:#ef6c00,color:#fff
```

---

## 🧪 Test Case Generator — Automated Data Validation

```mermaid
flowchart TD
    A([Start: 🧪 Test Case Generator]) --> B[Connect both SF + Snowflake]
    B --> C[Enter Snowflake Table\nand SF Object]
    C --> D[Enter Key Field\ne.g. External_ID__c]
    D --> E[Configure Column Mapping\nSnowflake col → SF field]
    E --> F[Select Test Types to Run]
    F --> G[🚀 Generate and Run]

    G --> H{Phase 1: Aggregate Tests\n15 parallel threads}
    H --> H1[Record Count\nCOUNT in both systems]
    H --> H2[Null Count\nSUM CASE WHEN field IS NULL]
    H --> H3[Picklist Validation\nGROUP BY field vs allowed values]
    H --> H4[Lookup Validation\nFK integrity check]
    H --> H5[RecordType Validation\nRecordTypeId assignments]
    H --> H6[Required Field Check\nNOT NULL assertion]

    H1 & H2 & H3 & H4 & H5 & H6 --> I{Phase 2: Full Data Match\nif selected}
    I -->|Yes| J[Download ALL rows\nfrom Snowflake + SF via Bulk API]
    I -->|No| K

    J --> L[Merge on Key Field\npandas merge outer join]
    L --> M[Vectorized Comparison\nper-field type-aware normalization]
    M --> N[Identify mismatches\nRecord IDs + field values]
    N --> K

    K[Compile Results\nPASS / FAIL per test] --> O[Generate Excel Report\nwith queries + failed IDs]
    O --> P([Done: Download Report 📥])

    style A fill:#1e88e5,color:#fff
    style P fill:#43a047,color:#fff
    style H fill:#f9a825,color:#000
    style I fill:#f9a825,color:#000
    style J fill:#7b1fa2,color:#fff
    style M fill:#0288d1,color:#fff
```

---

## 🔍 Record by Record Test — Row-Level Comparison

```mermaid
flowchart TD
    A([Start: 🔍 Record by Record Test]) --> B[Connect SF + Snowflake]
    B --> C[Enter Snowflake Table\nSF Object · Key Field]
    C --> D[Map Columns\nauto-mapped by name similarity]
    D --> E{Filter records?}
    E -->|No, compare all| F[Load all rows from both]
    E -->|Yes, WHERE clause| G[Load filtered rows]
    F --> H[Merge on Key Field]
    G --> H
    H --> I[For each matched row:\ncompare every mapped field]

    I --> J{Field value match?}
    J -->|✅ Match| K[Mark row: PASS]
    J -->|❌ Mismatch| L[Record: field · src value · tgt value]
    J -->|key not in SF| M[Mark row: NOT IN TARGET ❌]

    K & L & M --> N{More rows?}
    N -->|Yes| I
    N -->|No| O[Aggregate mismatch counts\nper column summary]
    O --> P[Generate Excel Report\ngreen=match · red=mismatch]
    P --> Q([Done: Download Report 📥])

    style A fill:#1e88e5,color:#fff
    style Q fill:#43a047,color:#fff
    style K fill:#43a047,color:#fff
    style L fill:#e53935,color:#fff
    style M fill:#e53935,color:#fff
```

---

## Overall System Architecture

```mermaid
flowchart LR
    subgraph UI [Streamlit UI — app.py]
        TAB1[📥 Insert]
        TAB2[🔄 Update]
        TAB3[🗑️ Delete]
        TAB4[🚀 Multi-Object]
        TAB5[❄️ Snowflake Load]
        TAB6[🔄❄️ SF→Snowflake]
        TAB7[🧪 Test Cases]
        TAB8[🔍 Record Test]
    end

    subgraph ENGINE [Bulk Engine — sf_bulk_loader.py]
        V2[Bulk API v2\nREST JSON]
        V1[Bulk API v1\nSOAP XML]
        REST[REST Composite\nJSON batches]
        THREAD[Thread Pool\nWorkers]
    end

    subgraph SF [Salesforce Cloud]
        INGEST[Ingest Jobs\n/services/data/v59.0/jobs/ingest]
        QUERY[Query Jobs\n/services/data/v59.0/jobs/query]
    end

    subgraph SNOW [Snowflake]
        STAGE[Stage]
        TABLE[(Tables)]
    end

    subgraph FILES [Local Files]
        CSV[CSV/TSV/TXT/XLSX]
        FAILED[failed/*.csv]
        CREDS[credentials.json]
    end

    TAB1 & TAB2 & TAB3 & TAB4 --> ENGINE
    TAB6 --> QUERY
    TAB5 --> SNOW
    TAB7 & TAB8 --> SF
    TAB7 & TAB8 --> SNOW

    ENGINE --> THREAD --> V2 & V1 & REST --> INGEST
    INGEST --> FAILED
    CSV --> TAB1 & TAB2 & TAB3 & TAB4 & TAB5
    CREDS --> UI

    style UI fill:#1565c0,color:#fff
    style ENGINE fill:#4a148c,color:#fff
    style SF fill:#00695c,color:#fff
    style SNOW fill:#01579b,color:#fff
    style FILES fill:#4e342e,color:#fff
```

---

## API Fallback Chain

```mermaid
flowchart LR
    START([Chunk Ready]) --> V2

    V2[Bulk API v2\nFastest\n75% of load]
    V2 -->|success| DONE([✅ Chunk uploaded])
    V2 -->|400 invalid object| ERR([❌ Fatal error])
    V2 -->|429 rate limit| V1

    V1[Bulk API v1\nXML SOAP\n20% of load]
    V1 -->|success| DONE
    V1 -->|429 rate limit| REST

    REST[REST Composite\nSafest / slowest\n5% of load]
    REST -->|success| DONE
    REST -->|429 rate limit| WAIT

    WAIT[Wait + reduce\nthread count] --> V2

    style V2 fill:#7b1fa2,color:#fff
    style V1 fill:#e65100,color:#fff
    style REST fill:#1565c0,color:#fff
    style DONE fill:#2e7d32,color:#fff
    style ERR fill:#b71c1c,color:#fff
    style WAIT fill:#f57f17,color:#000
```
