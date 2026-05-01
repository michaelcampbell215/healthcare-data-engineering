# Healthcare Data Engineering Pipeline
#### MySQL Star Schema → BigQuery + dbt Cloud Migration

[![MySQL](https://img.shields.io/badge/Phase_1-MySQL_Star_Schema-4479A1.svg)](https://www.mysql.com/)
[![BigQuery](https://img.shields.io/badge/Phase_2-BigQuery_%2F_dbt-4285F4.svg)](https://cloud.google.com/bigquery)
[![Tableau](https://img.shields.io/badge/Visualization-Tableau_Dashboard-E97627.svg)](https://public.tableau.com/views/HealthcarePaymentsCompliance/ComplianceDashboard)

> [!IMPORTANT]
> **Executive Summary:** This repository is a two-phase data engineering project. **Phase 1** delivered a production-grade MySQL Star Schema that ingested, cleaned, and warehoused **15.4M CMS Open Payments records** — achieving 100% checksum-validated accuracy, sub-second OLAP query performance, and recovering 18–20% of payment volume that client-side BI tools were systematically missing. **Phase 2 (active)** migrates the validated warehouse to BigQuery and builds a dbt transformation layer on top — converting the project from a local SQL-first pipeline into a scalable, version-controlled cloud analytics platform.

**Core Technical Assets:**
- **Business Case Study (Frontend):** [Healthcare Payments Compliance & Spend Analytics](https://github.com/michaelcampbell215/healthcare-payments-compliance)
- **Live Dashboard:** [View on Tableau Public](https://public.tableau.com/views/HealthcarePaymentsCompliance/ComplianceDashboard)
- **Deep-Dive Architecture:** [TECHNICAL.md](./TECHNICAL.md)

---

## Architecture — Full Data Lineage

```
CMS Open Payments (Federal Source)
         │
         ▼
  Local CSV (9GB Raw File)
         │
         ▼ upload_to_gcs.py (resumable, RAM-safe)
  GCS Bucket: raw-healthcare-payments-analysis/
  └── raw/raw_general_payments.csv
         │
         ▼ load_to_bq.py (load from GCS URI)
  BigQuery Dataset: cms_open_payments_raw
  └── raw_general_payments        (15.4M rows — 2024 program year, never mutated)
         │
         ▼ dbt staging models (BigQuery views — raw data never touched)
  cms_open_payments_raw
  └── stg_general_payments        (typed, cleaned, province/zip/city/specialty healed)
  └── stg_us_cities               (SimpleMaps US Cities golden record via dbt seed)
         │
         ▼ dbt mart models (BigQuery tables — 23/23 schema.yml tests passing)
  cms_open_payments_raw
  └── fct_payments                (17.8M rows — one row per payment × product slot)
  └── dim_physician               (650K unique physicians)
  └── dim_hospital                (1.3K unique teaching hospitals)
  └── dim_company                 (1.7K unique payer entities)
  └── dim_product                 (10.8K unique products)
  └── dim_nature_of_payment       (56 nature/form combinations)
         │
         ▼
     Tableau Dashboard
```

---

## Phase 1 — MySQL Star Schema

> [!NOTE]
> **Architecture Context:** The core design principle of this phase — shifting heavy computation from the BI layer to the warehouse tier — is the same principle that drives the dbt + BigQuery architecture in Phase 2. The Star Schema, COMMIT checkpointing, and checksum audit strategy built here carry directly into the cloud layer.

### The Problem

Raw CMS Open Payments data (15.4M rows) was completely unsuited for fast executive reporting. The legacy architecture over-relied on client-side BI tools to perform heavy data transformations — unpivoting wide nested records, executing complex geospatial calculations, and running window functions at query time — guaranteeing dashboard timeouts during critical compliance review periods.

The deeper problem: no one knew the data was broken. Client-side transformation limits were silently dropping 18–20% of product-level payment records, meaning every analysis upstream of this pipeline was systematically undercounting exposure.

### Star Schema Design

```
fact_payments (21,934,863 rows — grain: one row per payment × product combination)
  │
  ├── dim_recipient   → Physicians & Hospitals (NPI / CCN normalized, Type 1 SCD)
  ├── dim_product     → Device, Drug, Biologics classification
  ├── dim_payer       → Manufacturing entity normalized names
  ├── dim_date        → Standard calendar dimension
  └── dim_geography   → ZIP-to-MSA crosswalk (SimpleMaps Golden Record)
```

**Fact table grain:** The CMS source file stores up to 5 product columns per payment row (`Product_1` through `Product_5`). The `UNION ALL` unpivot in `23_populate_fact_payments.sql` normalizes each product into its own row, expanding 15,397,626 payment records into 21,934,863 product-level rows. The 6,537,237 additional rows represent secondary and tertiary product associations that standard `SELECT` queries and client-side BI tools were systematically dropping — the quantified basis of the "18–20% hidden payment volume" recovery.

**SCD note:** `dim_recipient` uses Type 1 SCD for query performance. Documented limitation: provider geography history is not preserved. A Type 2 migration is the recommended path for full audit-grade compliance history.

### Phase 1 Engineering Challenges

**The \$42M Checksum Catch**
During initial warehouse population, a join fanout caused `SUM(amount_usd)` in the warehouse to diverge from the source by $42M. Caught by `24_audit_checksum.sql` — a dedicated validation script that enforces `Source SUM(Amount) = Warehouse SUM(Amount)` before any downstream export is permitted. Status after remediation: **100.0% match.**

**The "Virtual Fact Table" — Recovering Hidden Payment Volume**
The CMS source file flattens multi-product payments into wide columns (`Product_1`, `Product_2`, `Product_3`...). Client-side BI tools were silently dropping secondary columns — missing 18–20% of payment volume. Fix: a `UNION ALL` CTE on the database server unpivots all product columns before any data reaches Tableau. Recovered payment volume that no BI tool configuration could capture.

**Zero-Downtime Schema Migration (Hot Patch)**
Mid-project, geospatial requirements required `lat`/`lng` columns. Reloading 15.4M fact rows would have caused 4+ hours of downtime. Solution: `dim_recipient` (~1M rows) received the new columns via in-place `ALTER TABLE`, populated via ZIP crosswalk join. All 15.4M fact records gained geospatial capability instantly through the existing foreign key relationship.

**Batch Processing at Scale**
Standard `UPDATE` statements on 15M rows caused transaction log overflows. All operations use Stored Procedures with `WHILE` loop cursor pagination (`LIMIT 50,000`), explicit `COMMIT` checkpoints, and transactional rollback capability at every batch boundary.

### Phase 1 Performance Results

| Metric | Before | After |
|---|---|---|
| Dashboard load time | >2 minutes | Sub-second |
| Payment volume captured | ~80% (client-side limit) | 100% (server-side unpivot) |
| Checksum accuracy | Not validated | 100.0% match |
| Schema change downtime | N/A (estimated 4+ hrs) | Zero (hot patch) |
| Row count accuracy | 15,397,627 (inflated) | 15,385,047 (validated in Phase 2) |

---

## Phase 2 — BigQuery Migration (Active)

> [!NOTE]
> All Phase 2 work lives on the `bigquery-migration` branch. The branch will be merged to `main` with a single descriptive commit once all phases are complete, preserving a clean commit history that documents the migration.

### Why Migrate?

Phase 1 was built SQL-first — stored procedures, transactions, and native window functions — to establish full proficiency at the database layer before introducing framework abstraction. Phase 2 migrates to BigQuery + dbt because the problem now outgrows what a local warehouse can address:

- **Scalability:** BigQuery handles 15.4M+ rows with no index tuning, no server maintenance, and no transaction log constraints
- **Version-controlled transformations:** dbt models replace stored procedures — every transformation is a reviewable, testable, documented SQL file in Git
- **Layered architecture:** Raw, staging, and mart datasets become independently queryable layers — analysts can access data at any stage of the pipeline without touching source tables

### Repo Structure (Phase 2 Additions)

```
healthcare-data-engineering/
│
├── scripts/                            # Phase 1 — MySQL pipeline (preserved)
│   ├── production/
│   │   ├── 05_load_staging_payments.sql  # raw CSV → MySQL staging table
│   │   ├── 22_populate_warehouse.sql     # batch warehouse population (50k chunks)
│   │   ├── 23_populate_fact_payments.sql # fact table population
│   │   └── 40_export_tableau_data.sql   # pre-aggregated Tableau export
│   └── schema/
│       ├── 06_create_schema.sql          # initial database creation
│       └── 21_create_warehouse_tables.sql
│
├── bigquery/                           # Phase 2 — Cloud ingestion layer
│   ├── migrate/
│   │   └── upload_to_gcs.py            # one-time: local CSV → GCS (resumable)
│   ├── load/
│   │   └── load_to_bq.py              # re-runnable: GCS URI → BigQuery raw tables
│   └── schemas/
│       ├── raw_general_payments.json   # explicit BigQuery column type definitions
│       ├── raw_physicians.json
│       └── raw_companies.json
│
├── transform/                          # Phase 2 — dbt transformation layer
│   ├── macros/
│   │   ├── clean_name.sql              # INITCAP + regex name standardization macro
│   │   └── clean_hospital_name.sql     # hospital system rollup (200+ CASE entries)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_general_payments.sql  # typed, cleaned, province/zip/city/specialty healed
│   │   │   ├── stg_us_cities.sql         # UNNEST(SPLIT()) replaces recursive CTE
│   │   │   ├── src_cms.yml               # source declarations
│   │   │   └── schema.yml                # staging model tests
│   │   └── marts/
│   │       ├── dim_physician.sql
│   │       ├── dim_hospital.sql
│   │       ├── dim_product.sql
│   │       ├── dim_company.sql
│   │       ├── dim_nature_of_payment.sql
│   │       ├── fct_payments.sql
│   │       └── schema.yml                # 23 tests: not_null, unique, relationships
│   ├── seeds/
│   │   └── uscities.csv                # SimpleMaps US Cities golden record
│   ├── packages.yml
│   └── dbt_project.yml
│
├── .env.example                       # environment variable template (safe to commit)
├── .gitignore                         # excludes credentials, CSVs, virtual env
├── requirements.txt                   # pinned Python dependencies (pip freeze)
├── TECHNICAL.md
└── README.md
```

### Key Phase 2 Engineering Decisions

**Decision 1 — Folder Naming: `migrate/` vs `upload/` vs `ingestion/`**
The GCS upload folder was named `migrate/` — not `upload/` or `ingestion/` — because it signals a one-time architectural move, not a repeatable operation. When a new engineer opens the repo, the folder name alone communicates that this action was performed once during the migration event. The `load/` folder handles the repeatable, re-runnable BigQuery population.

*Principle applied: Self-documenting architecture. Folder names are documentation.*

**Decision 2 — Separation of Concerns: `migrate/` vs `load/`**
Two scripts with a clear boundary:
- `upload_to_gcs.py` — local file → GCS (one-time migration event)
- `load_to_bq.py` — GCS → BigQuery tables (re-runnable, idempotent via `WRITE_TRUNCATE`)

These are separate responsibilities. Combining them into one script would make the upload re-run on every pipeline execution — transferring 9GB unnecessarily on every refresh cycle.

**Decision 3 — No Pandas in the Upload Script**
The GCS upload script uses `blob.upload_from_filename()` — not `pd.read_csv()`. Loading a 9GB file into a Pandas DataFrame first would consume all available RAM and potentially lock the system. `upload_from_filename()` streams the file directly from disk using Google's resumable upload protocol. No RAM overhead, no partial upload restarts from zero.

*This is the same batch-processing discipline applied in Phase 1 (50k-row COMMIT chunks) — now applied at the Python layer.*

**Decision 4 — Explicit Schema JSON Files (No Auto-Detect)**
BigQuery's auto-detect on 15M rows can silently mistype fields — a string that looks numeric in the first 1,000 rows will be typed as INTEGER, then fail on row 1,001 that contains a letter. Explicit schema JSON files enforce correct column types at load time. One JSON file per table, named after the table it describes (`raw_general_payments.json` → `raw_general_payments` table).

*Principle applied: Separation of configuration from execution. Schema files can evolve independently of load scripts.*

**Decision 5 — GCS-First Load Pattern**
Rather than loading directly from local CSV to BigQuery, the pipeline uses an intermediate GCS bucket (`raw-healthcare-payments-analysis`). When BigQuery loads from a `gs://` URI, the data transfer happens entirely within Google's internal network — dramatically faster and more reliable than local-to-cloud transfer at query time. The bucket serves as the durable, re-loadable source of truth for the raw data.

**Decision 6 — Authentication: ADC + Service Account Documentation**
The scripts use Application Default Credentials (ADC) — no credential files in code. `storage.Client(project=project_id)` resolves credentials automatically from the `gcloud auth application-default login` session. The `.env.example` documents the service account key path (`GOOGLE_APPLICATION_CREDENTIALS`) as a commented alternative for CI/CD and production pipeline contexts where interactive gcloud auth is unavailable.

**Decision 7 — Function-Based Upload for Reusability**
The upload logic is wrapped in an `upload_blob()` function rather than written inline. This allows the same function to handle General Payments today and Research Payments (Phase B) without code duplication. The function accepts `bucket_name`, `source_file_path`, and `destination_blob_name` as arguments — every upload decision is explicit and configurable at call time.

### Phase 2 Implementation Notes

**Repository Audit — Establishing a Single Source of Truth**
Prior to migration work, a repository audit identified MySQL pipeline scripts that had been developed locally but never committed to version control. These were captured in a dedicated commit before any migration work began, establishing the Git repository as the authoritative project record. Going forward, the repository is the project — local working directories are scratch space only.

**Encoding Standardization for `requirements.txt`**
PowerShell's `>` redirect operator outputs UTF-16 LE — which Git registers as binary (0 text insertions). Corrected by generating with `pip freeze | Out-File -Encoding utf8 requirements.txt` and amended via `git commit --amend` + `git push --force-with-lease` before any downstream pull. Amending (rather than adding a correction commit) preserves a clean, intention-accurate commit history.

**Configuration Scope — Architectural Constants vs. Environment Variables**
Initial design loaded a `GCS_PREFIX` variable from `.env` to construct the GCS object path. On review: a path prefix like `raw` is an architectural constant, not an environment-specific or sensitive value. The object path is hardcoded in the script (`raw/raw_general_payments.csv`), keeping `.env` scoped exclusively to credentials and deployment identifiers.

**Cross-System Row Count Validation — Migration Checksum Finding**
During BigQuery load validation, a 12,580-row discrepancy was identified between MySQL (15,397,627) and BigQuery (15,385,047). A raw Python line count of the source CSV confirmed the ground truth: **15,385,047 data rows**. BigQuery matched the source CSV exactly. Root cause: MySQL's `LOAD DATA LOCAL INFILE` was splitting 12,580 records containing embedded quoted newlines into two rows each — a 0.08% phantom row inflation that was visible in Phase 1 as a single "shifted row" in Workbench but not investigated at the time.

**Impact on Phase 1:** Dollar totals unaffected — phantom continuation rows had null payment amounts. Compliance findings, vendor rankings, and Z-score flags remain valid. Row count metrics were overstated by 0.08%.

**Migration Checksum Status: PASSES.** BigQuery source CSV match is 100%. BigQuery is the authoritative row count layer at **15,385,047**. Phase 1 MySQL is preserved as documented with this known limitation recorded in `TECHNICAL.md`.

### Milestone 2: dbt Transformation Layer — Staging + Dimension Models

**The Problem:** Phase 1's MySQL architecture performed all data cleaning through batch `UPDATE` stored procedures that mutated staging tables in-place — making the pipeline stateful, slow to re-run, and opaque to version control. Complex recursive CTEs were required just to parse space-delimited ZIP code lists.

**The Decision:** Adopted a cloud-native ELT pattern using **dbt** + **BigQuery**. Raw data is never touched after load; all cleaning is computed as views and materialized tables on top of the raw layer.

**Staging Layer (`stg_general_payments`, `stg_us_cities`):**
- Typed, renamed, and healed all columns from `cms_open_payments_raw.raw_general_payments` as a BigQuery view
- Ported Phase 1's `BatchCleanProvince` stored procedure into a `CASE/COALESCE` expression — CMS records that misroute state codes into `recipient_province` and zip codes into `recipient_postal_code` are corrected at the staging layer, healing all downstream models simultaneously
- Replaced recursive CTE ZIP parsing with BigQuery's native `UNNEST(SPLIT(zips, ' '))`, eliminating 40+ lines of MySQL loop logic

**Macros (`clean_name`, `clean_hospital_name`):**
- `clean_name.sql`: Wraps `INITCAP()` + regex to strip titles, collapse spaces, and null sentinel values — replaces Phase 1's 40-line MySQL `TitleCase` UDF
- `clean_hospital_name.sql`: 200+ CASE entry hospital system rollup (AdventHealth, HCA Healthcare, Mayo Clinic, etc.) ported from `14_clean_hospitals.sql` — accepts `hospital_name`, `city`, and `state` as parameters since system assignment requires geographic tiebreaking

**Dimension Models (all materialized as BigQuery tables):**
- `dim_physician` — Covered Recipient Physicians; `clean_name` macro applied; dual geographic join (zip-first, city+state fallback via `city_lookup` CTE) attaches lat/lng/population from SimpleMaps golden record; `QUALIFY ROW_NUMBER() PARTITION BY recipient_profile_id ORDER BY program_year DESC` enforces one row per physician
- `dim_hospital` — Teaching Hospitals; `clean_hospital_name` macro applied; same dual geo join and QUALIFY deduplication pattern
- `dim_product` — `UNION ALL` unpivot across all 5 CMS product slots recovers 18–20% of product associations silently dropped by single-column queries
- `dim_company` — Manufacturer/GPO payer dimension; full Tier 1 + Tier 2 brand-to-parent consolidation CASE (Janssen → Johnson & Johnson, Covidien → Medtronic, etc.) ported from `15_clean_payers.sql`; subsidiary names stripped of legal suffixes ("A Division Of", "D/b/a") via `REGEXP_EXTRACT`; parent/subsidiary separation preserved for compliance rollup analysis; QUALIFY deduplication on subsidiary_id
- `dim_nature_of_payment` — Distinct CMS payment nature + form combinations; grain is one row per nature/form pair

**Fact Table (`fct_payments`):**
- Grain: one row per payment × product combination — same `UNION ALL` unpivot pattern as Phase 1
- 15.4M CMS payment records expand to **17.8M rows** across 5 product slots
- Surrogate key `payment_id = generate_surrogate_key(['record_id', 'product_slot'])` ensures uniqueness after the unpivot
- Foreign keys: `recipient_profile_id → dim_physician`, `teaching_hospital_ccn → dim_hospital`, `subsidiary_id → dim_company`, `payment_nature → dim_nature_of_payment`
- `record_id` retained as a traceability column for auditing back to the CMS source record

**Phase 1 Cleaning Gaps Ported to Staging:**
- **Data quality purge** — filters `record_id = 'No'`, `program_year < 2000`, and numerically-corrupted `payer_name` values (previously in `07_data_quality_purge.sql`)
- **Specialty extraction** — `REGEXP_EXTRACT(specialty_col, r'[^|]+$')` pulls the last pipe segment from each of CMS's 6 raw specialty columns, replacing Phase 1's `SUBSTRING_INDEX(col, '|', -1)`; `recipient_primary_specialty` added as a dedicated column for dim_physician
- **Product name cleaning** — `INITCAP` + trademark strip (`\(R\)|\(TM\)`) applied to all 5 product name slots; replaces Phase 1's `CleanProductName()` UDF
- **Recipient name normalization** — `INITCAP(TRIM(...))` applied to first/middle/last/suffix columns; replaces Phase 1's `TitleCase(CleanName(...))`

**dbt Tests (`schema.yml`) — 23/23 passing:**
- `not_null` + `unique` on all natural and surrogate keys
- `relationships` tests on all 4 fct_payments foreign keys with `where` config to scope physician FK check to physician-type rows only (CMS assigns `recipient_profile_id` to all recipient types — hospital IDs would otherwise fail the physician FK test)

**The Trade-off:** Traded local stored procedure simplicity for dbt's Jinja macro system and BigQuery-specific SQL functions. The gain: every transformation is a reviewable, testable Git file; raw data is always preserved and re-queryable; the full pipeline re-runs from scratch in under 30 seconds rather than hours.


### Phase 2 Commit History

```
c34eb02  chore: add python dependency requirements file
b29c815  chore: initialize project architecture with empty bigquery directories
002b412  chore: initialize repository safety config with gitignore
c746a7c  chore: add untracked MySQL pipeline scripts to version control
```

*Conventional Commits format enforced throughout — `type: description` in imperative mood. Security configuration committed before architecture scaffold: a deliberate ordering that ensures the repo is protected before files are added.*

---

## Tech Stack

| Layer | Phase 1 | Phase 2 |
|---|---|---|
| Raw Ingestion | `LOAD DATA LOCAL INFILE` (MySQL) | `upload_to_gcs.py` → GCS → BigQuery |
| Data Warehouse | MySQL 8.0 Star Schema | BigQuery (cms_open_payments_raw) |
| Transformation | Stored Procedures + Window Functions | dbt Core (staging + mart models) |
| Data Quality | Checksum Audit Scripts | dbt tests (not_null, unique, relationships) |
| Orchestration | Manual execution | (Airflow — planned Phase 3) |
| Visualization | Tableau Public | Tableau Public (same dashboard) |
| Auth | N/A | GCP Application Default Credentials |

---

## Environment Setup (Phase 2)

```bash
# 1. Clone and branch
git clone https://github.com/michaelcampbell215/healthcare-data-engineering.git
git checkout bigquery-migration

# 2. Create virtual environment
py -m venv .venv
.venv\Scripts\activate    # Windows
source .venv/bin/activate  # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your GCP project ID, bucket name, and CSV path

# 5. Authenticate with GCP
gcloud auth application-default login
```

See `.env.example` for all required environment variables and authentication options.

---

## Next Steps

- ~~**Phase 2 — Cloud Ingestion:** `upload_to_gcs.py`, `load_to_bq.py`, schema JSON files~~ ✅ Complete
- ~~**Phase 2 — dbt Staging + Dimensions:** `stg_general_payments`, `stg_us_cities`, all dimension models~~ ✅ Complete
- ~~**Phase 2 — dbt Fact Table:** `fct_payments.sql` — 17.8M rows, surrogate key, 5-slot UNION ALL unpivot~~ ✅ Complete
- ~~**Phase 2 — dbt Tests:** `schema.yml` with 23 tests — not_null, unique, relationships — 23/23 passing~~ ✅ Complete
- **Phase 2 — Merge:** Merge `bigquery-migration` → `main`, tag `v2.0-bigquery-migration`
- **Phase 3 — Load Historical Years:** Extend pipeline to load 2013–2023 program years into the same BigQuery dataset without schema changes
- **Phase 3 — Research Payments:** Extend to `raw_research_payments`, build cross-dataset conflict-of-interest flag layer
- **Phase 3 — Orchestration:** Airflow DAG to automate the full pipeline refresh cycle
- **Phase 3 — Specialty Standardization:** Port `ref_specialties` synonym mappings ('Orthopaedic Surgery' → 'Orthopedic Surgery') as a dbt seed + CASE layer in `dim_physician`
- **SCD Type 2 on dim_physician/dim_hospital:** Implement full provider geography history for audit-grade compliance tracking across CMS reporting years