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
  └── raw_general_payments  (15.4M rows)
  └── raw_physicians
  └── raw_companies
         │
         ▼ dbt staging models (views)
  BigQuery Dataset: staging
  └── stg_general_payments
  └── stg_physicians
  └── stg_companies
         │
         ▼ dbt mart models (materialized tables)
  BigQuery Dataset: marts
  └── fct_payments
  └── dim_physician
  └── dim_company
  └── dim_nature_of_payment
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
fact_payments (15,397,627 rows — grain: individual payment / transfer of value)
  │
  ├── dim_recipient   → Physicians & Hospitals (NPI / CCN normalized, Type 1 SCD)
  ├── dim_product     → Device, Drug, Biologics classification
  ├── dim_payer       → Manufacturing entity normalized names
  ├── dim_date        → Standard calendar dimension
  └── dim_geography   → ZIP-to-MSA crosswalk (SimpleMaps Golden Record)
```

**Surrogate key strategy:** Auto-incrementing `fact_key` standardizes over the raw `record_id`, which is structurally inconsistent across CMS reporting years and vulnerable to loss during payment alterations.

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
├── dbt/                               # Phase 2 — Transformation layer
│   ├── models/
│   │   ├── staging/                   # views: clean, renamed columns from raw
│   │   └── marts/                     # tables: Star Schema fact + dimensions
│   ├── tests/
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

- **Phase 2 — Complete:** Finish `upload_to_gcs.py`, build `load_to_bq.py` with row count validation, define schema JSON files
- **Phase 2 — dbt:** Initialize dbt project, build staging models and Star Schema mart layer, add tests and schema.yml documentation
- **Phase 2 — Merge:** Merge `bigquery-migration` → `main`, tag `v2.0-bigquery-migration`
- **Phase 3 — Research Payments:** Extend to `raw_research_payments`, build cross-dataset conflict-of-interest flag layer
- **Phase 3 — Orchestration:** Airflow DAG to automate the full pipeline refresh cycle
- **SCD Type 2 on dim_recipient:** Implement full provider geography history for audit-grade compliance tracking across CMS reporting years