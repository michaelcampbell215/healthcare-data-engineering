# Healthcare Data Engineering Pipeline
#### MySQL Star Schema Architecture Powering the CMS Open Payments Compliance Dashboard

[![MySQL](https://img.shields.io/badge/MySQL-Enterprise_Data_Warehouse-4479A1.svg)](https://www.mysql.com/)
[![Tableau](https://img.shields.io/badge/Tableau-Visualization_Layer-E97627.svg)](https://public.tableau.com/views/HealthcarePaymentsCompliance/ComplianceDashboard)
[![BigQuery](https://img.shields.io/badge/Migration_In_Progress-BigQuery_%2F_dbt-4285F4.svg)](https://cloud.google.com/bigquery)

> [!IMPORTANT]
> **Executive Summary:** This repository contains the backend data engineering pipeline and Star Schema architecture that powers the [Healthcare Payments Compliance Dashboard](https://github.com/michaelcampbell215/healthcare-payments-compliance). By engineering a production-grade Enterprise Data Warehouse in MySQL, we successfully unpivoted and reconciled **15.4M CMS Open Payments records** via optimized server-side SQL — delivering sub-second executive reporting, a **100% checksum-validated ingestion audit**, and recovering **18–20% of payment volume** that client-side BI tools were systematically missing.

**Core Technical Assets:**
- **Business Case Study (Frontend):** [Healthcare Payments Compliance & Spend Analytics](https://github.com/michaelcampbell215/healthcare-payments-compliance)
- **Live Dashboard:** [View on Tableau Public](https://public.tableau.com/views/HealthcarePaymentsCompliance/ComplianceDashboard)
- **Deep-Dive Architecture:** [TECHNICAL.md](./TECHNICAL.md)

---

> [!NOTE]
> **Analytics Engineering Connection:** This project demonstrates the foundational pattern of modern analytics engineering: shifting heavy computation from the BI layer to the database tier. The Star Schema design, stored procedure batch ingestion, COMMIT checkpointing, and checksum audit strategy map directly to the dbt + BigQuery patterns used in production AE roles — with an active GCP migration underway.

---

## The Problem

Raw CMS Open Payments data (15.4M rows) was completely unsuited for fast executive reporting. The legacy architecture over-relied on client-side BI tools to perform heavy data transformations — unpivoting wide nested records, executing complex geospatial calculations, and running window functions at query time — virtually guaranteeing dashboard timeouts during critical compliance review periods.

The deeper problem: no one knew the data was broken. Client-side transformation limits were silently dropping 18–20% of product-level payment records, meaning every analysis upstream of this pipeline was systematically undercounting exposure.

## Architecture

Engineered a production-grade Star Schema with one central Fact Table and five Dimension Tables:

```
fact_payments (15,397,627 rows — grain: individual payment / transfer of value)
  │
  ├── dim_recipient   → Physicians & Hospitals (NPI / CCN normalized, Type 1 SCD)
  ├── dim_product     → Device, Drug, Biologics classification
  ├── dim_payer       → Manufacturing entity normalized names
  ├── dim_date        → Standard calendar dimension
  └── dim_geography   → ZIP-to-MSA crosswalk (SimpleMaps Golden Record)
```

**Surrogate key strategy:** Auto-incrementing `fact_key` standardizes over the raw `record_id`, which is structurally inconsistent across CMS reporting years and vulnerable to loss during payment alterations. Every transaction is uniquely identifiable and protected from partial update loss.

**SCD note:** `dim_recipient` uses Type 1 SCD for query performance. Documented limitation: provider geography history (e.g., CA → FL moves) is not preserved. A Type 2 migration is the recommended path for full audit-grade history.

```
├── assets/
│   └── EER_DIAGRAM.png                     # Entity-Relationship Diagram
├── scripts/
│   ├── production/
│   │   ├── 21_create_warehouse_tables.sql  # DDL — Fact & Dimension tables
│   │   ├── 22_populate_warehouse.sql       # Batch ingestion (50k chunks)
│   │   └── 40_export_tableau_data.sql      # Pre-aggregated export script
│   └── schema/
│       ├── 06_create_schema.sql            # Initial DB creation
│       └── 21_create_warehouse_tables.sql  # Fact/Dim construction
├── TECHNICAL.md                            # Deep-dive architecture documentation
```

## Engineering Challenges

### The $42M Checksum Catch
During initial warehouse population, a join fanout caused `SUM(amount_usd)` in the warehouse to diverge from the source by $42M. The error was caught by `24_audit_checksum.sql` — a dedicated script that validates `Source SUM(Amount) = Warehouse SUM(Amount)` before any downstream export is permitted. Status after remediation: **100.0% match**. This is the kind of silent data integrity failure that would have propagated undetected into every downstream compliance report without an explicit checksum gate.

### The "Virtual Fact Table" — Recovering Hidden Payment Volume
The CMS source file flattens multi-product payments into wide columns (`Product_1`, `Product_2`, `Product_3`...). Client-side BI tools processing this file at query time were dropping the secondary columns entirely — silently missing 18–20% of payment volume. The fix: a `UNION ALL` CTE executes directly on the database server, unpivoting all product columns into a normalized fact stream before any data reaches Tableau. This is a server-side unpivot that no BI tool configuration can replicate.

### Zero-Downtime Schema Migration (Hot Patch)
Mid-project, geospatial analysis requirements required `lat`/`lng` columns on the recipient dimension. Reloading 15.4M fact rows to add these columns would have caused 4+ hours of downtime. Solution: `dim_recipient` (~1M rows) received the new columns via an in-place ALTER, populated via ZIP crosswalk join. All 15.4M fact records gained the geospatial capability instantly through the existing foreign key relationship — zero fact table reload, zero downtime.

### Batch Processing at Scale
Standard `UPDATE` statements on 15M rows caused transaction log overflows. All data cleaning and population operations are encapsulated in Stored Procedures using `WHILE` loop cursor pagination with `LIMIT 50000` batches and explicit `COMMIT` checkpoints. This pattern prevents lock contention, enables rollback at any checkpoint, and keeps individual transactions within memory bounds.

## Performance Results

| Metric | Before | After |
|---|---|---|
| Dashboard load time | >2 minutes | Sub-second |
| Payment volume captured | ~80% (client-side limit) | 100% (server-side unpivot) |
| Checksum accuracy | Not validated | 100.0% match |
| Schema change downtime | N/A (estimated 4+ hrs) | Zero (hot patch) |

## Key Technical Decisions

**SQL-First Approach:** Deliberately avoided external ETL tooling (Airflow, dbt) in this iteration to demonstrate advanced native database capability — stored procedures, transactions, indexing, and window functions — without framework dependency. The BigQuery + dbt migration (in progress) will introduce orchestration at the appropriate scale.

**Pre-Aggregation over Client Computation:** `PERCENT_RANK()`, Z-Score calculations, and Lorenz Curve rankings all execute in `40_export_tableau_data.sql` before data is handed to Tableau. This is the architectural principle that drives the sub-second dashboard performance.

**Covering Indexes:** Added `idx_perf_recipient_spend` as a covering index on `(recipient_key, amount_usd)` to allow the query optimizer to read directly from the B-Tree index, bypassing the 15M-row heap scan entirely for the most common aggregation pattern.

## Tech Stack

| Layer | Technology |
|---|---|
| Data Warehouse | MySQL 8.0 |
| ETL / Ingestion | Stored Procedures + COMMIT Checkpointing |
| Data Quality | Checksum Audit Scripts |
| Geospatial | `ST_Distance_Sphere`, SimpleMaps ZIP crosswalk |
| Visualization | Tableau Public |
| Migration Target | BigQuery + dbt (in progress) |

## Next Steps

- **BigQuery Migration (In Progress):** Transition MySQL warehouse to BigQuery with dbt transformation models for scheduled, version-controlled refreshes and elimination of infrastructure maintenance overhead.
- **Automated Job Scheduling:** Transition manual execution of `22_populate_warehouse.sql` to a nightly Airflow DAG running during off-peak hours.
- **SCD Type 2 on dim_recipient:** Implement full history tracking on provider geography to preserve audit-grade compliance history across CMS reporting years.
- **Data Quality Alerting:** Automate checksum threshold alerts to notify engineering if ingested row counts deviate from the expected 15.4M CMS benchmark.