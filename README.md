# Healthcare Data Engineering Pipeline

[![MySQL](https://img.shields.io/badge/MySQL-Enterprise_Database-4479A1.svg)](https://www.mysql.com/)
[![Tableau](https://img.shields.io/badge/Tableau-Visualization-E97627.svg)](https://www.tableau.com/)

> [!IMPORTANT]
> **Executive Summary:** This repository contains the backend data engineering pipeline and Star Schema architecture that powers the Supply Chain Analytics Dashboard. By engineering a rigid Enterprise Data Warehouse in MySQL, we successfully unpivoted and reconciled 15.4M records via optimized server-side SQL to deliver sub-second executive reporting.

**Core Technical Assets:**
*   **Business Case Study (Frontend):** [Supply Chain Analytics Dashboard](../supply-chain-command-center)
*   **Live Dashboard:** [View on Tableau Public](https://public.tableau.com/views/SupplyChainCommandCenter/LogisticsDashboard)

---

## Project Overview

Raw operational data (15M+ rows) was completely unsuited for fast executive decision-making. The legacy architecture over-relied on client-side Business Intelligence (BI) tools to perform heavy data transformations, unpivoting, and complex geospatial math virtually guaranteeing system crashes and reporting timeouts during critical review periods.

1. **Description:** We engineered a rigid Enterprise Data Warehouse deployed via MySQL to force structure onto unstructured raw data and shift the "Heavy Lifting" from the BI layer to optimized, server-side SQL procedures.
2. **Objective:** Eliminate dashboard load timeouts, perfectly reconcile 15.4M transactional records, and provide a single source of truth for the Supply Chain Analytics Dashboard.

## Data Sources

1. **Primary Datasets:** 15.4M rows of raw, unstructured transactional healthcare data containing embedded and nested inventory records.
2. **Additional Data:** Standardized national provider directories, geospatial ZIP code databases, and product classification hierarchies.

## Process

*   Engineered an optimized Star Schema with a central Fact Table (`fact_payments`) and 5 specific Dimension Tables (Physicians, Geography, Products, Entities, Time) with reinforced primary/foreign key indexing.
*   Deployed robust stored procedures with built-in `COMMIT` checkpointing to batch-load 50,000-row chunks, preventing memory timeouts during massive ingestion.
*   Embedded Automated Data Definition Language (DDL) scripts to establish rock-solid data integrity checkpoints before any ingestion occurred.

## Technical Pivot

*   **The "Recovery Algorithm" (Unpivoting):** Legacy BI tools were crashing when attempting to unpivot millions of wide rows. We engineered a complex SQL `UNION ALL` algorithm directly on the server to extract "hidden" inventory buried in secondary columns. This critical pivot immediately recovered 18% to 20% of total product volume that legacy analytical models had simply been missing due to frontend calculation limits.

## Key Insights

*   **Stop Processing on the Frontend:** Modern BI tools (Tableau/Power BI) excel at visualization but fail catastrophically at high-volume data transformation. All transformation logic must be shifted to the database layer.
*   **Geospatial Processing is Heavy:** Calculating radial distances to national logistics hubs on the dashboard level caused exponential load times. Utilizing MySQL's native `ST_Distance_Sphere` function on the server side instantly solved the geographic bottleneck.
*   **Pre-Aggregation is Mandatory:** Moving heavy analytical functions like `PERCENT_RANK()` and Z-Scores out of Tableau and into the final SQL export scripts dropped dashboard load times from >2 minutes to absolute sub-seconds.

## Recommendations

*   **Enforce Star Schema Architecture:** Forbid ad-hoc "flat file" queries from being used in production dashboards; mandate that all executive reporting connects exclusively to the optimized Star Schema.
*   **Batch Ingestion Only:** Prohibit massive bulk uploads of raw transactional data. Mandate the use of the 50,000-row chunking procedures to ensure database stability and enable rollback capabilities.
*   **Server-Side Math:** All complex logistical calculations specifically geospatial proximity and statistical ranking must be performed natively in MySQL before export to the BI presentation layer.

## Next Steps & Action Plan

*   **Automated Job Scheduling:** Transition the manual execution of `scripts/production/22_populate_warehouse.sql` to an automated cron job that runs nightly during off-peak hours.
*   **Data Quality Alerts:** Implement automated threshold alerts within the ETL pipeline to notify engineering if the ingested row counts deviate significantly from the expected 15M benchmark.

---

### Repository Architecture & Usage

```text
├── assets/
│   └── EER_DIAGRAM.png              # Entity-Relationship Diagram
├── scripts/
│   ├── production/                  # Core ETL stored procedures and exports
│   │   ├── 21_create_warehouse_tables.sql  # DDL for Warehouse
│   │   ├── 22_populate_warehouse.sql       # Batch Ingestion Logic
│   │   └── 40_export_tableau_data.sql      # Aggregated Output Script
│   └── schema/                      # Raw Staging & DDL definition scripts
│       ├── 06_create_schema.sql            # Initial DB Creation
│       └── 21_create_warehouse_tables.sql  # Fact/Dim Construction
├── TECHNICAL.md                     # Deep-dive architecture documentation
```
