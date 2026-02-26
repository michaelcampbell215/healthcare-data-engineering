# Healthcare Data Engineering Pipeline

## **1. Project Overview**

### **Description:**
This project focuses on the backend architecture and data engineering required to transform 15 million rows of raw, unformatted government compliance data (CMS Open Payments) into a highly optimized Enterprise Data Warehouse. The business case required converting a massive "compliance" dataset into an internal operational asset for Supply Chain, Sales, and Risk management.

### **Objectives:**
- Design and deploy an optimized Star Schema database structure (MySQL).
- Engineer scalable ETL pipelines using custom SQL Stored Procedures.
- Cleanse, normalize, and unpivot hidden operational data to enable advanced analytics.

## **2. Data Sources**

### **Primary Datasets:**
- **CMS Open Payments Data (2020-2024 subset):** A massive regulatory dataset tracking financial transactions between pharmaceutical/device manufacturers and healthcare providers. Contains complex text fields, inconsistent geographic data, and embedded arrays.

### **Additional Data (if applicable):**
- **Geospatial Reference Data:** Custom tables linking US Zip Codes to accurate Latitude and Longitude coordinates to enable spatial querying and mapping.

## **3. Process**

### **Exploratory Data Analysis (EDA):**
- Profiled the 15-million-row dataset, identifying 82 wide columns with significant null values and un-normalized structures.
- Discovered critical business intelligence buried in secondary columns (e.g., transactions listing up to 5 distinct products in a single row instead of separate line items).
- Identified inconsistencies in raw geographic tracking (e.g., missing coordinates or invalid state abbreviations).

### **Analytical Techniques:**
- **Database Architecture:** Designed a Star Schema comprising a central `fact_payments` table linking to 5 optimized dimensions (`dim_recipient`, `dim_product`, `dim_date`, `dim_payer`, `dim_nature`).
- **Data Transformation algorithms:** Engineered a complex `UNION ALL` SQL unpivoting algorithm to extract "hidden" inventory volume, expanding the useable dataset by 18%.
- **Geospatial Engineering:** Implemented MySQL's `ST_Distance_Sphere` function to calculate the exact radial distance from payment locations to 5 static national logistics hubs, replacing front-end BI parameters with heavily optimized backend math.
- **Batch Processing:** Wrote robust Stored Procedures with built-in checkpointing to process millions of rows without memory timeouts.

## **4. Key Findings**
- **Data Quality as a Bottleneck:** The raw compliance data was entirely unsuited for operational reporting. By forcing it into a Star Schema, query performance for executive dashboards improved from minutes to sub-seconds.
- **The "Hidden Inventory" Discovery:** By failing to unpivot the raw data, previous supply chain models were undercounting physical product movement by nearly 20%, artificially lowering capacity requirements.
- **Spatial Backend Dominance:** Pushing the geospatial distance calculations down to the SQL engine level proved vastly more reliable and actionable than attempting complex buffer interactions within the BI layer.

## **5. Recommendations**
- **Automated Orchestration:** Wrap the SQL Stored Procedures into an orchestration tool (like Apache Airflow) to fully automate the data pipeline on a scheduled cadence.
- **Incremental Loading:** Implement a Delta-load strategy using `updated_at` timestamps to ingest only new records, rather than relying on full batch reloads.
- **Data Governance Checkpoints:** Add SQL validation scripts immediately post-ingestion to flag missing Zip Codes or critical "NULL Product" transactions before they enter the data warehouse.

## **6. Next Steps & Action Plan**
- Deploy this warehouse architecture to a cloud environment (AWS RDS or Snowflake) for virtually unlimited scalability.
- Collaborate with the BI team to consume this optimized Star Schema for frontend executive reporting.
