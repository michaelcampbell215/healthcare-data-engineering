# Supply Chain Optimization: Data Engineering Pipeline

## **Project Overview**

**The Chaos on the Ground:** Raw operational data was completely unsuited for fast decision-making. 15 million rows of unformatted regulatory compliance data were acting as a massive bottleneck, taking minutes to query and hiding critical unit volume details inside embedded arrays.
**The Solution:** Rather than relying on spreadsheet workarounds, I built a robust backend "Control Tower". By engineering an optimized Enterprise Data Warehouse, we forced structure onto the chaos, delivering sub-second query performance for supply chain, sales, and risk management teams.

## **Data Sources**

- **Regulatory Compliance Exports (CMS Open Payments):** A massive dataset (15M+ rows, 2020-2024 subset) tracking complex financial transactions, containing inconsistent geographic data and buried arrays.
- **Geospatial Reference Data:** Custom tables linking US Zip Codes to accurate Latitude and Longitude coordinates.

## **Process**

- **Database Architecture (SQL):** Designed a highly structured Star Schema comprising a central `fact_payments` table linked to 5 optimized dimensions, replacing ad-hoc reporting.
- **Data Transformation Algorithms:** Engineered a complex `UNION ALL` SQL unpivoting algorithm to extract "hidden" inventory volume that was previously buried in secondary columns.
- **Spatial Backend Dominance:** Implemented MySQL's `ST_Distance_Sphere` to calculate exact radial distances to national logistics hubs directly in the database engine, bypassing slow front-end BI parameters.
- **Batch Processing:** Deployed robust stored procedures with built-in checkpointing to process millions of rows without memory timeouts in our high-uptime environment.

## **Key Findings**

- **The "Hidden Inventory" Discovery:** By failing to unpivot the raw data, legacy supply chain models were undercounting physical product movement by nearly **20%**, artificially lowering true capacity requirements.
- **Speed to Insight:** Forcing the data into a Star Schema shifted query performance for massive executive dashboards from **minutes to sub-seconds**.
- **Geospatial Reliability:** Pushing distance calculations down to the SQL engine level proved vastly more reliable and actionable than complex buffer interactions within the visualization layer.

## **Recommendations (Technical Roadmap)**

- **Automated Orchestration:** Wrap the SQL Stored Procedures into an orchestration tool (like Apache Airflow) to fully automate the pipeline on a rigorous scheduled cadence.
- **Incremental Loading:** Shift to a Delta-load strategy using `updated_at` timestamps to ingest only new records, eliminating full batch reloads and maintaining high uptime.
- **Data Governance Checkpoints:** Deploy SQL validation scripts immediately post-ingestion to flag missing Zip Codes or critical "NULL Product" transactions before they infect the warehouse.

## **Next Steps**

- **Cloud Migration:** Deploy this warehouse architecture to a cloud environment (AWS RDS or Snowflake) for virtually unlimited scalability.
- **Golden Record Iteration:** Collaborate with the BI team to consume this optimized Star Schema as the single source of truth for frontend executive reporting.
