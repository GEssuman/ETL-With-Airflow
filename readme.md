
# ETL WITH AIRFLOW

An end-to-end data pipeline for analyzing user streaming behavior and generating business intelligence KPIs for a music streaming platform.
-- 
## Overview
This project processes music streaming data from multiple sources to provide actionable insights through automated ETL pipelines and comprehensive analytics.

### Architecture

- **Data Sources**: Amazon RDS (user/song metadata), Amazon S3 (streaming events)
- **Processing**: Extract, validate, and transform streaming data and metadata using PySpark
- **Analytics**: Amazon Redshift for data warehousing and BI queries (to be implemented)
- **Development**: Docker containerized environment for local development and testing
- **Deployment**: AWS cloud services (S3 for storage, Redshift for warehousing)
- **Orchestration**: Currently planned to be orchestrated using MWAA (Managed Workflows for Apache Airflow)

### Project Status

- ✅ Data is currently being extracted and transformed using Spark
- ✅ Transformed data is being stored in S3 (organized in Medallion architecture: raw_data/, curated/, presentation/)
- 🔜 Redshift integration to be implemented
- 🔜 MWAA orchestration to be added

## Getting Started

### Prerequisites
- *Docker (For Local Development)*
- *AWS Account*
