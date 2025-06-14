
# ETL WITH AIRFLOW

An end-to-end data pipeline for analyzing user streaming behavior and generating business intelligence KPIs for a music streaming platform.

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

- Docker (For Local Development)
- AWS Account with access to S3


*NB: the aws script to upload is in the aws directory*

## Running Locally

1. Clone the repository
2. Build and run the Docker containers
3. Execute Spark jobs locally using your preferred environment

## Running Locally

1. Clone the repository
2. Build and run the Docker containers
3. Execute Spark jobs locally using your preferred environment


## Setting on AWS 


## AWS Setup Instructions

### 1. S3 Bucket Structure
Create an S3 bucket for storing your data. Within this bucket:

### 2. Glue Catalog and Crawlers
- Create an **AWS Glue Crawler** to crawl the raw data folders.
- Choose **"Create a new database"** or use an existing one.
- Use an IAM role with `AmazonS3ReadOnlyAccess`, `AWSGlueServiceRole`, and other necessary permissions.
- Run the crawler to populate the Glue Data Catalog.

---

### 3. Glue Scripts
- Upload all your transformation scripts (from `aws/` folder) into AWS Glue Studio or directly as Glue Jobs.
- Assign the same IAM role used in the crawler.
- Set up job parameters (e.g., `--JOB_NAME`) and ensure S3 temp directories are defined for Redshift writes.
- Use **Glue DynamicFrame to JDBC** to push data into Redshift (via `write_dynamic_frame.from_jdbc_conf()`).

---

## Redshift Setup Instructions

### 1. Redshift Serverless (Recommended)
- Go to Amazon Redshift > **Serverless** and create a workgroup and namespace.
- Ensure it is in the same **VPC and subnet** as your Glue Jobs.

### 2. Redshift Connection in Glue
- Go to AWS Glue > **Connections**
- Create a new **JDBC Connection**
  - Type: Redshift
  - JDBC URL: from your Redshift cluster
  - Set credentials and test connection
- Attach this connection to your Glue job via `catalog_connection`
- Create vpc endpoints for the glue to access other services outside its network

### 3. Initialize Redshift Tables
- Run provided SQL scripts (in `sql/init_schema.sql`) to initialize the schema:
  - `staging.hourly_stream_insights`
  - `presentation.hourly_stream_insights`
  - `staging.most_popular_track_per_genre`
  - `presentation.most_popular_track_per_genre`

---



NB: Yet to Orchestrace using the MWAA(Manage Workflow Apache Airflow)



so that sample of the tables in the Redshift base in below

![Alt text](./docs/Screenshot%202025-06-17%20170402.png)


## Future Work

- Integrate Redshift as the target data warehouse
- Deploy DAGs on MWAA for scheduled orchestration
- Implement automated data quality checks
- Design Project System Architecture

---

*Note: This is a work in progress. Contributions and suggestions are welcome!*
