# 🚀 AWS Enterprise Data Pipeline

This project implements a complete **end-to-end AWS data engineering pipeline** for processing sales data from raw ingestion to business intelligence dashboards.

---

## 📊 Architecture Overview

Sales Upload → S3 (Raw) → Lambda → S3 (Clean) → Glue ETL → S3 (Processed) → Redshift → Power BI

---

## 🧩 Components

### 1. 📥 Data Ingestion
- Source: CSV / Excel upload
- Service: Amazon S3 (Raw Bucket)
- Bucket: `enterprise-data-raw`

---

### 2. ⚡ AWS Lambda (Data Cleaning & Validation)

Triggered automatically when a file is uploaded to S3.

#### Key Features:
- Validates required columns
- Removes:
  - Null values
  - Invalid price/quantity
  - Bad dates
  - Duplicate records
- Adds:
  - `revenue = price × quantity`

#### Output:
- Writes cleaned data to:
- s3://enterprise-data-clean/cleaned/

### 3. 🔄 AWS Glue (ETL Processing)

Triggered after Lambda execution.

#### Transformations:
- Remove duplicates
- Aggregate revenue
- Group by product

#### Output:
- Stored in Parquet format:
  
s3://enterprise-data-processed/


---

### 4. 🏢 Amazon Redshift (Data Warehouse)

- Loads processed data using `COPY` command
- Final Table:
  sales_summary

---

### 5. 📊 Power BI Dashboard

- Connects to Redshift
- Visualizes:
- Revenue by Product
- Sales Trends
- Region-wise performance

---

## 📂 Project Structure
enterprise-data-pipeline/
│
├── lambda/
│ └── datapipeline-lambda.py
│
├── glue/
│ └── lambda-etl-glue-job.py
│
├
│
├── data-files/
│ └── sales.csv
├── architecture-diagram/
│ └── arch.png
├── README.md
└── .gitignore