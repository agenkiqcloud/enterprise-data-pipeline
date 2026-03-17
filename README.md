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

#### Key Features

- Validates required columns
- Removes:
  - Null values
  - Invalid price/quantity
  - Bad dates
  - Duplicate records
- Adds:
  - `revenue = price × quantity`

#### Output: -store in clean bucket

- Writes cleaned data to:
- s3://enterprise-data-clean/cleaned/

### 3. 🔄 AWS Glue (ETL Processing)

Triggered after Lambda execution.

#### Transformations

- Remove duplicates
- Aggregate revenue
- Group by product

#### Output: - Stored in Parquet format
  
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
├── architecture-diagram/
│   └── arch.png                # AWS architecture diagram of the pipeline
│
├── data-files/
│   └── sales.csv              # Sample input dataset for processing
│
├── glue/
│   └── lambda-etl-glue-job.py # AWS Glue ETL job script
│
├── lambda/
│   ├── datapipeline-lambda.py # Lambda function to trigger/process pipeline
│   └── requirements.txt       # Python dependencies for Lambda
│
├── .gitignore                 # Files and folders ignored by Git
├── Dockerfile                 # Container setup (if used for deployment/testing)
├── LICENSE                    # Project license
└── README.md                  # Project documentation
