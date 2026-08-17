# Retail Data Platform

End-to-end Data Engineering pipeline built with Python, PostgreSQL, Pandas, PySpark, Apache Airflow and AWS S3.

The project extracts data from a PostgreSQL database, processes and validates it with Apache Spark, generates curated analytical datasets and stores the different data layers locally and in Amazon S3.

## Architecture
```text
                    PostgreSQL
                        │
                        ▼
                 Apache Airflow
                        │
                        ▼
                    Extract
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
       data/landing/       AWS S3 / landing
          CSV files
              │
              ▼
        Apache Spark
         Processing
              │
              ├───────────────────┐
              ▼                   ▼
      data/processed/      AWS S3 / processed
          Parquet
              │
              ▼
         Data Generation
       ┌───────┴────────┐
       ▼                ▼
    Sales       Customer Analytics
     │                │
     └───────┬────────┘
             ▼
         data/curated/
            Parquet
               │
               ▼
          AWS S3 / curated


Pipeline

The complete pipeline is orchestrated with Apache Airflow:

extract
   ↓
process
   ↓
sales
   ↓
customer_analytics
1. Extract

Data is extracted from PostgreSQL using Python and Pandas.

The extracted tables are saved as CSV files in:

data/landing/

The CSV files are also uploaded to Amazon S3:
s3://<bucket>/retail/landing/

2. Process

Apache Spark reads the CSV files from the landing layer and applies the corresponding schemas.

The processing stage performs several data quality checks:

Checks for empty datasets
Detects duplicate rows
Removes duplicate rows
Detects NULL values
Writes the processed data as Parquet

The processed datasets are stored locally in:

data/processed/

and uploaded to:

s3://<bucket>/retail/processed/
3. Sales Dataset

The pipeline combines the following datasets:

orders
order_items
products

to create a curated sales dataset.

The resulting dataset contains information such as:

order
customer
product
category
quantity
unit price
total amount

The dataset is stored in:

data/curated/sales.parquet/

and uploaded to:

s3://<bucket>/retail/curated/
4. Customer Analytics

The pipeline generates customer-level analytical data using:

customers
orders
sales

The resulting dataset contains metrics such as:

number of orders
last order date
total amount spent

The dataset is stored in:

data/curated/customer_analytics.parquet/

and uploaded to Amazon S3.

Technologies
Python
PostgreSQL
Pandas
PySpark
Apache Spark
Apache Airflow
AWS S3
Boto3
Git
GitHub
python-dotenv
Project Structure
retail-data-platform/
│
├── airflow/
│   └── dags/
│       └── retail_pipeline.py
│
├── data/
│   ├── landing/
│   ├── processed/
│   └── curated/
│
├── spark/
│   └── session.py
│
├── src/
│   ├── aws/
│   │   ├── __init__.py
│   │   └── s3.py
│   │
│   ├── config/
│   │   ├── database.py
│   │   ├── schemas.py
│   │   └── tables.py
│   │
│   ├── generators/
│   │   ├── customer_analytics.py
│   │   └── sales.py
│   │
│   ├── ingestion/
│   │   └── extract.py
│   │
│   ├── transforms/
│   │   └── process.py
│   │
│   └── utils/
│       └── logger.py
│
├── .env.example
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── README.md
Data Layers

The project uses three logical data layers.

Landing

Contains the data extracted from PostgreSQL in CSV format.

data/landing/

and:

S3: retail/landing/
Processed

Contains data processed and validated with Apache Spark and stored in Parquet format.

data/processed/

and:

S3: retail/processed/
Curated

Contains analytical datasets generated from the processed data.

data/curated/

and:

S3: retail/curated/
AWS S3 Structure

The S3 bucket is organized into the same logical layers:

retail/
│
├── landing/
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── products.csv
│   ├── payments.csv
│   └── shipments.csv
│
├── processed/
│   ├── customers.parquet/
│   ├── orders.parquet/
│   ├── order_items.parquet/
│   └── ...
│
└── curated/
    ├── sales.parquet/
    └── customer_analytics.parquet/
Configuration

Sensitive configuration is stored using environment variables and is not committed to the repository.

Example:

DB_HOST=localhost
DB_PORT=5432
DB_NAME=retail
DB_USER=your_user
DB_PASSWORD=your_password


S3_BUCKET_NAME=your-bucket
AWS_REGION=eu-west-1

AWS credentials are managed through the AWS CLI rather than being stored directly in the project.

Installation

Create and activate a virtual environment:

python -m venv .venv
source .venv/bin/activate

Install the project dependencies:

pip install -r requirements.txt

Configure the required environment variables using .env.

Make sure PostgreSQL and AWS credentials are available.

Running the Pipeline

Start Apache Airflow:

airflow standalone

Open the Airflow web interface and trigger:

retail_pipeline

The DAG executes the following tasks sequentially:

extract
   ↓
process
   ↓
sales
   ↓
customer_analytics

Each task is configured with retries to handle temporary failures.

Logging and Error Handling

The project includes logging throughout the pipeline.

The pipeline handles situations such as:

PostgreSQL extraction errors
Empty datasets
Duplicate records
NULL values
Spark processing errors
S3 upload errors

Airflow retries failed tasks according to the configured retry policy.

AWS Integration

AWS S3 is integrated using Boto3.

The project uses two upload functions:

upload_file_to_s3()

for individual files such as CSV files, and:

upload_directory_to_s3()

for the directories generated by Spark when writing Parquet datasets.

This allows the same pipeline to maintain local data layers while also storing the data in an AWS S3 data lake structure.

Purpose

This project was created as a practical Data Engineering portfolio project demonstrating an end-to-end data pipeline.

It covers:

Data extraction from PostgreSQL
ETL processing
Data quality checks
PySpark transformations
Parquet
Data layers
Analytical dataset generation
Apache Airflow orchestration
AWS S3 integration
Cloud data lake concepts
Logging and error handling
Git and GitHub