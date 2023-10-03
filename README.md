# batch-data-pipeline
This repo is to centralize the batch data processing (ELT) pipelines built on Apache Airflow that I have implemented in different personal projects.
Up to now there are 2 main projects:
- Data ingestion and transformation on [Youtube trending dataset](https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset/data):
  - Load JSON and CSV files from a local server to Azure Blob Storage.
  - Ingest them into Snowflake where transformation steps were performed.
  
- Data pre-processing for ML model on [nyc_taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) using Spark:
  - Load parquet files from a local server to Google Cloud Storage.
  - Clean this big dataset (~730 million records) using SparkSQL on Cloud Dataproc and load the cleaned data back to GCS.

Implemented pipelines are shown in the image below:

![batch-data-pipeline](https://github.com/phamthiminhtu/batch-data-pipeline/assets/56192840/312a280c-7751-4e34-b9c5-45ce386a0dad)

This repo is still in its early stages. New features and implementation are coming.
Future implementation:
- Ingest data from other databases (Mysql, Postgres, etc.)
- dbt for data transformation
