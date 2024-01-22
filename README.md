# batch-data-pipeline
This repo is to centralize the batch data processing (ELT) pipelines built on Apache Airflow that I have implemented in different personal projects.
  - Side note: the tools and technologies used in this project are for the purpose of exploring new tools. Therefore, they might not be the best architectures for a specific use case.

- Implemented pipelines are shown in the image below:
![batch-data-pipeline](https://github.com/phamthiminhtu/batch-data-pipeline/assets/56192840/2c3b32ec-61a0-4a90-8784-e379c51bcd59)


## Project overview:
Up to now there are 3 main projects:
- Project 1: Data ingestion and transformation with [Youtube trending dataset](https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset/data):
  - Load JSON and CSV files from a local server to Azure Blob Storage.
  - Ingest them into Snowflake where transformation steps were performed.
  - DAG location:
    - dags/youtube_trend_init.py
    - dags/youtube_trend.py
  
- Project 2: Data pre-processing for ML model with [nyc_taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) using Spark:
  - Load parquet files from a local server to Google Cloud Storage.
  - Clean this big dataset (~730 million records) using SparkSQL on Cloud Dataproc and load the cleaned data back to GCS.
  - DAG location: dags/nyc_taxi.py

- Project 3: Data ingestion and transformation with [Airbnb dataset](https://public.opendatasoft.com/explore/dataset/airbnb-listings/) using dbt, Astronomer Cosmos and Airflow:
  There are 2 phases in this project:
  - Phase 1: dbt on Postgres database.
    - Workflow:
      - Ingest data directly into Data warehouse (Postgres SQL hosted on Google Cloud SQL).
      - Transform the ingested data in Postgres SQL (hosted on Google Cloud SQL) using dbt.
      - Orchestrate dbt models using Airflow via Astronomer Cosmos.
    - Limitations:
      - Do not store raw data in the staging area -> can not perform "look-back-in-time" operations when needed.
      - Postgres database is a good fit for certain transactional workloads instead of analytics operations.
    - Code:
      - DAG location: [dags/airbnb/airbnb_postgres_dbt_cosmos.py](https://github.com/phamthiminhtu/batch-data-pipeline/blob/master/dags/airbnb/airbnb_postgres_dbt_cosmos.py)
      - dbt models: [dags/dbt/airbnb/airbnb_postgres](https://github.com/phamthiminhtu/batch-data-pipeline/tree/master/dags/dbt/airbnb_postgres)
  - :star: **Phase 2: dbt on BigQuery** 
    - To adddress the limitations in phase 1, the workflow is modified:
      - Load data and their schema from local to Google Cloud Storage (stored in hive partitions for better retrieval).
      - Ingest data from GCS into BigQuery.
      - Transform the ingested data using dbt.
      - Orchestrate dbt models using Airflow via Astronomer Cosmos.
    - Limitations & TO-DO:
      - Ingestion: truncating the whole table instead of merging -> TO-DO: design a more robust ingesting workflow: merging and deduplicating data.
    - Code:
      - DAG location: [dags/airbnb/airbnb_bigquery_dbt_cosmos.py](https://github.com/phamthiminhtu/batch-data-pipeline/blob/master/dags/airbnb/airbnb_bigquery_dbt_cosmos.py)
      - dbt models: [dags/dbt/airbnb_bigquery](https://github.com/phamthiminhtu/batch-data-pipeline/tree/master/dags/dbt/airbnb_bigquery)
    - Screen shot of the DAG:
<img width="1393" alt="Screenshot 2024-01-22 at 22 49 00" src="https://github.com/phamthiminhtu/batch-data-pipeline/assets/56192840/1ee96e62-0846-4c32-9751-173721043415">



