# batch-data-pipeline
This repo is to centralize the batch data processing (ELT) pipelines built on Apache Airflow that I have implemented in different personal projects.
This repo is still in its early stages. New features and implementation are coming.

Up to now there are 3 main projects:
- Data ingestion and transformation on [Youtube trending dataset](https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset/data):
  - Load JSON and CSV files from a local server to Azure Blob Storage.
  - Ingest them into Snowflake where transformation steps were performed.
  - DAG location:
    - dags/youtube_trend_init.py
    - dags/youtube_trend.py
  
- Data pre-processing for ML model on [nyc_taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) using Spark:
  - Load parquet files from a local server to Google Cloud Storage.
  - Clean this big dataset (~730 million records) using SparkSQL on Cloud Dataproc and load the cleaned data back to GCS.
  - DAG location: dags/nyc_taxi.py

- Data transformation on [Airbnb dataset](https://public.opendatasoft.com/explore/dataset/airbnb-listings/) using dbt, Astronomer Cosmos and Airflow:
  - Transform the ingested data in Postgres SQL (hosted on Google Cloud SQL) using dbt.
  - Orchestrate dbt models using Airflow via Astronomer Cosmos.
  - Code:
    - DAG location: dags/airbnb_dbt_cosmos.py
    - dbt models: dags/dbt/airbnb
  - TO-DO:
    - Incorporate data ingestion jobs: load data from Postgres database to modern data warehouse (Bigquery/ Snowflake).
    - Connect dbt with the new data warehouse instead of Postgres.

Implemented pipelines are shown in the image below:

![batch-data-pipeline (3)](https://github.com/phamthiminhtu/batch-data-pipeline/assets/56192840/c66098ce-21f6-42e3-b0e4-6620b411192f)

