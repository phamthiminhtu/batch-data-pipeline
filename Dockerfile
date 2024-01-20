ARG AIRFLOW_VERSION=2.7.0

FROM apache/airflow:${AIRFLOW_VERSION}

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} astronomer-cosmos --user
# Compulsory to switch parameter
ENV PIP_USER=false
RUN python3 -m venv /opt/airflow/dbt_venv
RUN . /opt/airflow/dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery
ENV PIP_USER=true
CMD ["/opt/airflow/dbt_venv/bin/dbt"]