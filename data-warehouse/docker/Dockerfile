FROM apache/airflow:2.11.2-python3.11

ARG AIRFLOW_VERSION=2.11.2
ARG PYTHON_VERSION=3.11

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "Faker==37.0.0" \
    "PyMySQL==1.1.2" \
    "pymongo==4.16.0" \
    "duckdb==1.4.4" \
    && pip check

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
      "dbt-core==1.9.4" \
      "dbt-duckdb==1.9.4" \
      "duckdb==1.4.4" \
    && /home/airflow/dbt-venv/bin/pip check \
    && ln -s /home/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
