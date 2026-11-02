# Imagen de Airflow del proyecto. Cada pipeline corre en su propio venv para que sus dependencias no
# choquen con las de Airflow ni entre si (mismo patron que el venv de dbt del DE-DW).
FROM apache/airflow:2.11.2-python3.11

# Bundle de CA de AWS para validar TLS contra RDS y DocumentDB.
USER root
RUN mkdir -p /opt/certs \
    && curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /opt/certs/rds-global-bundle.pem \
    && chmod 644 /opt/certs/rds-global-bundle.pem \
    && mkdir -p /opt/dbt-work && chown airflow:0 /opt/dbt-work
USER airflow

COPY data-lake/requirements.txt /tmp/dl-requirements.txt
RUN python -m venv /home/airflow/dl-venv \
    && /home/airflow/dl-venv/bin/pip install --no-cache-dir -r /tmp/dl-requirements.txt \
    && /home/airflow/dl-venv/bin/pip check \
    && /home/airflow/dl-venv/bin/pip freeze > /home/airflow/dl-venv/versiones.txt

COPY data-warehouse/requirements-aws.txt /tmp/dw-requirements.txt
RUN python -m venv /home/airflow/dw-venv \
    && /home/airflow/dw-venv/bin/pip install --no-cache-dir -r /tmp/dw-requirements.txt \
    && /home/airflow/dw-venv/bin/pip check \
    && /home/airflow/dw-venv/bin/pip freeze > /home/airflow/dw-venv/versiones.txt

# dbt en su propio venv, igual que en el DE-DW, con el adaptador de Redshift en lugar del de DuckDB.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.9.4" "dbt-redshift==1.9.5" \
    && /home/airflow/dbt-venv/bin/pip check \
    && /home/airflow/dbt-venv/bin/pip freeze > /home/airflow/dbt-venv/versiones.txt
