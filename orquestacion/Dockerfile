# Imagen de Airflow del proyecto. El codigo del pipeline corre en un venv aparte para que sus
# dependencias no choquen con las de Airflow (mismo patron que el venv de dbt del DE-DW).
FROM apache/airflow:2.11.2-python3.11

COPY data-lake/requirements.txt /tmp/dl-requirements.txt
RUN python -m venv /home/airflow/dl-venv \
    && /home/airflow/dl-venv/bin/pip install --no-cache-dir -r /tmp/dl-requirements.txt \
    && /home/airflow/dl-venv/bin/pip check \
    && /home/airflow/dl-venv/bin/pip freeze > /home/airflow/dl-venv/versiones.txt
