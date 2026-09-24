FROM apache/airflow:3.3.2-python3.12

# dbt in its own venv so it does not clash with Airflow's pinned deps
USER root
RUN python -m venv /opt/dbt-venv \
    && chown -R airflow: /opt/dbt-venv
USER airflow
RUN /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.5 \
        dbt-postgres==1.11.0 \
    && pip install --no-cache-dir psycopg2-binary==2.9.10
