FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-clickhouse==1.8.0 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0 \
    prometheus-client==0.20.0 \
  && /home/airflow/.local/bin/dbt --version

CMD ["webserver"]
