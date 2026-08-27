from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR = "/opt/dbt"
INGEST_SCRIPT = "/opt/ingestion/ingest.py"
# pip install target for the airflow user; not always on $PATH in bash subprocesses
DBT_BIN = "/home/airflow/.local/bin/dbt"


def push_pipeline_status(status: str, **context) -> None:
    """Push a pipeline health metric to the Prometheus Pushgateway."""
    import os
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    pushgateway_url = os.environ.get("PUSHGATEWAY_URL")
    if not pushgateway_url:
        return

    registry = CollectorRegistry()
    g = Gauge(
        "pipeline_last_run_status",
        "1 = success, 0 = failure",
        registry=registry,
    )
    g.set(1 if status == "success" else 0)

    ts = Gauge(
        "pipeline_last_run_timestamp",
        "Unix timestamp of last pipeline run",
        registry=registry,
    )
    ts.set(datetime.utcnow().timestamp())

    try:
        push_to_gateway(pushgateway_url, job="wb_pipeline", registry=registry)
    except Exception as exc:
        print(f"Could not push metrics: {exc}")


with DAG(
    dag_id="kiva_pipeline",
    description="Ingest Kiva loans → CDC to ClickHouse → dbt staging → mart",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["kiva", "ingestion", "cdc", "dbt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_kiva_loans",
        bash_command=f"python {INGEST_SCRIPT}",
    )

    # Allow time for Debezium CDC events to land in ClickHouse before running dbt
    wait_for_cdc = TimeDeltaSensor(
        task_id="wait_for_cdc_propagation",
        delta=timedelta(seconds=30),
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} deps --profiles-dir {DBT_DIR} 2>&1",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --select staging --profiles-dir {DBT_DIR} --no-use-colors 2>&1",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test --select staging --profiles-dir {DBT_DIR} --no-use-colors 2>&1",
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --select mart --profiles-dir {DBT_DIR} --no-use-colors 2>&1",
    )

    dbt_test_mart = BashOperator(
        task_id="dbt_test_mart",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test --select mart --profiles-dir {DBT_DIR} --no-use-colors 2>&1",
    )

    push_success = PythonOperator(
        task_id="push_success_metric",
        python_callable=push_pipeline_status,
        op_kwargs={"status": "success"},
    )

    (
        ingest
        >> wait_for_cdc
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_mart
        >> dbt_test_mart
        >> push_success
    )
