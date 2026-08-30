# World Bank Loans Analytics Pipeline

End-to-end, production-grade data engineering pipeline ingesting World Bank project loans data through a full CDC → OLAP → analytics stack. The entire platform starts with a single command.

**Data source:** [World Bank Projects API](https://search.worldbank.org/api/v2/projects) — public, no authentication required.

---

## Architecture

```
World Bank Projects API  (search.worldbank.org/api/v2/projects)
         │
         ▼  Python ingestion (batched upsert, retry logic)
  PostgreSQL 15  (OLTP — analytics_db)
         │
         │  WAL logical replication  (wal_level=logical)
         ▼
  Debezium 2.6  ──►  Kafka topic: analytics.public.loans
  (pgoutput)          ExtractNewRecordState SMT
         │
         │  Kafka engine pull
         ▼
  ClickHouse 24.3  (OLAP)
    raw.kafka_loans      ← Kafka engine table
         │
         ▼  Materialized View (raw.mv_loans) — type casting
    raw.loans            ← ReplacingMergeTree(updated_at)
         │
         ▼  dbt-clickhouse (staging layer)
    staging.stg_loans    ← ReplacingMergeTree, FINAL, _deleted=0
         │
         ▼  dbt-clickhouse (mart layer)
    mart.mart_loan_performance   ← aggregated by region/country/instrument/status
    mart.mart_country_portfolio  ← aggregated by country
    mart.mart_approval_trends    ← aggregated by fiscal year

  Orchestration:  Airflow 2.9  (hourly DAG: wb_pipeline)
  Observability:  Prometheus + Grafana
```

---

## Prerequisites

| Tool | Minimum version | Notes |
|---|---|---|
| Docker | 24.x | Required |
| Docker Compose | v2.20 | Required — use `docker compose` (v2), not `docker-compose` (v1) |
| Python | 3.11 | Only needed locally to run unit tests or generate the Fernet key |
| `jq` | any | Optional — used in validation commands below for readable JSON output |

No other local dependencies are required. All pipeline components run inside containers.

---

## Setup & Quickstart

### 1. Clone and configure environment

```bash
git clone <repo-url>
cd inkomoko

# Copy the env template and fill in the required values
cp .env.example .env
```

Open `.env` and set the following (defaults are fine for local development):

| Variable | Required | Description |
|---|---|---|
| `POSTGRES_USER` | Yes | PostgreSQL username (`dataeng`) |
| `POSTGRES_PASSWORD` | Yes | PostgreSQL password |
| `POSTGRES_DB` | Yes | OLTP database name (`analytics_db`) |
| `AIRFLOW_DB` | Yes | Airflow metadata DB name (`airflow`) |
| `CLICKHOUSE_USER` | Yes | ClickHouse username (`default`) |
| `CLICKHOUSE_PASSWORD` | Yes | ClickHouse password |
| `AIRFLOW_USERNAME` | Yes | Airflow UI login username |
| `AIRFLOW_PASSWORD` | Yes | Airflow UI login password |
| `AIRFLOW_FERNET_KEY` | Yes | Used to encrypt Airflow connection secrets — generate with the command below |
| `AIRFLOW_SECRET_KEY` | Yes | Airflow webserver session secret |
| `GRAFANA_USER` | Yes | Grafana admin username |
| `GRAFANA_PASSWORD` | Yes | Grafana admin password |
| `SOCRATA_APP_TOKEN` | No | Optional World Bank API token; increases rate limit from 1,000 to unlimited requests/hour. Register free at [data.worldbank.org](https://data.worldbank.org) |

Generate the Fernet key:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output as the value of `AIRFLOW_FERNET_KEY` in `.env`.

### 2. Start the full stack

```bash
docker compose up -d
```

This single command starts all 12 services in the correct dependency order:

| Service | Purpose | Port |
|---|---|---|
| `postgres` | OLTP source — WAL-enabled | 5432 |
| `zookeeper` | Kafka dependency | 2181 |
| `kafka` | CDC event streaming | 29092 (host) |
| `debezium` | Kafka Connect + Debezium | 8083 |
| `connector-init` | Registers Debezium connector on startup | — |
| `clickhouse` | OLAP database | 8123 (HTTP), 9000 (native), 9363 (metrics) |
| `ingestion` | Python ingest job (runs once on startup) | — |
| `airflow-init` | Initialises Airflow DB and admin user | — |
| `airflow-webserver` | Airflow UI | 8080 |
| `airflow-scheduler` | Airflow scheduler | — |
| `pushgateway` | Prometheus metrics receiver for batch jobs | 9091 |
| `prometheus` | Metrics collection | 9090 |
| `grafana` | Dashboards | 3000 |

### 3. Confirm all services are healthy

```bash
docker compose ps
```

All services should show `healthy` or `exited 0` (for one-shot services like `connector-init` and `airflow-init`). Allow 2–3 minutes for Airflow to initialise and Debezium to register the connector.

---

## Data Source

**API:** World Bank Projects API
**Base URL:** `https://search.worldbank.org/api/v2/projects`
**Authentication:** None required. The API is fully public.
**Optional token:** Register a free app token at [data.worldbank.org](https://data.worldbank.org) and set it as `SOCRATA_APP_TOKEN` in `.env` to raise the rate limit from 1,000 to unlimited requests/hour.

**Request pattern:** The ingestion script fetches projects in paginated batches of 500 rows, up to 20 batches (10,000 records maximum per run). Each page is fetched with:

```
GET /api/v2/projects?format=json&rows=500&os=<offset>
```

The API returns an object with a `projects` key containing a dictionary keyed by project ID. The ingestion script normalises this into typed rows and bulk-upserts them into PostgreSQL using `ON CONFLICT (project_id) DO UPDATE SET`.

No API key, OAuth token, or session cookie is needed to run the pipeline.

---

## Validating Data at Each Stage

Run these commands after startup to verify data has moved through each layer.

### Stage 1 — PostgreSQL (OLTP)

```bash
# Row count by status
docker compose exec postgres psql -U dataeng -d analytics_db \
  -c "SELECT status, count(*) FROM loans GROUP BY status ORDER BY 2 DESC;"

# Latest updated rows
docker compose exec postgres psql -U dataeng -d analytics_db \
  -c "SELECT project_id, country, status, updated_at FROM loans ORDER BY updated_at DESC LIMIT 5;"

# Confirm logical replication publication exists
docker compose exec postgres psql -U dataeng -d analytics_db \
  -c "SELECT pubname, schemaname, tablename FROM pg_publication_tables;"
```

Expected: `loans` table with thousands of rows; `dbz_publication` listed for `public.loans`.

### Stage 2 — Kafka (CDC events)

```bash
# Check the topic exists
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list | grep analytics

# Read the first 5 CDC events from the loans topic
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic analytics.public.loans \
  --from-beginning --max-messages 5 --timeout-ms 10000

# Check Debezium connector status
curl -s http://localhost:8083/connectors/postgres-connector/status | jq
```

Expected: connector status `RUNNING`, Kafka topic `analytics.public.loans` present and producing JSON records with `__op`, `__deleted`, and `__ts_ms` fields.

### Stage 3 — ClickHouse raw layer

```bash
# Row count and freshness in raw.loans
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT count(), max(_ingested_at) AS last_ingested FROM raw.loans"

# Confirm no data loss vs PostgreSQL
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT count() FROM raw.loans FINAL WHERE _deleted = 0"

# Inspect a sample row
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT project_id, country, status, total_commitment_usd, updated_at FROM raw.loans FINAL LIMIT 3"
```

Expected: row counts should be close to (not necessarily identical to) PostgreSQL — a small lag is normal while Kafka and the Materialized View process remaining batches.

### Stage 4 — dbt staging layer

```bash
# Row count in staging (FINAL deduplication applied)
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT count() FROM staging.stg_loans"

# Top 10 countries by project count
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT country_code, country, count() AS projects FROM staging.stg_loans GROUP BY country_code, country ORDER BY 3 DESC LIMIT 10"
```

Expected: staging row count matches raw (minus any soft-deleted rows); no null `project_id` values; `status` values are lowercase.

### Stage 5 — dbt mart layer

```bash
# Loan performance by region and status
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT region, status, project_count, sum_commitment_usd FROM mart.mart_loan_performance ORDER BY sum_commitment_usd DESC LIMIT 10"

# Country portfolio totals
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT country_code, total_projects, sum_commitment_usd, active_projects FROM mart.mart_country_portfolio ORDER BY sum_commitment_usd DESC LIMIT 10"

# Approval trends by year
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT approval_year, region, sum_commitment_usd FROM mart.mart_approval_trends WHERE approval_year >= 2015 ORDER BY approval_year DESC, sum_commitment_usd DESC LIMIT 20"
```

Expected: populated mart tables with aggregated data; non-zero commitment amounts; plausible row counts relative to the staging layer.

### Stage 6 — Observability

```bash
# Check Prometheus targets are up
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'

# Query pipeline health metric
curl -s 'http://localhost:9090/api/v1/query?query=pipeline_last_run_status' | jq '.data.result'

# Query ingestion count
curl -s 'http://localhost:9090/api/v1/query?query=wb_loans_ingested_count' | jq '.data.result'
```

Expected: all scrape targets in state `up`; `pipeline_last_run_status` = 1 after a successful DAG run.

---

## Accessing Services

### Airflow

URL: `http://localhost:8080`
Credentials: `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` from `.env` (default: `admin` / `admin_pass`)

The DAG `wb_pipeline` is pre-loaded. It runs hourly and executes:

```
ingest_wb_loans
    → wait_for_cdc_propagation  (30 s)
    → dbt_deps
    → dbt_run_staging
    → dbt_test_staging
    → dbt_run_mart
    → dbt_test_mart
    → push_success_metric
```

To trigger the DAG manually:

```bash
docker compose exec airflow-webserver airflow dags trigger wb_pipeline
```

To check DAG run status via CLI:

```bash
docker compose exec airflow-webserver airflow dags list-runs -d wb_pipeline --output table
```

### Grafana

URL: `http://localhost:3000`
Credentials: `GRAFANA_USER` / `GRAFANA_PASSWORD` from `.env` (default: `admin` / `admin_pass`)

The **Pipeline Health** dashboard is provisioned automatically. It includes panels for:
- Pipeline last run status (success/failure)
- Loans ingested per run
- Ingestion duration (seconds)
- Ingestion error count
- ClickHouse insert rate
- ClickHouse query duration
- Last pipeline run timestamp

### Prometheus

URL: `http://localhost:9090`
No authentication.

Useful queries:

| Query | Description |
|---|---|
| `pipeline_last_run_status` | 1 = last DAG run succeeded, 0 = failed |
| `pipeline_last_run_timestamp` | Unix timestamp of last run |
| `wb_loans_ingested_count` | Records ingested in the last run |
| `wb_ingestion_duration_seconds` | Duration of last ingestion run |
| `wb_ingestion_errors_total` | Cumulative ingestion errors |

### ClickHouse

**HTTP interface** (browser / curl):

```bash
curl "http://localhost:8123/?query=SELECT+count()+FROM+staging.stg_loans" \
  -u default:clickhouse_pass
```

**Native client** (inside container):

```bash
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass
```

**Databases:**

| Database | Contents |
|---|---|
| `raw` | CDC-consumed tables (`loans`) and Kafka engine + Materialized Views |
| `staging` | dbt staging models (`stg_loans`) |
| `mart` | dbt mart models (`mart_loan_performance`, `mart_country_portfolio`, `mart_approval_trends`) |

### PostgreSQL

```bash
docker compose exec postgres psql -U dataeng -d analytics_db
```

### Kafka Connect (Debezium)

```bash
# List registered connectors
curl http://localhost:8083/connectors | jq

# Check connector status
curl http://localhost:8083/connectors/postgres-connector/status | jq

# Re-register connector if needed
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-connector.json
```

---

## Running Tests

### Unit tests (no infrastructure required)

```bash
pip install pytest pytest-mock psycopg2-binary requests prometheus-client
pytest tests/unit/ -v
```

Covers: API response parsing, type coercion (`to_amount`, `to_date`), upsert SQL construction, batch ingestion loop, error handling and retry logic.

### Integration tests (requires running stack)

```bash
pip install pytest psycopg2-binary pytest-env
pytest tests/integration/ -v \
  --override-ini="env=POSTGRES_HOST=localhost,POSTGRES_USER=dataeng,POSTGRES_PASSWORD=dataeng_pass,POSTGRES_DB=analytics_db"
```

Or with environment variables:

```bash
POSTGRES_HOST=localhost \
POSTGRES_USER=dataeng \
POSTGRES_PASSWORD=dataeng_pass \
POSTGRES_DB=analytics_db \
pytest tests/integration/ -v
```

Covers: schema existence, logical replication publication, CDC publication, row-level data quality checks.

### dbt tests

```bash
# Inside the Airflow container (dbt is installed there)
docker compose exec airflow-webserver bash -c \
  "cd /opt/dbt && dbt test --profiles-dir . --no-use-colors"

# Or trigger via Airflow by running dbt_test_staging and dbt_test_mart tasks
```

dbt tests cover `not_null` and `unique` constraints on staging keys, and `not_null` on mart aggregation columns.

---

## CI/CD

GitHub Actions runs on every push to `main` or `develop`, and on every pull request targeting `main`.

### Job: Lint & Unit Tests (all branches)

Triggered on every push and pull request.

| Step | What it validates |
|---|---|
| `ruff check` | Python style and lint rules across `ingestion/`, `orchestration/dags/`, `tests/` |
| `pytest tests/unit/` | All unit tests pass without infrastructure |

### Job: Validate Configs (all branches)

Triggered on every push and pull request, runs in parallel with Lint & Unit Tests.

| Step | What it validates |
|---|---|
| `docker compose config --quiet` | `docker-compose.yml` is syntactically valid and all env substitutions resolve |
| `dbt deps` | dbt package dependencies resolve correctly |
| `dbt parse` | All dbt SQL models parse without syntax errors |
| `dbt compile` | All dbt models compile to valid SQL (Jinja rendered, refs resolved) |

### Job: Integration Tests (main branch only)

Triggered on push to `main` after both previous jobs pass.

| Step | What it validates |
|---|---|
| Start `postgres` + `clickhouse` | Services healthy before tests run |
| Run `ingestion` container | Ingestion completes successfully end-to-end |
| `pytest tests/integration/` | PostgreSQL schema, CDC publication, and data quality assertions against a live database |

### Workflow file

`.github/workflows/ci.yml`

To check CI status locally before pushing:

```bash
# Lint
ruff check ingestion/ orchestration/dags/ tests/

# Unit tests
pytest tests/unit/ -v

# Compose validation
docker compose config --quiet

# dbt validation
cd transform && dbt deps && dbt parse --profiles-dir . && dbt compile --profiles-dir .
```

---

## Stopping and Cleaning Up

```bash
# Stop all services (preserves data volumes)
docker compose down

# Stop and delete all data volumes (full reset)
docker compose down -v
```

---

## Troubleshooting

**Services stuck in `starting` or `unhealthy`**

Airflow and Debezium take the longest to initialise (~2 minutes). Check logs for a specific service:

```bash
docker compose logs --tail=50 airflow-webserver
docker compose logs --tail=50 debezium
```

**Debezium connector not registered**

The `connector-init` service runs once and exits. If it failed (e.g. Debezium was not ready), re-register manually:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-connector.json
```

**ClickHouse raw.loans is empty after ingestion**

Check that Debezium published CDC events to the Kafka topic:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic analytics.public.loans \
  --from-beginning --max-messages 3 --timeout-ms 10000
```

If the topic is empty, check the Debezium connector status. If it is populated, check the ClickHouse Kafka engine consumer group offset:

```bash
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group clickhouse_loans_consumer --describe
```

**dbt run fails with connection error**

Confirm ClickHouse is healthy and the Airflow environment variables are set:

```bash
docker compose exec airflow-webserver env | grep CLICKHOUSE
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass --query "SELECT 1"
```

**Airflow DAG not visible in the UI**

The DAG file is mounted via volume from `./orchestration/dags/`. Confirm the mount is correct:

```bash
docker compose exec airflow-webserver ls /opt/airflow/dags/
```

If the file is present but the DAG shows an import error, check the Airflow scheduler logs:

```bash
docker compose logs --tail=50 airflow-scheduler | grep ERROR
```
