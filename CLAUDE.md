# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

This is a Senior Data Engineer assessment submission for Inkomoko. The goal is an end-to-end, production-grade data pipeline: REST API ingestion → PostgreSQL (OLTP) → Debezium CDC → ClickHouse (OLAP) → dbt transformations → analytics-ready mart layer, fully orchestrated, containerized, and observable.

The full stack must start with a single command: `docker compose up`.

## Commands

```bash
# Start the full stack
docker compose up -d

# Stop and clean up volumes
docker compose down -v

# Run ingestion manually (inside container or with venv active)
python ingestion/ingest.py

# Run dbt transformations
dbt run --project-dir transform/

# Run dbt tests
dbt test --project-dir transform/

# Run pytest unit/integration tests
pytest tests/ -v

# Run a single test file
pytest tests/test_ingestion.py -v

# Check Debezium connector status
curl http://localhost:8083/connectors/postgres-connector/status

# Register/update Debezium connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-connector.json
```

## Architecture

### Data Flow

```
Public REST API
    ↓ (Python ingestion scripts)
PostgreSQL (OLTP source)
    ↓ (Debezium CDC → Kafka topics)
ClickHouse (OLAP, raw/staging tables via Kafka engine or ClickHouse Kafka integration)
    ↓ (dbt models)
ClickHouse staging layer  →  ClickHouse mart layer
    ↓
Orchestrator (Airflow/Dagster DAGs)
```

### Services (docker-compose.yml)

| Service | Purpose | Default Port |
|---|---|---|
| `postgres` | OLTP source DB, WAL-enabled for CDC | 5432 |
| `zookeeper` | Kafka dependency | 2181 |
| `kafka` | CDC event streaming | 9092 |
| `debezium` | Kafka Connect + Debezium connector | 8083 |
| `clickhouse` | OLAP database | 8123 / 9000 |
| `airflow` or `dagster` | Pipeline orchestration | 8080 |
| `prometheus` | Metrics collection | 9090 |
| `grafana` | Metrics dashboards | 3000 |
| `ingestion` | Python ingest job container | — |

### Repository Structure (target layout)

```
├── docker-compose.yml
├── .env.example                  # env template — never commit .env
├── ingestion/
│   ├── ingest.py                 # REST API → PostgreSQL
│   └── Dockerfile
├── connectors/
│   └── postgres-connector.json   # Debezium connector config
├── transform/                    # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/              # cleaned, typed, renamed
│   │   └── mart/                 # analytics-ready aggregations
│   └── tests/                    # dbt data tests
├── orchestration/
│   └── dags/                     # Airflow DAGs (or Dagster jobs)
├── observability/
│   ├── prometheus.yml
│   └── grafana/
│       └── dashboards/
├── tests/                        # pytest unit + integration tests
├── .github/
│   └── workflows/
│       └── ci.yml                # GitHub Actions CI/CD
└── CLAUDE.md
```

## Key Design Decisions

### PostgreSQL
- Must enable logical replication (`wal_level=logical`) for Debezium CDC to work.
- Set in `docker-compose.yml` via `command: postgres -c wal_level=logical`.

### Debezium → ClickHouse
- Debezium publishes CDC events to Kafka topics (one topic per table).
- ClickHouse consumes via the **Kafka table engine** + a **Materialized View** that writes into a `ReplacingMergeTree` destination table — this handles upserts from CDC `INSERT/UPDATE/DELETE` events.
- The `_sign` and `_version` columns (from Debezium's envelope) drive deduplication in `ReplacingMergeTree`.

### ClickHouse Table Design
- **Raw/Staging tables**: `ReplacingMergeTree` ordered by primary key; partitioned by date.
- **Mart tables**: Pre-aggregated using `SummingMergeTree` or `AggregatingMergeTree` where applicable; or `ReplacingMergeTree` for dimension tables.
- Avoid `JOIN`s at query time for large fact tables — denormalize into mart models.

### dbt on ClickHouse
- Use `dbt-clickhouse` adapter.
- Staging models: source from the raw Kafka-consumed tables; apply casts, null checks, rename columns to snake_case.
- Mart models: materialized as `table` (not `view`) for query performance.
- dbt tests cover `not_null`, `unique`, `accepted_values`, and custom row-count reconciliation tests.

### Orchestration
- Single DAG/job covers: trigger ingestion → wait for CDC lag to settle → run `dbt run` → run `dbt test` → emit pipeline health metric to Prometheus pushgateway.
- Schedule: run ingestion on a cron interval; CDC is continuous/streaming.

### Observability
- **Prometheus** scrapes: Kafka JMX exporter (consumer lag), Debezium Connect JMX, ClickHouse `system.metrics`, Airflow StatsD exporter, custom pipeline metrics from a Pushgateway.
- **Grafana** dashboards: pipeline run status, CDC consumer lag, ClickHouse insert rate, dbt test pass/fail, data freshness (max `updated_at` in mart vs wall clock).

### CI/CD (GitHub Actions)
- On every PR: lint Python (`ruff`), run `pytest tests/`, run `dbt parse` + `dbt compile` to validate SQL syntax, run `docker compose config` to validate compose file.
- On merge to main: build and push Docker images, run integration tests against ephemeral compose stack, deploy connector configs.
