# Kiva Microfinance Analytics Pipeline

End-to-end data engineering pipeline ingesting [Kiva.org](https://www.kiva.org) microfinance loan data through a full CDC → OLAP → analytics stack.

**Data source:** Kiva public REST API (`api.kivaws.org/v1`) — no authentication required.

---

## Architecture

```
Kiva REST API
    │
    ▼ (Python ingestion)
PostgreSQL 15  ─── WAL logical replication ──► Debezium 2.6
    │                                               │
    │                                               ▼
    │                                         Kafka topics
    │                                    analytics.public.loans
    │                                    analytics.public.borrowers
    │                                               │
    │                                               ▼ (Kafka engine + MV)
    │                                         ClickHouse raw.*
    │                                               │
    │                                               ▼ (dbt-clickhouse)
    │                                    staging.stg_loans
    │                                    staging.stg_borrowers
    │                                               │
    │                                               ▼
    │                                    mart.mart_loan_performance
    │                                    mart.mart_gender_analysis
    │                                    mart.mart_sector_summary
    │
    └── Orchestration: Airflow (hourly DAG)
    └── Observability: Prometheus + Grafana
```

---

## Quickstart

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env — generate AIRFLOW_FERNET_KEY with:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 2. Start the full stack
docker compose up -d

# 3. Verify all services are healthy
docker compose ps
```

The ingestion container runs automatically on startup. The Airflow DAG (`kiva_pipeline`) then runs hourly to refresh data and re-run transformations.

---

## Validating Data at Each Stage

### 1. PostgreSQL (OLTP)
```bash
docker compose exec postgres psql -U dataeng -d analytics_db \
  -c "SELECT status, count(*) FROM loans GROUP BY status;"
```

### 2. Kafka (CDC events)
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic analytics.public.loans \
  --from-beginning --max-messages 5
```

### 3. ClickHouse raw layer
```bash
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT count(), max(_ingested_at) FROM raw.loans"
```

### 4. dbt staging & mart
```bash
docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT country_code, count() FROM staging.stg_loans GROUP BY country_code ORDER BY 2 DESC LIMIT 10"

docker compose exec clickhouse clickhouse-client \
  --user default --password clickhouse_pass \
  --query "SELECT * FROM mart.mart_loan_performance LIMIT 5"
```

---

## Service Access

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` from `.env` |
| Grafana | http://localhost:3000 | `GRAFANA_USER` / `GRAFANA_PASSWORD` from `.env` |
| Prometheus | http://localhost:9090 | — |
| ClickHouse HTTP | http://localhost:8123 | `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` |
| Kafka Connect | http://localhost:8083 | — |

---

## Running Tests

```bash
# Unit tests (no infrastructure needed)
pip install -r tests/requirements.txt
pytest tests/unit/ -v

# Integration tests (requires running stack)
pytest tests/integration/ -v \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_USER=dataeng \
  -e POSTGRES_PASSWORD=dataeng_pass \
  -e POSTGRES_DB=analytics_db
```

---

## CI/CD

GitHub Actions (`.github/workflows/ci.yml`) runs on every push and pull request:

| Step | Trigger |
|---|---|
| Ruff lint | All branches |
| Unit tests (pytest) | All branches |
| `docker compose config` validation | All branches |
| `dbt parse` + `dbt compile` | All branches |
| Integration tests | `main` branch only |

---

## Debezium Connector

The connector is registered automatically by the `connector-init` service on startup. To check status or re-register manually:

```bash
# Check status
curl http://localhost:8083/connectors/postgres-connector/status | jq

# Re-register (if needed)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/postgres-connector.json
```

---

## ClickHouse Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Table engine (raw) | `ReplacingMergeTree(updated_at)` | Handles CDC upserts; deduplicates on merge using latest `updated_at` |
| Table engine (mart aggregations) | `SummingMergeTree` | Efficient incremental aggregation without full re-scans |
| Partitioning | `toYYYYMM(posted_date)` | Enables partition pruning for date-range queries |
| CDC ingestion | Kafka engine + Materialized View | Decouples consumption from writes; ClickHouse pulls at its own pace |
| Delete handling | `_deleted` flag + `FINAL` in staging | Avoids hard deletes; `FINAL` forces deduplication at query time |
