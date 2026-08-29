import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import requests
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# World Bank Projects API — public, no auth required
WB_PROJECTS_URL = "https://search.worldbank.org/api/v2/projects"
BATCH_SIZE = 500
MAX_BATCHES = 20
HEADERS = {"User-Agent": "wb-analytics-pipeline/1.0 (data engineering assessment)"}

# Throttle requests to stay within World Bank API rate limits
API_RATE_LIMIT_SLEEP_SECONDS = 0.3

_REQUIRED_ENV = ("POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
_RETRY_BACKOFF = (1, 3, 9)  # seconds between successive fetch attempts


def _validate_env() -> None:
    missing = [k for k in _REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        raise OSError(f"Missing required environment variables: {missing}")


def get_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def fetch_projects(offset: int = 0) -> list[dict]:
    params = {"format": "json", "rows": BATCH_SIZE, "os": offset}
    last_exc: Exception | None = None
    resp = None
    for attempt, wait in enumerate((*_RETRY_BACKOFF, None), start=1):
        try:
            resp = requests.get(WB_PROJECTS_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            break
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code < 500:
                raise  # don't retry 4xx client errors
            last_exc = exc
        if wait is None:
            raise last_exc  # type: ignore[misc]
        log.warning("fetch attempt %d failed (offset=%d): %s — retrying in %ds",
                    attempt, offset, last_exc, wait)
        time.sleep(wait)
    assert resp is not None
    if not resp.content or not resp.text.strip():
        return []
    data = resp.json()
    # API returns projects as a dict keyed by project ID, not a list
    return list(data.get("projects", {}).values())


def to_amount(val) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def to_date(val):
    if not val:
        return None
    val = str(val).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(val, fmt).date()  # noqa: DTZ007
        except ValueError:
            continue
    return None


def parse_project(raw: dict, ingested_at: datetime | None = None) -> dict | None:
    project_id = raw.get("id")
    if not project_id:
        return None

    countries = raw.get("countryname", [])
    codes = raw.get("countrycode", [])
    sources = raw.get("source", [])

    return {
        "project_id":            project_id,
        "project_name":          raw.get("project_name"),
        "country":               (countries[0] if isinstance(countries, list) and countries
                                  else raw.get("countryshortname")),
        "country_code":          (codes[0] if isinstance(codes, list) and codes else None),
        "region":                raw.get("regionname"),
        "status":                raw.get("projectstatusdisplay") or raw.get("status"),
        "lending_instrument":    raw.get("lendinginstr"),
        "total_commitment_usd":  to_amount(raw.get("curr_total_commitment")),
        "ibrd_commitment_usd":   to_amount(raw.get("ibrdcommamt")),
        "ida_commitment_usd":    to_amount(raw.get("idacommamt")),
        "total_project_cost_usd": to_amount(raw.get("lendprojectcost")),
        "board_approval_date":   to_date(raw.get("boardapprovaldate")),
        "closing_date":          to_date(raw.get("closingdate")),
        "approval_fy":           raw.get("approvalfy"),
        "source":                (sources[0] if isinstance(sources, list) and sources else None),
        "updated_at":            ingested_at or datetime.now(timezone.utc),
    }


# Only mutable fields are in DO UPDATE SET; immutable fields (project_name, country,
# region, approval_fy, etc.) are set on first insert only and never overwritten.
_UPSERT_COLUMNS = [
    "project_id", "project_name", "country", "country_code", "region",
    "status", "lending_instrument", "total_commitment_usd", "ibrd_commitment_usd",
    "ida_commitment_usd", "total_project_cost_usd", "board_approval_date",
    "closing_date", "approval_fy", "source", "updated_at",
]

_UPSERT_SQL = """
    INSERT INTO loans (
        project_id, project_name, country, country_code, region,
        status, lending_instrument, total_commitment_usd, ibrd_commitment_usd,
        ida_commitment_usd, total_project_cost_usd, board_approval_date,
        closing_date, approval_fy, source, updated_at
    )
    VALUES %s
    ON CONFLICT (project_id) DO UPDATE SET
        status                  = EXCLUDED.status,
        total_commitment_usd    = EXCLUDED.total_commitment_usd,
        ibrd_commitment_usd     = EXCLUDED.ibrd_commitment_usd,
        ida_commitment_usd      = EXCLUDED.ida_commitment_usd,
        closing_date            = EXCLUDED.closing_date,
        updated_at              = EXCLUDED.updated_at
"""


def upsert_projects(conn, projects: list[dict]) -> int:
    if not projects:
        return 0
    rows = [tuple(p[c] for c in _UPSERT_COLUMNS) for p in projects]
    with conn.cursor() as cur:
        execute_values(cur, _UPSERT_SQL, rows)
    conn.commit()
    return len(rows)


def push_metrics(registry: CollectorRegistry) -> None:
    url = os.environ.get("PUSHGATEWAY_URL")
    if not url:
        log.warning("PUSHGATEWAY_URL not set — skipping metrics push")
        return
    push_to_gateway(url, job="wb_ingestion", registry=registry)
    log.info("Metrics pushed to %s", url)


def _build_registry() -> tuple[CollectorRegistry, Gauge, Gauge, Counter]:
    registry = CollectorRegistry()
    loans_gauge    = Gauge("wb_loans_ingested_count",       "Projects ingested in this run", registry=registry)
    duration_gauge = Gauge("wb_ingestion_duration_seconds", "Ingestion run duration",        registry=registry)
    error_counter  = Counter("wb_ingestion_errors_total",   "Ingestion errors",              registry=registry)
    return registry, loans_gauge, duration_gauge, error_counter


def _ingest_all_batches(conn, error_counter: Counter) -> int:
    total = 0
    for batch in range(MAX_BATCHES):
        offset = batch * BATCH_SIZE
        try:
            raw_records = fetch_projects(offset=offset)
            if not raw_records:
                log.info("No more records at offset %d — done.", offset)
                break

            batch_ts = datetime.now(timezone.utc)
            parsed = [p for r in raw_records if (p := parse_project(r, ingested_at=batch_ts)) is not None]
            upsert_projects(conn, parsed)
            total += len(parsed)
            log.info("Batch %d: %d projects (total=%d)", batch + 1, len(parsed), total)

            if len(raw_records) < BATCH_SIZE:
                break

            time.sleep(API_RATE_LIMIT_SLEEP_SECONDS)

        except requests.HTTPError as exc:
            log.error("Batch %d HTTP error (offset=%d): %s", batch + 1, offset, exc)
            error_counter.inc()
            raise
        except psycopg2.DatabaseError as exc:
            log.error("Batch %d DB error (offset=%d): %s", batch + 1, offset, exc)
            error_counter.inc()
            raise

    return total


def run() -> int:
    _validate_env()
    registry, loans_gauge, duration_gauge, error_counter = _build_registry()
    start = time.time()

    with get_conn() as conn:
        total = _ingest_all_batches(conn, error_counter)

    duration = time.time() - start
    loans_gauge.set(total)
    duration_gauge.set(duration)

    try:
        push_metrics(registry)
    except Exception as exc:  # noqa: BLE001
        log.warning("Metrics push failed: %s", exc)

    log.info("Ingestion complete. projects=%d duration=%.1fs", total, duration)
    return total


if __name__ == "__main__":
    run()
