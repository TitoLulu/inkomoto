"""
Integration tests — run against the live docker compose stack.
Requires: docker compose up -d postgres clickhouse
"""
import os

import psycopg2
import pytest

POSTGRES_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "user": os.environ.get("POSTGRES_USER", "dataeng"),
    "password": os.environ.get("POSTGRES_PASSWORD", "dataeng_pass"),
    "dbname": os.environ.get("POSTGRES_DB", "analytics_db"),
}


@pytest.fixture(scope="module")
def pg_conn():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    yield conn
    conn.close()


def test_loans_table_exists(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'loans'"
        )
        assert cur.fetchone() is not None


def test_debezium_publication_exists(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'"
        )
        assert cur.fetchone() is not None, "Debezium publication not found"


def test_loans_have_data_after_ingestion(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM loans")
        count = cur.fetchone()[0]
    assert count > 0, "No loans found — has ingestion run?"


def test_no_loans_with_null_project_id(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM loans WHERE project_id IS NULL")
        assert cur.fetchone()[0] == 0


def test_no_loans_with_negative_commitment(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM loans WHERE total_commitment_usd < 0"
        )
        assert cur.fetchone()[0] == 0
