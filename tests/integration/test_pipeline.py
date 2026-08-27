"""
Integration tests — run against the live docker compose stack.
Requires: docker compose up -d postgres clickhouse
"""
import os
import pytest
import psycopg2


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


def test_borrowers_table_exists(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'borrowers'"
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


def test_no_loans_with_null_id(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM loans WHERE id IS NULL")
        assert cur.fetchone()[0] == 0


def test_no_loans_with_zero_loan_amount(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM loans WHERE loan_amount <= 0")
        assert cur.fetchone()[0] == 0


def test_borrowers_reference_valid_loans(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM borrowers b
            LEFT JOIN loans l ON b.loan_id = l.id
            WHERE l.id IS NULL
        """)
        orphans = cur.fetchone()[0]
    assert orphans == 0, f"Found {orphans} borrowers with no matching loan"
