import sys
import os
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../ingestion"))

from ingest import (
    parse_project, to_amount, to_date,
    fetch_projects, upsert_projects, _ingest_all_batches, _validate_env,
    push_metrics, _build_registry,
    BATCH_SIZE,
)


FULL_PROJECT = {
    "id": "P505244",
    "project_name": "Boosting Green Finance in Rwanda",
    "countryname": ["Republic of Rwanda"],
    "countrycode": ["RW"],
    "countryshortname": "Rwanda",
    "regionname": "Eastern and Southern Africa",
    "projectstatusdisplay": "Active",
    "lendinginstr": "Development Policy Lending",
    "curr_total_commitment": 200,
    "ibrdcommamt": "200,000,000",
    "idacommamt": "0",
    "lendprojectcost": "200,000,000",
    "boardapprovaldate": "2024-12-20T00:00:00Z",
    "closingdate": "12/20/2025 12:00:00 AM",
    "approvalfy": 2025,
    "source": ["IBRD"],
}

MINIMAL_PROJECT = {
    "id": "P123456",
    "project_name": "Kenya Transport",
    "countryshortname": "Kenya",
    "regionname": "Eastern and Southern Africa",
    "projectstatusdisplay": "Closed",
}


class TestParseProject:
    def test_parses_full_project(self):
        p = parse_project(FULL_PROJECT)
        assert p["project_id"] == "P505244"
        assert p["country_code"] == "RW"
        assert p["country"] == "Republic of Rwanda"
        assert p["status"] == "Active"
        assert p["ibrd_commitment_usd"] == 200_000_000.0
        assert p["approval_fy"] == 2025

    def test_board_approval_date_iso_format(self):
        p = parse_project(FULL_PROJECT)
        assert p["board_approval_date"] is not None
        assert str(p["board_approval_date"]) == "2024-12-20"

    def test_closing_date_us_format(self):
        p = parse_project(FULL_PROJECT)
        assert p["closing_date"] is not None
        assert str(p["closing_date"]) == "2025-12-20"

    def test_source_extracted_from_list(self):
        p = parse_project(FULL_PROJECT)
        assert p["source"] == "IBRD"

    def test_minimal_project_no_lists(self):
        p = parse_project(MINIMAL_PROJECT)
        assert p["project_id"] == "P123456"
        assert p["country"] == "Kenya"
        assert p["country_code"] is None
        assert p["ibrd_commitment_usd"] is None

    def test_missing_id_returns_none(self):
        assert parse_project({}) is None
        assert parse_project({"project_name": "No ID"}) is None

    def test_ingested_at_propagated(self):
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        p = parse_project(FULL_PROJECT, ingested_at=ts)
        assert p["updated_at"] == ts

    def test_ingested_at_defaults_to_now(self):
        before = datetime.now(timezone.utc)
        p = parse_project(FULL_PROJECT)
        after = datetime.now(timezone.utc)
        assert before <= p["updated_at"] <= after


class TestToAmount:
    def test_plain_number(self):
        assert to_amount(200) == 200.0

    def test_comma_formatted_string(self):
        assert to_amount("200,000,000") == 200_000_000.0

    def test_none_returns_none(self):
        assert to_amount(None) is None

    def test_empty_string_returns_none(self):
        assert to_amount("") is None


class TestToDate:
    def test_iso_format(self):
        d = to_date("2024-12-20T00:00:00Z")
        assert str(d) == "2024-12-20"

    def test_us_datetime_format(self):
        d = to_date("12/20/2025 12:00:00 AM")
        assert str(d) == "2025-12-20"

    def test_plain_date(self):
        d = to_date("2023-06-15")
        assert str(d) == "2023-06-15"

    def test_none_returns_none(self):
        assert to_date(None) is None

    def test_empty_returns_none(self):
        assert to_date("") is None


class TestValidateEnv:
    def test_raises_when_vars_missing(self, monkeypatch):
        for key in ("POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"):
            monkeypatch.delenv(key, raising=False)
        with pytest.raises(EnvironmentError, match="Missing required environment variables"):
            _validate_env()

    def test_passes_when_all_vars_present(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "pass")
        monkeypatch.setenv("POSTGRES_DB", "db")
        _validate_env()  # should not raise


class TestFetchProjects:
    def _make_response(self, records: list[dict], status_code: int = 200):
        resp = MagicMock()
        resp.status_code = status_code
        resp.content = b"data"
        resp.text = "data"
        resp.json.return_value = {"projects": {r["id"]: r for r in records}}
        resp.raise_for_status.return_value = None
        return resp

    def test_returns_list_of_projects(self):
        resp = self._make_response([FULL_PROJECT])
        with patch("ingest.requests.get", return_value=resp):
            result = fetch_projects(offset=0)
        assert len(result) == 1
        assert result[0]["id"] == "P505244"

    def test_empty_api_response_returns_empty_list(self):
        resp = MagicMock()
        resp.content = b""
        resp.text = ""
        resp.raise_for_status.return_value = None
        with patch("ingest.requests.get", return_value=resp):
            result = fetch_projects(offset=0)
        assert result == []

    def test_retries_on_5xx_then_succeeds(self):
        import requests as req
        err_resp = MagicMock()
        err_resp.status_code = 503
        http_err = req.HTTPError(response=err_resp)
        ok_resp = self._make_response([FULL_PROJECT])

        with patch("ingest.requests.get", side_effect=[
            req.ConnectionError("timeout"),
            ok_resp,
        ]), patch("ingest.time.sleep"):
            result = fetch_projects(offset=0)

        assert len(result) == 1

    def test_does_not_retry_on_4xx(self):
        import requests as req
        err_resp = MagicMock()
        err_resp.status_code = 404
        http_err = req.HTTPError(response=err_resp)
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = http_err

        with patch("ingest.requests.get", return_value=mock_resp), \
             patch("ingest.time.sleep") as mock_sleep:
            with pytest.raises(req.HTTPError):
                fetch_projects(offset=0)
        mock_sleep.assert_not_called()

    def test_raises_after_all_retries_exhausted(self):
        import requests as req
        with patch("ingest.requests.get", side_effect=req.ConnectionError("down")), \
             patch("ingest.time.sleep"):
            with pytest.raises(req.ConnectionError):
                fetch_projects(offset=0)


class TestUpsertProjects:
    def test_empty_list_returns_zero_without_db_call(self):
        conn = MagicMock()
        result = upsert_projects(conn, [])
        assert result == 0
        conn.cursor.assert_not_called()
        conn.commit.assert_not_called()

    def test_returns_count_of_upserted_rows(self):
        conn = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: MagicMock()
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        projects = [parse_project(FULL_PROJECT)]
        with patch("ingest.execute_values"):
            result = upsert_projects(conn, projects)
        assert result == 1
        conn.commit.assert_called_once()


class TestToAmountEdgeCases:
    def test_non_numeric_string_returns_none(self):
        assert to_amount("N/A") is None

    def test_list_input_returns_none(self):
        assert to_amount([]) is None


class TestPushMetrics:
    def test_skips_when_no_url(self, monkeypatch):
        monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)
        registry, *_ = _build_registry()
        with patch("ingest.push_to_gateway") as mock_push:
            push_metrics(registry)
        mock_push.assert_not_called()

    def test_calls_gateway_when_url_set(self, monkeypatch):
        monkeypatch.setenv("PUSHGATEWAY_URL", "http://fake-gw:9091")
        registry, *_ = _build_registry()
        with patch("ingest.push_to_gateway") as mock_push:
            push_metrics(registry)
        mock_push.assert_called_once_with("http://fake-gw:9091", job="wb_ingestion", registry=registry)


class TestIngestAllBatches:
    def _make_raw_batch(self, size: int, id_prefix: str = "P") -> list[dict]:
        return [{"id": f"{id_prefix}{i:06d}", "project_name": f"Project {i}"} for i in range(size)]

    def test_stops_on_empty_response(self):
        conn = MagicMock()
        error_counter = MagicMock()
        with patch("ingest.fetch_projects", return_value=[]), \
             patch("ingest.upsert_projects", return_value=0) as mock_upsert:
            total = _ingest_all_batches(conn, error_counter)
        assert total == 0
        mock_upsert.assert_not_called()

    def test_stops_when_batch_smaller_than_batch_size(self):
        partial = self._make_raw_batch(3)
        conn = MagicMock()
        error_counter = MagicMock()
        with patch("ingest.fetch_projects", return_value=partial), \
             patch("ingest.upsert_projects", return_value=3) as mock_upsert, \
             patch("ingest.time.sleep"):
            total = _ingest_all_batches(conn, error_counter)
        assert total == 3
        mock_upsert.assert_called_once()

    def test_accumulates_total_across_batches(self):
        full_batch = self._make_raw_batch(BATCH_SIZE, "A")
        partial_batch = self._make_raw_batch(2, "B")
        conn = MagicMock()
        error_counter = MagicMock()
        with patch("ingest.fetch_projects", side_effect=[full_batch, partial_batch]), \
             patch("ingest.upsert_projects", side_effect=[BATCH_SIZE, 2]) as mock_upsert, \
             patch("ingest.time.sleep"):
            total = _ingest_all_batches(conn, error_counter)
        assert total == BATCH_SIZE + 2
        assert mock_upsert.call_count == 2

    def test_reraises_http_error_and_increments_counter(self):
        import requests as req
        conn = MagicMock()
        error_counter = MagicMock()
        with patch("ingest.fetch_projects", side_effect=req.HTTPError(response=MagicMock(status_code=500))):
            with pytest.raises(req.HTTPError):
                _ingest_all_batches(conn, error_counter)
        error_counter.inc.assert_called_once()

    def test_reraises_db_error_and_increments_counter(self):
        import psycopg2
        conn = MagicMock()
        error_counter = MagicMock()
        batch = self._make_raw_batch(1)
        with patch("ingest.fetch_projects", return_value=batch), \
             patch("ingest.upsert_projects", side_effect=psycopg2.DatabaseError("constraint")):
            with pytest.raises(psycopg2.DatabaseError):
                _ingest_all_batches(conn, error_counter)
        error_counter.inc.assert_called_once()

    def test_batch_timestamp_is_shared_across_records(self):
        batch = self._make_raw_batch(3)
        conn = MagicMock()
        error_counter = MagicMock()
        captured = []

        def fake_upsert(conn, projects):
            captured.extend(projects)
            return len(projects)

        with patch("ingest.fetch_projects", return_value=batch), \
             patch("ingest.upsert_projects", side_effect=fake_upsert):
            _ingest_all_batches(conn, error_counter)

        timestamps = [p["updated_at"] for p in captured]
        assert len(set(timestamps)) == 1, "all records in a batch should share the same timestamp"
