import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../ingestion"))

from ingest import parse_project, to_amount, to_date


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
