"""Unit coverage for ingestion-time temporal coercion (GeoID #1333).

A property whose items-schema field is declared ``date`` / ``time`` /
``timestamp`` is normalised from common string representations to canonical
ISO-8601 during ingestion, because the typed write path accepts ISO-8601 but
not arbitrary date formats. These tests pin the two pure helpers directly;
full ``run_ingestion_task`` wiring is exercised by the integration suite.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from dynastore.tasks.ingestion.main_ingestion import (
    _coerce_temporal_value,
    apply_temporal_coercion,
)


# --------------------------------------------------------------------------
# _coerce_temporal_value
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value,expected",
    [
        ("2024-01-31T12:30:00", "2024-01-31T12:30:00"),
        ("2024-01-31 12:30:00", "2024-01-31T12:30:00"),
        ("Jan 31 2024 12:30", "2024-01-31T12:30:00"),
        ("31 December 2024 00:00", "2024-12-31T00:00:00"),
    ],
)
def test_coerce_timestamp_normalises_to_iso(value, expected):
    assert _coerce_temporal_value(value, "timestamp") == expected


def test_coerce_date_returns_date_only_iso():
    assert _coerce_temporal_value("31/12/2024", "date") == "2024-12-31"
    assert _coerce_temporal_value("2024-01-05T09:00:00", "date") == "2024-01-05"


def test_coerce_time_returns_time_only_iso():
    assert _coerce_temporal_value("2024-01-31T08:15:30", "time") == "08:15:30"


def test_coerce_month_first_default_for_ambiguous_numeric():
    # dateutil default is month-first; 01/02/2024 -> 2 Jan 2024.
    assert _coerce_temporal_value("01/02/2024", "date") == "2024-01-02"


def test_coerce_unparseable_string_is_unchanged():
    assert _coerce_temporal_value("not a date", "timestamp") == "not a date"
    assert _coerce_temporal_value("", "date") == ""
    assert _coerce_temporal_value("   ", "timestamp") == "   "


def test_coerce_non_string_is_unchanged():
    dt = datetime(2024, 1, 31, 12, 0, 0)
    assert _coerce_temporal_value(dt, "timestamp") is dt
    assert _coerce_temporal_value(None, "date") is None
    assert _coerce_temporal_value(42, "timestamp") == 42


# --------------------------------------------------------------------------
# apply_temporal_coercion
# --------------------------------------------------------------------------

def test_apply_coerces_only_declared_temporal_fields():
    props = {
        "start": "31/12/2024",
        "name": "site-a",
        "count": "01/02/2024",  # a string column that merely looks like a date
    }
    out = apply_temporal_coercion(props, {"start": "date"})
    assert out["start"] == "2024-12-31"
    assert out["name"] == "site-a"
    assert out["count"] == "01/02/2024"  # not declared temporal -> untouched


def test_apply_is_in_place_and_returns_same_dict():
    props = {"ts": "2024-01-31 00:00:00"}
    out = apply_temporal_coercion(props, {"ts": "timestamp"})
    assert out is props
    assert props["ts"] == "2024-01-31T00:00:00"


def test_apply_empty_temporal_fields_is_noop():
    props = {"start": "31/12/2024"}
    out = apply_temporal_coercion(props, {})
    assert out is props
    assert props["start"] == "31/12/2024"


def test_apply_tolerates_declared_field_absent_from_properties():
    props = {"other": "x"}
    # 'start' is declared temporal but not present in this row -> no error.
    out = apply_temporal_coercion(props, {"start": "date", "other": "string"})
    assert out == {"other": "x"}


def test_apply_leaves_unparseable_temporal_value_for_downstream_rejection():
    props = {"start": "garbage"}
    apply_temporal_coercion(props, {"start": "timestamp"})
    # Unparseable values pass through unchanged; the typed write rejects the
    # row via the 207 IngestionReport rather than failing the whole batch.
    assert props["start"] == "garbage"
