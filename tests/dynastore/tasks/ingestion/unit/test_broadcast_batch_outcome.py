"""Regression coverage for dynastore#263: detailed-report empty array.

Pre-fix, ``run_ingestion_task`` called every ``ReportingInterface`` hook
except ``process_batch_outcome``, so ``GcsDetailedReporter``'s buffer
stayed empty and the uploaded report was the literal ``[]``. The fix
introduces ``_broadcast_batch_outcome`` and wires it after each successful
upsert flush. These tests pin the helper's contract directly — full
``run_ingestion_task`` is exercised by the integration suite.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

import dynastore.models.protocols  # noqa: F401  (pre-warm registration chain)

from dynastore.tasks.ingestion.main_ingestion import (
    _broadcast_batch_outcome,
    _enrich_report_record,
)


class _RecordingReporter:
    """Captures every ``process_batch_outcome`` call for assertion."""

    def __init__(self) -> None:
        self.calls: List[List[Dict[str, Any]]] = []

    async def process_batch_outcome(self, batch_results: List[Dict[str, Any]]) -> None:
        self.calls.append(batch_results)


@pytest.mark.asyncio
async def test_broadcast_synthesizes_success_outcome_per_input_row():
    """One outcome per input row, all SUCCESS, ``record`` field populated."""
    batch = [{"id": "a", "geometry": None}, {"id": "b", "geometry": None}]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome([reporter], batch, upsert_result=None)

    assert len(reporter.calls) == 1
    outcomes = reporter.calls[0]
    assert len(outcomes) == len(batch)
    for outcome in outcomes:
        assert outcome["status"] == "SUCCESS"
        assert outcome["message"] is None
        assert "record" in outcome


@pytest.mark.asyncio
async def test_broadcast_prefers_upsert_result_as_record_when_aligned():
    """When upsert returns a list aligned with the batch, those items become
    the canonical ``record`` (carries server-assigned fields like item_id)."""
    batch = [{"id": "a"}, {"id": "b"}]
    upsert_result = [
        {"id": "a", "item_id": "uuid-a"},
        {"id": "b", "item_id": "uuid-b"},
    ]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome([reporter], batch, upsert_result)

    outcomes = reporter.calls[0]
    assert outcomes[0]["record"] == {"id": "a", "item_id": "uuid-a"}
    assert outcomes[1]["record"] == {"id": "b", "item_id": "uuid-b"}


@pytest.mark.asyncio
async def test_broadcast_falls_back_to_input_when_upsert_result_shape_mismatch():
    """If upsert returns a single dict, a different-length list, or anything
    non-aligned, drop back to the input feature so ``record`` is still
    populated rather than mis-zipped."""
    batch = [{"id": "a"}, {"id": "b"}]

    for bad_result in (None, {"id": "a"}, [{"id": "only-one"}], "garbage"):
        reporter = _RecordingReporter()
        await _broadcast_batch_outcome([reporter], batch, bad_result)
        outcomes = reporter.calls[0]
        assert [o["record"] for o in outcomes] == batch, (
            f"fallback failed for upsert_result={bad_result!r}"
        )


@pytest.mark.asyncio
async def test_broadcast_fans_out_to_every_reporter():
    """All reporters in the list receive the same payload — this was the
    root gap in #263: ``GcsDetailedReporter`` was registered but never
    invoked, so the buffered file stayed empty."""
    batch = [{"id": "a"}]
    r1 = _RecordingReporter()
    r2 = _RecordingReporter()
    r3 = _RecordingReporter()

    await _broadcast_batch_outcome([r1, r2, r3], batch, upsert_result=batch)

    for reporter in (r1, r2, r3):
        assert len(reporter.calls) == 1
        assert reporter.calls[0][0]["status"] == "SUCCESS"


@pytest.mark.asyncio
async def test_broadcast_with_empty_reporter_list_is_a_noop():
    """Reporter list may be empty when the task request omits ``reporting``."""
    await _broadcast_batch_outcome([], [{"id": "a"}], upsert_result=None)


# --- generated-info enrichment (geoid / external_id / asset_id / statistics) --


def test_enrich_stamps_identity_at_top_level_and_stats_into_properties():
    record = {"type": "Feature", "id": "geoid-a", "properties": {"NAME": "Friuli"}}
    generated = {
        "geoid": "geoid-a",
        "external_id": "1621",
        "asset_id": "ITAL1_01",
        "stats": {"area": 7845.0, "perimeter": 412.3},
    }
    out = _enrich_report_record(record, generated)
    assert out["geoid"] == "geoid-a"
    assert out["external_id"] == "1621"
    assert out["asset_id"] == "ITAL1_01"
    # generated statistics land under properties (the calculated attributes),
    # alongside the real attribute, which wins on a name clash.
    assert out["properties"] == {"NAME": "Friuli", "area": 7845.0, "perimeter": 412.3}


def test_enrich_omits_absent_identity_keys():
    """external_id / asset_id only appear when the ingestion produced them."""
    out = _enrich_report_record(
        {"properties": {}},
        {"geoid": "g", "external_id": None, "asset_id": None, "stats": {}},
    )
    assert out["geoid"] == "g"
    assert "external_id" not in out
    assert "asset_id" not in out


def test_enrich_does_not_overwrite_real_attribute_with_stat():
    out = _enrich_report_record(
        {"properties": {"area": "surveyed"}},
        {"geoid": "g", "stats": {"area": 7845.0}},
    )
    assert out["properties"]["area"] == "surveyed"


@pytest.mark.asyncio
async def test_broadcast_applies_generated_when_aligned_with_result():
    batch = [{"id": "1621"}, {"id": "1622"}]
    upsert_result = [
        {"type": "Feature", "id": "geo-1", "properties": {"NAME": "A"}},
        {"type": "Feature", "id": "geo-2", "properties": {"NAME": "B"}},
    ]
    generated = [
        {"geoid": "geo-1", "external_id": "1621", "asset_id": "X", "stats": {"area": 1.0}},
        {"geoid": "geo-2", "external_id": "1622", "asset_id": "X", "stats": {"area": 2.0}},
    ]
    reporter = _RecordingReporter()

    await _broadcast_batch_outcome([reporter], batch, upsert_result, generated)

    recs = [o["record"] for o in reporter.calls[0]]
    assert recs[0]["geoid"] == "geo-1"
    assert recs[0]["external_id"] == "1621"
    assert recs[0]["properties"] == {"NAME": "A", "area": 1.0}
    assert recs[1]["properties"] == {"NAME": "B", "area": 2.0}


@pytest.mark.asyncio
async def test_broadcast_drops_generated_on_input_batch_fallback():
    """Generated stats align with the upsert result, not the input batch, so a
    shape mismatch (which falls back to the input batch) must not stamp them."""
    batch = [{"id": "1621"}, {"id": "1622"}]
    generated = [{"geoid": "geo-1", "stats": {"area": 1.0}}]  # wrong length anyway
    reporter = _RecordingReporter()

    # single-dict upsert_result → fallback to input batch
    await _broadcast_batch_outcome([reporter], batch, {"id": "1621"}, generated)

    recs = [o["record"] for o in reporter.calls[0]]
    assert recs == batch
    for rec in recs:
        assert "geoid" not in rec
