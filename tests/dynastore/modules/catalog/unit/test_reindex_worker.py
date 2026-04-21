"""Unit tests for the M3.1 ``ReindexWorker`` scaffold.

Covers:

- ``handle_batch`` dispatches one ``_handle_one`` per event; non-
  ``catalog_metadata_changed`` events are ACK'd straight through.
- Hydration failure NACKs the event (should_retry=True).
- Indexer-resolution failure NACKs.
- No Indexers configured → the event ACKs cleanly.
- Successful driver dispatch ACKs.
- SLA policy translation: ``fail`` → NACK, ``degrade`` → ACK with
  recorded error, ``skip`` → clean ACK, no SLA → default ``fail``.
- Timeout path uses ``asyncio.wait_for``.
- Delete / soft_delete operations dispatch to ``delete_catalog_metadata``.
- Upsert on a deleted catalog (envelope=None) degrades to
  ``delete_catalog_metadata`` so the Indexer stays in sync.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.protocols.driver_roles import DriverSla
from dynastore.modules.catalog.reindex_worker import (
    ReindexWorker,
    _apply_sla_policy,
    _resolve_entry_sla,
)
from dynastore.modules.storage.routing_config import OperationDriverEntry


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_event(
    *, event_id="e-1", payload=None, event_type="catalog_metadata_changed",
):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "payload": payload or {
            "catalog_id": "cat-42", "operation": "upsert", "domain": "core",
        },
    }


def _make_driver(name: str, *, sla: "DriverSla | None" = None):
    d = MagicMock()
    d.upsert_catalog_metadata = AsyncMock()
    d.delete_catalog_metadata = AsyncMock()
    # Expose the class-level name via type(driver).__name__ — not used
    # by the worker itself but matches the production discovery shape.
    type(d).__name__ = name
    d.sla = sla
    return d


# ---------------------------------------------------------------------------
# handle_batch routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_catalog_metadata_events_are_acked_untouched():
    """The worker only owns catalog_metadata_changed; everything else ACKs."""
    # Zero-indexer worker so we don't even dispatch for other events.
    worker = ReindexWorker(
        resolve_indexers=lambda: [],
        get_catalog_metadata=AsyncMock(return_value={}),
    )
    other_event = _make_event(event_id="e-x", event_type="item_update")
    [result] = await worker.handle_batch([other_event])
    assert result.succeeded is True
    assert result.should_retry is False
    assert result.errors == []


@pytest.mark.asyncio
async def test_missing_catalog_id_is_treated_as_unrecoverable_ack():
    """An event without a catalog_id can never be retried — ACK it."""
    worker = ReindexWorker(
        resolve_indexers=lambda: [],
        get_catalog_metadata=AsyncMock(return_value={}),
    )
    event = _make_event(payload={"operation": "upsert"})  # no catalog_id
    [result] = await worker.handle_batch([event])
    assert result.succeeded is True
    assert result.should_retry is False
    assert "missing catalog_id" in result.errors


@pytest.mark.asyncio
async def test_no_indexers_configured_acks_event():
    """Zero Indexers → nothing to do → ACK (not NACK)."""
    worker = ReindexWorker(
        resolve_indexers=lambda: [],
        get_catalog_metadata=AsyncMock(return_value={"title": {"en": "T"}}),
    )
    [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is True
    assert result.should_retry is False


@pytest.mark.asyncio
async def test_indexer_resolution_failure_nacks_for_retry():
    """If the resolver itself raises, we don't know what to dispatch → retry."""
    def _broken_resolver():
        raise RuntimeError("config service down")

    worker = ReindexWorker(
        resolve_indexers=_broken_resolver,
        get_catalog_metadata=AsyncMock(return_value={}),
    )
    [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is False
    assert result.should_retry is True
    assert any("indexer resolution failed" in e for e in result.errors)


@pytest.mark.asyncio
async def test_hydrate_failure_nacks_for_retry():
    """Router hydration failing is transient — retry so the Indexer catches up."""
    driver = _make_driver("FakeIndexer")
    entry = OperationDriverEntry(driver_id="FakeIndexer")
    hydrate = AsyncMock(side_effect=RuntimeError("pg connection reset"))

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=hydrate,
    )
    [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is False
    assert result.should_retry is True
    assert any("hydrate failed" in e for e in result.errors)
    driver.upsert_catalog_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_successful_upsert_dispatch_acks_event():
    driver = _make_driver("FakeIndexer")
    entry = OperationDriverEntry(driver_id="FakeIndexer")
    envelope = {"title": {"en": "T"}, "stac_version": "1.1.0"}

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value=envelope),
    )
    [result] = await worker.handle_batch([_make_event()])

    assert result.succeeded is True
    assert result.should_retry is False
    driver.upsert_catalog_metadata.assert_awaited_once_with(
        "cat-42", envelope, db_resource=None,
    )


@pytest.mark.asyncio
async def test_delete_event_dispatches_hard_delete():
    driver = _make_driver("FakeIndexer")
    entry = OperationDriverEntry(driver_id="FakeIndexer")
    event = _make_event(payload={"catalog_id": "cat-42", "operation": "delete"})

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value={}),
    )
    [result] = await worker.handle_batch([event])
    assert result.succeeded is True
    driver.delete_catalog_metadata.assert_awaited_once_with(
        "cat-42", soft=False, db_resource=None,
    )
    driver.upsert_catalog_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_soft_delete_event_dispatches_soft_delete():
    driver = _make_driver("FakeIndexer")
    entry = OperationDriverEntry(driver_id="FakeIndexer")
    event = _make_event(
        payload={"catalog_id": "cat-42", "operation": "soft_delete"},
    )

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value={}),
    )
    await worker.handle_batch([event])
    driver.delete_catalog_metadata.assert_awaited_once_with(
        "cat-42", soft=True, db_resource=None,
    )


@pytest.mark.asyncio
async def test_upsert_with_none_envelope_falls_through_to_delete():
    """Primary says the catalog is gone → Indexer deletes to stay in sync."""
    driver = _make_driver("FakeIndexer")
    entry = OperationDriverEntry(driver_id="FakeIndexer")

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value=None),
    )
    [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is True
    driver.delete_catalog_metadata.assert_awaited_once_with(
        "cat-42", soft=False, db_resource=None,
    )
    driver.upsert_catalog_metadata.assert_not_awaited()


# ---------------------------------------------------------------------------
# SLA policy translation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fail_policy_on_driver_exception_nacks(caplog):
    """Default SLA (on_timeout=fail) → driver raise bubbles as NACK."""
    driver = _make_driver("FakeIndexer")
    driver.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("ES cluster red"),
    )
    entry = OperationDriverEntry(
        driver_id="FakeIndexer",
        sla=DriverSla(timeout_ms=1000, on_timeout="fail"),
    )

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value={"title": "T"}),
    )
    with caplog.at_level(logging.ERROR):
        [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is False
    assert result.should_retry is True
    assert any("ES cluster red" in e for e in result.errors)


@pytest.mark.asyncio
async def test_degrade_policy_acks_with_recorded_error(caplog):
    """on_timeout=degrade: driver failure ACKs the event with a warning."""
    driver = _make_driver("FakeIndexer")
    driver.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("ES quota exceeded"),
    )
    entry = OperationDriverEntry(
        driver_id="FakeIndexer",
        sla=DriverSla(timeout_ms=1000, on_timeout="degrade"),
    )

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value={"title": "T"}),
    )
    with caplog.at_level(logging.WARNING):
        [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is True
    assert result.should_retry is False
    assert any("ES quota exceeded" in e for e in result.errors)


@pytest.mark.asyncio
async def test_skip_policy_treats_failure_as_clean_success(caplog):
    """on_timeout=skip: driver failure yields a clean ACK, no errors recorded."""
    driver = _make_driver("FakeIndexer")
    driver.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("sink disabled"),
    )
    entry = OperationDriverEntry(
        driver_id="FakeIndexer",
        sla=DriverSla(timeout_ms=1000, on_timeout="skip"),
    )

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value={"title": "T"}),
    )
    [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is True
    assert result.should_retry is False
    assert result.errors == []


@pytest.mark.asyncio
async def test_timeout_triggers_sla_policy():
    """Slow driver gets interrupted by asyncio.wait_for and SLA policy runs."""
    driver = _make_driver("SlowIndexer")

    async def _slow(*args, **kwargs):
        await asyncio.sleep(10.0)   # far beyond the 50ms SLA

    driver.upsert_catalog_metadata = AsyncMock(side_effect=_slow)
    entry = OperationDriverEntry(
        driver_id="SlowIndexer",
        sla=DriverSla(timeout_ms=50, on_timeout="degrade"),
    )

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value={"title": "T"}),
    )
    [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is True  # degrade → ACK
    assert any("timeout after 50ms" in e for e in result.errors)


@pytest.mark.asyncio
async def test_no_sla_anywhere_defaults_to_fail(caplog):
    """No entry SLA and no class SLA → conservative fail (NACK for retry)."""
    driver = _make_driver("FakeIndexer")  # no class-level sla
    driver.upsert_catalog_metadata = AsyncMock(
        side_effect=RuntimeError("boom"),
    )
    entry = OperationDriverEntry(driver_id="FakeIndexer")  # no entry sla

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=AsyncMock(return_value={"title": "T"}),
    )
    [result] = await worker.handle_batch([_make_event()])
    assert result.succeeded is False
    assert result.should_retry is True


# ---------------------------------------------------------------------------
# _resolve_entry_sla + _apply_sla_policy unit tests
# ---------------------------------------------------------------------------


def test_resolve_entry_sla_prefers_entry_override():
    entry_sla = DriverSla(timeout_ms=42, on_timeout="skip")
    entry = OperationDriverEntry(driver_id="X", sla=entry_sla)
    class_sla = DriverSla(timeout_ms=9999, on_timeout="fail")
    driver = MagicMock()
    driver.sla = class_sla
    assert _resolve_entry_sla(entry, driver) is entry_sla


def test_resolve_entry_sla_falls_back_to_class():
    class_sla = DriverSla(timeout_ms=100, on_timeout="degrade")
    entry = OperationDriverEntry(driver_id="X")
    driver = MagicMock()
    driver.sla = class_sla
    assert _resolve_entry_sla(entry, driver) is class_sla


def test_apply_sla_policy_skip_returns_none():
    sla = DriverSla(timeout_ms=100, on_timeout="skip")
    assert _apply_sla_policy(
        sla=sla, driver_id="X", catalog_id="c", reason="r",
    ) is None


def test_apply_sla_policy_fail_returns_fatal():
    sla = DriverSla(timeout_ms=100, on_timeout="fail")
    outcome = _apply_sla_policy(
        sla=sla, driver_id="X", catalog_id="c", reason="r",
    )
    assert outcome is not None
    is_fatal, message = outcome
    assert is_fatal is True
    assert "X@c" in message


def test_apply_sla_policy_degrade_returns_non_fatal():
    sla = DriverSla(timeout_ms=100, on_timeout="degrade")
    outcome = _apply_sla_policy(
        sla=sla, driver_id="X", catalog_id="c", reason="r",
    )
    assert outcome is not None
    is_fatal, _ = outcome
    assert is_fatal is False


def test_apply_sla_policy_without_sla_defaults_to_fail():
    outcome = _apply_sla_policy(
        sla=None, driver_id="X", catalog_id="c", reason="r",
    )
    assert outcome is not None
    assert outcome[0] is True  # fatal


# ---------------------------------------------------------------------------
# Multi-event batch behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_processes_each_event_independently():
    """A single bad event must not poison the rest of the batch."""
    driver = _make_driver("FakeIndexer")
    entry = OperationDriverEntry(driver_id="FakeIndexer")

    # First event hydrates ok; second hydrates ok; third event is the
    # wrong type and short-circuits to ACK.
    hydrate_calls = {"n": 0}

    async def _hydrate(catalog_id, *, db_resource=None):
        hydrate_calls["n"] += 1
        return {"title": catalog_id}

    worker = ReindexWorker(
        resolve_indexers=lambda: [(entry, driver)],
        get_catalog_metadata=_hydrate,
    )

    events = [
        _make_event(event_id="e-1", payload={
            "catalog_id": "cat-1", "operation": "upsert",
        }),
        _make_event(event_id="e-2", payload={
            "catalog_id": "cat-2", "operation": "upsert",
        }),
        _make_event(event_id="e-3", event_type="item_update"),
    ]
    results = await worker.handle_batch(events)
    assert len(results) == 3
    assert [r.succeeded for r in results] == [True, True, True]
    # Two hydrations (only for catalog_metadata_changed events).
    assert hydrate_calls["n"] == 2
    # Driver saw both catalogs.
    assert driver.upsert_catalog_metadata.await_count == 2
