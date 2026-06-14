#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for the fail-closed-to-empty signal in listing visibility.

Verifies that _signal_fail_closed is emitted (counter bumped + structured log
entry) ONLY for outage/misconfiguration cases, and never for the legitimate
deny_all answer or for IAM-off (None context).
"""
from __future__ import annotations

import logging
from typing import Optional

import pytest

import dynastore.models.protocols.visibility as _vis_mod
from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
)
from dynastore.models.protocols.visibility import (
    RequestVisibility,
    extract_id_constraint,
    get_fail_closed_counts,
    reset_request_visibility,
    resolve_asset_listing_ids,
    resolve_catalog_listing_ids,
    resolve_collection_listing_ids,
    set_request_visibility,
)
from dynastore.tools.discovery import register_plugin, unregister_plugin


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_fail_closed_counts():
    """Clear the module-level monotonic counter before every test."""
    _vis_mod._FAIL_CLOSED_COUNTS.clear()
    yield
    _vis_mod._FAIL_CLOSED_COUNTS.clear()


def _visibility(*principals: str) -> RequestVisibility:
    return RequestVisibility(principals=principals)


# ---------------------------------------------------------------------------
# Provider fixtures
# ---------------------------------------------------------------------------


class _BrokenProvider:
    """Raises on every method — simulates a transient outage."""

    async def catalog_listing_filter(self, visibility: RequestVisibility) -> AccessFilter:
        raise RuntimeError("simulated provider outage")

    async def collection_listing_filter(
        self, visibility: RequestVisibility, catalog_id: str
    ) -> AccessFilter:
        raise RuntimeError("simulated provider outage")

    async def asset_listing_filter(
        self,
        visibility: RequestVisibility,
        catalog_id: str,
        collection_id: Optional[str],
    ) -> AccessFilter:
        raise RuntimeError("simulated provider outage")


class _UnsupportedShapeProvider:
    """Returns an AccessFilter with a union — unsupported by extract_id_constraint."""

    async def catalog_listing_filter(self, visibility: RequestVisibility) -> AccessFilter:
        sub = AccessFilter.from_clauses(
            [AccessClause((FieldPredicate("id", ("cat-a",)),))]
        )
        return AccessFilter.union_of([sub, sub])

    async def collection_listing_filter(
        self, visibility: RequestVisibility, catalog_id: str
    ) -> AccessFilter:
        sub = AccessFilter.from_clauses(
            [AccessClause((FieldPredicate("id", ("col-a",)),))]
        )
        return AccessFilter.union_of([sub, sub])

    async def asset_listing_filter(
        self,
        visibility: RequestVisibility,
        catalog_id: str,
        collection_id: Optional[str],
    ) -> AccessFilter:
        sub = AccessFilter.from_clauses(
            [AccessClause((FieldPredicate("id", ("asset-a",)),))]
        )
        return AccessFilter.union_of([sub, sub])


class _DenyAllProvider:
    """Returns deny_everything() — a LEGITIMATE answer, not an outage."""

    async def catalog_listing_filter(self, visibility: RequestVisibility) -> AccessFilter:
        return AccessFilter.deny_everything()

    async def collection_listing_filter(
        self, visibility: RequestVisibility, catalog_id: str
    ) -> AccessFilter:
        return AccessFilter.deny_everything()

    async def asset_listing_filter(
        self,
        visibility: RequestVisibility,
        catalog_id: str,
        collection_id: Optional[str],
    ) -> AccessFilter:
        return AccessFilter.deny_everything()


# ---------------------------------------------------------------------------
# provider_missing — catalog scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_provider_missing_returns_empty_and_signals(caplog):
    """No provider + context set → frozenset() and provider_missing counter bump."""
    token = set_request_visibility(_visibility("role:reader"))
    try:
        with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
            result = await resolve_catalog_listing_ids()
    finally:
        reset_request_visibility(token)

    assert result == frozenset()
    counts = get_fail_closed_counts()
    assert counts.get("provider_missing", 0) >= 1

    # Structured log record carries the right event marker.
    signal_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "listing_visibility_fail_closed"
        and getattr(r, "reason", None) == "provider_missing"
    ]
    assert signal_records, "Expected at least one structured log record for provider_missing"


# ---------------------------------------------------------------------------
# provider_missing — collection scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_provider_missing_returns_empty_and_signals(caplog):
    """No provider + context set → frozenset() and provider_missing counter bump."""
    token = set_request_visibility(_visibility("role:reader"))
    try:
        with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
            result = await resolve_collection_listing_ids("cat-x")
    finally:
        reset_request_visibility(token)

    assert result == frozenset()
    assert get_fail_closed_counts().get("provider_missing", 0) >= 1


# ---------------------------------------------------------------------------
# provider_missing — asset scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_asset_provider_missing_returns_empty_and_signals(caplog):
    """No provider + context set → frozenset() and provider_missing counter bump."""
    token = set_request_visibility(_visibility("role:reader"))
    try:
        with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
            result = await resolve_asset_listing_ids("cat-x", "col-y")
    finally:
        reset_request_visibility(token)

    assert result == frozenset()
    counts = get_fail_closed_counts()
    assert counts.get("provider_missing", 0) >= 1

    signal_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "listing_visibility_fail_closed"
        and getattr(r, "reason", None) == "provider_missing"
    ]
    assert signal_records


# ---------------------------------------------------------------------------
# provider_error — catalog scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_provider_raises_returns_empty_signals_and_logs_traceback(caplog):
    """Provider that raises → frozenset(), provider_error counter bump, exc_info logged."""
    provider = _BrokenProvider()
    register_plugin(provider)
    token = set_request_visibility(_visibility("role:reader"))
    try:
        with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
            result = await resolve_catalog_listing_ids()
    finally:
        reset_request_visibility(token)
        unregister_plugin(provider)

    assert result == frozenset()
    assert get_fail_closed_counts().get("provider_error", 0) >= 1

    # Traceback must be attached to the log record.
    error_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "listing_visibility_fail_closed"
        and getattr(r, "reason", None) == "provider_error"
    ]
    assert error_records, "Expected structured log record for provider_error"
    assert error_records[0].exc_info is not None and error_records[0].exc_info[0] is not None, (
        "exc_info must be set on provider_error log record so the traceback is captured"
    )


# ---------------------------------------------------------------------------
# provider_error — asset scope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_asset_provider_raises_returns_empty_signals_and_logs_traceback(caplog):
    """Asset provider that raises → frozenset(), provider_error with exc_info."""
    provider = _BrokenProvider()
    register_plugin(provider)
    token = set_request_visibility(_visibility("role:reader"))
    try:
        with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
            result = await resolve_asset_listing_ids("cat-x", "col-y")
    finally:
        reset_request_visibility(token)
        unregister_plugin(provider)

    assert result == frozenset()
    assert get_fail_closed_counts().get("provider_error", 0) >= 1

    error_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "listing_visibility_fail_closed"
        and getattr(r, "reason", None) == "provider_error"
    ]
    assert error_records
    assert error_records[0].exc_info is not None and error_records[0].exc_info[0] is not None


# ---------------------------------------------------------------------------
# unsupported_filter_shape — via extract_id_constraint directly
# ---------------------------------------------------------------------------


def test_extract_id_constraint_unsupported_shape_signals(caplog):
    """A union-backed filter → extract_id_constraint returns frozenset() and signals."""
    sub = AccessFilter.from_clauses(
        [AccessClause((FieldPredicate("id", ("cat-a",)),))]
    )
    union_filter = AccessFilter.union_of([sub, sub])

    with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
        result = extract_id_constraint(union_filter)

    assert result == frozenset()
    assert get_fail_closed_counts().get("unsupported_filter_shape", 0) >= 1

    signal_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "listing_visibility_fail_closed"
        and getattr(r, "reason", None) == "unsupported_filter_shape"
    ]
    assert signal_records


# ---------------------------------------------------------------------------
# unsupported_filter_shape — via allow_all with deny
# ---------------------------------------------------------------------------


def test_extract_id_constraint_allow_all_with_deny_signals(caplog):
    """allow_all=True with deny clauses — unsupported, not unconditional — signals."""
    flt = AccessFilter(
        allow_all=True,
        deny=(AccessClause((FieldPredicate("id", ("cat-x",)),)),),
    )

    with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
        result = extract_id_constraint(flt)

    assert result == frozenset()
    assert get_fail_closed_counts().get("unsupported_filter_shape", 0) >= 1


# ---------------------------------------------------------------------------
# unsupported_filter_shape — via provider returning union filter (end-to-end)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unsupported_shape_provider_returns_empty_and_signals(caplog):
    """Provider returning a union filter → extract signals unsupported_filter_shape."""
    provider = _UnsupportedShapeProvider()
    register_plugin(provider)
    token = set_request_visibility(_visibility("role:reader"))
    try:
        with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
            result = await resolve_catalog_listing_ids()
    finally:
        reset_request_visibility(token)
        unregister_plugin(provider)

    assert result == frozenset()
    assert get_fail_closed_counts().get("unsupported_filter_shape", 0) >= 1


# ---------------------------------------------------------------------------
# deny_all — LEGITIMATE empty answer, no counter bump
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deny_all_returns_empty_without_signal(caplog):
    """deny_all is a correct policy answer; must NOT bump any counter."""
    provider = _DenyAllProvider()
    register_plugin(provider)
    token = set_request_visibility(_visibility("user:anonymous"))
    try:
        with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
            result = await resolve_catalog_listing_ids()
    finally:
        reset_request_visibility(token)
        unregister_plugin(provider)

    assert result == frozenset()
    counts = get_fail_closed_counts()
    assert counts.get("provider_missing", 0) == 0
    assert counts.get("provider_error", 0) == 0
    assert counts.get("unsupported_filter_shape", 0) == 0

    # No listing_visibility_fail_closed event must be logged.
    signal_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "listing_visibility_fail_closed"
    ]
    assert not signal_records, (
        "deny_all is a legitimate answer — must not emit a fail-closed signal"
    )


def test_deny_all_filter_extract_returns_empty_without_signal():
    """extract_id_constraint on deny_all must not bump any counter."""
    flt = AccessFilter.deny_everything()
    result = extract_id_constraint(flt)
    assert result == frozenset()
    counts = get_fail_closed_counts()
    assert not any(counts.values()), f"Unexpected counter bumps: {counts}"


# ---------------------------------------------------------------------------
# IAM-off (context is None) — no signal, no counter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_context_returns_none_without_signal(caplog):
    """When no visibility context is set (IAM off), resolve returns None and is silent."""
    # Ensure no contextvar is set (default state).
    with caplog.at_level(logging.WARNING, logger="dynastore.models.protocols.visibility"):
        cat_result = await resolve_catalog_listing_ids()
        col_result = await resolve_collection_listing_ids("cat-x")
        asset_result = await resolve_asset_listing_ids("cat-x", "col-y")

    assert cat_result is None
    assert col_result is None
    assert asset_result is None

    counts = get_fail_closed_counts()
    assert not any(counts.values()), f"Unexpected counter bumps with IAM off: {counts}"

    signal_records = [
        r for r in caplog.records
        if getattr(r, "event", None) == "listing_visibility_fail_closed"
    ]
    assert not signal_records


# ---------------------------------------------------------------------------
# Counter is monotonic across multiple calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_counter_is_monotonic_across_calls():
    """Each provider_missing event increments the counter by exactly one."""
    token = set_request_visibility(_visibility("role:reader"))
    try:
        await resolve_catalog_listing_ids()
        await resolve_catalog_listing_ids()
        await resolve_collection_listing_ids("cat-a")
    finally:
        reset_request_visibility(token)

    # Three calls with no provider → counter must be exactly 3.
    assert get_fail_closed_counts()["provider_missing"] == 3


# ---------------------------------------------------------------------------
# get_fail_closed_counts returns a snapshot (mutation does not affect module state)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_fail_closed_counts_is_a_snapshot():
    """get_fail_closed_counts() returns a copy; mutating it does not affect module."""
    token = set_request_visibility(_visibility("role:reader"))
    try:
        await resolve_catalog_listing_ids()
    finally:
        reset_request_visibility(token)

    snapshot = get_fail_closed_counts()
    original_value = snapshot["provider_missing"]
    snapshot["provider_missing"] = 9999  # mutate the copy

    # Module state is unchanged.
    assert get_fail_closed_counts()["provider_missing"] == original_value
