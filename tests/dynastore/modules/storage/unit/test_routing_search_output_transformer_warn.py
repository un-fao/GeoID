"""#1567 — SEARCH ``output_transformers`` are only honoured on the tier whose
search drivers invoke ``restore_from_index`` (asset Elasticsearch today).

On every other tier the declaration validates but never runs, so
``_warn_deferred_transformer_hops`` must surface the silent no-op. These tests
pin that warn and the per-tier ``_search_output_chain_wired`` flag that drives
it.
"""

from __future__ import annotations

import logging

import pytest

from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    _DEFERRED_HOP_WARNED,
    _warn_deferred_transformer_hops,
)

_LOGGER_NAME = "dynastore.modules.storage.routing_config"


@pytest.fixture(autouse=True)
def _clear_warn_dedup():
    """The warn helper dedups across the process via a module-level set; clear
    it around each test so a one-time warn is observable."""
    _DEFERRED_HOP_WARNED.clear()
    yield
    _DEFERRED_HOP_WARNED.clear()


def _search_ops_with_output_transformer():
    return {
        Operation.SEARCH: [
            OperationDriverEntry(driver_ref="some_es", output_transformers=("t",)),
        ]
    }


def test_search_output_transformer_warns_when_restore_chain_unwired(caplog):
    """SEARCH output_transformers on a tier whose search path doesn't invoke
    the restore chain → a one-time #1567 no-op WARN."""
    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        _warn_deferred_transformer_hops(
            _search_ops_with_output_transformer(),
            "ItemsRoutingConfig",
            search_output_chain_wired=False,
        )
    msgs = [r.getMessage() for r in caplog.records]
    assert any("#1567" in m and "restore_from_index" in m for m in msgs), msgs
    assert any("some_es" in m for m in msgs), msgs


def test_search_output_transformer_silent_when_restore_chain_wired(caplog):
    """On the asset tier (restore chain wired) the same declaration is honoured,
    so no no-op warning is emitted."""
    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        _warn_deferred_transformer_hops(
            _search_ops_with_output_transformer(),
            "AssetRoutingConfig",
            search_output_chain_wired=True,
        )
    msgs = [r.getMessage() for r in caplog.records]
    assert not any("#1567" in m for m in msgs), msgs


def test_unwired_hop_warn_still_fires_for_non_search_output(caplog):
    """The pre-existing unwired-hop warn (output_transformers on a non-SEARCH
    op) is unchanged — the new SEARCH branch must not shadow it."""
    ops = {
        Operation.READ: [
            OperationDriverEntry(driver_ref="pg", output_transformers=("t",)),
        ]
    }
    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        _warn_deferred_transformer_hops(ops, "ItemsRoutingConfig", False)
    msgs = [r.getMessage() for r in caplog.records]
    assert any("not yet wired in this release" in m for m in msgs), msgs
    # The READ hop is genuinely unwired — not the #1567 SEARCH-restore case.
    assert not any("#1567" in m for m in msgs), msgs


def test_only_asset_tier_marks_search_output_chain_wired():
    """Exactly one tier (assets) declares its SEARCH restore chain wired; the
    other three default to unwired so their SEARCH output_transformers warn."""
    assert AssetRoutingConfig._search_output_chain_wired is True
    for tier in (ItemsRoutingConfig, CollectionRoutingConfig, CatalogRoutingConfig):
        assert tier._search_output_chain_wired is False, tier.__name__
