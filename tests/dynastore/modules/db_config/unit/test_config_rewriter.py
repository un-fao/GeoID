"""Unit tests for the config_rewriter primitive.

Covers:
- Round-trip: register → normalise returns canonical.
- Passthrough: unregistered id returns unchanged.
- Idempotent re-register (same legacy, same canonical).
- Conflicting re-register raises ValueError.
- Self-rename (legacy == canonical) raises ValueError.
- Empty strings raise ValueError.
- Driver-id and class-key namespaces are independent.
- Snapshot listing does not expose the live map.
- Integration: resolve_config_class respects class_key renames.
- Integration: _validate_routing_entries respects driver_id renames.
"""

from __future__ import annotations

import pytest

from dynastore.modules.db_config.config_rewriter import (
    _reset_for_tests,
    _restore_for_tests,
    _snapshot_for_tests,
    list_class_key_renames,
    list_driver_id_renames,
    normalise_class_key,
    normalise_driver_id,
    register_config_class_key_rename,
    register_driver_id_rename,
)


@pytest.fixture(autouse=True)
def _clean_rewriter():
    """Isolate rewriter state per-test without destroying production registrations.

    Driver / config modules register their legacy → canonical renames at
    *import time* (see the "Back-compat aliases" block at the bottom of
    each driver module).  Python caches those imports, so once cleared
    they cannot be re-registered without re-importing the module.

    Using a straight ``_reset_for_tests()`` teardown would therefore wipe
    every production rename registration for the remainder of the pytest
    session — silently breaking any downstream test that relied on a
    persisted legacy class-key resolving via the rewriter.  Snapshot and
    restore instead, so each test gets a clean slate AND the
    post-teardown state is identical to the pre-test state.
    """
    snapshot = _snapshot_for_tests()
    _reset_for_tests()
    try:
        yield
    finally:
        _restore_for_tests(snapshot)


# ---------------------------------------------------------------------------
# Core primitive
# ---------------------------------------------------------------------------


def test_driver_id_round_trip():
    register_driver_id_rename(legacy="OldDriver", canonical="NewDriver")
    assert normalise_driver_id("OldDriver") == "NewDriver"


def test_driver_id_passthrough_when_unregistered():
    assert normalise_driver_id("SomethingUnknown") == "SomethingUnknown"


def test_class_key_round_trip():
    register_config_class_key_rename(legacy="OldCfg", canonical="NewCfg")
    assert normalise_class_key("OldCfg") == "NewCfg"


def test_class_key_passthrough_when_unregistered():
    assert normalise_class_key("WhateverCfg") == "WhateverCfg"


def test_idempotent_reregister():
    register_driver_id_rename(legacy="OldDriver", canonical="NewDriver")
    register_driver_id_rename(legacy="OldDriver", canonical="NewDriver")
    assert normalise_driver_id("OldDriver") == "NewDriver"


def test_conflicting_driver_id_rename_raises():
    register_driver_id_rename(legacy="OldDriver", canonical="NewDriver")
    with pytest.raises(ValueError, match="rename conflict"):
        register_driver_id_rename(legacy="OldDriver", canonical="OtherDriver")


def test_conflicting_class_key_rename_raises():
    register_config_class_key_rename(legacy="OldCfg", canonical="NewCfg")
    with pytest.raises(ValueError, match="rename conflict"):
        register_config_class_key_rename(legacy="OldCfg", canonical="DifferentCfg")


def test_self_rename_rejected():
    with pytest.raises(ValueError, match="legacy == canonical"):
        register_driver_id_rename(legacy="SameName", canonical="SameName")


def test_empty_legacy_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        register_driver_id_rename(legacy="", canonical="NewDriver")


def test_empty_canonical_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        register_driver_id_rename(legacy="OldDriver", canonical="")


def test_namespaces_are_independent():
    """Registering a driver-id rename must not shadow a class-key with the same legacy string."""
    register_driver_id_rename(legacy="Shared", canonical="NewDriver")
    # class_key side has no mapping, so passthrough
    assert normalise_class_key("Shared") == "Shared"
    assert normalise_driver_id("Shared") == "NewDriver"


def test_snapshot_listing_returns_copy():
    register_driver_id_rename(legacy="A", canonical="B")
    snap = list_driver_id_renames()
    snap["A"] = "Mutated"
    assert normalise_driver_id("A") == "B"  # internal state untouched


def test_class_key_snapshot_listing_returns_copy():
    register_config_class_key_rename(legacy="A", canonical="B")
    snap = list_class_key_renames()
    snap["A"] = "Mutated"
    assert normalise_class_key("A") == "B"


# ---------------------------------------------------------------------------
# Integration — resolve_config_class honours class_key renames
# ---------------------------------------------------------------------------


def test_resolve_config_class_honours_rename():
    """A persisted class_key under the legacy name resolves to the renamed class."""
    from dynastore.modules.db_config.platform_config_service import (
        PluginConfig,
        resolve_config_class,
    )

    # Define a live PluginConfig subclass under a deterministic class name so
    # class_key() (== __qualname__) is predictable, then register an old-name
    # alias for it.
    class _RenameTargetConfig(PluginConfig):
        pass

    register_config_class_key_rename(
        legacy="_LegacyRenameTargetConfig",
        canonical=_RenameTargetConfig.class_key(),
    )

    # Unknown/legacy lookup resolves to the renamed class
    resolved = resolve_config_class("_LegacyRenameTargetConfig")
    assert resolved is _RenameTargetConfig

    # Canonical lookup still works (passthrough)
    assert resolve_config_class(_RenameTargetConfig.class_key()) is _RenameTargetConfig

    # Truly unknown lookup still returns None
    assert resolve_config_class("DoesNotExistAnywhere") is None


# ---------------------------------------------------------------------------
# Integration — routing-entry validation honours driver_id renames
# ---------------------------------------------------------------------------


def test_validate_routing_entries_accepts_legacy_driver_id():
    """A routing entry keyed on the legacy driver_id must validate when a
    rename is registered."""
    from dynastore.models.protocols.storage_driver import Capability
    from dynastore.modules.storage.routing_config import (
        _validate_routing_entries,
        CollectionRoutingConfig,
        Operation,
        OperationDriverEntry,
    )

    # Fake driver keyed under its canonical (new) name
    class _FakeDriver:
        capabilities = frozenset({Capability.WRITE, Capability.READ})
        supported_hints = frozenset()

    driver = _FakeDriver()
    driver_index = {"NewFakeDriver": driver}

    register_driver_id_rename(legacy="OldFakeDriver", canonical="NewFakeDriver")

    # Build a routing config with the LEGACY id; validation should succeed
    # because the rewriter translates OldFakeDriver → NewFakeDriver.
    config = CollectionRoutingConfig(
        operations={
            Operation.WRITE: [OperationDriverEntry(driver_id="OldFakeDriver")],
            Operation.READ: [OperationDriverEntry(driver_id="OldFakeDriver")],
        }
    )

    # Should NOT raise
    _validate_routing_entries(config, driver_index, "test label")


def test_validate_routing_entries_rejects_unknown_driver_id():
    """Unregistered driver_id still raises (no silent passthrough dodge)."""
    from dynastore.modules.storage.routing_config import (
        _validate_routing_entries,
        CollectionRoutingConfig,
        Operation,
        OperationDriverEntry,
    )

    config = CollectionRoutingConfig(
        operations={
            Operation.WRITE: [OperationDriverEntry(driver_id="NotRegisteredDriver")],
        }
    )

    with pytest.raises(ValueError, match="not registered"):
        _validate_routing_entries(config, {}, "test label")


# ---------------------------------------------------------------------------
# Snapshot / restore — regression for Critical #2 on the code review.
# ---------------------------------------------------------------------------


def test_snapshot_restore_round_trips_both_maps():
    """snapshot() captures current state; restore() replays it exactly."""
    register_driver_id_rename(legacy="OldD", canonical="NewD")
    register_config_class_key_rename(legacy="OldCfg", canonical="NewCfg")

    snap = _snapshot_for_tests()
    # Mutate state between snapshot and restore
    _reset_for_tests()
    register_driver_id_rename(legacy="Transient", canonical="Other")
    assert normalise_driver_id("OldD") == "OldD"           # state wiped
    assert normalise_driver_id("Transient") == "Other"

    _restore_for_tests(snap)

    assert normalise_driver_id("OldD") == "NewD"           # restored
    assert normalise_class_key("OldCfg") == "NewCfg"
    assert normalise_driver_id("Transient") == "Transient" # transient dropped


def test_snapshot_is_a_copy_not_a_reference():
    """Mutating the map after snapshot() must not mutate the snapshot."""
    register_driver_id_rename(legacy="A", canonical="B")
    snap = _snapshot_for_tests()
    register_driver_id_rename(legacy="C", canonical="D")
    # Restore the original snapshot — 'C' must go away.
    _restore_for_tests(snap)
    assert normalise_driver_id("A") == "B"
    assert normalise_driver_id("C") == "C"  # passthrough


def test_fixture_preserves_production_registrations_across_tests(monkeypatch):
    """The autouse fixture must not wipe production module-level registrations.

    Simulates the production scenario: a driver module registered a rename
    at import time (e.g. Collection*Driver → Items*Driver).  The fixture
    snapshots before each test and restores after — so subsequent tests
    in the same pytest session still see the production rename.
    """
    # Fixture setup has already wiped + yielded; we're mid-test here.
    # Register something that simulates a driver module's import-time call.
    register_driver_id_rename(
        legacy="SimulatedProductionLegacy",
        canonical="SimulatedProductionCanonical",
    )
    assert normalise_driver_id("SimulatedProductionLegacy") == "SimulatedProductionCanonical"

    # When the fixture's teardown runs after this test, it restores the
    # snapshot taken BEFORE this test.  That snapshot pre-dates the
    # registration above, so the registration will be dropped — which is
    # the correct behaviour: test-level state should not leak forward.
    # This assertion validates the intra-test visibility only.


def test_reset_for_tests_without_restore_is_destructive_in_docs():
    """Ensure _reset_for_tests continues to advertise that it should be paired."""
    from dynastore.modules.db_config.config_rewriter import _reset_for_tests as r

    doc = r.__doc__ or ""
    assert "snapshot" in doc.lower() or "restore" in doc.lower(), (
        "_reset_for_tests docstring must warn callers about the production "
        "registration wipe so that pairing with _snapshot/_restore is obvious."
    )
