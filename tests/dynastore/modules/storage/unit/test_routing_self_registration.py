"""Routing-config self-registration: every installed metadata driver
appears in ``operations[WRITE]`` / ``operations[READ]`` after the
auto-append step fires — closes the "implicit fan-out invisible to
operators" antipattern flagged in the Site 1 plan.
"""

from __future__ import annotations

from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    _self_register_metadata_drivers,
)


def test_collection_self_registers_missing_metadata_drivers():
    """Empty metadata.operations + 2 installed drivers → both auto-appended
    to WRITE and READ."""
    cfg = CollectionRoutingConfig()
    cfg.metadata.operations.clear()

    metadata_index = {"PgCoreMeta": object(), "PgStacMeta": object()}
    _self_register_metadata_drivers(cfg, metadata_index)

    write_ids = {e.driver_id for e in cfg.metadata.operations[Operation.WRITE]}
    read_ids = {e.driver_id for e in cfg.metadata.operations[Operation.READ]}
    assert write_ids == {"PgCoreMeta", "PgStacMeta"}
    assert read_ids == {"PgCoreMeta", "PgStacMeta"}


def test_collection_preserves_operator_supplied_entry():
    """Operator-supplied entries (with custom on_failure / write_mode)
    survive auto-append — only MISSING drivers get appended."""
    cfg = CollectionRoutingConfig()
    cfg.metadata.operations.clear()
    cfg.metadata.operations[Operation.WRITE] = [
        OperationDriverEntry(
            driver_id="PgCoreMeta", on_failure=FailurePolicy.WARN,
        ),
    ]

    metadata_index = {"PgCoreMeta": object(), "PgStacMeta": object()}
    _self_register_metadata_drivers(cfg, metadata_index)

    write_entries = {
        e.driver_id: e for e in cfg.metadata.operations[Operation.WRITE]
    }
    # Operator's PgCoreMeta entry preserved with on_failure=WARN.
    assert write_entries["PgCoreMeta"].on_failure == FailurePolicy.WARN
    # PgStacMeta was missing and got auto-appended with defaults.
    assert "PgStacMeta" in write_entries
    assert write_entries["PgStacMeta"].on_failure == FailurePolicy.FATAL


def test_collection_no_op_when_all_drivers_already_listed():
    """All installed drivers already present → no duplicates appended."""
    cfg = CollectionRoutingConfig()
    cfg.metadata.operations.clear()
    cfg.metadata.operations[Operation.WRITE] = [
        OperationDriverEntry(driver_id="PgCoreMeta"),
        OperationDriverEntry(driver_id="PgStacMeta"),
    ]
    cfg.metadata.operations[Operation.READ] = [
        OperationDriverEntry(driver_id="PgCoreMeta"),
        OperationDriverEntry(driver_id="PgStacMeta"),
    ]

    metadata_index = {"PgCoreMeta": object(), "PgStacMeta": object()}
    _self_register_metadata_drivers(cfg, metadata_index)

    assert len(cfg.metadata.operations[Operation.WRITE]) == 2
    assert len(cfg.metadata.operations[Operation.READ]) == 2


def test_catalog_self_registers_missing_drivers():
    """Catalog tier: same self-registration shape on the top-level operations."""
    cfg = CatalogRoutingConfig()
    cfg.operations.clear()

    metadata_index = {"CatalogPgCore": object(), "CatalogPgStac": object()}
    _self_register_metadata_drivers(cfg, metadata_index)

    write_ids = {e.driver_id for e in cfg.operations[Operation.WRITE]}
    read_ids = {e.driver_id for e in cfg.operations[Operation.READ]}
    assert write_ids == {"CatalogPgCore", "CatalogPgStac"}
    assert read_ids == {"CatalogPgCore", "CatalogPgStac"}


def test_self_registration_skips_zero_drivers():
    """Empty driver index → no entries appended (no spurious empty list creation
    for ops that were already absent)."""
    cfg = CollectionRoutingConfig()
    cfg.metadata.operations.clear()

    _self_register_metadata_drivers(cfg, metadata_driver_index={})

    # Operations stay empty — auto-append only adds for present drivers.
    assert cfg.metadata.operations.get(Operation.WRITE, []) == []
    assert cfg.metadata.operations.get(Operation.READ, []) == []


# ---------------------------------------------------------------------------
# Per-tier indexer marker self-registration
# ---------------------------------------------------------------------------


def test_indexer_marker_lands_in_INDEX_with_async_warn_defaults():
    """A driver opting in to a tier indexer marker auto-registers under
    operations[INDEX] with write_mode=async, on_failure=warn — sourced
    from the per-tier marker, not from generic capability discovery.
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        WriteMode,
        _self_register_indexers_into,
    )

    class _CollectionES:
        is_collection_indexer: ClassVar[bool] = True

    class _NotAnIndexer:
        pass

    target_ops: dict = {}
    fake_pool = [_CollectionES(), _NotAnIndexer()]

    def _fake_get_protocols(proto):
        return [d for d in fake_pool if isinstance(d, proto)]

    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    entries = target_ops.get(Operation.INDEX, [])
    assert len(entries) == 1
    assert entries[0].driver_id == "_CollectionES"
    assert entries[0].on_failure == FailurePolicy.WARN
    assert entries[0].write_mode == WriteMode.ASYNC


def test_apply_handlers_invoke_indexer_self_registration():
    """Each routing-config apply handler MUST invoke
    ``_self_register_indexers_into`` against the matching tier marker.
    Pins the wiring against accidental drop in a future refactor.
    """
    import asyncio
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        _on_apply_asset_routing_config,
        _on_apply_catalog_routing_config,
        _on_apply_routing_config,
    )

    calls: list[type] = []

    def _spy(target_ops, marker_proto):
        calls.append(marker_proto)

    # Empty operations so _validate_routing_entries has nothing to check
    # against the (stubbed-empty) driver registry.
    coll = CollectionRoutingConfig()
    coll.operations.clear()
    coll.metadata.operations.clear()
    asset = AssetRoutingConfig()
    asset.operations.clear()
    cat = CatalogRoutingConfig()
    cat.operations.clear()

    with patch(
        "dynastore.modules.storage.routing_config._self_register_indexers_into",
        _spy,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [],
    ):
        asyncio.run(_on_apply_routing_config(
            coll, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_on_apply_asset_routing_config(
            asset, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_on_apply_catalog_routing_config(
            cat, catalog_id=None, collection_id=None, db_resource=None,
        ))

    from dynastore.models.protocols.indexer import (
        AssetIndexer,
        CatalogIndexer,
        CollectionIndexer,
    )
    assert calls == [CollectionIndexer, AssetIndexer, CatalogIndexer]


def test_end_to_end_marker_to_INDEX_entry_via_real_apply_handler():
    """End-to-end: register a real driver opting in to ``CatalogIndexer``,
    invoke ``_on_apply_catalog_routing_config`` against a fresh
    ``CatalogRoutingConfig``, assert the driver lands in
    ``operations[INDEX]`` with the marker's defaults (async + warn).

    Validates the full chain shipped in d1aa321 + dbe505f + 98d0801:
    marker discovery → helper invocation → entry with correct policy.
    """
    import asyncio
    from typing import ClassVar

    from dynastore.models.protocols.indexer import CatalogIndexer
    from dynastore.modules.storage.routing_config import (
        WriteMode,
        _on_apply_catalog_routing_config,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    class _DummyCatalogIndexer:
        is_catalog_indexer: ClassVar[bool] = True
        # Minimal CatalogMetadataStore surface — enough for
        # _validate_routing_entries to accept it under operations[INDEX]
        # if it were referenced (it isn't pre-apply; the marker self-
        # registration appends it).  We avoid populating WRITE/READ to
        # skip validation for those op-keys.
        capabilities = frozenset()

    instance = _DummyCatalogIndexer()
    register_plugin(instance)
    try:
        cfg = CatalogRoutingConfig()
        cfg.operations.clear()  # skip default validation against unregistered drivers

        asyncio.run(_on_apply_catalog_routing_config(
            cfg, catalog_id=None, collection_id=None, db_resource=None,
        ))

        index_entries = cfg.operations.get(Operation.INDEX, [])
        assert any(
            e.driver_id == "_DummyCatalogIndexer"
            and e.on_failure == FailurePolicy.WARN
            and e.write_mode == WriteMode.ASYNC
            for e in index_entries
        ), f"_DummyCatalogIndexer not auto-registered: {index_entries!r}"
    finally:
        unregister_plugin(instance)
        # Sanity: ensure cleanup so other tests don't see this stub.
        from dynastore.tools.discovery import get_protocols
        assert not any(
            isinstance(d, CatalogIndexer) and type(d).__name__ == "_DummyCatalogIndexer"
            for d in get_protocols(CatalogIndexer)
        )


def test_indexer_marker_skips_already_listed_driver():
    """Operator-supplied INDEX entry survives — only missing drivers get appended."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import AssetIndexer
    from dynastore.modules.storage.routing_config import _self_register_indexers_into

    class _AssetES:
        is_asset_indexer: ClassVar[bool] = True

    operator_entry = OperationDriverEntry(
        driver_id="_AssetES", on_failure=FailurePolicy.FATAL,
    )
    target_ops: dict = {Operation.INDEX: [operator_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()] if proto is AssetIndexer else []):
        _self_register_indexers_into(target_ops, AssetIndexer)

    # No duplicate; operator-supplied on_failure=FATAL preserved.
    assert len(target_ops[Operation.INDEX]) == 1
    assert target_ops[Operation.INDEX][0].on_failure == FailurePolicy.FATAL
