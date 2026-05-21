"""Routing-config self-registration: every installed store driver
appears in ``operations[WRITE]`` / ``operations[READ]`` after the
auto-append step fires — closes the "implicit fan-out invisible to
operators" antipattern.
"""

from __future__ import annotations

from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    _self_register_store_drivers,
)


def test_collection_self_registers_missing_store_drivers():
    """Empty metadata.operations + 2 installed drivers → both auto-appended
    to WRITE and READ."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()

    metadata_index = {"pg_core_meta": object(), "pg_stac_meta": object()}
    _self_register_store_drivers(cfg, metadata_index)

    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    read_ids = {e.driver_ref for e in cfg.operations[Operation.READ]}
    assert write_ids == {"pg_core_meta", "pg_stac_meta"}
    assert read_ids == {"pg_core_meta", "pg_stac_meta"}


def test_collection_operator_managed_list_locks_out_auto_augment():
    """Option A (#792 / #889): once an operator has touched the list,
    the self-register helpers do not augment it.  Other discoverable
    drivers stay out until the operator either lists them explicitly
    or drops the list back to defaults.
    """
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(
            driver_ref="pg_core_meta", on_failure=FailurePolicy.WARN,
            # Field default is source="operator"; explicit here for clarity.
            source="operator",
        ),
    ]

    metadata_index = {"pg_core_meta": object(), "pg_stac_meta": object()}
    _self_register_store_drivers(cfg, metadata_index)

    write_entries = {
        e.driver_ref: e for e in cfg.operations[Operation.WRITE]
    }
    assert write_entries["pg_core_meta"].on_failure == FailurePolicy.WARN
    assert write_entries["pg_core_meta"].source == "operator"
    # PgStacMeta was missing and stays missing — operator-managed list.
    assert "pg_stac_meta" not in write_entries


def test_collection_no_op_when_all_drivers_already_listed():
    """All installed drivers already present → no duplicates appended."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(driver_ref="pg_core_meta"),
        OperationDriverEntry(driver_ref="pg_stac_meta"),
    ]
    cfg.operations[Operation.READ] = [
        OperationDriverEntry(driver_ref="pg_core_meta"),
        OperationDriverEntry(driver_ref="pg_stac_meta"),
    ]

    metadata_index = {"pg_core_meta": object(), "pg_stac_meta": object()}
    _self_register_store_drivers(cfg, metadata_index)

    assert len(cfg.operations[Operation.WRITE]) == 2
    assert len(cfg.operations[Operation.READ]) == 2


def test_catalog_self_registers_missing_drivers():
    """Catalog tier: same self-registration shape on the top-level operations."""
    cfg = CatalogRoutingConfig()
    cfg.operations.clear()

    metadata_index = {"catalog_pg_core": object(), "catalog_pg_stac": object()}
    _self_register_store_drivers(cfg, metadata_index)

    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    read_ids = {e.driver_ref for e in cfg.operations[Operation.READ]}
    assert write_ids == {"catalog_pg_core", "catalog_pg_stac"}
    assert read_ids == {"catalog_pg_core", "catalog_pg_stac"}


def test_self_registration_skips_zero_drivers():
    """Empty driver index → no entries appended (no spurious empty list creation
    for ops that were already absent)."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()

    _self_register_store_drivers(cfg, store_driver_index={})

    # Operations stay empty — auto-append only adds for present drivers.
    assert cfg.operations.get(Operation.WRITE, []) == []
    assert cfg.operations.get(Operation.READ, []) == []


# ---------------------------------------------------------------------------
# Per-tier indexer marker self-registration
# ---------------------------------------------------------------------------


def test_indexer_marker_lands_in_write_with_async_outbox_defaults():
    """A driver opting in to a tier indexer marker auto-registers under
    operations[WRITE] as a secondary index (secondary_index=True) with
    write_mode=async, on_failure=outbox — sourced from the per-tier
    marker, not from generic capability discovery.  OUTBOX (not WARN) is
    the default so transient indexer failures enqueue a durable retry row
    instead of dropping silently.
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        WriteMode,
        _self_register_indexers_into,
        secondary_index_entries,
    )

    class _CollectionES:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    class _NotAnIndexer:
        pass

    target_ops: dict = {}
    fake_pool = [_CollectionES(), _NotAnIndexer()]

    def _fake_get_protocols(proto):
        return [d for d in fake_pool if isinstance(d, proto)]

    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    entries = secondary_index_entries(target_ops)
    assert len(entries) == 1
    assert entries[0].driver_ref == "_collection_es"
    assert entries[0].on_failure == FailurePolicy.OUTBOX
    assert entries[0].write_mode == WriteMode.ASYNC
    assert entries[0].secondary_index is True


def test_validate_handlers_invoke_indexer_self_registration():
    """Each routing-config validate handler MUST invoke
    ``_self_register_indexers_into`` against the matching tier marker.
    Pins the wiring against accidental drop in a future refactor.

    Self-registration moved apply→validate in #738/#747 — it must run
    pre-persist so the auto-registered ``source="auto"`` entries are
    actually serialized into the stored config.
    """
    import asyncio
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        _validate_asset_routing_config,
        _validate_catalog_routing_config,
        _validate_collection_routing_config,
        _validate_items_routing_config,
    )

    calls: list[type] = []

    def _spy(target_ops, marker_proto, **_kwargs):
        # Accept arbitrary kwargs (e.g. ``search_caps`` on the searcher
        # helper) so the spy works for both helper signatures.
        calls.append(marker_proto)

    # Empty operations so _validate_routing_entries has nothing to check
    # against the (stubbed-empty) driver registry.
    items = ItemsRoutingConfig()
    items.operations.clear()
    coll = CollectionRoutingConfig()
    coll.operations.clear()
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
        asyncio.run(_validate_items_routing_config(
            items, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_collection_routing_config(
            coll, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_asset_routing_config(
            asset, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_catalog_routing_config(
            cat, catalog_id=None, collection_id=None, db_resource=None,
        ))

    from dynastore.models.protocols.indexer import (
        AssetIndexer,
        CatalogIndexer,
        CollectionIndexer,
        ItemIndexer,
    )
    # Each tier's validate handler invokes indexer registration with its own
    # marker Protocol. Order matches the invocation order above.
    assert calls == [ItemIndexer, CollectionIndexer, AssetIndexer, CatalogIndexer]


def test_end_to_end_marker_to_write_index_entry_via_real_validate_handler():
    """End-to-end: register a real driver opting in to ``CatalogIndexer``,
    invoke ``_validate_catalog_routing_config`` against a fresh
    ``CatalogRoutingConfig``, assert the driver lands in
    ``operations[WRITE]`` as a secondary index with the marker's defaults
    (async + outbox).

    Validates the full chain: marker discovery → helper invocation →
    entry with correct durable-retry policy.  Self-registration moved
    apply→validate in #738/#747.
    """
    import asyncio
    from typing import ClassVar

    from dynastore.models.protocols.indexer import CatalogIndexer
    from dynastore.modules.storage.routing_config import (
        WriteMode,
        _validate_catalog_routing_config,
        secondary_index_entries,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    class _DummyCatalogIndexer:
        is_catalog_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})
        # Minimal CatalogStore surface — enough for
        # _validate_routing_entries to accept it under operations[WRITE]
        # if it were referenced (it isn't pre-apply; the marker self-
        # registration appends it).  We avoid populating WRITE/READ to
        # skip validation for those op-keys.
        capabilities = frozenset()

    instance = _DummyCatalogIndexer()
    register_plugin(instance)
    try:
        cfg = CatalogRoutingConfig()
        cfg.operations.clear()  # skip default validation against unregistered drivers

        asyncio.run(_validate_catalog_routing_config(
            cfg, catalog_id=None, collection_id=None, db_resource=None,
        ))

        index_entries = secondary_index_entries(cfg.operations)
        assert any(
            e.driver_ref == "_dummy_catalog_indexer"
            and e.on_failure == FailurePolicy.OUTBOX
            and e.write_mode == WriteMode.ASYNC
            for e in index_entries
        ), f"_DummyCatalogIndexer not auto-registered: {index_entries!r}"
    finally:
        unregister_plugin(instance)
        # Sanity: ensure cleanup so other tests don't see this stub.
        from dynastore.tools.discovery import get_protocols
        assert not any(
            isinstance(d, CatalogIndexer) and type(d).__name__ == "_dummy_catalog_indexer"
            for d in get_protocols(CatalogIndexer)
        )


def test_indexer_marker_skips_already_listed_driver():
    """Operator-supplied WRITE secondary-index entry survives — only missing
    drivers get appended."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import AssetIndexer
    from dynastore.modules.storage.routing_config import _self_register_indexers_into

    class _AssetES:
        is_asset_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    operator_entry = OperationDriverEntry(
        driver_ref="_asset_es", on_failure=FailurePolicy.FATAL,
    )
    target_ops: dict = {Operation.WRITE: [operator_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()] if proto is AssetIndexer else []):
        _self_register_indexers_into(target_ops, AssetIndexer)

    # No duplicate; operator-supplied on_failure=FATAL preserved. The
    # discovered indexer marker stamps secondary_index=True upward.
    assert len(target_ops[Operation.WRITE]) == 1
    assert target_ops[Operation.WRITE][0].on_failure == FailurePolicy.FATAL
    assert target_ops[Operation.WRITE][0].secondary_index is True


# ---------------------------------------------------------------------------
# SEARCH self-registration helper
# ---------------------------------------------------------------------------


def test_searcher_helper_picks_up_drivers_opting_into_search():
    """Drivers declaring ``Operation.SEARCH`` in
    ``auto_register_for_routing`` land in operations[SEARCH].  Drivers
    that don't opt in stay out.

    The previous cap-based gate (``EntityStoreCapability.SEARCH`` /
    ``Capability.FULLTEXT`` / …) was retired alongside the migration to
    the per-Operation auto-default set: capabilities are structural facts
    only; per-request flavours (fulltext/aggregation/count) live in the
    ``Hint`` catalogue.  Auto-augmentation gates purely on the Op-set.
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _ESCat:
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    class _IndexerOnly:
        # Opts into WRITE (secondary index) but not SEARCH — should NOT land
        # in SEARCH.
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    class _NotOptedIn:
        # No declaration → defaults to frozenset() → not auto-augmented.
        pass

    target_ops: dict = {}
    fake_pool = [_ESCat(), _IndexerOnly(), _NotOptedIn()]

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: fake_pool):
        _self_register_searchers_into(target_ops, CatalogStore)

    ids = {e.driver_ref for e in target_ops.get(Operation.SEARCH, [])}
    assert ids == {"_es_cat"}


def test_searcher_helper_skips_drivers_without_search_optin():
    """Driver without ``Operation.SEARCH`` in its Op-set is not added
    to SEARCH, regardless of which capabilities it declares.

    Capabilities are structural facts; they do not gate auto-augmentation
    under the new model.
    """
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import (
        CatalogStore,
        EntityStoreCapability,
    )
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _PgPrimary:
        # Has lots of caps but no Op-set → no auto-default into SEARCH.
        capabilities = frozenset({
            EntityStoreCapability.READ, EntityStoreCapability.WRITE,
            EntityStoreCapability.SEARCH,  # cap is no longer the gate
        })

    target_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_PgPrimary()]):
        _self_register_searchers_into(target_ops, CatalogStore)
    assert target_ops.get(Operation.SEARCH, []) == []


def test_searcher_helper_idempotent():
    """Repeated calls don't add duplicates; operator-supplied entry survives."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _ESCat:
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    from dynastore.modules.storage.hints import Hint
    op_entry = OperationDriverEntry(driver_ref="_es_cat", hints={Hint.METADATA})
    target_ops: dict = {Operation.SEARCH: [op_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ESCat()]):
        _self_register_searchers_into(target_ops, CatalogStore)
        _self_register_searchers_into(target_ops, CatalogStore)

    assert len(target_ops[Operation.SEARCH]) == 1
    assert target_ops[Operation.SEARCH][0].hints == {Hint.METADATA}


# ---------------------------------------------------------------------------
# Read-time model_validator augmentation
# ---------------------------------------------------------------------------


def test_catalog_routing_validator_augments_write_index_and_search():
    """Constructing a default CatalogRoutingConfig must fold in
    discoverable CatalogIndexer (as a WRITE secondary index) + SEARCH-capable
    drivers."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import EntityStoreCapability
    from dynastore.modules.storage.routing_config import secondary_index_entries

    class _CatES:
        is_catalog_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE, Operation.SEARCH})

    instance = _CatES()

    def _fake_get_protocols(proto):
        # Marker check + capability check both rely on isinstance —
        # the fake instance has the right ClassVar + duck-typed
        # `capabilities` set so it satisfies both runtime_checkable
        # Protocols (CatalogIndexer + CatalogStore via
        # the SEARCH cap predicate inside _self_register_searchers_into).
        return [instance]

    with patch("dynastore.tools.discovery.get_protocols", _fake_get_protocols):
        cfg = CatalogRoutingConfig()

    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    search_ids = {e.driver_ref for e in cfg.operations.get(Operation.SEARCH, [])}
    assert "_cat_es" in index_ids
    assert "_cat_es" in search_ids
    # Primary WRITE entry unchanged — the registered CatalogStore is the
    # ``catalog_postgresql_driver`` composition wrapper (#732), which fans
    # CRUD across the catalog_core + catalog_stac sidecars internally. The
    # discovered ES indexer joins the same WRITE list as a secondary index
    # (role distinguished by ``secondary_index``), not as a primary store.
    primary_write_ids = {
        e.driver_ref for e in cfg.operations[Operation.WRITE]
        if not e.secondary_index
    }
    assert primary_write_ids == {"catalog_postgresql_driver"}


def test_catalog_routing_validator_no_op_when_no_indexers_discoverable():
    """No discoverable indexer/searcher → operations stays at the
    default-factory shape: primary WRITE+READ only. Secondary-index and
    SEARCH are both discovery-driven, so with nothing discoverable the ES
    secondary-index hop is NOT present (#1069 / #1073) — a PG-only deployment
    must not pin an undrainable OUTBOX hop into tasks.tasks."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import secondary_index_entries

    with patch("dynastore.tools.discovery.get_protocols", lambda proto: []):
        cfg = CatalogRoutingConfig()

    # Nothing discoverable → no ES secondary-index hop folded in.
    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "catalog_elasticsearch_driver" not in index_ids
    # SEARCH is purely discovery-driven — empty when nothing is discoverable.
    assert Operation.SEARCH not in cfg.operations or cfg.operations[Operation.SEARCH] == []


def test_collection_routing_validator_augments_write_index_and_search():
    """ItemsRoutingConfig validator augments metadata.operations
    (not top-level operations)."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import EntityStoreCapability
    from dynastore.modules.storage.routing_config import secondary_index_entries

    class _ColES:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE, Operation.SEARCH})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ColES()]):
        cfg = CollectionRoutingConfig()

    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    search_ids = {e.driver_ref for e in cfg.operations.get(Operation.SEARCH, [])}
    assert "_col_es" in index_ids
    assert "_col_es" in search_ids


def test_items_routing_validator_augments_write_index_and_search():
    """The model_validator on ItemsRoutingConfig augments its `operations`
    with discoverable ItemIndexer drivers (→ WRITE secondary index) and
    CollectionItemsStore drivers declaring storage SEARCH-family caps
    (FULLTEXT / SPATIAL_FILTER / ATTRIBUTE_FILTER) (→ SEARCH).
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.storage_driver import Capability
    from dynastore.modules.storage.routing_config import secondary_index_entries

    class _ItemsES:
        is_item_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE, Operation.SEARCH})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES()]):
        cfg = ItemsRoutingConfig()

    top_index = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    top_search = {e.driver_ref for e in cfg.operations.get(Operation.SEARCH, [])}
    assert "_items_es" in top_index
    assert "_items_es" in top_search
    # Primary PG WRITE entry retained; the indexer is appended as a
    # secondary index alongside it.
    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert write_ids == {"items_postgresql_driver", "_items_es"} or \
           "items_postgresql_driver" in write_ids


def test_items_routing_search_optin_gate():
    """ItemsRoutingConfig SEARCH gate is the per-Operation opt-in set.
    The cap-based gate (storage ``Capability.{FULLTEXT, SPATIAL_FILTER,
    ATTRIBUTE_FILTER}`` vs metadata ``EntityStoreCapability.SEARCH``)
    was retired alongside PR #3a — capabilities are structural facts
    only.  A driver lands in items SEARCH iff its class declares
    ``Operation.SEARCH`` in ``auto_register_for_routing``.
    """
    from typing import ClassVar
    from unittest.mock import patch

    class _OptedInSearcher:
        is_item_indexer: ClassVar[bool] = False
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    class _OptedOutSearcher:
        # Capabilities are irrelevant under the new model — only the
        # Op-set decides auto-augmentation.
        is_item_indexer: ClassVar[bool] = False

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_OptedInSearcher(), _OptedOutSearcher()]):
        cfg = ItemsRoutingConfig()

    top_search = {e.driver_ref for e in cfg.operations.get(Operation.SEARCH, [])}
    assert "_opted_in_searcher" in top_search
    assert "_opted_out_searcher" not in top_search


def test_asset_routing_validator_augments_write_index_only():
    """AssetRoutingConfig validator augments WRITE with a secondary index
    but NOT SEARCH — assets aren't search-addressable the way collection
    items are."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        secondary_index_entries,
    )

    class _AssetES:
        is_asset_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AssetES()]):
        cfg = AssetRoutingConfig()

    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "_asset_es" in index_ids
    assert Operation.SEARCH not in cfg.operations or cfg.operations[Operation.SEARCH] == []


def test_validator_failure_in_discovery_does_not_break_construction():
    """If `get_protocols` raises (e.g. discovery not ready during test
    fixture loading), the validator must not propagate — a debug log is
    enough; the apply-handler path is the safety net."""
    from unittest.mock import patch

    def _boom(proto):
        raise RuntimeError("discovery not ready")

    with patch("dynastore.tools.discovery.get_protocols", _boom):
        cfg = CatalogRoutingConfig()  # must not raise

    # Default WRITE/READ unaffected — the registered CatalogStore is the
    # ``catalog_postgresql_driver`` composition wrapper (#732).
    write_ids = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    assert write_ids == {"catalog_postgresql_driver"}
    # Discovery augmentation was skipped, and the ES secondary-index hop is no
    # longer hard-coded (#1069 / #1073) — so there is no ES secondary index;
    # SEARCH is discovery-only, so it stays absent.
    from dynastore.modules.storage.routing_config import secondary_index_entries
    index_ids = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "catalog_elasticsearch_driver" not in index_ids
    assert Operation.SEARCH not in cfg.operations or cfg.operations[Operation.SEARCH] == []


# ---------------------------------------------------------------------------
# Apply-handler parity for SEARCH
# ---------------------------------------------------------------------------


# Imports needed for the source-provenance test block below.
from dynastore.modules.storage.routing_config import (  # noqa: E402
    _self_register_store_drivers,
)


def test_default_entry_source_is_operator():
    """An entry constructed without ``source`` defaults to ``operator`` —
    the assumption is that any explicit construction is operator-driven
    unless an auto helper marks it otherwise."""
    e = OperationDriverEntry(driver_ref="X")
    assert e.source == "operator"


def test_indexer_helper_marks_entries_as_auto():
    """Entries created by `_self_register_indexers_into` carry
    `source="auto"` so operators can distinguish them in the API
    response."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
    )

    class _ColES:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    target_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ColES()]):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    assert len(target_ops[Operation.WRITE]) == 1
    assert target_ops[Operation.WRITE][0].source == "auto"


def test_searcher_helper_marks_entries_as_auto():
    """Entries created by `_self_register_searchers_into` also carry
    `source="auto"`."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _ESCat:
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    target_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ESCat()]):
        _self_register_searchers_into(target_ops, CatalogStore)

    assert len(target_ops[Operation.SEARCH]) == 1
    assert target_ops[Operation.SEARCH][0].source == "auto"


def test_store_driver_helper_marks_entries_as_auto():
    """`_self_register_store_drivers` also marks new entries as auto."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()

    metadata_index = {"pg_core_meta": object()}
    _self_register_store_drivers(cfg, metadata_index)

    for op in (Operation.WRITE, Operation.READ):
        entries = cfg.operations[op]
        assert len(entries) == 1
        assert entries[0].source == "auto"


def test_operator_managed_list_locks_out_auto_augment():
    """Option A (#792 / #889): a single operator-source entry locks the
    whole operation's list.  Discoverable drivers do NOT get auto-appended
    onto the side of an operator-managed entry — that semantic gave rise
    to #792 where deleted SEARCH/secondary-index drivers came back on every
    read.

    Inverse shape pin (do not collapse to a single assertion): the test
    constructs the SAME mixed-discovery surface the old behaviour relied
    on, then asserts only the operator entry survives — guarding against
    a future "simplify" patch that drops the operator-managed gate.
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
    )

    class _AutoDriver:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    class _OpDriver:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    operator_entry = OperationDriverEntry(
        driver_ref="_op_driver", on_failure=FailurePolicy.FATAL,
    )
    target_ops: dict = {Operation.WRITE: [operator_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_AutoDriver(), _OpDriver()]):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    by_id = {e.driver_ref: e for e in target_ops[Operation.WRITE]}
    assert set(by_id) == {"_op_driver"}
    assert by_id["_op_driver"].source == "operator"
    assert by_id["_op_driver"].on_failure == FailurePolicy.FATAL


def test_source_field_serialises_in_model_dump():
    """The new field appears in `model_dump()` output so it surfaces in
    the configs API response without any endpoint-side changes."""
    e_op = OperationDriverEntry(driver_ref="X")
    e_auto = OperationDriverEntry(driver_ref="Y", source="auto")
    assert e_op.model_dump()["source"] == "operator"
    assert e_auto.model_dump()["source"] == "auto"


def test_source_field_round_trips_via_model_validate():
    """Persisted JSONB rows that include `source` deserialise correctly.
    Rows that DON'T include it (older persisted data) get the default
    `operator` — backwards-compatible."""
    e_new = OperationDriverEntry.model_validate({"driver_ref": "X", "source": "auto"})
    assert e_new.source == "auto"
    e_legacy = OperationDriverEntry.model_validate({"driver_ref": "X"})
    assert e_legacy.source == "operator"


def test_source_field_rejects_invalid_value():
    """Literal[\"operator\", \"auto\"] is enforced — typos fail validation."""
    import pytest as _pytest
    from pydantic import ValidationError

    with _pytest.raises(ValidationError):
        OperationDriverEntry(driver_ref="X", source="bogus")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Apply-handler parity for SEARCH (existing test below)
# ---------------------------------------------------------------------------


def test_validate_handlers_invoke_searcher_self_registration():
    """The items-tier, collection-tier and catalog-tier validate handlers MUST
    each call `_self_register_searchers_into` (asset tier doesn't, by design —
    no SEARCH op for assets).  Self-registration moved apply→validate in
    #738/#747."""
    import asyncio
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        AssetRoutingConfig,
        _validate_asset_routing_config,
        _validate_catalog_routing_config,
        _validate_collection_routing_config,
        _validate_items_routing_config,
    )

    calls: list[type] = []

    def _spy(target_ops, marker_proto, **_kwargs):
        # Accept arbitrary kwargs (e.g. ``search_caps`` on the searcher
        # helper) so the spy works for both helper signatures.
        calls.append(marker_proto)

    items = ItemsRoutingConfig()
    items.operations.clear()
    coll = CollectionRoutingConfig()
    coll.operations.clear()
    asset = AssetRoutingConfig()
    asset.operations.clear()
    cat = CatalogRoutingConfig()
    cat.operations.clear()

    with patch(
        "dynastore.modules.storage.routing_config._self_register_searchers_into",
        _spy,
    ), patch(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: [],
    ):
        asyncio.run(_validate_items_routing_config(
            items, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_collection_routing_config(
            coll, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_asset_routing_config(
            asset, catalog_id=None, collection_id=None, db_resource=None,
        ))
        asyncio.run(_validate_catalog_routing_config(
            cat, catalog_id=None, collection_id=None, db_resource=None,
        ))

    from dynastore.models.protocols.entity_store import (
        CatalogStore,
        CollectionStore,
    )
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    # Each tier's validate handler invokes searcher registration once with its
    # own marker Protocol. Order matches invocation order above.
    assert calls == [
        CollectionItemsStore,
        CollectionStore,
        CatalogStore,
    ]


# ---------------------------------------------------------------------------
# Transformer self-registration parity (sister to indexer / searcher helpers)
# ---------------------------------------------------------------------------


def test_transformer_helper_picks_up_entity_transform_protocol_implementers():
    """Any registered EntityTransformProtocol implementer lands in
    operations[TRANSFORM] keyed by class name (matching indexer/searcher
    convention)."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_transformers_into,
    )

    class TransformerOne:
        async def transform_for_index(self, entity, **_): return entity
        async def restore_from_index(self, doc, **_): return doc

    class TransformerTwo:
        async def transform_for_index(self, entity, **_): return entity
        async def restore_from_index(self, doc, **_): return doc

    target_ops: dict = {}
    fake_pool = [TransformerOne(), TransformerTwo()]
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: fake_pool):
        _self_register_transformers_into(target_ops)

    ids = {e.driver_ref for e in target_ops.get(Operation.TRANSFORM, [])}
    assert ids == {"transformer_one", "transformer_two"}


def test_transformer_helper_idempotent_and_preserves_operator_entry():
    """Repeated calls are no-ops; an operator-supplied entry survives."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_transformers_into,
    )

    class CustomTransformer:
        async def transform_for_index(self, entity, **_): return entity
        async def restore_from_index(self, doc, **_): return doc

    from dynastore.modules.storage.hints import Hint
    op_entry = OperationDriverEntry(driver_ref="custom_transformer", hints={Hint.METADATA})
    target_ops: dict = {Operation.TRANSFORM: [op_entry]}

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [CustomTransformer()]):
        _self_register_transformers_into(target_ops)
        _self_register_transformers_into(target_ops)

    assert len(target_ops[Operation.TRANSFORM]) == 1
    assert target_ops[Operation.TRANSFORM][0].hints == {Hint.METADATA}


def test_transformer_helper_no_op_when_no_implementers():
    """Empty discovery → operations[TRANSFORM] stays absent (no implicit empty key)."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_transformers_into,
    )

    target_ops: dict = {}
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: []):
        _self_register_transformers_into(target_ops)
    assert target_ops.get(Operation.TRANSFORM, []) == []


# ---------------------------------------------------------------------------
# Option A regression suite (#792 / #889) — list-level operator override
# ---------------------------------------------------------------------------


def test_option_a_is_operator_managed_predicate_basic():
    """``_is_operator_managed`` returns True iff any entry in ``operations[op]``
    has ``source='operator'``.  Foundation for the list-level lock."""
    from dynastore.modules.storage.routing_config import _is_operator_managed

    op_entry = OperationDriverEntry(driver_ref="x", source="operator")
    au_entry = OperationDriverEntry(driver_ref="y", source="auto")

    assert _is_operator_managed({Operation.SEARCH: [op_entry]}, Operation.SEARCH)
    assert not _is_operator_managed({Operation.SEARCH: [au_entry]}, Operation.SEARCH)
    assert _is_operator_managed({Operation.SEARCH: [au_entry, op_entry]}, Operation.SEARCH)
    assert not _is_operator_managed({Operation.SEARCH: []}, Operation.SEARCH)
    assert not _is_operator_managed({}, Operation.SEARCH)


def test_option_a_indexer_helper_skips_operator_managed_list():
    """Indexer self-register helper is a no-op for an operation whose
    list contains any ``source='operator'`` entry (#792 / #889 — the
    deletion-comes-back symptom)."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.indexer import CollectionIndexer
    from dynastore.modules.storage.routing_config import (
        _self_register_indexers_into,
    )

    class _NewIndexer:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    target_ops: dict = {
        Operation.WRITE: [
            OperationDriverEntry(
                driver_ref="some_other_driver", source="operator",
            ),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_NewIndexer()]):
        _self_register_indexers_into(target_ops, CollectionIndexer)

    refs = {e.driver_ref for e in target_ops[Operation.WRITE]}
    assert refs == {"some_other_driver"}


def test_option_a_searcher_helper_skips_operator_managed_list():
    """Searcher self-register helper: same lock-out shape."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.entity_store import CatalogStore
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    class _NewSearcher:
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    target_ops: dict = {
        Operation.SEARCH: [
            OperationDriverEntry(driver_ref="es_cat", source="operator"),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_NewSearcher()]):
        _self_register_searchers_into(target_ops, CatalogStore)

    refs = {e.driver_ref for e in target_ops[Operation.SEARCH]}
    assert refs == {"es_cat"}


def test_option_a_store_drivers_helper_locks_per_operation():
    """``_self_register_store_drivers`` iterates op_keys; the operator lock
    must apply per-operation independently (so an operator-managed WRITE
    list locks WRITE without blocking auto-augment on READ)."""
    cfg = CollectionRoutingConfig()
    cfg.operations.clear()
    cfg.operations[Operation.WRITE] = [
        OperationDriverEntry(driver_ref="pg_core_meta", source="operator"),
    ]
    cfg.operations[Operation.READ] = []  # empty → auto-augmentable

    metadata_index = {"pg_core_meta": object(), "pg_stac_meta": object()}
    _self_register_store_drivers(cfg, metadata_index)

    write_refs = {e.driver_ref for e in cfg.operations[Operation.WRITE]}
    read_refs = {e.driver_ref for e in cfg.operations[Operation.READ]}
    # WRITE locked: pg_stac_meta NOT appended.
    assert write_refs == {"pg_core_meta"}
    # READ free: both auto-appended with source=auto.
    assert read_refs == {"pg_core_meta", "pg_stac_meta"}
    for entry in cfg.operations[Operation.READ]:
        assert entry.source == "auto"


def test_option_a_upload_helper_skips_operator_managed_list():
    """Upload self-register helper: same lock-out shape."""
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.asset_upload import AssetUploadProtocol
    from dynastore.modules.storage.routing_config import (
        _self_register_upload_into,
    )

    class _NewUploader:
        auto_register_for_routing: ClassVar = frozenset({Operation.UPLOAD})

    target_ops: dict = {
        Operation.UPLOAD: [
            OperationDriverEntry(driver_ref="gcs_upload", source="operator"),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_NewUploader()]):
        _self_register_upload_into(target_ops, AssetUploadProtocol)

    refs = {e.driver_ref for e in target_ops[Operation.UPLOAD]}
    assert refs == {"gcs_upload"}


def test_option_a_transformer_helper_skips_operator_managed_list():
    """Transformer self-register helper: same lock-out shape."""
    from unittest.mock import patch

    from dynastore.modules.storage.routing_config import (
        _self_register_transformers_into,
    )

    class _NewTransformer:
        async def transform_for_index(self, entity, **_): return entity
        async def restore_from_index(self, doc, **_): return doc

    target_ops: dict = {
        Operation.TRANSFORM: [
            OperationDriverEntry(driver_ref="pinned_tf", source="operator"),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_NewTransformer()]):
        _self_register_transformers_into(target_ops)

    refs = {e.driver_ref for e in target_ops[Operation.TRANSFORM]}
    assert refs == {"pinned_tf"}


def test_option_a_default_factory_entries_are_auto_sourced():
    """Default-factory operations entries must carry ``source='auto'``
    so a fresh boot stays augmentable under Option A.  An old default of
    ``source='operator'`` would lock auto-registration out at first read
    — the #792 deletion semantic must NOT apply to boot defaults."""
    from dynastore.modules.storage.routing_config import AssetRoutingConfig

    cfg_items = ItemsRoutingConfig.model_construct(
        operations=ItemsRoutingConfig.model_fields["operations"].default_factory(),
    )
    cfg_coll = CollectionRoutingConfig.model_construct(
        operations=CollectionRoutingConfig.model_fields["operations"].default_factory(),
    )
    cfg_asset = AssetRoutingConfig.model_construct(
        operations=AssetRoutingConfig.model_fields["operations"].default_factory(),
    )
    cfg_cat = CatalogRoutingConfig.model_construct(
        operations=CatalogRoutingConfig.model_fields["operations"].default_factory(),
    )
    for label, cfg in (
        ("ItemsRoutingConfig", cfg_items),
        ("CollectionRoutingConfig", cfg_coll),
        ("AssetRoutingConfig", cfg_asset),
        ("CatalogRoutingConfig", cfg_cat),
    ):
        for op, entries in cfg.operations.items():
            for entry in entries:
                assert entry.source == "auto", (
                    f"{label}.operations[{op}] entry '{entry.driver_ref}' "
                    f"has source={entry.source!r} — must be 'auto' for "
                    f"Option A boot-time augmentation to work"
                )


def test_option_a_fresh_construct_still_auto_augments():
    """End-to-end: constructing a config with no operator overrides
    still picks up discoverable drivers — boot defaults (source='auto')
    do not lock the helper."""
    from typing import ClassVar
    from unittest.mock import patch

    class _DiscoverableIndexer:
        is_collection_indexer: ClassVar[bool] = True
        auto_register_for_routing: ClassVar = frozenset({Operation.WRITE})

    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_DiscoverableIndexer()]):
        cfg = CollectionRoutingConfig()

    from dynastore.modules.storage.routing_config import secondary_index_entries
    index_refs = {e.driver_ref for e in secondary_index_entries(cfg.operations)}
    assert "_discoverable_indexer" in index_refs


def test_option_a_792_reproducer_search_index_lock():
    """End-to-end reproducer for #792: operator PUTs an explicit SEARCH
    list (single entry, source='operator'), then a downstream code path
    runs the search self-register helper — the missing driver MUST stay
    missing (the symptom on #792 was the missing driver re-appearing).
    """
    from typing import ClassVar
    from unittest.mock import patch

    from dynastore.models.protocols.storage_driver import (
        CollectionItemsStore,
    )
    from dynastore.modules.storage.routing_config import (
        _self_register_searchers_into,
    )

    # Driver pool: ES is operator-pinned; PG offers SEARCH opt-in but is
    # explicitly omitted from the operator's list.
    class _ItemsES:
        is_item_indexer: ClassVar[bool] = False
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    class _ItemsPG:
        is_item_indexer: ClassVar[bool] = False
        auto_register_for_routing: ClassVar = frozenset({Operation.SEARCH})

    target_ops: dict = {
        Operation.SEARCH: [
            OperationDriverEntry(
                driver_ref="items_elasticsearch_driver",
                source="operator",
            ),
        ],
    }
    with patch("dynastore.tools.discovery.get_protocols",
               lambda proto: [_ItemsES(), _ItemsPG()]):
        _self_register_searchers_into(target_ops, CollectionItemsStore)

    refs = {e.driver_ref for e in target_ops[Operation.SEARCH]}
    # The exact #792 symptom — missing driver re-appearing — does not occur.
    assert refs == {"items_elasticsearch_driver"}
