"""#733 — routing-config-driven privacy cascade.

Cascade rule:
    collection-private REQUIRES items-private. Reverse direction allowed.
    Items-public + collection-private is rejected.

Detection (no more ``CollectionPrivacy.is_private`` flag — privacy lives
in the routing configs themselves):
- Items privacy:      presence of ``items_elasticsearch_private_driver`` in
                       any operation of ``ItemsRoutingConfig.operations``.
- Collection privacy: presence of ``collection_elasticsearch_private_driver``
                       in any operation of ``CollectionRoutingConfig.operations``.

Enforcement points (validate handlers on the routing configs themselves —
pure intra-routing invariant):
- On ``CollectionRoutingConfig``: when the new routing pins the
  collection-private driver and the sibling items routing lacks the
  items-private driver, reject.
- On ``ItemsRoutingConfig``: when the new routing drops the items-private
  driver and the sibling collection routing still pins the collection-
  private driver, reject.

The handlers no-op when ConfigsProtocol discovery is unavailable (early
test fixtures, partial deployments) — the OTHER side of the cascade
catches the violation on its next write.
"""
from __future__ import annotations

from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
    _collection_routing_has_private_driver,
    _enforce_collection_routing_privacy_cascade,
    _enforce_items_routing_privacy_cascade,
    _items_routing_has_private_driver,
    _validate_catalog_routing_config,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _items_routing_with_private(*, operation: str = Operation.INDEX) -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            operation: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                ),
            ],
        },
    )


def _items_routing_without_private() -> ItemsRoutingConfig:
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _collection_routing_with_private(
    *, operation: str = Operation.INDEX,
) -> CollectionRoutingConfig:
    return CollectionRoutingConfig(
        operations={
            operation: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                ),
            ],
        },
    )


def _collection_routing_without_private() -> CollectionRoutingConfig:
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _catalog_routing_with_private(
    *, operation: str = Operation.INDEX,
) -> CatalogRoutingConfig:
    return CatalogRoutingConfig(
        operations={
            operation: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_private_driver",
                    on_failure=FailurePolicy.OUTBOX,
                    write_mode=WriteMode.ASYNC,
                ),
            ],
        },
    )


def _catalog_routing_without_private() -> CatalogRoutingConfig:
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
        },
    )


def _stub_configs_protocol(returned_config: Optional[object]) -> MagicMock:
    """A get_protocol(ConfigsProtocol) stub that returns ``returned_config``
    for every ``get_config(...)`` call regardless of class arg."""
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=returned_config)
    return proto


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def test_items_has_private_driver_detects_pinned_entry():
    assert _items_routing_has_private_driver(_items_routing_with_private()) is True


def test_items_has_private_driver_returns_false_when_absent():
    assert _items_routing_has_private_driver(_items_routing_without_private()) is False


def test_items_has_private_driver_finds_entry_in_any_operation():
    for op in (Operation.WRITE, Operation.READ, Operation.SEARCH, Operation.INDEX):
        routing = _items_routing_with_private(operation=op)
        assert _items_routing_has_private_driver(routing) is True, (
            f"private driver in operations[{op}] must satisfy the cascade gate"
        )


def test_collection_has_private_driver_detects_pinned_entry():
    assert _collection_routing_has_private_driver(
        _collection_routing_with_private(),
    ) is True


def test_collection_has_private_driver_returns_false_when_absent():
    assert _collection_routing_has_private_driver(
        _collection_routing_without_private(),
    ) is False


# ---------------------------------------------------------------------------
# _enforce_collection_routing_privacy_cascade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_collection_cascade_passes_when_collection_routing_is_public():
    """Public collection routing under a public catalog imposes no
    constraint on items routing — the only fetch is the catalog-tier
    guard (#960) check, which returns a non-CatalogRoutingConfig stub
    and therefore short-circuits as ``not pinned private``."""
    cfg = _collection_routing_without_private()
    proto = _stub_configs_protocol(_items_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_routing_privacy_cascade(
            cfg, "cat-a", "col-a", None,
        )
    # Exactly one call — the catalog-tier guard fetch. The sibling items
    # routing fetch is short-circuited because the new collection routing
    # is public.
    proto.get_config.assert_called_once()
    args, kwargs = proto.get_config.call_args
    assert args[0] is CatalogRoutingConfig
    assert kwargs == {"catalog_id": "cat-a", "collection_id": None}


@pytest.mark.asyncio
async def test_collection_cascade_passes_when_items_routing_has_private_driver():
    cfg = _collection_routing_with_private()
    proto = _stub_configs_protocol(_items_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_routing_privacy_cascade(
            cfg, "cat-a", "col-a", None,
        )
    proto.get_config.assert_called_once()


@pytest.mark.asyncio
async def test_collection_routing_pinning_private_without_items_private_rejected():
    """Regression — applying a CollectionRoutingConfig with the
    collection-private driver while items routing lacks the items-private
    driver MUST raise ValueError citing the routing-config mechanism."""
    cfg = _collection_routing_with_private()
    proto = _stub_configs_protocol(_items_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        with pytest.raises(
            ValueError,
            match=r"Privacy cascade violation.*collection_elasticsearch_private_driver",
        ):
            await _enforce_collection_routing_privacy_cascade(
                cfg, "cat-a", "col-a", None,
            )


@pytest.mark.asyncio
async def test_collection_cascade_noop_at_platform_scope():
    """Apply at platform/catalog scope (no collection_id) bypasses cascade."""
    cfg = _collection_routing_with_private()
    proto = _stub_configs_protocol(_items_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_collection_routing_privacy_cascade(cfg, None, None, None)
        await _enforce_collection_routing_privacy_cascade(cfg, "cat-a", None, None)
    proto.get_config.assert_not_called()


@pytest.mark.asyncio
async def test_collection_cascade_noop_when_configs_protocol_unavailable():
    """ConfigsProtocol discovery unavailable → cascade defers."""
    cfg = _collection_routing_with_private()
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=None,
    ):
        # No exception — the OTHER side enforces.
        await _enforce_collection_routing_privacy_cascade(
            cfg, "cat-a", "col-a", None,
        )


# ---------------------------------------------------------------------------
# _enforce_items_routing_privacy_cascade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_items_cascade_passes_when_routing_keeps_private_driver():
    """Routing still pins the private driver — cascade trivially OK."""
    routing = _items_routing_with_private()
    proto = _stub_configs_protocol(_collection_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(
            routing, "cat-a", "col-a", None,
        )
    # Routing has the private driver → we never need to look up the collection.
    proto.get_config.assert_not_called()


@pytest.mark.asyncio
async def test_items_cascade_passes_when_collection_routing_is_public():
    routing = _items_routing_without_private()
    proto = _stub_configs_protocol(_collection_routing_without_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(
            routing, "cat-a", "col-a", None,
        )


@pytest.mark.asyncio
async def test_items_routing_drops_private_while_collection_still_private_rejected():
    """Regression — applying an ItemsRoutingConfig without the items-
    private driver while sibling CollectionRoutingConfig still pins the
    collection-private driver MUST raise."""
    routing = _items_routing_without_private()
    proto = _stub_configs_protocol(_collection_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        with pytest.raises(
            ValueError,
            match=r"Privacy cascade violation.*items_elasticsearch_private_driver",
        ):
            await _enforce_items_routing_privacy_cascade(
                routing, "cat-a", "col-a", None,
            )


@pytest.mark.asyncio
async def test_items_cascade_noop_at_platform_scope():
    routing = _items_routing_without_private()
    proto = _stub_configs_protocol(_collection_routing_with_private())
    with patch(
        "dynastore.tools.discovery.get_protocol", return_value=proto,
    ):
        await _enforce_items_routing_privacy_cascade(routing, None, None, None)
        await _enforce_items_routing_privacy_cascade(routing, "cat-a", None, None)
    proto.get_config.assert_not_called()


# ---------------------------------------------------------------------------
# Registration regression — both handlers must be wired at module-load
# ---------------------------------------------------------------------------


def test_cascade_handlers_registered_on_both_routing_configs():
    items_handlers = ItemsRoutingConfig.get_validate_handlers()
    coll_handlers = CollectionRoutingConfig.get_validate_handlers()
    assert _enforce_items_routing_privacy_cascade in items_handlers
    assert _enforce_collection_routing_privacy_cascade in coll_handlers


# ---------------------------------------------------------------------------
# apply_catalog_default_routing_seed — happy-path regression
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Catalog-tier cascade (#960 scope 4)
#
# Rule: pinning ``catalog_elasticsearch_private_driver`` in CatalogRouting
# requires every collection under the catalog to already have BOTH
# items-private AND collection-private pinned. Symmetrically, dropping
# privacy on items/collection routing under a catalog that's already pinned
# private is rejected — the catalog envelope would leak through the public
# items/collection paths.
# ---------------------------------------------------------------------------


def _make_collections_proto_listing(collection_ids: list[str]) -> MagicMock:
    """Stub for CollectionsProtocol.list_collections — returns minimal
    objects with ``.id`` attributes."""
    proto = MagicMock()

    async def _list(catalog_id: str, **_kwargs):  # noqa: ARG001
        return [MagicMock(id=cid) for cid in collection_ids]

    proto.list_collections = AsyncMock(side_effect=_list)
    return proto


def _make_configs_proto_for_descendants(
    items_by_coll: dict[str, ItemsRoutingConfig],
    colls_by_coll: dict[str, CollectionRoutingConfig],
) -> MagicMock:
    """Stub for ConfigsProtocol.get_config keyed on (cls, collection_id)."""
    proto = MagicMock()

    async def _get(cls, *, catalog_id=None, collection_id=None, **_kwargs):  # noqa: ARG001
        if cls is ItemsRoutingConfig:
            return items_by_coll.get(collection_id)
        if cls is CollectionRoutingConfig:
            return colls_by_coll.get(collection_id)
        return None

    proto.get_config = AsyncMock(side_effect=_get)
    return proto


def _patch_protocols(configs_proto, collections_proto):
    """Return a context manager that patches get_protocol to dispatch by
    protocol class name (ConfigsProtocol → configs_proto; everything else
    → collections_proto)."""
    from contextlib import contextmanager

    @contextmanager
    def _cm():
        def _resolve(proto_cls):
            return (
                configs_proto
                if "ConfigsProtocol" in proto_cls.__name__
                else collections_proto
            )

        with patch(
            "dynastore.tools.discovery.get_protocol",
            side_effect=_resolve,
        ):
            yield

    return _cm()


@pytest.mark.asyncio
async def test_catalog_validator_rejects_private_pin_with_public_descendant():
    """Pinning catalog-private must reject if any descendant collection has
    public items/collection routing — would leak the envelope."""
    cfg = _catalog_routing_with_private()
    configs = _make_configs_proto_for_descendants(
        items_by_coll={"col-A": _items_routing_without_private()},
        colls_by_coll={"col-A": _collection_routing_without_private()},
    )
    collections = _make_collections_proto_listing(["col-A"])
    with _patch_protocols(configs, collections):
        with pytest.raises(
            ValueError,
            match=r"Catalog privacy cascade.*col-A",
        ):
            await _validate_catalog_routing_config(cfg, "cat-1", None, None)


@pytest.mark.asyncio
async def test_catalog_validator_accepts_private_pin_with_private_descendants():
    """Pinning catalog-private + every descendant already private → accept."""
    cfg = _catalog_routing_with_private()
    configs = _make_configs_proto_for_descendants(
        items_by_coll={"col-A": _items_routing_with_private()},
        colls_by_coll={"col-A": _collection_routing_with_private()},
    )
    collections = _make_collections_proto_listing(["col-A"])
    with _patch_protocols(configs, collections):
        # Should not raise — note `from None` re-raise pattern not needed
        # because cascade-private branch returns cleanly when satisfied.
        await _validate_catalog_routing_config(cfg, "cat-1", None, None)


@pytest.mark.asyncio
async def test_catalog_validator_accepts_private_pin_on_empty_catalog():
    """Empty catalog (no collections yet) accepts the private pin —
    descendants will be enforced as they are created via the bidirectional
    items/collection guards."""
    cfg = _catalog_routing_with_private()
    configs = _make_configs_proto_for_descendants(
        items_by_coll={}, colls_by_coll={},
    )
    collections = _make_collections_proto_listing([])
    with _patch_protocols(configs, collections):
        await _validate_catalog_routing_config(cfg, "cat-1", None, None)


@pytest.mark.asyncio
async def test_catalog_validator_public_pin_skips_descendant_scan():
    """Public catalog routing imposes no descendant constraint — no scan."""
    cfg = _catalog_routing_without_private()
    configs = _make_configs_proto_for_descendants(
        items_by_coll={"col-A": _items_routing_without_private()},
        colls_by_coll={"col-A": _collection_routing_without_private()},
    )
    collections = _make_collections_proto_listing(["col-A"])
    with _patch_protocols(configs, collections):
        await _validate_catalog_routing_config(cfg, "cat-1", None, None)
    collections.list_collections.assert_not_called()


@pytest.mark.asyncio
async def test_items_cascade_rejects_public_under_private_catalog():
    """Writing public items routing under a catalog already pinned private
    is rejected (envelope would still be private, but item geometry leaks
    via the public items index)."""
    routing = _items_routing_without_private()
    # collection routing is public too (not the failure trigger here)
    configs = _make_configs_proto_for_descendants(
        items_by_coll={}, colls_by_coll={},
    )
    # Catalog routing get for the parent catalog — pinned private.
    async def _get(cls, *, catalog_id=None, collection_id=None, **_kwargs):  # noqa: ARG001
        if cls is CatalogRoutingConfig and collection_id is None:
            return _catalog_routing_with_private()
        if cls is CollectionRoutingConfig:
            return _collection_routing_without_private()
        return None
    configs.get_config = AsyncMock(side_effect=_get)
    with patch("dynastore.tools.discovery.get_protocol", return_value=configs):
        with pytest.raises(
            ValueError,
            match=r"under private catalog",
        ):
            await _enforce_items_routing_privacy_cascade(
                routing, "cat-1", "col-A", None,
            )


@pytest.mark.asyncio
async def test_collection_cascade_rejects_public_under_private_catalog():
    """Writing public collection routing under a catalog already pinned
    private is rejected."""
    routing = _collection_routing_without_private()
    configs = MagicMock()

    async def _get(cls, *, catalog_id=None, collection_id=None, **_kwargs):  # noqa: ARG001
        if cls is CatalogRoutingConfig and collection_id is None:
            return _catalog_routing_with_private()
        if cls is ItemsRoutingConfig:
            return _items_routing_without_private()
        return None
    configs.get_config = AsyncMock(side_effect=_get)
    with patch("dynastore.tools.discovery.get_protocol", return_value=configs):
        with pytest.raises(
            ValueError,
            match=r"under private catalog",
        ):
            await _enforce_collection_routing_privacy_cascade(
                routing, "cat-1", "col-A", None,
            )


# ---------------------------------------------------------------------------
# apply_catalog_default_routing_seed — happy-path regression
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_catalog_default_routing_seed_writes_both_templates():
    """Happy path — CatalogRoutingTemplates.collection_defaults with both templates
    set ⇒ helper writes both per-collection configs."""
    from dynastore.modules.catalog.catalog_config import (
        CatalogRoutingTemplates,
        _build_private_collection_routing,
        _build_private_items_routing,
        apply_catalog_default_routing_seed,
    )
    from dynastore.modules.storage.routing_config import CatalogRoutingDefaults

    policy = CatalogRoutingTemplates(
        collection_defaults=CatalogRoutingDefaults(
            items_routing=_build_private_items_routing(),
            collection_routing=_build_private_collection_routing(),
        ),
    )
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=policy)
    proto.set_config = AsyncMock()

    applied = await apply_catalog_default_routing_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is True
    assert proto.set_config.await_count == 2
    classes_written = [
        call.args[0] for call in proto.set_config.await_args_list
    ]
    assert classes_written == [ItemsRoutingConfig, CollectionRoutingConfig]
