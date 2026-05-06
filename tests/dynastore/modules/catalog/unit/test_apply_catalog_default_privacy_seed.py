"""Cycle E.2.c / F.0d slice — pin the create-time seed flow.

``apply_catalog_default_privacy_seed`` is invoked from
``CollectionService.create_collection`` to seed a freshly-created
collection's privacy state from the catalog's
``CatalogPrivacy.collection_defaults.is_private``.

These tests pin the helper's contract:
- catalog policy missing → no-op (False return)
- catalog policy public (``is_private=False``) → no-op (False return)
- catalog policy private → writes BOTH ``ItemsRoutingConfig`` (with
  the private items driver pinned) AND ``CollectionPrivacy`` (with
  ``is_private=True``), in that exact order so the cascade validator
  on the second write finds the private driver already pinned
- ConfigsProtocol unavailable → no-op (False return), defer to the
  cascade validator on the operator's next config write
- Exception during catalog-policy lookup → no-op (False return),
  same defer-to-cascade behaviour
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.catalog.catalog_config import (
    CatalogPrivacy,
    CollectionPrivacy,
    CollectionPrivacyDefaults,
    apply_catalog_default_privacy_seed,
)
from dynastore.modules.storage.routing_config import (
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    Operation,
)


def _configs_returning(policy_or_none) -> MagicMock:
    proto = MagicMock()
    proto.get_config = AsyncMock(return_value=policy_or_none)
    proto.set_config = AsyncMock(return_value=None)
    return proto


def _private_default_catalog_privacy() -> CatalogPrivacy:
    return CatalogPrivacy(
        collection_defaults=CollectionPrivacyDefaults(is_private=True),
    )


@pytest.mark.asyncio
async def test_seed_noop_when_configs_protocol_is_none():
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=None,
    )
    assert applied is False


@pytest.mark.asyncio
async def test_seed_noop_when_catalog_has_no_policy_row():
    """Catalog default is implicitly public (``is_private=False``) —
    nothing to seed."""
    proto = _configs_returning(None)
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_noop_when_policy_is_public():
    proto = _configs_returning(CatalogPrivacy())  # default is_private=False
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_noop_when_get_config_raises():
    """Transient ConfigsProtocol failure → defer to the cascade
    validator on the operator's next config write."""
    proto = MagicMock()
    proto.get_config = AsyncMock(side_effect=RuntimeError("transient"))
    proto.set_config = AsyncMock()
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is False
    proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_seed_writes_three_configs_in_cascade_satisfying_order_when_private():
    """Cascade-satisfying order is load-bearing for the FIRST TWO writes:
    ``ItemsRoutingConfig`` MUST land BEFORE
    ``CollectionPrivacy(is_private=True)`` so the cascade validator
    on the second write finds the private driver already pinned.

    Cycle E.2.c slice 3 added a third write — ``CollectionRoutingConfig``
    pinning ``collection_elasticsearch_private_driver`` — independent
    of the cascade gate (it covers collection-envelope routing, not
    items routing); we put it LAST for clarity.
    """
    proto = _configs_returning(_private_default_catalog_privacy())
    applied = await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    assert applied is True

    # Three set_config calls — order matters for the first two
    # (cascade), incidental for the third.
    assert proto.set_config.await_count == 3
    classes_in_order = [
        proto.set_config.await_args_list[i].args[0]
        for i in range(3)
    ]
    assert classes_in_order[0] is ItemsRoutingConfig, (
        "Items routing must land FIRST so the cascade validator on the "
        "CollectionPrivacy apply finds the private driver already pinned."
    )
    assert classes_in_order[1] is CollectionPrivacy
    assert classes_in_order[2] is CollectionRoutingConfig, (
        "Collection-envelope routing seed (slice 3) lands last for "
        "clarity — independent of the cascade gate."
    )

    # The items-routing payload must pin the private items driver in WRITE
    # (or any operation — cascade gate is "in any operation").
    items_routing: ItemsRoutingConfig = (
        proto.set_config.await_args_list[0].args[1]
    )
    items_pinned = {
        e.driver_ref
        for entries in items_routing.operations.values()
        for e in entries
    }
    assert "items_elasticsearch_private_driver" in items_pinned
    # And the public driver is NOT pinned in any operation — privacy
    # safety: the seed must NOT introduce a leak path to the public
    # per-tenant index.
    assert "items_elasticsearch_driver" not in items_pinned

    # The CollectionPrivacy payload must have is_private=True.
    coll_payload: CollectionPrivacy = (
        proto.set_config.await_args_list[1].args[1]
    )
    assert coll_payload.is_private is True

    # The collection-routing payload must pin the collection-envelope
    # private driver — same privacy-safety property: the public
    # collection driver must NOT be pinned in this seed.
    coll_routing: CollectionRoutingConfig = (
        proto.set_config.await_args_list[2].args[1]
    )
    coll_pinned = {
        e.driver_ref
        for entries in coll_routing.operations.values()
        for e in entries
    }
    assert "collection_elasticsearch_private_driver" in coll_pinned
    assert "collection_elasticsearch_driver" not in coll_pinned, (
        "Privacy safety: seed must NOT pin the PUBLIC collection ES "
        "driver — that would put the envelope into the shared "
        "{prefix}-collections index."
    )


@pytest.mark.asyncio
async def test_seed_passes_catalog_and_collection_ids_through_to_set_config():
    proto = _configs_returning(_private_default_catalog_privacy())
    await apply_catalog_default_privacy_seed(
        "cat-x", "col-y", configs=proto,
    )
    for call in proto.set_config.await_args_list:
        kwargs = call.kwargs
        assert kwargs["catalog_id"] == "cat-x"
        assert kwargs["collection_id"] == "col-y"


@pytest.mark.asyncio
async def test_seed_routing_payload_has_postgresql_in_write_for_durability():
    """WRITE without PG = no SoR.  The seed routing pins PG WRITE
    (FATAL) + private ES (ASYNC + OUTBOX) so a private collection
    still has a durable write target."""
    proto = _configs_returning(_private_default_catalog_privacy())
    await apply_catalog_default_privacy_seed(
        "cat-a", "col-a", configs=proto,
    )
    routing_payload: ItemsRoutingConfig = proto.set_config.await_args_list[0].args[1]
    write_entries = routing_payload.operations.get(Operation.WRITE, [])
    write_drivers = [e.driver_ref for e in write_entries]
    assert "items_postgresql_driver" in write_drivers, (
        "PG must be the durable WRITE target for a private collection — "
        "without it the seed routing has no source-of-truth."
    )
