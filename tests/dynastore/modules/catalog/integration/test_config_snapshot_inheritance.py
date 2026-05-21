"""Integration tests for the catalog defaults snapshot (#1079 option c).

Proves the anti-drift property: once a catalog is created, a later change to a
platform/code default no longer re-resolves into that catalog (it reads its
creation-time snapshot), while a *newly* created catalog does pick up the new
default. This is the divergence #1079 describes, fixed at the source.
"""

import asyncio

import pytest
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.storage.driver_config import (
    ItemsWritePolicy,
    WriteConflictPolicy,
)


@pytest.mark.asyncio
async def test_catalog_snapshot_shadows_later_platform_default_change(
    app_lifespan, catalog_obj, catalog_id
):
    """A platform-default change after creation must not leak into the catalog.

    The catalog's creation-time snapshot is the inheritance base, so bumping the
    platform default for a snapshotted value-config re-resolves only for
    catalogs created *after* the bump — existing catalogs keep their frozen
    default. Without the snapshot, the waterfall would propagate the new
    platform value into the existing catalog on the next read (#1079).
    """
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)

    default_oc = ItemsWritePolicy().on_conflict
    other_oc = next(m for m in WriteConflictPolicy if m != default_oc)

    await catalogs.delete_catalog(catalog_id, force=True)
    await asyncio.sleep(1)

    cat2_id = f"{catalog_id}b"
    await catalogs.delete_catalog(cat2_id, force=True)
    await asyncio.sleep(1)

    try:
        # 1. Create the catalog → snapshot captures the current (code) default.
        await catalogs.create_catalog(catalog_obj)
        before = await configs.get_config(ItemsWritePolicy, catalog_id)
        assert before.on_conflict == default_oc

        # 2. Change the PLATFORM default for this config to a different value.
        await configs.set_config(
            ItemsWritePolicy,
            ItemsWritePolicy(on_conflict=other_oc),
            check_immutability=False,
        )

        # 3. The existing catalog still resolves to its snapshot value — the
        #    platform change is shadowed (the #1079 anti-drift property).
        after = await configs.get_config(ItemsWritePolicy, catalog_id)
        assert after.on_conflict == default_oc, (
            "existing catalog re-resolved to the new platform default — "
            "snapshot did not shadow the change"
        )

        # 4. Positive control: a catalog created AFTER the change captures the
        #    new platform default in its own snapshot.
        cat2 = catalog_obj.model_copy(update={"id": cat2_id})
        await catalogs.create_catalog(cat2)
        fresh = await configs.get_config(ItemsWritePolicy, cat2_id)
        assert fresh.on_conflict == other_oc, (
            "newly created catalog did not capture the updated platform default"
        )
    finally:
        # Reset the platform tier so the override does not leak to other tests.
        await configs.delete_config(ItemsWritePolicy)
        await catalogs.delete_catalog(catalog_id, force=True)
        await catalogs.delete_catalog(cat2_id, force=True)


@pytest.mark.asyncio
async def test_collection_inherits_catalog_snapshot_not_live_default(
    app_lifespan, catalog_obj, catalog_id, collection_obj, collection_id
):
    """A collection inherits the catalog's frozen snapshot, not the live default.

    A collection that sets no override of its own reads the catalog snapshot
    base; a later platform-default bump does not re-resolve into it.
    """
    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)

    default_oc = ItemsWritePolicy().on_conflict
    other_oc = next(m for m in WriteConflictPolicy if m != default_oc)

    await catalogs.delete_catalog(catalog_id, force=True)
    await asyncio.sleep(1)

    try:
        await catalogs.create_catalog(catalog_obj)
        await catalogs.create_collection(catalog_id, collection_obj)

        # Bump the platform default after the catalog (and its snapshot) exist.
        await configs.set_config(
            ItemsWritePolicy,
            ItemsWritePolicy(on_conflict=other_oc),
            check_immutability=False,
        )

        resolved = await configs.get_config(
            ItemsWritePolicy, catalog_id, collection_id
        )
        assert resolved.on_conflict == default_oc, (
            "collection re-resolved to the new platform default instead of the "
            "catalog snapshot base"
        )
    finally:
        await configs.delete_config(ItemsWritePolicy)
        await catalogs.delete_catalog(catalog_id, force=True)
