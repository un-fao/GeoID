"""private_catalog preset (#847) — PG-only envelopes + items-tier private ES (#1047)."""
from __future__ import annotations

from dynastore.modules.storage.presets import get_preset
from dynastore.modules.storage.routing_config import (
    AssetRoutingConfig,
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    _items_routing_has_private_driver,
)


def test_private_catalog_preset_registered():
    p = get_preset("private_catalog")
    assert p.name == "private_catalog"
    assert p.description, "preset must carry a non-empty description"


def test_private_catalog_bundle_shape():
    bundle = get_preset("private_catalog").build("cat-priv")
    assert isinstance(bundle.catalog_routing, CatalogRoutingConfig)
    assert isinstance(bundle.collection_template, CollectionRoutingConfig)
    assert isinstance(bundle.items_template, ItemsRoutingConfig)
    assert isinstance(bundle.asset_template, AssetRoutingConfig)
    assert bundle.audience_configs == {}


def test_private_catalog_catalog_routing_is_pg_only():
    """Catalog envelopes are PG-only for private catalogs — no ES private index."""
    bundle = get_preset("private_catalog").build("cat-priv")
    cat_refs = [
        e.driver_ref
        for entries in bundle.catalog_routing.operations.values()
        for e in entries
    ]
    assert "catalog_postgresql_driver" in cat_refs
    assert "catalog_elasticsearch_private_driver" not in cat_refs
    assert "catalog_elasticsearch_driver" not in cat_refs


def test_private_catalog_collection_routing_is_pg_only():
    """Collection envelopes are PG-only for private catalogs — no ES private index."""
    bundle = get_preset("private_catalog").build("cat-priv")
    coll_refs = [
        e.driver_ref
        for entries in bundle.collection_template.operations.values()
        for e in entries
    ]
    assert "collection_postgresql_driver" in coll_refs
    assert "collection_elasticsearch_private_driver" not in coll_refs
    assert "collection_elasticsearch_driver" not in coll_refs


def test_private_catalog_items_pins_private_driver():
    """Items tier pins the private ES driver."""
    bundle = get_preset("private_catalog").build("cat-priv")
    assert _items_routing_has_private_driver(bundle.items_template)
    items_refs = [
        e.driver_ref
        for entries in bundle.items_template.operations.values()
        for e in entries
    ]
    assert "items_elasticsearch_private_driver" in items_refs
    assert "items_elasticsearch_driver" not in items_refs


def test_private_catalog_asset_routing_is_pg_only():
    """Assets are PG-only for private catalogs — they must NOT inherit the
    public asset ES driver and leak into public search (Part B)."""
    bundle = get_preset("private_catalog").build("cat-priv")
    asset_refs = [
        e.driver_ref
        for entries in bundle.asset_template.operations.values()
        for e in entries
    ]
    assert "asset_postgresql_driver" in asset_refs
    assert "asset_elasticsearch_driver" not in asset_refs
    assert "asset_elasticsearch_private_driver" not in asset_refs


def test_private_catalog_asset_routing_has_write_and_read():
    """The PG-only asset routing must cover both WRITE and READ; UPLOAD is
    left to validation-time auto-augmentation (not forced to an ES driver)."""
    from dynastore.modules.storage.routing_config import Operation

    bundle = get_preset("private_catalog").build("cat-priv")
    ops = bundle.asset_template.operations
    assert Operation.WRITE in ops
    assert Operation.READ in ops
