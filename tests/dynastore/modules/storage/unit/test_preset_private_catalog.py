"""private_catalog preset (#847) — pins three private drivers + zero audiences."""
from __future__ import annotations

from dynastore.modules.storage.presets import get_preset
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
    _catalog_routing_has_private_driver,
    _collection_routing_has_private_driver,
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
    assert bundle.audience_configs == {}


def test_private_catalog_pins_three_private_drivers():
    bundle = get_preset("private_catalog").build("cat-priv")
    assert _catalog_routing_has_private_driver(bundle.catalog_routing)
    assert _collection_routing_has_private_driver(bundle.collection_template)
    assert _items_routing_has_private_driver(bundle.items_template)


def test_private_catalog_uses_expected_driver_ids():
    bundle = get_preset("private_catalog").build("cat-priv")
    cat_refs = [
        e.driver_ref
        for entries in bundle.catalog_routing.operations.values()
        for e in entries
    ]
    assert "catalog_elasticsearch_private_driver" in cat_refs
    assert "catalog_postgresql_driver" in cat_refs
    assert "catalog_elasticsearch_driver" not in cat_refs

    items_refs = [
        e.driver_ref
        for entries in bundle.items_template.operations.values()
        for e in entries
    ]
    assert "items_elasticsearch_private_driver" in items_refs
    assert "items_elasticsearch_driver" not in items_refs
