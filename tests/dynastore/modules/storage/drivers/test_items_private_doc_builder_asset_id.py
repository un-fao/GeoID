"""Private items driver — ``asset_id`` projection (Part C).

``build_tenant_feature_doc`` strips all ``_``-prefixed keys, which used to
drop the ingestion-context asset identity. These tests pin the explicit
top-level ``asset_id`` projection (sourced from the write-context arg, with
fallback to the source's ``_asset_id``) and confirm ``TENANT_FEATURE_MAPPING``
declares the keyword field so it indexes deterministically.
"""
from __future__ import annotations

from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
    build_tenant_feature_doc,
)
from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
    TENANT_FEATURE_MAPPING,
)


def test_mapping_declares_asset_id_keyword():
    props = TENANT_FEATURE_MAPPING["properties"]
    assert props["asset_id"] == {"type": "keyword"}


def test_asset_id_from_explicit_arg_is_projected_top_level():
    item = {
        "id": "geo-1",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"name": "x"},
    }
    doc = build_tenant_feature_doc(
        item, catalog_id="cat", collection_id="col", asset_id="asset-123",
    )
    assert doc["asset_id"] == "asset-123"
    # asset_id is top-level, not buried in (and not stripped from) properties.
    assert "asset_id" not in doc.get("properties", {})


def test_asset_id_falls_back_to_underscore_source_key():
    item = {
        "id": "geo-2",
        "_asset_id": "asset-from-src",
        "properties": {"name": "y"},
    }
    doc = build_tenant_feature_doc(
        item, catalog_id="cat", collection_id="col",
    )
    assert doc["asset_id"] == "asset-from-src"


def test_explicit_arg_overrides_source_underscore_key():
    item = {"id": "geo-3", "_asset_id": "src", "properties": {}}
    doc = build_tenant_feature_doc(
        item, catalog_id="cat", collection_id="col", asset_id="explicit",
    )
    assert doc["asset_id"] == "explicit"


def test_no_asset_id_omits_the_field():
    item = {"id": "geo-4", "properties": {"name": "z"}}
    doc = build_tenant_feature_doc(
        item, catalog_id="cat", collection_id="col",
    )
    assert "asset_id" not in doc


def test_asset_id_is_stringified():
    item = {"id": "geo-5", "properties": {}}
    doc = build_tenant_feature_doc(
        item, catalog_id="cat", collection_id="col", asset_id=42,
    )
    assert doc["asset_id"] == "42"
