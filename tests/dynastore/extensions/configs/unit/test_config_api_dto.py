"""Tests for the reshaped composed-config DTOs.

Asserts that the new DTOs carry no legacy envelope keys (``class_key``,
``value``, ``source``, ``resolved_drivers``) and that the nested
``configs`` tree + optional ``meta`` field roundtrip through pydantic
cleanly.
"""

from dynastore.extensions.configs.config_api_dto import (
    CatalogConfigResponse,
    CollectionConfigResponse,
    DriverRef,
    PatchConfigBody,
    PlatformConfigResponse,
)


def test_driver_ref_defaults():
    ref = DriverRef(driver_id="catalog_core_postgresql_driver",
                    config_ref="catalog_core_postgresql_driver")
    assert ref.on_failure == "fatal"
    assert ref.write_mode == "sync"


def test_driver_ref_null_config_ref_is_allowed():
    ref = DriverRef(driver_id="SomeDriver")
    assert ref.config_ref is None


def test_platform_response_defaults_slim():
    r = PlatformConfigResponse()
    assert r.scope == "platform"
    assert r.configs == {}
    assert r.meta is None
    # ``categories`` and ``routing_resolution`` were retired in Cycle C.
    assert not hasattr(r, "categories")
    assert not hasattr(r, "routing_resolution")


def test_platform_response_no_legacy_keys():
    r = PlatformConfigResponse(configs={"platform": {"web": {"WebConfig": {"brand_name": "x"}}}})
    dumped = r.model_dump()
    # The composed tree must not carry any of the old envelope keys.
    flat = str(dumped)
    assert "class_key" not in flat
    assert "resolved_drivers" not in flat


def test_catalog_response_nested_tree_roundtrip():
    r = CatalogConfigResponse(
        catalog_id="cat_1",
        configs={
            "storage": {
                "routing": {
                    "catalog_routing_config": {
                        "enabled": True,
                        "operations": {
                            "WRITE": [
                                {"driver_id": "catalog_core_postgresql_driver",
                                 "config_ref": "catalog_core_postgresql_driver",
                                 "on_failure": "fatal",
                                 "write_mode": "sync"}
                            ],
                        },
                    },
                },
                "drivers": {
                    "catalog": {"catalog_core_postgresql_driver": {"enabled": True}},
                },
            },
        },
    )
    assert r.catalog_id == "cat_1"
    assert r.meta is None
    assert "routing" in r.configs["storage"]


def test_collection_response_carries_ids():
    r = CollectionConfigResponse(catalog_id="cat", collection_id="coll", configs={})
    assert r.catalog_id == "cat"
    assert r.collection_id == "coll"


def test_meta_hierarchical_payload():
    """Cycle B: ``meta`` mirrors the configs tree shape.  Each leaf is
    a plain dict carrying ``{field_docs}`` or ``{json_schema}``."""
    r = CatalogConfigResponse(
        catalog_id="cat",
        configs={},
        meta={
            "platform": {
                "web": {
                    "web_config": {
                        "field_docs": {"brand_name": "Display name."},
                    },
                },
            },
        },
    )
    assert r.meta is not None
    assert r.meta["platform"]["web"]["web_config"]["field_docs"]["brand_name"] == "Display name."


def test_patch_body_accepts_null_for_delete():
    body = PatchConfigBody.model_validate({"WebConfig": {"brand_name": "x"}, "StatsConfig": None})
    assert body.root["WebConfig"] == {"brand_name": "x"}
    assert body.root["StatsConfig"] is None
