from dynastore.extensions.configs.config_api_dto import (
    ResolvedDriverEntry,
    ConfigEntry,
    ConfigPage,
    CollectionConfigResponse,
    CatalogConfigResponse,
    PlatformConfigResponse,
)


def test_config_page_serializes_none_items():
    page = ConfigPage(category="collections", total=100, page=1, page_size=15, links=[])
    data = page.model_dump()
    assert data["items"] is None
    assert data["total"] == 100
    assert data["page_size"] == 15


def test_config_page_with_items():
    page = ConfigPage(
        category="records", total=3, page=1, page_size=15, links=[], items=[{"id": "a"}]
    )
    assert page.items is not None
    assert len(page.items) == 1


def test_config_entry_source_values():
    entry = ConfigEntry(
        class_key="CollectionRoutingConfig", value={"enabled": True}, source="catalog"
    )
    assert entry.source == "catalog"
    assert entry.resolved_drivers is None


def test_config_entry_with_resolved_drivers():
    driver = ResolvedDriverEntry(
        driver_id="CollectionPostgresqlDriver",
        on_failure="fatal",
        write_mode="sync",
        config_class_key="CollectionPostgresqlDriverConfig",
        config={"enabled": True},
    )
    entry = ConfigEntry(
        class_key="CollectionRoutingConfig",
        value={"enabled": True},
        source="collection",
        resolved_drivers={"WRITE": [driver]},
    )
    assert entry.resolved_drivers is not None
    assert entry.resolved_drivers["WRITE"][0].driver_id == "CollectionPostgresqlDriver"


def test_collection_config_response_structure():
    response = CollectionConfigResponse(
        collection_id="landuse",
        catalog_id="my-catalog",
        configs={},
        categories=None,
    )
    assert response.categories is None


def test_catalog_config_response_structure():
    response = CatalogConfigResponse(catalog_id="my-catalog", configs={}, categories=None)
    assert response.catalog_id == "my-catalog"
    assert response.categories is None


def test_platform_config_response_scope_is_frozen():
    response = PlatformConfigResponse(configs={}, categories=None)
    assert response.scope == "platform"
