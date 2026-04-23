"""Tests for the reshaped composed-config composer.

After the reshape:
  * ``_get_effective_configs`` returns ``(by_class, sources)``.
  * ``_compose_tree`` nests configs into ``scope -> topic -> [sub ->] ClassName -> payload``
    in one pass, dropping classes that don't belong at the active scope
    and optionally emitting a ``meta`` dict.
  * ``_build_routing_refs`` rewrites ``operations[OP][*]`` into slim
    ``DriverRef`` dicts, resolving driver→config via the
    ``{DriverName}Config`` naming convention against the PluginConfig
    registry.
  * ``compose_*_config`` endpoints accept ``meta: bool`` to opt into tier
    diagnostics.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.configs.config_api_service import ConfigApiService


def _default_config_side_effect(config_cls, catalog_id=None, collection_id=None, **_):
    if isinstance(config_cls, str):
        from dynastore.modules.db_config.platform_config_service import (
            resolve_config_class,
        )
        config_cls = resolve_config_class(config_cls)
    return config_cls() if config_cls else None


@pytest.fixture()
def mock_config_service():
    svc = MagicMock()
    svc.list_configs = AsyncMock(return_value={"items": [], "total": 0})
    svc.get_config = AsyncMock(side_effect=_default_config_side_effect)
    return svc


# ---------------------------------------------------------------------------
# _get_effective_configs — returns (by_class, sources)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_effective_configs_source_default(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    by_class, sources = await svc._get_effective_configs(
        catalog_id=None, collection_id=None, resolved=True,
    )
    assert isinstance(by_class, dict)
    assert len(by_class) > 0
    for class_name in by_class:
        assert sources[class_name] == "default"


@pytest.mark.asyncio
async def test_get_effective_configs_catalog_source(mock_config_service):
    from dynastore.modules.storage.routing_config import CollectionRoutingConfig

    async def list_side_effect(catalog_id=None, collection_id=None, **_):
        if catalog_id and not collection_id:
            return {
                "items": [
                    {"plugin_id": "CollectionRoutingConfig",
                     "config_data": {"enabled": False}},
                ],
                "total": 1,
            }
        return {"items": [], "total": 0}

    async def get_side_effect(config_cls, catalog_id=None, collection_id=None, **_):
        if isinstance(config_cls, str):
            from dynastore.modules.db_config.platform_config_service import (
                resolve_config_class,
            )
            config_cls = resolve_config_class(config_cls)
        if config_cls is CollectionRoutingConfig and catalog_id and not collection_id:
            return CollectionRoutingConfig(enabled=False)
        return config_cls() if config_cls else None

    mock_config_service.list_configs.side_effect = list_side_effect
    mock_config_service.get_config.side_effect = get_side_effect

    svc = ConfigApiService(config_service=mock_config_service)
    _, sources = await svc._get_effective_configs(
        catalog_id="my-catalog", collection_id=None, resolved=True,
    )
    assert sources["CollectionRoutingConfig"] == "catalog"


# ---------------------------------------------------------------------------
# _compose_tree — scope/topic tree + optional meta in a single pass
# ---------------------------------------------------------------------------

def _stub_registry(**classes):
    """Build a fake ``list_registered_configs()`` dict.

    Each entry is ``name -> {"__module__": "..."}``.  The stub class's
    name-prefix and module drive placement + filtering via ``_place()``.
    """
    out = {}
    for name, attrs in classes.items():
        cls = type(name, (), {})
        cls.__module__ = attrs.get("__module__", "test.stub")
        out[name] = cls
    return out


def test_compose_tree_places_classes_by_module_path():
    by_class = {
        "WebConfig": {"brand_name": "x"},
        "CatalogCorePostgresqlDriverConfig": {"enabled": True},
    }
    registry = _stub_registry(
        WebConfig={"__module__": "dynastore.extensions.web.web"},
        CatalogCorePostgresqlDriverConfig={
            "__module__": "dynastore.modules.storage.drivers.metadata_postgresql",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, meta = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="catalog", include_meta=False,
        )
    assert tree["platform"]["web"]["WebConfig"] == {"brand_name": "x"}
    assert "CatalogCorePostgresqlDriverConfig" in tree["storage"]["drivers"]["catalog"]
    assert meta is None


def test_compose_tree_filters_collection_only_from_catalog():
    # Collection-prefixed classes are collection-only by name convention.
    by_class = {"CollectionWritePolicy": {"on_conflict": "update"}}
    registry = _stub_registry(
        CollectionWritePolicy={"__module__": "dynastore.modules.storage.driver_config"},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="catalog", include_meta=False,
        )
    assert "storage" not in tree or "policy" not in tree.get("storage", {})


def test_compose_tree_includes_collection_only_at_collection_scope():
    by_class = {"CollectionWritePolicy": {"on_conflict": "update"}}
    registry = _stub_registry(
        CollectionWritePolicy={"__module__": "dynastore.modules.storage.driver_config"},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection", include_meta=False,
        )
    assert tree["storage"]["policy"]["CollectionWritePolicy"] == {"on_conflict": "update"}


def test_compose_tree_drops_abstract_bases():
    by_class = {"DriverPluginConfig": {"enabled": True}}
    registry = _stub_registry(
        DriverPluginConfig={"__module__": "dynastore.modules.storage.driver_config"},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        tree, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="platform", include_meta=False,
        )
    assert tree == {}


# ---------------------------------------------------------------------------
# _build_routing_refs — slim DriverRef dicts, no inline driver-config
# ---------------------------------------------------------------------------

def test_build_routing_refs_replaces_entries_with_slim_refs():
    by_class = {
        "CatalogRoutingConfig": {
            "enabled": True,
            "operations": {
                "WRITE": [
                    {"driver_id": "CatalogCorePostgresqlDriver",
                     "on_failure": "fatal", "write_mode": "sync",
                     "hints": [], "sla": {"foo": 1}},
                ],
            },
        },
        "CatalogCorePostgresqlDriverConfig": {"enabled": True},
    }
    registry = _stub_registry(CatalogCorePostgresqlDriverConfig={"__module__": "m"})
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        svc = ConfigApiService(config_service=MagicMock())
        svc._build_routing_refs(by_class)

    write = by_class["CatalogRoutingConfig"]["operations"]["WRITE"]
    assert len(write) == 1
    ref = write[0]
    assert ref["driver_id"] == "CatalogCorePostgresqlDriver"
    assert ref["config_ref"] == "CatalogCorePostgresqlDriverConfig"
    assert ref["on_failure"] == "fatal"
    assert ref["write_mode"] == "sync"
    # Hints / sla / other legacy fields must not survive into the ref.
    assert "hints" not in ref
    assert "sla" not in ref


def test_build_routing_refs_missing_driver_yields_null_config_ref():
    by_class = {
        "CatalogRoutingConfig": {
            "operations": {
                "WRITE": [{"driver_id": "UnknownDriver",
                           "on_failure": "warn", "write_mode": "sync"}]
            },
        },
    }
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value={},
    ):
        svc = ConfigApiService(config_service=MagicMock())
        svc._build_routing_refs(by_class)
    ref = by_class["CatalogRoutingConfig"]["operations"]["WRITE"][0]
    assert ref["driver_id"] == "UnknownDriver"
    assert ref["config_ref"] is None


# ---------------------------------------------------------------------------
# Pagination helpers (unchanged behaviour, retained)
# ---------------------------------------------------------------------------

def test_next_link_on_first_page():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page(
        "http://test/config", "collections", 30, 1, 15, {"depth": 1},
    )
    assert any(link["rel"] == "next" for link in page.links)
    assert not any(link["rel"] == "prev" for link in page.links)
    next_href = next(link["href"] for link in page.links if link["rel"] == "next")
    assert "collections_page=2" in next_href
    assert "depth=1" in next_href


def test_prev_link_on_page_3():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page("http://test/config", "collections", 100, 3, 15, {})
    assert any(link["rel"] == "prev" for link in page.links)


def test_no_next_on_last_page():
    svc = ConfigApiService(config_service=MagicMock())
    page = svc._build_config_page("http://test/config", "collections", 10, 1, 15, {})
    assert not any(link["rel"] == "next" for link in page.links)


# ---------------------------------------------------------------------------
# compose_* — categories + meta wiring
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compose_collection_config_depth0_no_categories(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()):
        response = await svc.compose_collection_config(
            base_url="http://test", catalog_id="c", collection_id="col", depth=0,
        )
    assert response.categories is None
    assert response.meta is None


@pytest.mark.asyncio
async def test_compose_catalog_meta_flag_populates_meta_dict(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    by_class = {"WebConfig": {"brand_name": "x"}}
    sources = {"WebConfig": "default"}
    registry = _stub_registry(
        WebConfig={"__module__": "dynastore.extensions.web.web"},
    )
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=(by_class, sources))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()), \
         patch(
             "dynastore.extensions.configs.config_api_service.list_registered_configs",
             return_value=registry,
         ):
        r = await svc.compose_catalog_config(
            base_url="http://test", catalog_id="c", depth=0, meta=True,
        )
    assert r.meta is not None
    assert r.meta["WebConfig"].source == "default"


@pytest.mark.asyncio
async def test_compose_platform_config_sets_platform_scope(mock_config_service):
    svc = ConfigApiService(config_service=mock_config_service)
    with patch.object(svc, "_get_effective_configs",
                      new=AsyncMock(return_value=({}, {}))), \
         patch.object(svc, "_build_routing_refs", new=MagicMock()):
        r = await svc.compose_platform_config(base_url="http://test", depth=0)
    assert r.scope == "platform"
    assert r.categories is None
