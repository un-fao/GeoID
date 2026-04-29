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
async def test_get_effective_configs_resolved_batches_to_three_list_calls(mock_config_service):
    """Resolved-mode loader must do exactly THREE list_configs (one per tier)
    and ZERO per-class ``get_config`` calls — replaces the previous N+3 loop.
    """
    svc = ConfigApiService(config_service=mock_config_service)
    await svc._get_effective_configs(
        catalog_id="cat-x", collection_id="coll-y", resolved=True,
    )
    assert mock_config_service.list_configs.await_count == 3
    assert mock_config_service.get_config.await_count == 0


@pytest.mark.asyncio
async def test_get_effective_configs_resolved_merges_tier_deltas():
    """Tier rows are deltas; the loader merges them onto the code default in
    waterfall order (platform > catalog > collection, last wins) — no per-class
    ``get_config`` round-trip.
    """
    from dynastore.modules.storage.routing_config import CollectionRoutingConfig

    async def list_side_effect(catalog_id=None, collection_id=None, **_):
        if catalog_id and collection_id:
            return {
                "items": [
                    {"plugin_id": "CollectionRoutingConfig",
                     "config_data": {"enabled": False}},
                ],
                "total": 1,
            }
        return {"items": [], "total": 0}

    svc_mock = MagicMock()
    svc_mock.list_configs = AsyncMock(side_effect=list_side_effect)
    svc_mock.get_config = AsyncMock(side_effect=AssertionError(
        "batched loader must NOT call get_config in resolved mode"
    ))

    svc = ConfigApiService(config_service=svc_mock)
    by_class, sources = await svc._get_effective_configs(
        catalog_id="cat-x", collection_id="coll-y", resolved=True,
    )
    assert sources["CollectionRoutingConfig"] == "collection"
    assert by_class["CollectionRoutingConfig"]["enabled"] is False
    # round-trips through the model so other defaults are present
    default_keys = set(CollectionRoutingConfig().model_dump().keys())
    assert set(by_class["CollectionRoutingConfig"].keys()) == default_keys


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

    Each entry is ``name -> {"_address": (s,t,sub), "_visibility": str|None,
                              "abstract": <bool>?, "__module__": "..."?}``.
    Placement is now driven by the explicit ``_address`` ClassVar on each
    concrete config (mandatory in production, enforced by
    ``PluginConfig.__init_subclass__``).  ``_visibility`` is the optional
    scope filter; ``abstract=True`` flips ``is_abstract_base``.
    """
    out = {}
    for name, attrs in classes.items():
        body = {}
        if attrs.get("abstract"):
            body["is_abstract_base"] = True
        if "_address" in attrs:
            body["_address"] = attrs["_address"]
        if "_visibility" in attrs:
            body["_visibility"] = attrs["_visibility"]
        cls = type(name, (), body)
        cls.__module__ = attrs.get("__module__", "test.stub")
        out[name] = cls
    return out


def test_compose_tree_places_classes_by_address():
    by_class = {
        "WebConfig": {"brand_name": "x"},
        "CatalogCorePostgresqlDriver": {"enabled": True},
    }
    registry = _stub_registry(
        WebConfig={"_address": ("platform", "web", None)},
        CatalogCorePostgresqlDriver={
            "_address": ("storage", "drivers", "catalog"),
            "_visibility": "catalog",
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
    assert "CatalogCorePostgresqlDriver" in tree["storage"]["drivers"]["catalog"]
    assert meta is None


def test_compose_tree_filters_collection_only_from_catalog():
    # ``_visibility = "collection"`` → hidden at non-collection scopes.
    by_class = {"CollectionWritePolicy": {"on_conflict": "update"}}
    registry = _stub_registry(
        CollectionWritePolicy={
            "_address": ("storage", "policy", None),
            "_visibility": "collection",
        },
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
        CollectionWritePolicy={
            "_address": ("storage", "policy", None),
            "_visibility": "collection",
        },
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
    # All four legacy abstract bases (PluginConfig, _PluginDriverConfig,
    # DriverPluginConfig, CollectionDriverConfig, AssetDriverConfig) MUST
    # be filtered out at every active scope.  Filter is now driven by the
    # ``is_abstract_base = True`` ClassVar on the class itself.
    by_class = {
        "DriverPluginConfig":      {"enabled": True},
        "_PluginDriverConfig":     {"enabled": True},
        "CollectionDriverConfig":  {"enabled": True},
        "AssetDriverConfig":       {"enabled": True},
    }
    registry = _stub_registry(
        DriverPluginConfig={"__module__": "dynastore.modules.storage.driver_config", "abstract": True},
        _PluginDriverConfig={"__module__": "dynastore.models.protocols.typed_driver", "abstract": True},
        CollectionDriverConfig={"__module__": "dynastore.modules.storage.driver_config", "abstract": True},
        AssetDriverConfig={"__module__": "dynastore.modules.storage.driver_config", "abstract": True},
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        for scope in ("platform", "catalog", "collection"):
            tree, _ = ConfigApiService._compose_tree(
                by_class, sources={}, active_scope=scope, include_meta=False,
            )
            assert tree == {}, (
                f"abstract bases leaked into the tree at scope={scope!r}: {tree!r}"
            )


def test_compose_tree_real_plugin_driver_config_does_not_leak():
    """Regression: ``_PluginDriverConfig`` must never appear in the composed tree.

    The legacy ``_ABSTRACT_BASES`` frozenset in this module was missing the
    underscore-prefixed name, so it leaked as ``platform.misc._PluginDriverConfig:
    {enabled: true}`` in the production deep view.  After the marker move, the
    filter reads ``cls.__dict__.get("is_abstract_base", False)`` and the real
    class declares it; the leak is structurally impossible.
    """
    from dynastore.models.protocols.typed_driver import _PluginDriverConfig

    assert _PluginDriverConfig.__dict__.get("is_abstract_base") is True

    by_class = {"_PluginDriverConfig": {"enabled": True}}
    registry = {"_PluginDriverConfig": _PluginDriverConfig}
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        for scope in ("platform", "catalog", "collection"):
            tree, _ = ConfigApiService._compose_tree(
                by_class, sources={}, active_scope=scope, include_meta=False,
            )
            assert tree == {}, f"_PluginDriverConfig leaked at scope={scope!r}: {tree!r}"


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
        "CatalogCorePostgresqlDriver": {"enabled": True},
    }
    # Stub key matches the wire key (TypedDriver bind drops Config suffix).
    registry = _stub_registry(CatalogCorePostgresqlDriver={"__module__": "m"})
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
    assert ref["config_ref"] == "CatalogCorePostgresqlDriver"
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
        WebConfig={"_address": ("platform", "web", None)},
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


def test_compose_tree_address_visibility_filters_correctly():
    """``_visibility = "catalog"`` hides the class at collection scope."""
    by_class = {"CatalogOnly": {"x": 1}}
    registry = _stub_registry(
        CatalogOnly={
            "_address": ("storage", "drivers", "catalog"),
            "_visibility": "catalog",
        },
    )
    with patch(
        "dynastore.extensions.configs.config_api_service.list_registered_configs",
        return_value=registry,
    ):
        # Visible at platform / catalog
        for scope in ("platform", "catalog"):
            tree, _ = ConfigApiService._compose_tree(
                by_class, sources={}, active_scope=scope, include_meta=False,
            )
            assert "CatalogOnly" in tree["storage"]["drivers"]["catalog"]
        # Hidden at collection
        tree, _ = ConfigApiService._compose_tree(
            by_class, sources={}, active_scope="collection", include_meta=False,
        )
        assert tree == {}


# ---------------------------------------------------------------------------
# Production placement-bug regression tests — were misplaced under the
# heuristic; explicit `_address` fixes them.
# ---------------------------------------------------------------------------

def test_es_catalog_config_lands_at_catalog_scope():
    """``ElasticsearchCatalogConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.es_catalog_config import (
        ElasticsearchCatalogConfig,
    )

    assert ElasticsearchCatalogConfig._address == ("catalog", "elasticsearch", None)
    assert ElasticsearchCatalogConfig._visibility == "catalog"


def test_es_collection_config_lands_at_collection_scope():
    """``ElasticsearchCollectionConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.es_collection_config import (
        ElasticsearchCollectionConfig,
    )

    assert ElasticsearchCollectionConfig._address == ("collection", "elasticsearch", None)
    assert ElasticsearchCollectionConfig._visibility == "collection"


def test_catalog_es_driver_lands_under_storage_drivers_catalog():
    """``CatalogElasticsearchDriverConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.catalog_es_driver import (
        CatalogElasticsearchDriverConfig,
    )

    assert CatalogElasticsearchDriverConfig._address == ("storage", "drivers", "catalog")
    assert CatalogElasticsearchDriverConfig._visibility == "catalog"


def test_collection_es_driver_lands_under_storage_drivers_collection():
    """``CollectionElasticsearchDriverConfig`` was leaking to ``platform.misc``."""
    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriverConfig,
    )

    assert CollectionElasticsearchDriverConfig._address == ("storage", "drivers", "collection")
    assert CollectionElasticsearchDriverConfig._visibility == "catalog"


def test_assets_plugin_config_visible_at_all_scopes():
    """Extension config was incorrectly gated to collection by ``Asset*`` name."""
    from dynastore.extensions.assets.config import AssetsPluginConfig

    assert AssetsPluginConfig._address == ("extensions", "assets", None)
    assert AssetsPluginConfig._visibility is None  # visible everywhere


# ---------------------------------------------------------------------------
# Enforcement — every concrete PluginConfig must declare _address.
# ---------------------------------------------------------------------------

def test_concrete_subclass_without_address_raises():
    """``PluginConfig.__init_subclass__`` enforces ``_address`` on concrete subclasses."""
    from typing import ClassVar

    from dynastore.modules.db_config.platform_config_service import PluginConfig

    with pytest.raises(TypeError, match=r"does not declare ``_address``"):
        class _BadConcreteConfig(PluginConfig):  # noqa: F841 — deliberate
            field: ClassVar[int] = 1


def test_concrete_subclass_with_address_ok():
    from typing import ClassVar, Optional, Tuple

    from dynastore.modules.db_config.platform_config_service import PluginConfig

    class _GoodConcreteConfig(PluginConfig):
        _address: ClassVar[Tuple[str, str, Optional[str]]] = ("platform", "misc", None)

    assert _GoodConcreteConfig._address == ("platform", "misc", None)


def test_abstract_subclass_without_address_ok():
    """Abstract bases (``is_abstract_base = True``) opt out of the check."""
    from typing import ClassVar

    from dynastore.modules.db_config.platform_config_service import PluginConfig

    class _AbstractIntermediate(PluginConfig):
        is_abstract_base: ClassVar[bool] = True

    assert _AbstractIntermediate.is_abstract_base is True
