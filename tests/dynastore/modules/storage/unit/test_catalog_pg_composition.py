#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Composition-driver tests for ``CatalogPostgresqlDriver``.

Mirrors ``test_collection_pg_composition.py`` for the catalog tier.
PR 1e step 3c lands the wrapper with its entry-point active from day
one (no library-only hop) so these tests pin the production-active
shape directly.

Includes the ``stac_metadata_columns()`` delegation regression guard
from day one — closing the loop on the 3b follow-up (`747477d`) by
ensuring the catalog tier doesn't have to ship a follow-up to fix the
same capability-marker drift.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, FrozenSet, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.entity_store import (
    CatalogStore,
    EntityStoreCapability,
)
from dynastore.modules.storage.drivers.catalog_postgresql import (
    CatalogCoreSidecarConfig,
    CatalogStacSidecarConfig,
    CatalogPostgresqlDriver,
    CatalogPostgresqlDriverConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry


# ---------------------------------------------------------------------------
# Test doubles — minimal CatalogStore stand-ins
# ---------------------------------------------------------------------------


class _FakeInner:
    """Minimal inner driver — captures every call so tests can assert
    fan-out happened on every configured sidecar.
    """

    def __init__(
        self,
        *,
        slice_value: Optional[Dict[str, Any]] = None,
        capabilities: Optional[FrozenSet[str]] = None,
    ):
        self.slice_value = slice_value
        self.capabilities = capabilities or frozenset({
            EntityStoreCapability.READ,
            EntityStoreCapability.WRITE,
            EntityStoreCapability.SOFT_DELETE,
        })
        self.upsert_calls = []
        self.delete_calls = []

    async def is_available(self) -> bool:
        return True

    async def get_catalog_metadata(
        self, catalog_id: str, *, context=None, db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        return self.slice_value

    async def upsert_catalog_metadata(
        self, catalog_id: str, metadata: Dict[str, Any], *, db_resource=None,
    ) -> None:
        self.upsert_calls.append({"catalog_id": catalog_id, "metadata": metadata})

    async def delete_catalog_metadata(
        self, catalog_id: str, *, soft: bool = False, db_resource=None,
    ) -> None:
        self.delete_calls.append({"catalog_id": catalog_id, "soft": soft})

    async def get_driver_config(self, catalog_id: str, *, db_resource=None) -> Any:
        return None


class _FakeCoreCls:
    _instance: Optional[_FakeInner] = None

    def __new__(cls):  # type: ignore[misc]
        if cls._instance is None:
            cls._instance = _FakeInner(
                slice_value={"title": "core-title", "description": "core-desc"},
                capabilities=frozenset({
                    EntityStoreCapability.READ, EntityStoreCapability.WRITE,
                    EntityStoreCapability.SOFT_DELETE,
                    EntityStoreCapability.QUERY_FALLBACK_SOURCE,
                }),
            )
        return cls._instance


class _FakeStacCls:
    _instance: Optional[_FakeInner] = None

    def __new__(cls):  # type: ignore[misc]
        if cls._instance is None:
            cls._instance = _FakeInner(
                slice_value={"stac_version": "1.0.0", "conforms_to": ["x"]},
                capabilities=frozenset({
                    EntityStoreCapability.READ, EntityStoreCapability.WRITE,
                    EntityStoreCapability.SOFT_DELETE,
                }),
            )
        return cls._instance


@pytest.fixture(autouse=True)
def _reset_registry():
    saved = dict(SidecarRegistry._catalog_registry)
    saved_loaded = SidecarRegistry._catalog_defaults_loaded
    SidecarRegistry.clear_catalog_registry()
    SidecarRegistry._catalog_registry["catalog_core"] = _FakeCoreCls  # type: ignore[assignment]
    SidecarRegistry._catalog_registry["catalog_stac"] = _FakeStacCls  # type: ignore[assignment]
    SidecarRegistry._catalog_defaults_loaded = True
    _FakeCoreCls._instance = None
    _FakeStacCls._instance = None
    # Flush per-catalog sidecar cache (PR 1e step 4) — the cache key
    # ignores ``self`` for production efficiency, so without this clear
    # test 2 would see test 1's resolved inners.
    try:
        CatalogPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        yield
    finally:
        SidecarRegistry.clear_catalog_registry()
        SidecarRegistry._catalog_registry.update(saved)
        SidecarRegistry._catalog_defaults_loaded = saved_loaded
        try:
            CatalogPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()  # type: ignore[attr-defined]
        except Exception:
            pass


@contextmanager
def _operator_sidecars(*sidecar_configs):
    """Force the per-catalog resolver to use an explicit operator override.

    Post-opt-in, STAC is no longer a *default* slice — the composition
    fan-out tests below exercise the multi-inner write/read mechanism, which
    now requires an explicit ``[core, stac]`` configuration.  (Resolution of
    the STAC slice *from* a ``StacStorageConfig`` is asserted directly in
    ``test_stac_storage_config_enables_stac_slice``.)
    """
    from dynastore.models.protocols.configs import ConfigsProtocol

    cfg = CatalogPostgresqlDriverConfig(sidecars=list(sidecar_configs))
    fake = MagicMock()
    fake.get_config = AsyncMock(return_value=cfg)
    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake if p is ConfigsProtocol else None,
    ):
        CatalogPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        try:
            yield
        finally:
            CatalogPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()


# ---------------------------------------------------------------------------
# TypedDriver bind + capability set
# ---------------------------------------------------------------------------


def test_typed_driver_bind_resolves():
    assert CatalogPostgresqlDriver.config_cls() is CatalogPostgresqlDriverConfig
    assert CatalogPostgresqlDriverConfig.class_key() == "catalog_postgresql_driver"


def test_capabilities_union_covers_inner_capabilities():
    """Catalog tier doesn't include SEARCH/SPATIAL_FILTER/PHYSICAL_ADDRESSING
    — neither raw catalog driver advertises them.  Union must NOT
    accidentally inherit collection-tier caps via copy-paste from the
    sister wrapper.
    """
    caps = CatalogPostgresqlDriver.capabilities
    for required in (
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.QUERY_FALLBACK_SOURCE,
    ):
        assert required in caps
    # Negative assertions — would surface accidental cap drift from the
    # collection wrapper.
    assert EntityStoreCapability.SEARCH not in caps
    assert EntityStoreCapability.SPATIAL_FILTER not in caps
    assert EntityStoreCapability.PHYSICAL_ADDRESSING not in caps


# ---------------------------------------------------------------------------
# Sidecar registry
# ---------------------------------------------------------------------------


def test_default_sidecars_is_core_only_even_when_stac_registered():
    """Opt-in flip: STAC is no longer a default slice.

    Even though the autouse fixture registers ``catalog_stac``, the default
    slice list is CORE only.  The ``catalog_stac`` slice is materialized per
    catalog by ``_resolve_sidecars_for_catalog`` when a ``StacStorageConfig``
    enables the catalog tier with PG storage (see
    ``test_stac_storage_config_enables_stac_slice``).
    """
    sidecars = SidecarRegistry.default_catalog_sidecars()
    assert [s.sidecar_type for s in sidecars] == ["catalog_core"]


def test_default_sidecars_omits_stac_when_unregistered():
    SidecarRegistry._catalog_registry.pop("catalog_stac", None)
    sidecars = SidecarRegistry.default_catalog_sidecars()
    assert [s.sidecar_type for s in sidecars] == ["catalog_core"]


def test_unknown_sidecar_type_skipped_with_warning(caplog):
    driver = CatalogPostgresqlDriver()
    bogus = CatalogCoreSidecarConfig.model_construct(
        sidecar_type="catalog_metadata_does_not_exist",
    )
    with caplog.at_level("WARNING"):
        inners = driver._resolve_inner_drivers([bogus])
    assert inners == []
    assert any(
        "catalog_metadata_does_not_exist" in r.message for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Composition fan-out — write/delete go to every inner
# ---------------------------------------------------------------------------


async def test_upsert_catalog_metadata_fans_out_to_every_configured_sidecar():
    payload = {"title": "T", "description": "D", "stac_version": "1.0.0"}
    with _operator_sidecars(CatalogCoreSidecarConfig(), CatalogStacSidecarConfig()):
        driver = CatalogPostgresqlDriver()
        await driver.upsert_catalog_metadata("cat-a", payload)
    core = _FakeCoreCls()
    stac = _FakeStacCls()
    assert len(core.upsert_calls) == 1
    assert len(stac.upsert_calls) == 1
    # Wrapper hands the FULL payload to every inner — each inner is
    # responsible for filtering to its own column set
    # (existing ``_PgCatalogCoreBase._filter_payload`` invariant).
    assert core.upsert_calls[0]["metadata"] == payload
    assert stac.upsert_calls[0]["metadata"] == payload


async def test_delete_catalog_metadata_fans_out_with_soft_flag_preserved():
    with _operator_sidecars(CatalogCoreSidecarConfig(), CatalogStacSidecarConfig()):
        driver = CatalogPostgresqlDriver()
        await driver.delete_catalog_metadata("cat-a", soft=True)
    core, stac = _FakeCoreCls(), _FakeStacCls()
    assert core.delete_calls == [{"catalog_id": "cat-a", "soft": True}]
    assert stac.delete_calls == [{"catalog_id": "cat-a", "soft": True}]


# ---------------------------------------------------------------------------
# Read fan-in — slices merged shallow
# ---------------------------------------------------------------------------


async def test_get_catalog_metadata_merges_slices_from_every_inner():
    with _operator_sidecars(CatalogCoreSidecarConfig(), CatalogStacSidecarConfig()):
        driver = CatalogPostgresqlDriver()
        out = await driver.get_catalog_metadata("cat-a")
    assert out is not None
    assert out["title"] == "core-title"
    assert out["stac_version"] == "1.0.0"


async def test_get_catalog_metadata_returns_none_when_every_inner_returns_none():
    _FakeCoreCls._instance = _FakeInner(slice_value=None)
    _FakeStacCls._instance = _FakeInner(slice_value=None)
    driver = CatalogPostgresqlDriver()
    assert await driver.get_catalog_metadata("cat-a") is None


async def test_get_catalog_metadata_swallows_per_inner_failure_and_returns_other_slices():
    failing = AsyncMock(spec=CatalogStore)
    failing.get_catalog_metadata.side_effect = RuntimeError("boom")
    failing.is_available.return_value = True
    failing.capabilities = frozenset({EntityStoreCapability.READ})

    class _FailingCls:
        def __new__(cls):  # type: ignore[misc]
            return failing

    SidecarRegistry._catalog_registry["catalog_core"] = _FailingCls  # type: ignore[assignment]
    with _operator_sidecars(CatalogCoreSidecarConfig(), CatalogStacSidecarConfig()):
        driver = CatalogPostgresqlDriver()
        out = await driver.get_catalog_metadata("cat-a")
    assert out is not None
    # Only the STAC slice survived.
    assert "stac_version" in out
    assert "title" not in out


# ---------------------------------------------------------------------------
# Wrapper config — sidecar discriminated union round-trip
# ---------------------------------------------------------------------------


def test_wrapper_config_sidecars_discriminated_union_round_trips():
    cfg = CatalogPostgresqlDriverConfig(
        sidecars=[
            CatalogCoreSidecarConfig(),
            CatalogStacSidecarConfig(),
        ],
    )
    dumped = cfg.model_dump(exclude_unset=True)
    restored = CatalogPostgresqlDriverConfig.model_validate(dumped)
    assert [s.sidecar_type for s in restored.sidecars] == [
        "catalog_core", "catalog_stac",
    ]


def test_wrapper_config_default_sidecars_is_empty_list():
    cfg = CatalogPostgresqlDriverConfig()
    assert cfg.sidecars == []


# ---------------------------------------------------------------------------
# STAC capability marker — built in from day one (3b lesson learned)
# ---------------------------------------------------------------------------


def test_wrapper_satisfies_stac_capability_when_stac_inner_loaded():
    """Closes the loop on the 3b follow-up `747477d` by ensuring the
    catalog wrapper structurally satisfies ``StacCatalogEntityStoreCapability``
    when a STAC inner is loaded — without needing a separate
    follow-up commit.
    """
    from dynastore.extensions.stac.protocols import (
        StacCatalogEntityStoreCapability,
    )

    def _stac_cols(self):
        return ("stac_version", "conforms_to", "links", "assets")
    _FakeStacCls._instance = _FakeInner()
    _FakeStacCls._instance.stac_metadata_columns = _stac_cols.__get__(  # type: ignore[attr-defined]
        _FakeStacCls._instance, _FakeInner,
    )
    driver = CatalogPostgresqlDriver()
    assert isinstance(driver, StacCatalogEntityStoreCapability)
    cols = driver.stac_metadata_columns()
    assert "stac_version" in cols
    assert "conforms_to" in cols


def test_wrapper_returns_empty_columns_when_no_stac_inner_loaded():
    """Deployment without the stac extra: registry has only catalog_core.
    Wrapper's stac_metadata_columns() must return () so
    ``stac_service._has_stac`` correctly identifies STAC as unavailable
    and the catalog-tier hard-reject still fires.
    """
    SidecarRegistry._catalog_registry.pop("catalog_stac", None)
    driver = CatalogPostgresqlDriver()
    assert driver.stac_metadata_columns() == ()


# ---------------------------------------------------------------------------
# Apply-handler warning — sister surface to the collection wrapper's
# silent-drop guard.  Honest acknowledgment that ``sidecars`` overrides
# aren't yet honored at runtime.
# ---------------------------------------------------------------------------


async def test_apply_handler_invalidates_cache_and_logs_info(caplog):
    """PR 1e step 4 sister test: handler clears cache + logs INFO."""
    from dynastore.modules.storage.drivers.catalog_postgresql import (
        _on_apply_catalog_pg_driver_config,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    wrapper = CatalogPostgresqlDriver()
    register_plugin(wrapper)
    try:
        cfg = CatalogPostgresqlDriverConfig(
            sidecars=[CatalogCoreSidecarConfig()],
        )
        with caplog.at_level("INFO"):
            await _on_apply_catalog_pg_driver_config(
                cfg, catalog_id="some-catalog", collection_id=None,
                db_resource=None,
            )
        assert any(
            "sidecar cache invalidated" in r.message
            and "catalog_core" in r.message
            for r in caplog.records
        )
    finally:
        unregister_plugin(wrapper)


async def test_apply_handler_silent_for_empty_sidecars(caplog):
    from dynastore.modules.storage.drivers.catalog_postgresql import (
        _on_apply_catalog_pg_driver_config,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    wrapper = CatalogPostgresqlDriver()
    register_plugin(wrapper)
    try:
        cfg = CatalogPostgresqlDriverConfig()
        with caplog.at_level("INFO"):
            await _on_apply_catalog_pg_driver_config(
                cfg, catalog_id="cat", collection_id=None, db_resource=None,
            )
        assert not any(
            "sidecar cache invalidated" in r.message for r in caplog.records
        )
    finally:
        unregister_plugin(wrapper)


async def test_apply_handler_ignores_non_wrapper_configs():
    from dynastore.modules.storage.drivers.catalog_postgresql import (
        _on_apply_catalog_pg_driver_config,
    )

    class _Unrelated:
        pass

    await _on_apply_catalog_pg_driver_config(
        _Unrelated(), catalog_id="cat", collection_id=None, db_resource=None,
    )


async def test_apply_handler_safe_when_wrapper_not_yet_registered():
    from dynastore.modules.storage.drivers.catalog_postgresql import (
        _on_apply_catalog_pg_driver_config,
    )

    cfg = CatalogPostgresqlDriverConfig(
        sidecars=[CatalogCoreSidecarConfig()],
    )
    await _on_apply_catalog_pg_driver_config(
        cfg, catalog_id="cat", collection_id=None, db_resource=None,
    )


# ---------------------------------------------------------------------------
# Operator override actually changes runtime fan-out (PR 1e step 4)
# ---------------------------------------------------------------------------


async def test_operator_override_actually_changes_runtime_fanout():
    custom_cfg = CatalogPostgresqlDriverConfig(
        sidecars=[CatalogCoreSidecarConfig()],  # core only
    )
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=custom_cfg)

    from dynastore.models.protocols.configs import ConfigsProtocol

    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake_configs if p is ConfigsProtocol else None,
    ):
        CatalogPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        driver = CatalogPostgresqlDriver()
        await driver.upsert_catalog_metadata("cat-a", {"title": "T"})

    assert len(_FakeCoreCls().upsert_calls) == 1
    assert len(_FakeStacCls().upsert_calls) == 0


async def test_registry_default_is_core_only_without_stac_storage_config():
    """No operator override AND no ``StacStorageConfig`` → CORE only.

    Opt-in default: even with ``catalog_stac`` registered, the per-catalog
    write path fans out to CORE only until a ``StacStorageConfig`` enables
    the catalog tier.
    """
    empty_cfg = CatalogPostgresqlDriverConfig()
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=empty_cfg)

    from dynastore.models.protocols.configs import ConfigsProtocol

    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake_configs if p is ConfigsProtocol else None,
    ):
        CatalogPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        driver = CatalogPostgresqlDriver()
        await driver.upsert_catalog_metadata("cat-a", {"title": "T"})

    assert len(_FakeCoreCls().upsert_calls) == 1
    assert len(_FakeStacCls().upsert_calls) == 0


async def test_stac_storage_config_enables_stac_slice():
    """Opt-in resolution path: a ``StacStorageConfig`` with the catalog tier
    enabled AND PG storage makes the per-catalog write path fan out to the
    ``catalog_stac`` slice — no operator ``sidecars`` override required.
    """
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.stac.stac_storage_config import (
        StacLevel,
        StacStorageBackend,
        StacStorageConfig,
    )

    async def _get_config(cfg_cls, **_kw):
        if cfg_cls is StacStorageConfig:
            return StacStorageConfig(
                stac_level=StacLevel.COLLECTION,
                stac_storage=StacStorageBackend.ES_PG,
            )
        # Wrapper config fetch — no explicit operator sidecars override.
        return CatalogPostgresqlDriverConfig()

    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(side_effect=_get_config)

    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake_configs if p is ConfigsProtocol else None,
    ):
        CatalogPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        driver = CatalogPostgresqlDriver()
        await driver.upsert_catalog_metadata("cat-a", {"title": "T"})

    assert len(_FakeCoreCls().upsert_calls) == 1
    assert len(_FakeStacCls().upsert_calls) == 1


async def test_wrapper_is_discoverable_via_get_protocols():
    """Sister test to the collection wrapper's discovery integration
    test — verify ``get_protocols(CatalogStore)`` returns the
    catalog wrapper after ``register_plugin``, and that no raw
    ``CatalogCorePostgresqlDriver`` surfaces.
    """
    from dynastore.tools.discovery import (
        get_protocols,
        register_plugin,
        unregister_plugin,
    )

    wrapper = CatalogPostgresqlDriver()
    register_plugin(wrapper)
    try:
        discovered = list(get_protocols(CatalogStore))
        assert wrapper in discovered
        from dynastore.modules.storage.drivers.core_postgresql import (
            CatalogCorePostgresqlDriver,
        )
        for d in discovered:
            assert not isinstance(d, CatalogCorePostgresqlDriver), (
                "Raw CatalogCorePostgresqlDriver must NOT surface as a "
                "discovered plugin after PR 1e step 3c cutover — composition "
                "is the only PG-tier path."
            )
    finally:
        unregister_plugin(wrapper)


# ---------------------------------------------------------------------------
# Storage-lifecycle contract (ensure_storage / drop_storage) — catalog tier
# ---------------------------------------------------------------------------


async def test_wrapper_ensure_storage_is_noop():
    """The wrapper owns no SQL — ensure_storage is a no-op (tables are
    created out-of-band by the catalog-provisioning DDL)."""
    wrapper = CatalogPostgresqlDriver()
    assert await wrapper.ensure_storage("cat-x") is None
    assert await wrapper.ensure_storage() is None


async def test_wrapper_drop_storage_fans_to_inners_failsoft():
    """drop_storage fans to every inner that implements it (passing ``soft``
    through) and fail-soft skips inners without one."""
    calls: list = []

    class _InnerWithDrop:
        async def drop_storage(self, catalog_id, *, soft=False):
            calls.append((catalog_id, soft))

    class _InnerNoDrop:
        pass  # no drop_storage → must be skipped, not raise

    wrapper = CatalogPostgresqlDriver()
    # cached_property stores under the attribute name in the instance __dict__
    wrapper.__dict__["_default_inner_drivers"] = [_InnerWithDrop(), _InnerNoDrop()]
    await wrapper.drop_storage("cat-x", soft=True)
    assert calls == [("cat-x", True)]


def test_all_catalog_implementers_conform_to_lifecycle_contract():
    """Trap guard (cf. the #1756 collection regression): after CatalogStore
    gained ``ensure_storage`` + ``drop_storage``, EVERY implementer must still
    structurally satisfy the protocol — otherwise it silently drops out of
    ``get_protocols(CatalogStore)`` and the driver vanishes from discovery."""
    from dynastore.modules.storage.drivers.core_postgresql import (
        CatalogCorePostgresqlDriver,
    )
    from dynastore.modules.elasticsearch.catalog_es_driver import (
        CatalogElasticsearchDriver,
    )
    from dynastore.modules.storage.drivers.catalog_log_indexer import (
        LogCatalogIndexer,
    )

    implementers = [
        CatalogPostgresqlDriver(),
        CatalogCorePostgresqlDriver(),
        CatalogElasticsearchDriver(),
        LogCatalogIndexer(),
    ]
    for drv in implementers:
        name = type(drv).__name__
        assert isinstance(drv, CatalogStore), (
            f"{name} no longer satisfies CatalogStore — missing a method "
            f"(likely the new lifecycle pair). It will drop out of discovery."
        )
        assert callable(getattr(drv, "ensure_storage", None)), name
        assert callable(getattr(drv, "drop_storage", None)), name
