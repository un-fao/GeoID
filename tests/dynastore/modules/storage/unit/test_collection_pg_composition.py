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

"""Composition-driver tests for ``CollectionPostgresqlDriver``.

The wrapper is library-only at the time of landing — no
``[project.entry-points]`` registration, so production routing still
fans out to ``CollectionCorePostgresqlDriver`` +
``CollectionStacPostgresqlDriver`` directly.  These tests pin the
composition surface so the eventual cutover commit can flip the
entry-point registration as a one-line change.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, FrozenSet, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.protocols.entity_store import (
    CollectionStore,
    EntityStoreCapability,
)
from dynastore.modules.storage.drivers.collection_postgresql import (
    CollectionPostgresqlDriver,
    CollectionPostgresqlDriverConfig,
    CollectionCoreSidecarConfig,
    CollectionPgSidecarRegistry,
    CollectionStacSidecarConfig,
)


# ---------------------------------------------------------------------------
# Test doubles — minimal CollectionStore stand-ins
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
        self.upsert_calls: List[Dict[str, Any]] = []
        self.delete_calls: List[Dict[str, Any]] = []
        self.search_calls: List[Dict[str, Any]] = []

    async def is_available(self) -> bool:
        return True

    async def get_metadata(
        self, catalog_id: str, collection_id: str, *, context=None, db_resource=None,
    ) -> Optional[Dict[str, Any]]:
        return self.slice_value

    async def upsert_metadata(
        self, catalog_id: str, collection_id: str, metadata: Dict[str, Any],
        *, db_resource=None,
    ) -> None:
        self.upsert_calls.append({
            "catalog_id": catalog_id, "collection_id": collection_id,
            "metadata": metadata,
        })

    async def delete_metadata(
        self, catalog_id: str, collection_id: str,
        *, soft: bool = False, db_resource=None,
    ) -> None:
        self.delete_calls.append({
            "catalog_id": catalog_id, "collection_id": collection_id,
            "soft": soft,
        })

    async def search_metadata(
        self, catalog_id: str, *, q=None, bbox=None, datetime_range=None,
        filter_cql=None, limit: int = 100, offset: int = 0,
        context=None, db_resource=None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        self.search_calls.append({"catalog_id": catalog_id, "q": q})
        return [{"id": "from-search"}], 1

    async def get_driver_config(self, catalog_id: str, *, db_resource=None) -> Any:
        return None

    async def location(self, catalog_id: str, collection_id: str) -> Any:
        from dynastore.modules.storage.storage_location import StorageLocation
        return StorageLocation(
            backend="postgresql",
            canonical_uri="postgresql://test",
            identifiers={"catalog_id": catalog_id, "collection_id": collection_id},
            display_label="fake",
        )

    async def ensure_storage(
        self, catalog_id: str, collection_id: Optional[str] = None, **kwargs,
    ) -> None:
        return None


class _FakeCoreCls:
    """Class wrapper — registry expects classes, not instances."""
    _instance: Optional[_FakeInner] = None

    def __new__(cls):  # type: ignore[misc]
        if cls._instance is None:
            cls._instance = _FakeInner(
                slice_value={"title": "core-title", "description": "core-desc"},
                capabilities=frozenset({
                    EntityStoreCapability.READ, EntityStoreCapability.WRITE,
                    EntityStoreCapability.SEARCH, EntityStoreCapability.SOFT_DELETE,
                    EntityStoreCapability.PHYSICAL_ADDRESSING,
                }),
            )
        return cls._instance


class _FakeStacCls:
    _instance: Optional[_FakeInner] = None

    def __new__(cls):  # type: ignore[misc]
        if cls._instance is None:
            cls._instance = _FakeInner(
                slice_value={"extent": {"bbox": [[1, 2, 3, 4]]}, "providers": []},
                capabilities=frozenset({
                    EntityStoreCapability.READ, EntityStoreCapability.WRITE,
                    EntityStoreCapability.SPATIAL_FILTER, EntityStoreCapability.SOFT_DELETE,
                }),
            )
        return cls._instance


@pytest.fixture(autouse=True)
def _reset_registry():
    """Replace the production registry with our two fake classes for the
    duration of each test, then restore.  Also clears the wrapper's
    per-catalog sidecar cache (PR 1e step 4) so cache entries from
    previous tests don't leak across instances — the cache key
    deliberately ignores ``self`` for production efficiency, which
    means without this clear, test 2 would see test 1's resolved inners.
    """
    saved = dict(CollectionPgSidecarRegistry._registry)
    saved_loaded = CollectionPgSidecarRegistry._defaults_loaded
    CollectionPgSidecarRegistry.clear()
    CollectionPgSidecarRegistry._registry["collection_core"] = _FakeCoreCls  # type: ignore[assignment]
    CollectionPgSidecarRegistry._registry["collection_stac"] = _FakeStacCls  # type: ignore[assignment]
    CollectionPgSidecarRegistry._defaults_loaded = True
    # Clear singletons between tests.
    _FakeCoreCls._instance = None
    _FakeStacCls._instance = None
    # Flush the wrapper's per-catalog sidecar cache.
    try:
        CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        yield
    finally:
        CollectionPgSidecarRegistry.clear()
        CollectionPgSidecarRegistry._registry.update(saved)
        CollectionPgSidecarRegistry._defaults_loaded = saved_loaded
        try:
            CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()  # type: ignore[attr-defined]
        except Exception:
            pass


# ---------------------------------------------------------------------------
# TypedDriver bind + capability set
# ---------------------------------------------------------------------------


def test_typed_driver_bind_resolves():
    assert CollectionPostgresqlDriver.config_cls() is CollectionPostgresqlDriverConfig
    assert CollectionPostgresqlDriverConfig.class_key() == "collection_postgresql_driver"


def test_capabilities_union_covers_inner_capabilities():
    """Wrapper advertises union — every cap declared by either inner today
    must appear so the routing layer recognises the wrapper as a drop-in
    for the two raw drivers.
    """
    caps = CollectionPostgresqlDriver.capabilities
    for required in (
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SOFT_DELETE,
        EntityStoreCapability.SEARCH,         # CORE
        EntityStoreCapability.SPATIAL_FILTER, # STAC
        EntityStoreCapability.PHYSICAL_ADDRESSING,
    ):
        assert required in caps


# ---------------------------------------------------------------------------
# Sidecar registry
# ---------------------------------------------------------------------------


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

    cfg = CollectionPostgresqlDriverConfig(sidecars=list(sidecar_configs))
    fake = MagicMock()
    fake.get_config = AsyncMock(return_value=cfg)
    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake if p is ConfigsProtocol else None,
    ):
        CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        try:
            yield
        finally:
            CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()


def test_default_sidecars_is_core_only_even_when_stac_registered():
    """Opt-in flip: STAC is no longer a default slice.

    Even though the autouse fixture registers ``collection_stac``, the
    default slice list is CORE only.  The ``collection_stac`` slice is
    materialized per collection by ``_resolve_sidecars_for_catalog`` when a
    ``StacStorageConfig`` enables the collection tier with PG storage (see
    ``test_stac_storage_config_enables_stac_slice``).
    """
    sidecars = CollectionPgSidecarRegistry.default_sidecars()
    types = [s.sidecar_type for s in sidecars]
    assert types == ["collection_core"]


def test_default_sidecars_omits_stac_when_unregistered():
    """Simulates a deployment without the stac extra installed."""
    CollectionPgSidecarRegistry._registry.pop("collection_stac", None)
    sidecars = CollectionPgSidecarRegistry.default_sidecars()
    assert [s.sidecar_type for s in sidecars] == ["collection_core"]


def test_unknown_sidecar_type_skipped_with_warning(caplog):
    """A sidecar config whose type isn't in the registry skips with a
    warning — better than crashing the whole write.
    """
    driver = CollectionPostgresqlDriver()
    bogus = CollectionCoreSidecarConfig.model_construct(sidecar_type="metadata_does_not_exist")
    with caplog.at_level("WARNING"):
        inners = driver._resolve_inner_drivers([bogus])
    assert inners == []
    assert any("metadata_does_not_exist" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Composition fan-out — write/delete go to every inner
# ---------------------------------------------------------------------------


async def test_upsert_metadata_fans_out_to_every_configured_sidecar():
    payload = {"title": "T", "description": "D", "extent": {"bbox": [[0, 0, 1, 1]]}}
    with _operator_sidecars(CollectionCoreSidecarConfig(), CollectionStacSidecarConfig()):
        driver = CollectionPostgresqlDriver()
        await driver.upsert_metadata("cat-a", "col-a", payload)
    core = _FakeCoreCls()
    stac = _FakeStacCls()
    assert len(core.upsert_calls) == 1
    assert len(stac.upsert_calls) == 1
    # Wrapper hands the FULL payload to every inner — each inner is
    # responsible for filtering to its own column set (existing
    # `_PgCollectionCoreBase._filter_payload` invariant).
    assert core.upsert_calls[0]["metadata"] == payload
    assert stac.upsert_calls[0]["metadata"] == payload


async def test_delete_metadata_fans_out_with_soft_flag_preserved():
    with _operator_sidecars(CollectionCoreSidecarConfig(), CollectionStacSidecarConfig()):
        driver = CollectionPostgresqlDriver()
        await driver.delete_metadata("cat-a", "col-a", soft=True)
    core, stac = _FakeCoreCls(), _FakeStacCls()
    assert core.delete_calls == [{"catalog_id": "cat-a", "collection_id": "col-a", "soft": True}]
    assert stac.delete_calls == [{"catalog_id": "cat-a", "collection_id": "col-a", "soft": True}]


# ---------------------------------------------------------------------------
# Read fan-in — slices merged shallow, last-wins on key collision
# ---------------------------------------------------------------------------


async def test_get_metadata_merges_slices_from_every_inner():
    with _operator_sidecars(CollectionCoreSidecarConfig(), CollectionStacSidecarConfig()):
        driver = CollectionPostgresqlDriver()
        out = await driver.get_metadata("cat-a", "col-a")
    assert out is not None
    # Both slices present in the merge.
    assert out["title"] == "core-title"
    assert out["extent"] == {"bbox": [[1, 2, 3, 4]]}


async def test_get_metadata_returns_none_when_every_inner_returns_none():
    _FakeCoreCls._instance = _FakeInner(slice_value=None)
    _FakeStacCls._instance = _FakeInner(slice_value=None)
    driver = CollectionPostgresqlDriver()
    assert await driver.get_metadata("cat-a", "col-a") is None


async def test_get_metadata_swallows_per_inner_failure_and_returns_other_slices():
    """An inner driver crashing on read must not blank out the whole
    response — mirrors the router's `_safe_get` graceful-degrade shape.
    """
    failing = AsyncMock(spec=CollectionStore)
    failing.get_metadata.side_effect = RuntimeError("boom")
    failing.is_available.return_value = True
    failing.capabilities = frozenset({EntityStoreCapability.READ})

    class _FailingCls:
        def __new__(cls):  # type: ignore[misc]
            return failing

    CollectionPgSidecarRegistry._registry["collection_core"] = _FailingCls  # type: ignore[assignment]
    with _operator_sidecars(CollectionCoreSidecarConfig(), CollectionStacSidecarConfig()):
        driver = CollectionPostgresqlDriver()
        out = await driver.get_metadata("cat-a", "col-a")
    assert out is not None
    # Only the STAC slice survived.
    assert "extent" in out
    assert "title" not in out


# ---------------------------------------------------------------------------
# Search — first SEARCH-capable inner wins
# ---------------------------------------------------------------------------


async def test_search_metadata_delegates_to_first_search_capable_inner():
    """CORE has SEARCH; STAC doesn't.  Wrapper must route the search call
    to CORE only — never to STAC (which would return [], 0 today and
    silently shadow CORE results).
    """
    driver = CollectionPostgresqlDriver()
    rows, total = await driver.search_metadata("cat-a", q="foo")
    assert total == 1
    assert rows == [{"id": "from-search"}]
    core, stac = _FakeCoreCls(), _FakeStacCls()
    assert len(core.search_calls) == 1
    assert stac.search_calls == []


async def test_search_metadata_returns_empty_when_no_inner_has_search():
    _FakeCoreCls._instance = _FakeInner(
        capabilities=frozenset({EntityStoreCapability.READ}),
    )
    _FakeStacCls._instance = _FakeInner(
        capabilities=frozenset({EntityStoreCapability.READ}),
    )
    driver = CollectionPostgresqlDriver()
    rows, total = await driver.search_metadata("cat-a", q="foo")
    assert rows == [] and total == 0


# ---------------------------------------------------------------------------
# Search hydration — each CORE search hit is merged with the non-SEARCH
# (STAC) sidecar slices so the composition driver returns COMPLETE
# collections.  Without this, a PG-routed listing (e.g. the private_catalog
# preset, whose collection SEARCH is PG-first) silently drops every STAC
# field that single-collection GET still returns.
# ---------------------------------------------------------------------------


async def test_search_metadata_hydrates_hits_with_stac_slice():
    """With the STAC inner configured, a CORE search hit is hydrated with the
    STAC slice (extent/providers) — the search itself still runs ONLY on the
    SEARCH-capable CORE inner, never on STAC.
    """
    with _operator_sidecars(
        CollectionCoreSidecarConfig(), CollectionStacSidecarConfig(),
    ):
        driver = CollectionPostgresqlDriver()
        rows, total = await driver.search_metadata("cat-a", q="foo")
    assert total == 1
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == "from-search"                  # CORE id preserved
    assert row["extent"] == {"bbox": [[1, 2, 3, 4]]}   # STAC slice merged in
    assert row["providers"] == []
    core, stac = _FakeCoreCls(), _FakeStacCls()
    assert len(core.search_calls) == 1
    assert stac.search_calls == []                     # STAC never searched


async def test_search_hydration_preserves_core_values_on_key_overlap():
    """CORE search values win on key collision — hydration uses ``setdefault``,
    so a STAC slice that carries an overlapping key (here a stale ``id``)
    cannot clobber the CORE search result's identity or ordering.
    """
    _FakeStacCls._instance = _FakeInner(
        slice_value={"id": "STALE-stac-id", "extent": {"bbox": [[9, 9, 9, 9]]}},
        capabilities=frozenset({
            EntityStoreCapability.READ, EntityStoreCapability.SPATIAL_FILTER,
        }),
    )
    with _operator_sidecars(
        CollectionCoreSidecarConfig(), CollectionStacSidecarConfig(),
    ):
        driver = CollectionPostgresqlDriver()
        rows, _ = await driver.search_metadata("cat-a", q="foo")
    assert rows[0]["id"] == "from-search"              # CORE wins, not STALE
    assert rows[0]["extent"] == {"bbox": [[9, 9, 9, 9]]}


async def test_search_hydration_degrades_when_hydrator_fails(caplog):
    """A hydrator's ``get_metadata`` raising must not blank the CORE result —
    the slice is skipped with a warning and the search rows still return.
    """
    failing_stac = _FakeInner(
        capabilities=frozenset({
            EntityStoreCapability.READ, EntityStoreCapability.SPATIAL_FILTER,
        }),
    )

    async def _boom(*_a, **_k):
        raise RuntimeError("stac slice down")

    failing_stac.get_metadata = _boom  # type: ignore[assignment]
    _FakeStacCls._instance = failing_stac
    with _operator_sidecars(
        CollectionCoreSidecarConfig(), CollectionStacSidecarConfig(),
    ):
        driver = CollectionPostgresqlDriver()
        with caplog.at_level("WARNING"):
            rows, total = await driver.search_metadata("cat-a", q="foo")
    assert rows == [{"id": "from-search"}]             # CORE result survives
    assert total == 1
    assert any("hydrate" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Wrapper config — sidecar discriminated union round-trip
# ---------------------------------------------------------------------------


def test_wrapper_config_sidecars_discriminated_union_round_trips():
    cfg = CollectionPostgresqlDriverConfig(
        sidecars=[
            CollectionCoreSidecarConfig(),
            CollectionStacSidecarConfig(),
        ],
    )
    dumped = cfg.model_dump(exclude_unset=True)
    restored = CollectionPostgresqlDriverConfig.model_validate(dumped)
    assert [s.sidecar_type for s in restored.sidecars] == [
        "collection_core", "collection_stac",
    ]


def test_wrapper_config_default_sidecars_is_empty_list():
    """Empty default — driver falls back to registry default at use time
    (mirrors the items-tier default-fast invariant from M1b.2).
    """
    cfg = CollectionPostgresqlDriverConfig()
    assert cfg.sidecars == []


# ---------------------------------------------------------------------------
# STAC capability marker — wrapper must structurally satisfy
# ``StacCollectionEntityStoreCapability`` IFF a STAC inner is loaded.
# Regression guard for PR 1e step 3b: before the fix, the wrapper had no
# ``stac_metadata_columns`` method, so ``isinstance(wrapper,
# StacCollectionEntityStoreCapability)`` returned False and
# ``stac_service._assert_stac_capable_collection_stack`` warned
# "STAC slice will be dropped on write" even though the wrapper's
# STAC sidecar actually persisted it.
# ---------------------------------------------------------------------------


def test_wrapper_satisfies_stac_capability_when_stac_inner_loaded():
    from dynastore.extensions.stac.protocols import (
        StacCollectionEntityStoreCapability,
    )

    # Make the STAC fake actually expose the marker method so the wrapper
    # can delegate to it.
    def _stac_cols(self):
        return ("extent", "providers", "stac_version")
    _FakeStacCls._instance = _FakeInner()
    _FakeStacCls._instance.stac_metadata_columns = _stac_cols.__get__(  # type: ignore[attr-defined]
        _FakeStacCls._instance, _FakeInner,
    )
    driver = CollectionPostgresqlDriver()
    assert isinstance(driver, StacCollectionEntityStoreCapability)
    cols = driver.stac_metadata_columns()
    assert "extent" in cols
    assert "stac_version" in cols


def test_wrapper_returns_empty_columns_when_no_stac_inner_loaded():
    """Deployment without the stac extra: registry has only collection_core,
    wrapper's stac_metadata_columns() must return () so
    ``stac_service._has_stac`` correctly identifies STAC as unavailable.
    """
    CollectionPgSidecarRegistry._registry.pop("collection_stac", None)
    driver = CollectionPostgresqlDriver()
    assert driver.stac_metadata_columns() == ()


# ---------------------------------------------------------------------------
# Apply-handler warning — surfaces silent-drop of operator sidecars override
# until runtime fetch is wired.  Honest acknowledgment of the half-finished
# state of the ``sidecars`` config field; without this warning an operator
# patching the routing config with custom sidecars would see no behaviour
# change and assume their override took effect.
# ---------------------------------------------------------------------------


async def test_apply_handler_invalidates_cache_and_logs_info(caplog):
    """PR 1e step 4: handler clears the per-catalog sidecar cache and
    logs INFO so the operator-submitted override takes effect on the
    next metadata operation instead of waiting for the TTL to expire.
    """
    from dynastore.modules.storage.drivers.collection_postgresql import (
        _on_apply_collection_pg_driver_config,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    wrapper = CollectionPostgresqlDriver()
    register_plugin(wrapper)
    try:
        cfg = CollectionPostgresqlDriverConfig(
            sidecars=[CollectionCoreSidecarConfig()],  # non-empty → log INFO
        )
        with caplog.at_level("INFO"):
            await _on_apply_collection_pg_driver_config(
                cfg, catalog_id="some-catalog", collection_id=None,
                db_resource=None,
            )
        assert any(
            "sidecar cache invalidated" in r.message and "collection_core" in r.message
            for r in caplog.records
        ), "Apply handler must log INFO when invalidating cache for non-empty override"
    finally:
        unregister_plugin(wrapper)


async def test_apply_handler_silent_for_empty_sidecars(caplog):
    """Default-empty config: handler still invalidates the cache (cheap)
    but suppresses the INFO log to avoid noise — operator expressed no
    explicit override worth surfacing.
    """
    from dynastore.modules.storage.drivers.collection_postgresql import (
        _on_apply_collection_pg_driver_config,
    )
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    wrapper = CollectionPostgresqlDriver()
    register_plugin(wrapper)
    try:
        cfg = CollectionPostgresqlDriverConfig()  # empty default
        with caplog.at_level("INFO"):
            await _on_apply_collection_pg_driver_config(
                cfg, catalog_id="cat", collection_id=None, db_resource=None,
            )
        assert not any(
            "sidecar cache invalidated" in r.message
            for r in caplog.records
        ), "Empty override must not emit the INFO log"
    finally:
        unregister_plugin(wrapper)


async def test_apply_handler_ignores_non_wrapper_configs():
    """Apply-handler is registered globally on the config-apply pipeline;
    must no-op cleanly when handed a config of a different class.
    """
    from dynastore.modules.storage.drivers.collection_postgresql import (
        _on_apply_collection_pg_driver_config,
    )

    class _Unrelated:
        pass

    # Must not raise on unrelated config types.
    await _on_apply_collection_pg_driver_config(
        _Unrelated(), catalog_id="cat", collection_id=None, db_resource=None,
    )


async def test_apply_handler_safe_when_wrapper_not_yet_registered():
    """Early-lifespan apply flushes can run before the wrapper is
    register_plugin'd — handler must no-op cleanly, not crash.  The
    cold cache will start fresh on first use anyway.
    """
    from dynastore.modules.storage.drivers.collection_postgresql import (
        _on_apply_collection_pg_driver_config,
    )

    cfg = CollectionPostgresqlDriverConfig(
        sidecars=[CollectionCoreSidecarConfig()],
    )
    # No register_plugin — handler should silently return.
    await _on_apply_collection_pg_driver_config(
        cfg, catalog_id="cat", collection_id=None, db_resource=None,
    )


# ---------------------------------------------------------------------------
# Operator-override behavior — the whole point of PR 1e step 4.
# Pins that a non-empty CollectionPostgresqlDriverConfig.sidecars
# fetched via ConfigsProtocol actually changes which inner drivers
# get fanned to at runtime, vs. registry-default fallback.
# ---------------------------------------------------------------------------


async def test_operator_override_actually_changes_runtime_fanout():
    """Mock ConfigsProtocol.get_config to return a config with ONLY
    collection_core in sidecars (no collection_stac).  Verify the wrapper
    fans out to ONLY core, not both — proving the runtime override
    is honored, not silently dropped (which was the v0.5.70 state).
    """
    custom_cfg = CollectionPostgresqlDriverConfig(
        sidecars=[CollectionCoreSidecarConfig()],  # core only — no stac
    )
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=custom_cfg)

    from dynastore.models.protocols.configs import ConfigsProtocol

    # The wrapper imports get_protocol locally inside the resolver
    # method, so patch it where it's used (the discovery module).
    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake_configs if p is ConfigsProtocol else None,
    ):
        CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        driver = CollectionPostgresqlDriver()
        payload = {"title": "T", "extent": {"bbox": [[0, 0, 1, 1]]}}
        await driver.upsert_metadata("cat-a", "col-a", payload)

    # Core inner saw the call (operator opted in).
    assert len(_FakeCoreCls().upsert_calls) == 1
    # STAC inner did NOT (operator opted out via config).
    assert len(_FakeStacCls().upsert_calls) == 0


async def test_registry_default_is_core_only_without_stac_storage_config():
    """No operator override AND no ``StacStorageConfig`` → CORE only.

    Opt-in default: even with ``collection_stac`` registered, the
    per-collection write path fans out to CORE only until a
    ``StacStorageConfig`` enables the collection tier.
    """
    empty_cfg = CollectionPostgresqlDriverConfig()  # no sidecars
    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(return_value=empty_cfg)

    from dynastore.models.protocols.configs import ConfigsProtocol

    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake_configs if p is ConfigsProtocol else None,
    ):
        CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        driver = CollectionPostgresqlDriver()
        await driver.upsert_metadata("cat-a", "col-a", {"title": "T"})

    assert len(_FakeCoreCls().upsert_calls) == 1
    assert len(_FakeStacCls().upsert_calls) == 0


async def test_stac_storage_config_enables_stac_slice():
    """Opt-in resolution path: a ``StacStorageConfig`` with the collection
    tier enabled AND PG storage makes the per-collection write path fan out
    to the ``collection_stac`` slice — no operator ``sidecars`` override
    required.
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
        return CollectionPostgresqlDriverConfig()

    fake_configs = MagicMock()
    fake_configs.get_config = AsyncMock(side_effect=_get_config)

    with patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda p: fake_configs if p is ConfigsProtocol else None,
    ):
        CollectionPostgresqlDriver._resolve_sidecars_for_catalog.cache_clear()
        driver = CollectionPostgresqlDriver()
        await driver.upsert_metadata("cat-a", "col-a", {"title": "T"})

    assert len(_FakeCoreCls().upsert_calls) == 1
    assert len(_FakeStacCls().upsert_calls) == 1


# ---------------------------------------------------------------------------
# Discovery integration — wrapper IS the discovered CollectionStore
# plugin after register_plugin().  Closes a real gap: existing router tests
# inject mocks via ``drivers=`` and don't exercise the production discovery
# path.  This test catches "I forgot to register the wrapper" or "I forgot
# to remove a raw driver entry-point" — exactly the cutover-class
# regressions PR 1e step 3b would have introduced if not caught.
# ---------------------------------------------------------------------------


async def test_wrapper_is_discoverable_via_get_protocols():
    """Verify ``get_protocols(CollectionStore)`` returns the
    wrapper after ``register_plugin``, NOT a raw inner driver.  Mirrors
    the production discovery path that ``collection_router._resolve_drivers``
    uses when called without an explicit ``drivers=`` kwarg.
    """
    from dynastore.models.protocols.entity_store import (
        CollectionStore,
    )
    from dynastore.tools.discovery import (
        get_protocols,
        register_plugin,
        unregister_plugin,
    )

    wrapper = CollectionPostgresqlDriver()
    register_plugin(wrapper)
    try:
        discovered = list(get_protocols(CollectionStore))
        # Wrapper IS in the discovery results.
        assert wrapper in discovered
        # And every PG-tier discovered instance is a wrapper, not a raw inner.
        # (Other CollectionStore implementers — e.g. ES drivers — may
        # also appear; this test only pins the PG-tier shape.)
        from dynastore.modules.storage.drivers.core_postgresql import (
            CollectionCorePostgresqlDriver,
        )
        for d in discovered:
            assert not isinstance(d, CollectionCorePostgresqlDriver), (
                "Raw CollectionCorePostgresqlDriver must NOT surface as a "
                "discovered plugin after PR 1e step 3b cutover — composition "
                "is the only PG-tier path."
            )
    finally:
        unregister_plugin(wrapper)
