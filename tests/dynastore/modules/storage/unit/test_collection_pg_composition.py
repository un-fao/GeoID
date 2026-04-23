"""Composition-driver tests for ``CollectionPostgresqlDriver``.

The wrapper is library-only at the time of landing — no
``[project.entry-points]`` registration, so production routing still
fans out to ``CollectionCorePostgresqlDriver`` +
``CollectionStacPostgresqlDriver`` directly.  These tests pin the
composition surface so the eventual cutover commit can flip the
entry-point registration as a one-line change.
"""

from __future__ import annotations

from typing import Any, Dict, FrozenSet, List, Optional, Tuple
from unittest.mock import AsyncMock

import pytest

from dynastore.models.protocols.metadata_driver import (
    CollectionMetadataStore,
    MetadataCapability,
)
from dynastore.modules.storage.drivers.collection_metadata_postgresql import (
    CollectionPostgresqlDriver,
    CollectionPostgresqlDriverConfig,
    MetadataCoreSidecarConfig,
    MetadataPgSidecarRegistry,
    MetadataStacSidecarConfig,
)


# ---------------------------------------------------------------------------
# Test doubles — minimal CollectionMetadataStore stand-ins
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
            MetadataCapability.READ,
            MetadataCapability.WRITE,
            MetadataCapability.SOFT_DELETE,
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
                    MetadataCapability.READ, MetadataCapability.WRITE,
                    MetadataCapability.SEARCH, MetadataCapability.SOFT_DELETE,
                    MetadataCapability.PHYSICAL_ADDRESSING,
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
                    MetadataCapability.READ, MetadataCapability.WRITE,
                    MetadataCapability.SPATIAL_FILTER, MetadataCapability.SOFT_DELETE,
                }),
            )
        return cls._instance


@pytest.fixture(autouse=True)
def _reset_registry():
    """Replace the production registry with our two fake classes for the
    duration of each test, then restore.
    """
    saved = dict(MetadataPgSidecarRegistry._registry)
    saved_loaded = MetadataPgSidecarRegistry._defaults_loaded
    MetadataPgSidecarRegistry.clear()
    MetadataPgSidecarRegistry._registry["metadata_core"] = _FakeCoreCls  # type: ignore[assignment]
    MetadataPgSidecarRegistry._registry["metadata_stac"] = _FakeStacCls  # type: ignore[assignment]
    MetadataPgSidecarRegistry._defaults_loaded = True
    # Clear singletons between tests.
    _FakeCoreCls._instance = None
    _FakeStacCls._instance = None
    try:
        yield
    finally:
        MetadataPgSidecarRegistry.clear()
        MetadataPgSidecarRegistry._registry.update(saved)
        MetadataPgSidecarRegistry._defaults_loaded = saved_loaded


# ---------------------------------------------------------------------------
# TypedDriver bind + capability set
# ---------------------------------------------------------------------------


def test_typed_driver_bind_resolves():
    assert CollectionPostgresqlDriver.config_cls() is CollectionPostgresqlDriverConfig
    assert CollectionPostgresqlDriverConfig.class_key() == "CollectionPostgresqlDriver"


def test_capabilities_union_covers_inner_capabilities():
    """Wrapper advertises union — every cap declared by either inner today
    must appear so the routing layer recognises the wrapper as a drop-in
    for the two raw drivers.
    """
    caps = CollectionPostgresqlDriver.capabilities
    for required in (
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SOFT_DELETE,
        MetadataCapability.SEARCH,         # CORE
        MetadataCapability.SPATIAL_FILTER, # STAC
        MetadataCapability.PHYSICAL_ADDRESSING,
    ):
        assert required in caps


# ---------------------------------------------------------------------------
# Sidecar registry
# ---------------------------------------------------------------------------


def test_default_sidecars_includes_core_and_stac_when_both_registered():
    sidecars = MetadataPgSidecarRegistry.default_sidecars()
    types = [s.sidecar_type for s in sidecars]
    assert types == ["metadata_core", "metadata_stac"]


def test_default_sidecars_omits_stac_when_unregistered():
    """Simulates a deployment without the stac extra installed."""
    MetadataPgSidecarRegistry._registry.pop("metadata_stac", None)
    sidecars = MetadataPgSidecarRegistry.default_sidecars()
    assert [s.sidecar_type for s in sidecars] == ["metadata_core"]


def test_unknown_sidecar_type_skipped_with_warning(caplog):
    """A sidecar config whose type isn't in the registry skips with a
    warning — better than crashing the whole write.
    """
    driver = CollectionPostgresqlDriver()
    bogus = MetadataCoreSidecarConfig.model_construct(sidecar_type="metadata_does_not_exist")
    with caplog.at_level("WARNING"):
        inners = driver._resolve_inner_drivers([bogus])
    assert inners == []
    assert any("metadata_does_not_exist" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Composition fan-out — write/delete go to every inner
# ---------------------------------------------------------------------------


async def test_upsert_metadata_fans_out_to_every_configured_sidecar():
    driver = CollectionPostgresqlDriver()
    payload = {"title": "T", "description": "D", "extent": {"bbox": [[0, 0, 1, 1]]}}
    await driver.upsert_metadata("cat-a", "col-a", payload)
    core = _FakeCoreCls()
    stac = _FakeStacCls()
    assert len(core.upsert_calls) == 1
    assert len(stac.upsert_calls) == 1
    # Wrapper hands the FULL payload to every inner — each inner is
    # responsible for filtering to its own column set (existing
    # `_PgCollectionMetadataBase._filter_payload` invariant).
    assert core.upsert_calls[0]["metadata"] == payload
    assert stac.upsert_calls[0]["metadata"] == payload


async def test_delete_metadata_fans_out_with_soft_flag_preserved():
    driver = CollectionPostgresqlDriver()
    await driver.delete_metadata("cat-a", "col-a", soft=True)
    core, stac = _FakeCoreCls(), _FakeStacCls()
    assert core.delete_calls == [{"catalog_id": "cat-a", "collection_id": "col-a", "soft": True}]
    assert stac.delete_calls == [{"catalog_id": "cat-a", "collection_id": "col-a", "soft": True}]


# ---------------------------------------------------------------------------
# Read fan-in — slices merged shallow, last-wins on key collision
# ---------------------------------------------------------------------------


async def test_get_metadata_merges_slices_from_every_inner():
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
    failing = AsyncMock(spec=CollectionMetadataStore)
    failing.get_metadata.side_effect = RuntimeError("boom")
    failing.is_available.return_value = True
    failing.capabilities = frozenset({MetadataCapability.READ})

    class _FailingCls:
        def __new__(cls):  # type: ignore[misc]
            return failing

    MetadataPgSidecarRegistry._registry["metadata_core"] = _FailingCls  # type: ignore[assignment]
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
        capabilities=frozenset({MetadataCapability.READ}),
    )
    _FakeStacCls._instance = _FakeInner(
        capabilities=frozenset({MetadataCapability.READ}),
    )
    driver = CollectionPostgresqlDriver()
    rows, total = await driver.search_metadata("cat-a", q="foo")
    assert rows == [] and total == 0


# ---------------------------------------------------------------------------
# Wrapper config — sidecar discriminated union round-trip
# ---------------------------------------------------------------------------


def test_wrapper_config_sidecars_discriminated_union_round_trips():
    cfg = CollectionPostgresqlDriverConfig(
        sidecars=[
            MetadataCoreSidecarConfig(),
            MetadataStacSidecarConfig(),
        ],
    )
    dumped = cfg.model_dump(exclude_unset=True)
    restored = CollectionPostgresqlDriverConfig.model_validate(dumped)
    assert [s.sidecar_type for s in restored.sidecars] == [
        "metadata_core", "metadata_stac",
    ]


def test_wrapper_config_default_sidecars_is_empty_list():
    """Empty default — driver falls back to registry default at use time
    (mirrors the items-tier default-fast invariant from M1b.2).
    """
    cfg = CollectionPostgresqlDriverConfig()
    assert cfg.sidecars == []


# ---------------------------------------------------------------------------
# STAC capability marker — wrapper must structurally satisfy
# ``StacCollectionMetadataCapability`` IFF a STAC inner is loaded.
# Regression guard for PR 1e step 3b: before the fix, the wrapper had no
# ``stac_metadata_columns`` method, so ``isinstance(wrapper,
# StacCollectionMetadataCapability)`` returned False and
# ``stac_service._assert_stac_capable_metadata_stack`` warned
# "STAC slice will be dropped on write" even though the wrapper's
# STAC sidecar actually persisted it.
# ---------------------------------------------------------------------------


def test_wrapper_satisfies_stac_capability_when_stac_inner_loaded():
    from dynastore.extensions.stac.protocols import (
        StacCollectionMetadataCapability,
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
    assert isinstance(driver, StacCollectionMetadataCapability)
    cols = driver.stac_metadata_columns()
    assert "extent" in cols
    assert "stac_version" in cols


def test_wrapper_returns_empty_columns_when_no_stac_inner_loaded():
    """Deployment without the stac extra: registry has only metadata_core,
    wrapper's stac_metadata_columns() must return () so
    ``stac_service._has_stac`` correctly identifies STAC as unavailable.
    """
    MetadataPgSidecarRegistry._registry.pop("metadata_stac", None)
    driver = CollectionPostgresqlDriver()
    assert driver.stac_metadata_columns() == ()
