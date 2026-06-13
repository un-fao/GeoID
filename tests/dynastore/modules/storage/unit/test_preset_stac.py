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

"""StacPreset + StacStorageConfig no-DB unit tests.

Verifies:
- StacPreset is registered and retrievable by name.
- build() matrix: for (level x storage) combos, the bundle contains the
  right StacStorageConfig values and the expected routing driver_refs per
  tier.  PG combos include the stac slice in wrapper driver-config sidecars.
- Sidecar resolution gate: StacItemsSidecar.get_default_config injects iff
  stac_items_pg=True in context (and collection_type != RECORDS).
- default_catalog_sidecars / default_sidecars return CORE only by default.
- _resolve_stac_items_pg helper: returns False when no ConfigsProtocol.
"""
from __future__ import annotations

import pytest

from dynastore.modules.stac.stac_storage_config import (
    StacLevel,
    StacStorageBackend,
    StacStorageConfig,
    catalog_stac_enabled,
    collection_stac_enabled,
    es_stac,
    items_stac_enabled,
    pg_stac,
)
from dynastore.modules.storage.presets import get_preset
from dynastore.modules.storage.presets.preset import CompositePreset
from dynastore.modules.storage.presets.stac import (
    StacPreset,
    StacPresetParams,
    StacRoutingPreset,
    StacStoragePreset,
    _build_stac_routing_bundle,
    _build_stac_storage_bundle,
)
from dynastore.modules.storage.routing_config import (
    Operation,
)


def _routing(params: StacPresetParams):
    """The routing-only bundle for ``params`` (no StacStorageConfig)."""
    return _build_stac_routing_bundle(params, catalog_id="cat-1")


def _storage(params: StacPresetParams):
    """The SSOT-only bundle for ``params`` (single StacStorageConfig)."""
    return _build_stac_storage_bundle(params, catalog_id="cat-1")


# ---------------------------------------------------------------------------
# StacStorageConfig helpers
# ---------------------------------------------------------------------------


def test_stac_level_helpers_none():
    assert not catalog_stac_enabled(StacLevel.NONE)
    assert not collection_stac_enabled(StacLevel.NONE)
    assert not items_stac_enabled(StacLevel.NONE)


def test_stac_level_helpers_catalog():
    assert catalog_stac_enabled(StacLevel.CATALOG)
    assert not collection_stac_enabled(StacLevel.CATALOG)
    assert not items_stac_enabled(StacLevel.CATALOG)


def test_stac_level_helpers_collection():
    assert catalog_stac_enabled(StacLevel.COLLECTION)
    assert collection_stac_enabled(StacLevel.COLLECTION)
    assert not items_stac_enabled(StacLevel.COLLECTION)


def test_stac_level_helpers_items():
    assert catalog_stac_enabled(StacLevel.ITEMS)
    assert collection_stac_enabled(StacLevel.ITEMS)
    assert items_stac_enabled(StacLevel.ITEMS)


def test_stac_backend_helpers():
    assert pg_stac(StacStorageBackend.PG)
    assert pg_stac(StacStorageBackend.ES_PG)
    assert not pg_stac(StacStorageBackend.ES)
    assert es_stac(StacStorageBackend.ES)
    assert es_stac(StacStorageBackend.ES_PG)
    assert not es_stac(StacStorageBackend.PG)


# ---------------------------------------------------------------------------
# Preset registration
# ---------------------------------------------------------------------------


def test_stac_preset_registered():
    p = get_preset("stac")
    assert p.name == "stac"
    assert p.description, "preset must carry a non-empty description"
    assert isinstance(p, StacPreset)
    # ``stac`` is now the composite of the two single-responsibility children,
    # applied routing-first so the drivers the SSOT references exist before the
    # signal flips.
    assert isinstance(p, CompositePreset)
    assert p.compose == ("stac_routing", "stac_storage")


def test_stac_children_registered():
    rp = get_preset("stac_routing")
    sp = get_preset("stac_storage")
    assert isinstance(rp, StacRoutingPreset)
    assert isinstance(sp, StacStoragePreset)
    # Both children share the same params model so the composite can forward
    # one validated ``StacPresetParams`` to each.
    assert rp.params_model is StacPresetParams
    assert sp.params_model is StacPresetParams
    # Both are catalog-scopable (apply to a catalog scope, not just platform).
    assert rp.catalog_scopable
    assert sp.catalog_scopable


# ---------------------------------------------------------------------------
# The split invariant — routing carries NO SSOT; storage carries ONLY the SSOT
# ---------------------------------------------------------------------------


def test_split_invariant_routing_has_no_ssot_storage_has_only_ssot():
    params = StacPresetParams(stac_level=StacLevel.ITEMS, stac_storage=StacStorageBackend.ES_PG)
    rb = _routing(params)
    sb = _storage(params)
    # Storage bundle: exactly one entry, the StacStorageConfig SSOT.
    assert len(sb.entries) == 1
    assert sb.entries[0].config_cls is StacStorageConfig
    # Routing bundle: zero StacStorageConfig entries (routing is orthogonal).
    assert all(e.config_cls is not StacStorageConfig for e in rb.entries)


# ---------------------------------------------------------------------------
# Bundle shape — stac_level=NONE
# ---------------------------------------------------------------------------


def test_stac_none_storage_bundle_contains_only_ssot():
    params = StacPresetParams(stac_level=StacLevel.NONE, stac_storage=StacStorageBackend.ES_PG)
    sb = _storage(params)
    # Only the StacStorageConfig entry, carrying NONE (revoke removes it).
    assert len(sb.entries) == 1
    entry = sb.entries[0]
    assert entry.config_cls is StacStorageConfig
    assert isinstance(entry.instance, StacStorageConfig)
    assert entry.instance.stac_level == StacLevel.NONE


def test_stac_none_routing_bundle_is_empty():
    params = StacPresetParams(stac_level=StacLevel.NONE, stac_storage=StacStorageBackend.ES_PG)
    rb = _routing(params)
    assert len(rb.entries) == 0


# ---------------------------------------------------------------------------
# Bundle shape — level=CATALOG x storage=ES
# ---------------------------------------------------------------------------


def _all_driver_refs(bundle) -> set:
    refs = set()
    for e in bundle.entries:
        cfg = e.instance
        if hasattr(cfg, "operations"):
            for entries in cfg.operations.values():
                for op_entry in entries:
                    refs.add(op_entry.driver_ref)
    return refs


def test_stac_catalog_es_bundle():
    params = StacPresetParams(stac_level=StacLevel.CATALOG, stac_storage=StacStorageBackend.ES)
    rb = _routing(params)
    sb = _storage(params)

    # SSOT (in the storage bundle) carries the requested level + backend.
    ssot = next(e for e in sb.entries if e.config_cls is StacStorageConfig)
    assert ssot.instance.stac_level == StacLevel.CATALOG
    assert ssot.instance.stac_storage == StacStorageBackend.ES

    # Catalog routing entry present, no collection or items entries.
    assert rb.catalog_routing is not None
    assert rb.collection_template is None
    assert rb.items_template is None

    # ES driver present in catalog routing.
    refs = _all_driver_refs(rb)
    assert "catalog_elasticsearch_driver" in refs
    assert "catalog_postgresql_driver" not in refs

    # No PG driver-config entries (ES-only).
    from dynastore.modules.storage.drivers.catalog_postgresql import (
        CatalogPostgresqlDriverConfig,
    )
    pg_cfg_entries = [e for e in rb.entries if e.config_cls is CatalogPostgresqlDriverConfig]
    assert len(pg_cfg_entries) == 0


# ---------------------------------------------------------------------------
# Bundle shape — level=COLLECTION x storage=ES_PG
# ---------------------------------------------------------------------------


def test_stac_collection_es_pg_bundle():
    params = StacPresetParams(
        stac_level=StacLevel.COLLECTION, stac_storage=StacStorageBackend.ES_PG
    )
    rb = _routing(params)
    sb = _storage(params)

    ssot = next(e for e in sb.entries if e.config_cls is StacStorageConfig)
    assert ssot.instance.stac_level == StacLevel.COLLECTION
    assert ssot.instance.stac_storage == StacStorageBackend.ES_PG

    # Both catalog and collection routing present, no items.
    assert rb.catalog_routing is not None
    assert rb.collection_template is not None
    assert rb.items_template is None

    refs = _all_driver_refs(rb)
    assert "catalog_postgresql_driver" in refs
    assert "catalog_elasticsearch_driver" in refs
    assert "collection_postgresql_driver" in refs
    assert "collection_elasticsearch_driver" in refs
    assert "items_postgresql_driver" not in refs
    assert "items_elasticsearch_driver" not in refs

    # The PG stac slice is NOT authored into the bundle: StacStorageConfig is
    # the single SSOT and the catalog/collection wrappers add the stac slice at
    # runtime by reading it. So neither bundle carries wrapper driver-config
    # entries — only the SSOT (storage) + routing.
    from dynastore.modules.storage.drivers.catalog_postgresql import (
        CatalogPostgresqlDriverConfig,
    )
    from dynastore.modules.storage.drivers.collection_postgresql import (
        CollectionPostgresqlDriverConfig,
    )
    entry_cls = {type(e.instance) for e in rb.entries} | {type(e.instance) for e in sb.entries}
    assert CatalogPostgresqlDriverConfig not in entry_cls
    assert CollectionPostgresqlDriverConfig not in entry_cls


# ---------------------------------------------------------------------------
# Bundle shape — level=ITEMS x storage=PG
# ---------------------------------------------------------------------------


def test_stac_items_pg_bundle():
    params = StacPresetParams(stac_level=StacLevel.ITEMS, stac_storage=StacStorageBackend.PG)
    rb = _routing(params)
    sb = _storage(params)

    ssot = next(e for e in sb.entries if e.config_cls is StacStorageConfig)
    assert ssot.instance.stac_level == StacLevel.ITEMS
    assert ssot.instance.stac_storage == StacStorageBackend.PG

    # All three tiers present in routing.
    assert rb.catalog_routing is not None
    assert rb.collection_template is not None
    assert rb.items_template is not None

    refs = _all_driver_refs(rb)
    assert "catalog_postgresql_driver" in refs
    assert "collection_postgresql_driver" in refs
    assert "items_postgresql_driver" in refs
    # No ES when PG-only.
    assert "catalog_elasticsearch_driver" not in refs
    assert "collection_elasticsearch_driver" not in refs
    assert "items_elasticsearch_driver" not in refs


# ---------------------------------------------------------------------------
# Bundle shape — level=ITEMS x storage=ES_PG (default params)
# ---------------------------------------------------------------------------


def test_stac_items_es_pg_bundle():
    params = StacPresetParams(stac_level=StacLevel.ITEMS, stac_storage=StacStorageBackend.ES_PG)
    rb = _routing(params)

    assert rb.catalog_routing is not None
    assert rb.collection_template is not None
    assert rb.items_template is not None

    refs = _all_driver_refs(rb)
    assert "catalog_postgresql_driver" in refs
    assert "catalog_elasticsearch_driver" in refs
    assert "collection_postgresql_driver" in refs
    assert "collection_elasticsearch_driver" in refs
    assert "items_postgresql_driver" in refs
    assert "items_elasticsearch_driver" in refs

    # ES SEARCH entries in items routing.
    search_refs = [
        e.driver_ref
        for e in rb.items_template.operations.get(Operation.SEARCH, [])
    ]
    assert "items_elasticsearch_driver" in search_refs


# ---------------------------------------------------------------------------
# Sidecar resolution gate — StacItemsSidecar.get_default_config
# ---------------------------------------------------------------------------


def test_stac_items_sidecar_not_injected_by_default():
    """No stac_items_pg in context => no sidecar injected (opt-in default)."""
    from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar

    result = StacItemsSidecar.get_default_config({})
    assert result is None


def test_stac_items_sidecar_not_injected_when_false():
    from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar

    result = StacItemsSidecar.get_default_config({"stac_items_pg": False})
    assert result is None


def test_stac_items_sidecar_injected_when_stac_items_pg_true():
    from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar
    from dynastore.extensions.stac.stac_metadata_config import StacItemsSidecarConfig

    result = StacItemsSidecar.get_default_config({"stac_items_pg": True})
    assert isinstance(result, StacItemsSidecarConfig)


def test_stac_items_sidecar_skipped_for_records_even_when_pg_true():
    """RECORDS guard takes priority regardless of stac_items_pg."""
    from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar

    result = StacItemsSidecar.get_default_config(
        {"stac_items_pg": True, "collection_type": "RECORDS"}
    )
    assert result is None


# ---------------------------------------------------------------------------
# default_catalog_sidecars — CORE only by default
# ---------------------------------------------------------------------------


def test_default_catalog_sidecars_core_only():
    from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry

    SidecarRegistry.clear_catalog_registry()
    sidecars = SidecarRegistry.default_catalog_sidecars()
    types = [getattr(s, "sidecar_type", None) for s in sidecars]
    assert "catalog_core" in types
    assert "catalog_stac" not in types, (
        "catalog_stac must NOT be in the default list (opt-in via StacPreset)"
    )


# ---------------------------------------------------------------------------
# CollectionPgSidecarRegistry.default_sidecars — CORE only by default
# ---------------------------------------------------------------------------


def test_default_collection_sidecars_core_only():
    from dynastore.modules.storage.drivers.collection_postgresql import (
        CollectionPgSidecarRegistry,
    )

    CollectionPgSidecarRegistry.clear()
    sidecars = CollectionPgSidecarRegistry.default_sidecars()
    types = [getattr(s, "sidecar_type", None) for s in sidecars]
    assert "collection_core" in types
    assert "collection_stac" not in types, (
        "collection_stac must NOT be in the default list (opt-in via StacPreset)"
    )


# ---------------------------------------------------------------------------
# _resolve_stac_items_pg — no ConfigsProtocol => False
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_no_protocol_returns_false():
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=None)
    assert result is False


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_none_config_returns_false():
    """ConfigsProtocol returns None for StacStorageConfig => no STAC."""
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is False


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_none_level_returns_false():
    """StacStorageConfig with stac_level=NONE => stac_items_pg=False."""
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            if cls is StacStorageConfig:
                return StacStorageConfig(
                    stac_level=StacLevel.NONE,
                    stac_storage=StacStorageBackend.ES_PG,
                )
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is False


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_items_level_pg_returns_true():
    """StacStorageConfig(ITEMS, ES_PG) => stac_items_pg=True."""
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            if cls is StacStorageConfig:
                return StacStorageConfig(
                    stac_level=StacLevel.ITEMS,
                    stac_storage=StacStorageBackend.ES_PG,
                )
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is True


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_collection_level_returns_false():
    """stac_level=COLLECTION => items not enabled => False even with PG."""
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            if cls is StacStorageConfig:
                return StacStorageConfig(
                    stac_level=StacLevel.COLLECTION,
                    stac_storage=StacStorageBackend.PG,
                )
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is False


@pytest.mark.asyncio
async def test_resolve_stac_items_pg_with_items_es_only_returns_false():
    """stac_level=ITEMS but storage=ES => pg_stac=False => False."""
    from dynastore.modules.storage.drivers.postgresql import _resolve_stac_items_pg

    class FakeConfigs:
        async def get_config(self, cls, **kwargs):
            if cls is StacStorageConfig:
                return StacStorageConfig(
                    stac_level=StacLevel.ITEMS,
                    stac_storage=StacStorageBackend.ES,
                )
            return None

    result = await _resolve_stac_items_pg("cat-1", "coll-1", configs=FakeConfigs())
    assert result is False
