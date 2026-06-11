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

"""GeoVolumes demo preset — Den Haag LoD2 CityJSON seed.

Seeds a ``demo-3d`` catalog / ``denhaag`` collection with CityJSON buildings
from The Hague, Netherlands (EPSG:7415, LoD2, 2498 CityObjects).  Running
``apply`` is idempotent — the catalog and collection are created only when
absent, and ingestion is skipped when the collection already contains items.
Running ``revoke`` removes the collection and catalog ONLY if this preset
created them and the collection is empty after revoke.

Dataset facts (for reference and assertions):
- CityJSON version: 2.0
- CRS: EPSG:7415
- CityObjects: 2498
- transform: scale [0.001, 0.001, 0.001], translate [78248.66, 457604.591, 2.463]
- Source URL: https://3d.bk.tudelft.nl/opendata/cityjson/3dcities/v2.0/DenHaag_01.city.json
"""
from __future__ import annotations

import logging
import pathlib
import urllib.request
from typing import Any, ClassVar, Optional, Tuple, Type

from pydantic import BaseModel

from dynastore.extensions.geovolumes.cityjson_ingest import (
    CITYOBJECT_FEATURE_TYPE,
    ingest_cityjson_file,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    PresetContext,
    PresetPlan,
    PresetPlanEntry,
)
from dynastore.modules.storage.presets.protocol import PresetTier

logger = logging.getLogger(__name__)

_DEFAULT_SOURCE_URL = (
    "https://3d.bk.tudelft.nl/opendata/cityjson/3dcities/v2.0/DenHaag_01.city.json"
)
_DEFAULT_CATALOG_ID = "demo-3d"
_DEFAULT_COLLECTION_ID = "denhaag"
_TILESET_TASK_TYPE = "geovolumes_tileset"


# ---------------------------------------------------------------------------
# Params model
# ---------------------------------------------------------------------------


class GeoVolumesDemoParams(BaseModel):
    """Parameters for the geovolumes_demo preset."""

    catalog_id: str = _DEFAULT_CATALOG_ID
    collection_id: str = _DEFAULT_COLLECTION_ID
    source_url: str = _DEFAULT_SOURCE_URL
    local_path: Optional[str] = None
    max_features: Optional[int] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _download_source(source_url: str, catalog_id: str, collection_id: str) -> pathlib.Path:
    """Download ``source_url`` to a cache directory and return the local path."""
    import tempfile

    cache_dir = pathlib.Path(tempfile.gettempdir()) / "dynastore_geovolumes" / catalog_id / collection_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    filename = source_url.rsplit("/", 1)[-1] or "dataset.city.json"
    dest = cache_dir / filename
    if not dest.exists():
        logger.info("Downloading %s -> %s", source_url, dest)
        urllib.request.urlretrieve(source_url, dest)  # noqa: S310
    else:
        logger.info("Using cached file %s", dest)
    return dest


async def _collection_item_count(catalogs: Any, catalog_id: str, collection_id: str) -> int:
    """Return the number of items in the collection (best-effort; 0 on error)."""
    try:
        from dynastore.models.query_builder import QueryRequest

        features = await catalogs.search_items(
            catalog_id, collection_id, QueryRequest(limit=1),
        )
        return len(features)
    except Exception as exc:
        logger.debug("item-count probe %s/%s failed: %s", catalog_id, collection_id, exc)
        return 0


async def _catalog_has_collections(catalogs: Any, catalog_id: str) -> bool:
    """Return True if the catalog has at least one collection (fail-safe: True on error)."""
    try:
        cols = await catalogs.list_collections(catalog_id, limit=1)
        return bool(cols)
    except Exception as exc:
        logger.warning("collection probe for catalog %s failed: %s", catalog_id, exc)
        return True


async def _enqueue_tileset_task(ctx: PresetContext, catalog_id: str, collection_id: str) -> None:
    """Submit the geovolumes_tileset background task via the OGC Process engine.

    Deferred import keeps the preset free of a hard dependency on the tasks
    module — if it is absent (e.g. the tasks extension is not installed) the
    error surfaces at apply time with a clear message rather than at import.
    """
    try:
        from dynastore.modules.processes.processes_module import execute_process
        from dynastore.modules.processes import models as _proc_models
        from dynastore.models.auth_models import SYSTEM_USER_ID

        principal = ctx.principal
        caller_id: str = SYSTEM_USER_ID
        if principal is not None:
            pid = getattr(principal, "id", None) or getattr(principal, "principal_id", None)
            if pid is not None:
                caller_id = str(pid)

        exec_request = _proc_models.ExecuteRequest(
            inputs={"catalog_id": catalog_id, "collection_id": collection_id}
        )
        await execute_process(
            _TILESET_TASK_TYPE,
            exec_request,
            engine=ctx.db,
            caller_id=caller_id,
            preferred_mode=_proc_models.JobControlOptions.ASYNC_EXECUTE,
            dedup_key=f"{_TILESET_TASK_TYPE}:{catalog_id}/{collection_id}",
        )
    except ImportError:
        logger.warning(
            "geovolumes_demo: tasks module not available — skipping %s task enqueue",
            _TILESET_TASK_TYPE,
        )


# ---------------------------------------------------------------------------
# Catalog / collection payloads
# ---------------------------------------------------------------------------

_CATALOG_DATA: dict[str, Any] = {
    "id": _DEFAULT_CATALOG_ID,
    "title": {"en": "3D Demo Catalog"},
    "description": {"en": "Demo catalog for 3D GeoVolumes datasets."},
    "keywords": ["demo", "3d", "cityjson", "geovolumes"],
    "license": "CC-BY-4.0",
}

_COLLECTION_DATA: dict[str, Any] = {
    "id": _DEFAULT_COLLECTION_ID,
    "title": {"en": "Den Haag LoD2 Buildings"},
    "description": {
        "en": (
            "CityJSON LoD2 building models for The Hague (Den Haag), Netherlands. "
            "Source: 3D BAG / TU Delft, EPSG:7415, 2498 CityObjects."
        )
    },
    "type": "Feature",
    "feature_type": CITYOBJECT_FEATURE_TYPE,
}


# ---------------------------------------------------------------------------
# Preset class
# ---------------------------------------------------------------------------


class _GeoVolumesDemoPreset:
    """Idempotent preset that seeds the Den Haag LoD2 CityJSON demo dataset."""

    name: ClassVar[str] = "geovolumes_demo"
    description: ClassVar[str] = (
        "Seed a demo-3d catalog with Den Haag LoD2 CityJSON buildings "
        "(2498 CityObjects, EPSG:7415) and enqueue the tileset task."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("demo", "geovolumes", "3d", "cityjson", "platform")
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar[Type[BaseModel]] = GeoVolumesDemoParams

    async def dry_run(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> PresetPlan:
        p = params if isinstance(params, GeoVolumesDemoParams) else GeoVolumesDemoParams()
        entries = (
            PresetPlanEntry(
                kind="ensure_catalog",
                target=p.catalog_id,
                detail={"create_if_absent": True},
            ),
            PresetPlanEntry(
                kind="ensure_collection",
                target=f"{p.catalog_id}/{p.collection_id}",
                detail={"create_if_absent": True, "feature_type": "CITYOBJECT_FEATURE_TYPE"},
            ),
            PresetPlanEntry(
                kind="ingest_cityjson",
                target=p.local_path or p.source_url,
                detail={"max_features": p.max_features},
            ),
            PresetPlanEntry(
                kind="trigger_task",
                target=_TILESET_TASK_TYPE,
                detail={"catalog_id": p.catalog_id, "collection_id": p.collection_id},
            ),
        )
        return PresetPlan(preset_name=self.name, scope_key=scope, entries=entries)

    async def apply(
        self,
        params: BaseModel,
        scope: str,
        ctx: PresetContext,
    ) -> AppliedDescriptor:
        p = params if isinstance(params, GeoVolumesDemoParams) else GeoVolumesDemoParams()
        catalogs = ctx.catalogs

        created_catalog = False
        created_collection = False

        # --- Catalog (create only when absent) ---
        existing_catalog = await catalogs.get_catalog_model(p.catalog_id)
        if existing_catalog is None:
            cat_payload = dict(_CATALOG_DATA)
            cat_payload["id"] = p.catalog_id
            await catalogs.create_catalog(cat_payload, lang="*")
            created_catalog = True

        # --- Collection (create only when absent) ---
        existing_collection = await catalogs.get_collection(p.catalog_id, p.collection_id, lang="*")
        if existing_collection is None:
            coll_payload = dict(_COLLECTION_DATA)
            coll_payload["id"] = p.collection_id
            await catalogs.create_collection(p.catalog_id, coll_payload, lang="*")
            created_collection = True

        # --- Idempotency: skip ingestion when items already present ---
        item_count = await _collection_item_count(catalogs, p.catalog_id, p.collection_id)
        if item_count > 0:
            logger.info(
                "geovolumes_demo: collection %s/%s already has %d item(s) — skipping ingestion",
                p.catalog_id, p.collection_id, item_count,
            )
            return AppliedDescriptor(payload={
                "catalog_id": p.catalog_id,
                "collection_id": p.collection_id,
                "created_catalog": created_catalog,
                "created_collection": created_collection,
                "skipped": True,
            })

        # --- Resolve source file ---
        if p.local_path:
            source_path: pathlib.Path = pathlib.Path(p.local_path)
        else:
            source_path = _download_source(p.source_url, p.catalog_id, p.collection_id)

        # --- Ingest ---
        ingest_result = await ingest_cityjson_file(
            p.catalog_id,
            p.collection_id,
            source_path,
            item_service=catalogs,
            catalog_service=catalogs,
        )
        logger.info(
            "geovolumes_demo: ingested %d item(s), %d failed, %d warning(s)",
            ingest_result.get("items", 0),
            ingest_result.get("failed", 0),
            len(ingest_result.get("warnings", [])),
        )

        # --- Enqueue tileset task ---
        await _enqueue_tileset_task(ctx, p.catalog_id, p.collection_id)

        return AppliedDescriptor(payload={
            "catalog_id": p.catalog_id,
            "collection_id": p.collection_id,
            "created_catalog": created_catalog,
            "created_collection": created_collection,
            "skipped": False,
        })

    async def revoke(
        self,
        applied_descriptor: AppliedDescriptor,
        ctx: PresetContext,
    ) -> None:
        payload = applied_descriptor.payload

        # If apply was a skip, there is nothing to undo.
        if payload.get("skipped"):
            return

        catalog_id: str = payload.get("catalog_id", _DEFAULT_CATALOG_ID)
        collection_id: str = payload.get("collection_id", _DEFAULT_COLLECTION_ID)
        created_catalog: bool = bool(payload.get("created_catalog", False))
        created_collection: bool = bool(payload.get("created_collection", False))
        catalogs = ctx.catalogs

        # --- Collection (only if we created it and it is empty) ---
        collection_deleted = False
        if created_collection:
            item_count = await _collection_item_count(catalogs, catalog_id, collection_id)
            if item_count > 0:
                logger.warning(
                    "geovolumes_demo: collection %s/%s still holds %d item(s) — leaving in place",
                    catalog_id, collection_id, item_count,
                )
            else:
                try:
                    await catalogs.delete_collection(catalog_id, collection_id, force=True)
                    collection_deleted = True
                except Exception as exc:
                    logger.warning(
                        "geovolumes_demo: delete_collection %s/%s failed: %s",
                        catalog_id, collection_id, exc,
                    )

        # --- Catalog (only if we created it AND the collection was removed, so the
        #     catalog may now be empty; never attempt if we just left the collection
        #     alive — it would fail the emptiness check anyway) ---
        if created_catalog and collection_deleted:
            if await _catalog_has_collections(catalogs, catalog_id):
                logger.warning(
                    "geovolumes_demo: catalog %s still has collections — leaving in place",
                    catalog_id,
                )
            else:
                try:
                    await catalogs.delete_catalog(catalog_id, force=True)
                except Exception as exc:
                    logger.warning(
                        "geovolumes_demo: delete_catalog %s failed: %s", catalog_id, exc,
                    )


# ---------------------------------------------------------------------------
# Preset instance + registration
# ---------------------------------------------------------------------------

GEOVOLUMES_DEMO_PRESET = _GeoVolumesDemoPreset()

from dynastore.modules.storage.presets.registry import register_preset as _register_preset  # noqa: E402

_register_preset(GEOVOLUMES_DEMO_PRESET)
