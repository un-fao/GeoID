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

"""``file_backed`` preset — a collection whose items are read directly from an
attached geospatial file asset (read-only), with optional global ES discovery.

Applying this preset at a collection scope binds the DuckDB driver to a catalog
asset and routes the collection so it reads features straight from the file
(GeoParquet/GeoPackage/Shapefile/GeoJSON/CSV) instead of from PostgreSQL. When
``discoverable`` (the default), the collection also routes a secondary
Elasticsearch indexer and the bundle requests geometry simplification, so the
file's features — simplified — land in the global searchable index while the file
stays the source of truth for exact geometry.

Parameters:

- ``asset_id`` — catalog asset to read from (resolved to its storage URI at
  dispatch). Either ``asset_id`` or ``path`` must be provided.
- ``path`` — direct file path/glob, when not binding a catalog asset.
- ``format`` (default ``parquet``) — parquet/csv/json or gpkg/shp/geojson/fgb.
- ``id_column`` — source column holding the native feature id used to derive a
  stable geoid; a content hash is used when unset.
- ``discoverable`` (default ``True``) — index simplified features into the global
  ES index and make them searchable.
- ``simplify_geometry`` (default ``True``) — simplify geometry for the ES index
  (ignored when not discoverable).
"""
from __future__ import annotations

import logging
from typing import ClassVar, Optional

from pydantic import BaseModel

from dynastore.modules.storage.driver_config import (
    ItemsDuckdbDriverConfig,
    ItemsElasticsearchDriverConfig,
)
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import (
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

from .bundle_preset import BundlePreset, _scope_to_kwargs
from .protocol import PresetBundle, PresetBundleEntry, PresetTier

logger = logging.getLogger(__name__)


class FileBackedPresetParams(BaseModel):
    """Parameters for the ``file_backed`` preset."""

    asset_id: Optional[str] = None
    path: Optional[str] = None
    format: str = "parquet"
    id_column: Optional[str] = None
    discoverable: bool = True
    simplify_geometry: bool = True


def _file_backed_routing(params: FileBackedPresetParams) -> ItemsRoutingConfig:
    """Route reads to the file driver; when discoverable, fan a secondary ES
    indexer on WRITE and prefer ES for SEARCH (file driver as fallback)."""
    # READ: the file is the exact source of truth — GEOMETRY_EXACT resolves the
    # DuckDB file driver (which advertises that hint).
    read = [
        OperationDriverEntry(
            driver_ref="items_duckdb_driver",
            hints={Hint.GEOMETRY_EXACT},
        ),
    ]
    operations: dict = {Operation.READ: read}

    if params.discoverable:
        operations[Operation.WRITE] = [
            OperationDriverEntry(
                driver_ref="items_elasticsearch_driver",
                write_mode=WriteMode.ASYNC,
                on_failure=FailurePolicy.OUTBOX,
                secondary_index=True,
                source="auto",
            ),
        ]
        operations[Operation.SEARCH] = [
            OperationDriverEntry(driver_ref="items_elasticsearch_driver", source="auto"),
            OperationDriverEntry(driver_ref="items_duckdb_driver"),
        ]
    else:
        operations[Operation.SEARCH] = [
            OperationDriverEntry(driver_ref="items_duckdb_driver"),
        ]

    return ItemsRoutingConfig(operations=operations)


class FileBackedPreset(BundlePreset):
    """Read a collection's items directly from an attached file asset."""

    name = "file_backed"
    tier: ClassVar[PresetTier] = PresetTier.COLLECTION
    catalog_scopable: ClassVar[bool] = False
    params_model: ClassVar = FileBackedPresetParams
    keywords: ClassVar = ("routing", "file", "duckdb", "asset", "geoparquet", "geopackage")
    description = (
        "Read a collection's items directly from an attached geospatial file "
        "asset (GeoParquet/GeoPackage/Shapefile/GeoJSON/CSV) via DuckDB instead "
        "of ingesting into PostgreSQL. Read-only; when discoverable (default), "
        "simplified features are indexed into the global Elasticsearch index "
        "while the file remains the source of truth for exact geometry."
    )

    def _build_bundle(self, params: BaseModel, scope_kwargs) -> PresetBundle:  # type: ignore[override]
        p = params if isinstance(params, FileBackedPresetParams) else \
            FileBackedPresetParams.model_validate(params.model_dump())

        entries = [
            PresetBundleEntry(
                slot="duckdb_driver_config",
                config_cls=ItemsDuckdbDriverConfig,
                instance=ItemsDuckdbDriverConfig(
                    asset_id=p.asset_id,
                    path=p.path,
                    format=p.format,
                    id_column=p.id_column,
                ),
                rollback_priority=10,
            ),
            PresetBundleEntry(
                slot="items_routing",
                config_cls=ItemsRoutingConfig,
                instance=_file_backed_routing(p),
                rollback_priority=20,
            ),
        ]
        if p.discoverable:
            entries.append(
                PresetBundleEntry(
                    slot="es_driver_config",
                    config_cls=ItemsElasticsearchDriverConfig,
                    instance=ItemsElasticsearchDriverConfig(
                        simplify_geometry=p.simplify_geometry,
                    ),
                    rollback_priority=5,
                )
            )
        return PresetBundle(entries=tuple(entries))

    async def apply(self, params, scope, ctx):  # type: ignore[override]
        descriptor = await super().apply(params, scope, ctx)
        # When discoverable, kick off the initial file->ES reindex so the global
        # index is populated without waiting for a write that never comes (the
        # file is read-only). Best-effort — a failure here must not fail the
        # preset apply; the reindex can also be triggered manually.
        try:
            p = params if isinstance(params, FileBackedPresetParams) else \
                FileBackedPresetParams.model_validate(params.model_dump())
            if p.discoverable:
                await self._enqueue_initial_reindex(scope)
        except Exception:
            logger.warning(
                "file_backed preset: initial reindex enqueue failed for scope=%s; "
                "the collection is configured but the global index is not yet "
                "populated. Trigger a reindex manually.", scope, exc_info=True,
            )
        return descriptor

    @staticmethod
    async def _enqueue_initial_reindex(scope: str) -> None:
        scope_kwargs = _scope_to_kwargs(scope)
        catalog_id = scope_kwargs.get("catalog_id")
        collection_id = scope_kwargs.get("collection_id")
        if not catalog_id or not collection_id:
            return
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols import DatabaseProtocol
        from dynastore.tasks.ingestion.main_ingestion import enqueue_collection_reindex_task

        db = get_protocol(DatabaseProtocol)
        engine = getattr(db, "engine", None) if db else None
        await enqueue_collection_reindex_task(
            catalog_id, collection_id, pg_conn=engine,
        )
