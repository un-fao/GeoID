#    Copyright 2025 FAO
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

import logging
import asyncio
from typing import Dict, List, Tuple, Optional, Any
from shapely.geometry import box
import morecantile

from dynastore.modules.db_config.query_executor import managed_transaction

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.tasks.models import TaskPayload, TaskUpdate, TaskStatusEnum
from dynastore.modules.tasks import tasks_module
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.modules.tiles.tiles_config import (
    TILES_PLUGIN_CONFIG_ID,
    TILES_PRESEED_CONFIG_ID,
    TilesPluginConfig,
    TilesPreseedConfig,
)
from dynastore.modules.tiles import tiles_module
from dynastore.modules.tiles import tiles_db
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS
from dynastore.tools.geospatial import SimplificationAlgorithm
from .models import TilePreseedRequest
from .definition import TILES_PRESEED_PROCESS_DEFINITION
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.processes.models import Process, ExecuteRequest, StatusInfo
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)
class TilePreseedTask(
    ProcessTaskProtocol[Process, TaskPayload[TilePreseedRequest], Optional[StatusInfo]]
):
    """
    OGC Process to pre-seed tiles.
    Iterates through configured TMS/Levels/BBOX and generates tiles to storage.
    """

    @staticmethod
    def get_definition() -> Process:
        return TILES_PRESEED_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None):
        self.app_state = app_state
        # Get engine via protocol system
        self.engine = get_engine()

    async def run(
        self, payload: TaskPayload[TilePreseedRequest]
    ) -> Optional[StatusInfo]:
        request = payload.inputs

        config_manager = get_protocol(ConfigsProtocol)
        catalogs = get_protocol(CatalogsProtocol)
        if config_manager is None or catalogs is None or self.engine is None:
            raise RuntimeError("Required protocols/engine unavailable for tiles preseed task.")

        catalog_id = request.catalog_id
        schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=self.engine)
        )
        if schema is None:
            raise RuntimeError(f"Cannot resolve physical schema for catalog {catalog_id!r}.")

        # Ensure task table exists (Cellular mode - schema might exist but module table might be missing)
        async with managed_transaction(self.engine) as conn:
            await tasks_module.ensure_task_storage_exists(conn, schema)

        # Report Starting
        if payload.task_id:
            await tasks_module.update_task(
                self.engine,
                payload.task_id,
                TaskUpdate(status=TaskStatusEnum.RUNNING, progress=0),
                schema=schema,
            )

        results: Dict[str, Any] = {"generated": 0, "skipped": 0, "errors": 0}

        try:
            # Fetch Configs
            preseed_config = await config_manager.get_config(
                TILES_PRESEED_CONFIG_ID, catalog_id, request.collection_id
            )
            if (
                not isinstance(preseed_config, TilesPreseedConfig)
                or not preseed_config.enabled
            ):
                logger.info(
                    f"Pre-seeding disabled or not configured for {catalog_id}:{request.collection_id}"
                )
                return None  # skipped: pre-seeding disabled

            runtime_config = await config_manager.get_config(
                TILES_PLUGIN_CONFIG_ID, catalog_id, request.collection_id
            )
            if not isinstance(runtime_config, TilesPluginConfig):
                runtime_config = TilesPluginConfig()

            # Resolve Collections
            target_collections = []
            if request.collection_id:
                target_collections = [request.collection_id]
            elif preseed_config.collections_to_preseed:
                target_collections = preseed_config.collections_to_preseed
            else:
                # TODO: Fetch all collections from catalog if none specified?
                # For now, safe default
                logger.warning(
                    f"No specific collection targeted and no list in config for {catalog_id}. Skipping."
                )
                return None  # skipped: no target collections

            # Resolve BBOX
            effective_bboxes = (
                request.update_bbox
                or preseed_config.bboxes
                or runtime_config.bbox
                or [(-180, -90, 180, 90)]
            )

            # Resolve TMS from Input or Config
            target_tms_ids = request.tms_ids or preseed_config.target_tms_ids
            formats = request.formats or preseed_config.formats

            # Resolve Storage (Always use the one selected at boot time)
            storage = tiles_module.get_active_storage_provider()
            if not storage:
                # Fallback to resolving now if boot-time failed or was skipped in some environments
                storage = tiles_module.get_storage_provider(
                    preseed_config.storage_priority
                )

            if not storage:
                raise RuntimeError(f"No storage provider available for pre-seeding.")

            # Initialization Loop to calculate total tiles?
            # Skipping exact total calculation for now to avoid double iteration performance hit.
            # We will use "infinite" progress or just count up.

            total_processed = 0

            for col_id in target_collections:
                # Resolve metadata once per collection (handled by tiles_module with caching)
                meta = await tiles_module.get_tile_resolution_params(catalog_id, col_id)
                if not meta:
                    logger.error(
                        f"Collection {catalog_id}:{col_id} metadata resolution failed."
                    )
                    results["errors"] += 1
                    continue

                for tms_id in target_tms_ids:
                    tms_def = await tiles_module.get_custom_tms(
                        catalog_id=catalog_id, tms_id=tms_id
                    )
                    if not tms_def:
                        tms_def = BUILTIN_TILE_MATRIX_SETS.get(tms_id)
                        if not tms_def:
                            if morecantile is None:
                                logger.error(
                                    f"TMS {tms_id} not found and 'morecantile' is not installed."
                                )
                                results["errors"] += 1
                                continue
                            try:
                                tms_def = morecantile.tms.get(tms_id)
                            except Exception:
                                logger.error(f"TMS {tms_id} not found.")
                                results["errors"] += 1
                                continue

                    if tms_def:
                        # Ensure it's a morecantile.TileMatrixSet to use .tiles() helper
                        if not hasattr(tms_def, "tiles") and morecantile:
                            try:
                                # Convert our internal TileMatrixSet model to morecantile
                                tms_dict = (
                                    tms_def.model_dump(exclude_none=True)
                                    if hasattr(tms_def, "model_dump")
                                    else tms_def
                                )
                                tms_def = morecantile.TileMatrixSet.model_validate(
                                    tms_dict
                                )
                            except Exception as e:
                                logger.error(
                                    f"Failed to convert TMS {tms_id} to morecantile: {e}"
                                )
                                results["errors"] += 1
                                continue

                    # Resolve Target SRID from TMS
                    target_srid = 3857  # Default
                    if hasattr(tms_def, "crs"):
                        try:
                            # Use resolve_srid to handle URIs, EPSG, and custom CRS
                            # cast to string just in case it's a pyproj.CRS object
                            target_srid = await tiles_module.resolve_srid(
                                self.engine, str(tms_def.crs), catalog_id
                            )
                        except Exception as e:
                            logger.warning(
                                f"Failed to resolve SRID for CRS {tms_def.crs}: {e}. Falling back to 3857."
                            )

                    # .tiles() is the morecantile-only helper; ensure we have one.
                    mc_tms: Any = tms_def
                    # Acquire connection once for all zooms/tiles using managed_transaction
                    async with managed_transaction(self.engine) as conn:
                        # Iterate Zooms
                        for z in range(
                            runtime_config.min_zoom, runtime_config.max_zoom + 1
                        ):
                            for bbox in effective_bboxes:
                                # Generate Tile Iterator
                                try:
                                    tiles = mc_tms.tiles(*bbox, zooms=[z])
                                except Exception as e:
                                    logger.error(
                                        f"Error generating tiles for bbox {bbox}: {e}"
                                    )
                                    continue

                                for tile in tiles:
                                    total_processed += 1

                                    # Generate for each format
                                    for fmt in formats:
                                        # Generate Logic
                                        try:
                                            simplification = (
                                                preseed_config.simplification_by_zoom_override.get(
                                                    z
                                                )
                                                if preseed_config.simplification_by_zoom_override
                                                else None
                                            )

                                            data = await tiles_db.get_features_as_mvt_filtered(
                                                conn=conn,
                                                resolved_collections=[meta],
                                                tms_def=mc_tms,
                                                target_srid=target_srid,
                                                z=str(z),
                                                x=tile.x,
                                                y=tile.y,
                                                simplification=simplification,
                                                simplification_algorithm=runtime_config.simplification_algorithm
                                                or SimplificationAlgorithm.TOPOLOGY_PRESERVING,
                                            )

                                            if data:
                                                await storage.save_tile(
                                                    catalog_id=catalog_id,
                                                    collection_id=col_id,
                                                    tms_id=tms_id,
                                                    z=z,
                                                    x=tile.x,
                                                    y=tile.y,
                                                    data=data,
                                                    format=fmt,
                                                )
                                                results["generated"] += 1
                                            else:
                                                logger.debug(
                                                    f"Skipped tile {z}/{tile.x}/{tile.y} (empty content)."
                                                )
                                                results["skipped"] += 1  # Empty tile

                                        except Exception as e:
                                            logger.error(
                                                f"Error generating/saving tile {z}/{tile.x}/{tile.y}: {e}"
                                            )
                                            results["errors"] += 1

                                # Update Progress periodically (e.g. every 100 tiles)
                                if total_processed % 100 == 0 and payload.task_id:
                                    # We don't have total_count, so we can't do percentage accurately.
                                    # We can store "processed_count" in outputs or similar.
                                    # Or assume a large number/heuristic.
                                    # Standardizing on 50% ? No.
                                    # Just update 'outputs' with current stats.
                                    new_outputs = results.copy()
                                    new_outputs["processed_tiles"] = total_processed
                                    await tasks_module.update_task(
                                        self.engine,
                                        payload.task_id,
                                        TaskUpdate(outputs=new_outputs),
                                        schema=schema,
                                    )

            # Completion
            if payload.task_id:
                results["processed_tiles"] = total_processed
                await tasks_module.update_task(
                    self.engine,
                    payload.task_id,
                    TaskUpdate(
                        status=TaskStatusEnum.COMPLETED, progress=100, outputs=results
                    ),
                    schema=schema,
                )

            results["status"] = TaskStatusEnum.COMPLETED.value
            return None  # status persisted via tasks_module.update_task

        except Exception as e:
            logger.error(f"Pre-seed task failed: {e}", exc_info=True)
            if payload.task_id:
                await tasks_module.update_task(
                    self.engine,
                    payload.task_id,
                    TaskUpdate(status=TaskStatusEnum.FAILED, error_message=str(e)),
                    schema=schema,
                )
            raise e
