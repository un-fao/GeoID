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
from typing import Any, Dict, List, Optional

from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.processes.protocols import ProcessTaskProtocol
from dynastore.modules.processes.models import ExecuteRequest, Process, StatusInfo
from dynastore.modules.tasks.models import TaskPayload, TaskUpdate, TaskStatusEnum
from dynastore.modules.tasks import tasks_module
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.modules.tiles.tiles_config import TilesConfig, TilesPreseedConfig
from dynastore.modules.tiles.tiles_module import TileStorageProtocol
from dynastore.modules.tiles.tile_cache_sync import (
    DEFAULT_MAX_TILES_PER_BATCH,
    SERVED_TILE_FORMATS,
    affected_tiles_for_batch,
)
from dynastore.tools.protocol_helpers import get_engine
from dynastore.models.driver_context import DriverContext
from .definition import TILES_INVALIDATE_PROCESS_DEFINITION
from .models import TileInvalidateRequest

logger = logging.getLogger(__name__)


class TileInvalidateTask(
    ProcessTaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]]
):
    """In-process tile cache invalidation task.

    Light write-reactive path: for the changed bbox(es) + zoom range, compute
    the covered tiles and DELETE them via the registered TileStorageProtocol.
    No MVT render, no save. Runs as a background task on the catalog service
    rather than a Cloud Run Job — the delete-only path is fast enough to keep
    in-process.

    Deleting an absent tile is a no-op success, so the next read repopulates
    lazily from fresh data.
    """

    @staticmethod
    def get_definition() -> Process:
        return TILES_INVALIDATE_PROCESS_DEFINITION

    def __init__(self, app_state: Any = None) -> None:
        self.app_state = app_state
        self.engine = get_engine()

    async def run(
        self, payload: TaskPayload[ExecuteRequest]
    ) -> Optional[StatusInfo]:
        # OGC Process dispatch wraps the body in an ExecuteRequest:
        # payload.inputs.inputs is the user-supplied dict. Same unwrap pattern
        # as TilePreseedTask, DwhJoinExportTask, ExportFeaturesTask.
        request = TileInvalidateRequest.model_validate(payload.inputs.inputs)
        config_manager = get_protocol(ConfigsProtocol)
        catalogs = get_protocol(CatalogsProtocol)
        if config_manager is None or catalogs is None or self.engine is None:
            raise RuntimeError(
                "Required protocols/engine unavailable for tiles invalidate task."
            )
        engine: DbResource = self.engine

        catalog_id = request.catalog_id
        schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=engine)
        )
        if schema is None:
            raise RuntimeError(
                f"Cannot resolve physical schema for catalog {catalog_id!r}."
            )

        if payload.task_id:
            await tasks_module.update_task(
                engine,
                payload.task_id,
                TaskUpdate(status=TaskStatusEnum.RUNNING, progress=0),
                schema=schema,
            )

        results: Dict[str, Any] = {
            "deleted": 0,
            "errors": 0,
            "operation": "invalidate",
        }

        try:
            preseed_config = await config_manager.get_config(
                TilesPreseedConfig, catalog_id, request.collection_id
            )
            runtime_config = await config_manager.get_config(
                TilesConfig, catalog_id, request.collection_id
            )
            if not isinstance(runtime_config, TilesConfig):
                runtime_config = TilesConfig()

            # Resolve target collections exactly like the preseed render path.
            target_collections: List[str] = []
            if request.collection_id:
                target_collections = [request.collection_id]
            elif (
                isinstance(preseed_config, TilesPreseedConfig)
                and preseed_config.collections_to_preseed
            ):
                target_collections = preseed_config.collections_to_preseed
            else:
                logger.warning(
                    "tiles_invalidate: no target collections for %s — "
                    "skipping invalidation.",
                    catalog_id,
                )
                if payload.task_id:
                    await tasks_module.update_task(
                        engine,
                        payload.task_id,
                        TaskUpdate(
                            status=TaskStatusEnum.COMPLETED,
                            progress=100,
                            outputs=results,
                        ),
                        schema=schema,
                    )
                return None

            # Served TMS ids + zoom range from TilesConfig. The request may
            # override the TMS ids; preseed_config.target_tms_ids is the
            # configured fallback, mirroring the preseed render path.
            tms_ids = (
                request.tms_ids
                or (
                    preseed_config.target_tms_ids
                    if isinstance(preseed_config, TilesPreseedConfig)
                    else None
                )
                or runtime_config.supported_tms_ids
                or ["WebMercatorQuad"]
            )
            min_zoom = int(runtime_config.min_zoom)
            max_zoom = int(runtime_config.max_zoom)

            # The bboxes to invalidate. In practice the write hook always
            # supplies update_bbox; fall back to preseed/runtime/world bbox.
            effective_bboxes = (
                request.update_bbox
                or (
                    preseed_config.bboxes
                    if isinstance(preseed_config, TilesPreseedConfig)
                    else None
                )
                or runtime_config.bbox
                or [(-180.0, -85.0511287798, 180.0, 85.0511287798)]
            )

            storage = get_protocol(TileStorageProtocol)
            if storage is None:
                # Degrade-safe: nothing to invalidate without a tile store.
                logger.warning(
                    "tiles_invalidate: no TileStorageProtocol registered "
                    "for %s — nothing to invalidate.",
                    catalog_id,
                )
                if payload.task_id:
                    await tasks_module.update_task(
                        engine,
                        payload.task_id,
                        TaskUpdate(
                            status=TaskStatusEnum.COMPLETED,
                            progress=100,
                            outputs=results,
                        ),
                        schema=schema,
                    )
                return None

            # Reuse the Phase-1 coverage math so the fan-out is bounded exactly
            # as the write path bounded it (continent-scale bbox is capped).
            # Prior (pre-write) extents are already unioned into update_bbox at
            # enqueue time, so effective_bboxes is the full coverage to drop.
            coverage_bboxes = [
                (float(b[0]), float(b[1]), float(b[2]), float(b[3]))
                for b in effective_bboxes
            ]
            tiles = affected_tiles_for_batch(
                coverage_bboxes,
                list(tms_ids),
                min_zoom,
                max_zoom,
                max_tiles=DEFAULT_MAX_TILES_PER_BATCH,
            )

            # Prefer the coordinate-variant primitive (drops every served format
            # + every effective_cache_id variant of the coordinate in one call);
            # fall back to per-format delete_tile for providers that lack it.
            delete_variants = getattr(storage, "delete_tile_variants", None)
            delete_tile = getattr(storage, "delete_tile", None)

            for col_id in target_collections:
                for (tms_id, z, x, y) in tiles:
                    try:
                        if delete_variants is not None:
                            await delete_variants(
                                catalog_id, col_id, tms_id, z, x, y,
                                SERVED_TILE_FORMATS,
                            )
                            results["deleted"] += 1
                        elif delete_tile is not None:
                            # Legacy fallback: delete every served format under
                            # the bare collection id (cache-id-suffixed variants
                            # are unreachable here).
                            for fmt in SERVED_TILE_FORMATS:
                                await delete_tile(
                                    catalog_id, col_id, tms_id, z, x, y, fmt
                                )
                            results["deleted"] += 1
                        else:
                            logger.warning(
                                "tiles_invalidate: provider has no "
                                "delete_tile / delete_tile_variants for %s — "
                                "cannot invalidate.",
                                catalog_id,
                            )
                            break
                    except Exception as exc:
                        logger.warning(
                            "tiles_invalidate: delete failed for "
                            "%s/%s %s/%d/%d/%d: %s",
                            catalog_id, col_id, tms_id, z, x, y, exc,
                        )
                        results["errors"] += 1

            logger.info(
                "tiles_invalidate: deleted %d tile(s) for %s "
                "(%d collection(s), %d bbox(es), zoom %d..%d)",
                results["deleted"],
                catalog_id,
                len(target_collections),
                len(effective_bboxes),
                min_zoom,
                max_zoom,
            )

            if payload.task_id:
                await tasks_module.update_task(
                    engine,
                    payload.task_id,
                    TaskUpdate(
                        status=TaskStatusEnum.COMPLETED,
                        progress=100,
                        outputs=results,
                    ),
                    schema=schema,
                )
            return None

        except Exception as e:
            logger.error(
                "Tile invalidation task failed: %s", e, exc_info=True
            )
            if payload.task_id:
                await tasks_module.update_task(
                    engine,
                    payload.task_id,
                    TaskUpdate(
                        status=TaskStatusEnum.FAILED, error_message=str(e)
                    ),
                    schema=schema,
                )
            raise
