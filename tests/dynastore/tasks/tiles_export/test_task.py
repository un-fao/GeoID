"""Integration test for TilesExportTask."""

import pytest

from tests.dynastore.test_utils import generate_test_id
from dynastore.tasks.tiles_export.task import TilesExportTask
from dynastore.tasks.tiles_export.models import TilesExportRequest
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum, TaskCreate
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tiles.tiles_config import TilesConfig
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
from dynastore.models.driver_context import DriverContext


@pytest.mark.asyncio
@pytest.mark.xdist_group(name="serial")
@pytest.mark.enable_modules(
    "db_config", "db", "catalog", "stac", "tiles", "tasks", "crs",
    "collection_postgresql", "catalog_postgresql",
)
@pytest.mark.enable_extensions("tiles", "assets", "features", "configs")
async def test_tiles_export_task_run_integration(app_lifespan, in_process_client):
    """
    Integration test for TilesExportTask.

    Verifies that:
    1. The task completes without error.
    2. A PMTiles archive is written to the pmtiles_exports table.
    3. The archive starts with the 'PMTiles' magic bytes.
    4. Task outputs contain the expected keys.
    """
    catalog_id = f"cat_export_{generate_test_id(12)}"
    collection_id = f"coll_export_{generate_test_id(12)}"

    # 1. Create catalog and collection.
    resp = await in_process_client.post(
        "/features/catalogs",
        json={"id": catalog_id, "title": "PMTiles Export Test Catalog"},
    )
    assert resp.status_code == 201, resp.text

    resp = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections",
        json={
            "id": collection_id,
            "title": "PMTiles Export Test Collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
            },
        },
    )
    assert resp.status_code == 201, resp.text

    # 2. Ingest a feature so tiles are non-empty.
    resp = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items",
        json={
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
                    },
                    "properties": {"name": "PMTiles Test Area"},
                }
            ],
        },
    )

    # 3. Configure TilesConfig (limit zoom for speed).
    config_service = get_protocol(ConfigsProtocol)
    await config_service.set_config(
        TilesConfig,
        TilesConfig(min_zoom=0, max_zoom=2, supported_tms_ids=["WebMercatorQuad"]),
        catalog_id=catalog_id,
    )

    # 4. Build task payload.
    task_inst = TilesExportTask(app_lifespan)
    request = TilesExportRequest(
        catalog_id=catalog_id,
        collection_id=collection_id,
        tms_id="WebMercatorQuad",
        min_zoom=0,
        max_zoom=2,
    )

    catalogs = get_protocol(CatalogsProtocol)
    schema = await catalogs.resolve_physical_schema(
        catalog_id, ctx=DriverContext(db_resource=app_lifespan.engine)
    )

    db_task = await tasks_module.create_task(
        app_lifespan.engine,
        TaskCreate(
            task_type="tiles_export",
            caller_id="test_runner",
            inputs=request.model_dump(),
            collection_id=collection_id,
        ),
        schema=schema,
    )

    payload = TaskPayload(
        task_id=db_task.task_id,
        inputs=request,
        caller_id="test_admin",
    )

    # 5. Run task.
    await task_inst.run(payload)

    # 6. Verify a PMTiles archive was stored in the DB.
    async with app_lifespan.engine.connect() as conn:
        query = (
            f'SELECT id, n_tiles, n_empty_tiles, total_bytes, data '
            f'FROM "{schema}".pmtiles_exports '
            f'WHERE collection_id = :col ORDER BY created_at DESC LIMIT 1'
        )
        row = await DQLQuery(query, result_handler=ResultHandler.ONE_OR_NONE).execute(
            conn, col=collection_id
        )

    assert row is not None, "No PMTiles export record found"
    export_id, n_tiles, n_empty, total_bytes, raw_data = row

    # PMTiles magic bytes check
    assert raw_data[:7] == b"PMTiles", "Archive missing PMTiles magic header"
    assert raw_data[7] == 3, "PMTiles version must be 3"

    # Stats sanity
    assert n_tiles >= 0
    assert n_empty >= 0
    assert total_bytes == len(raw_data)

    # 7. Verify task outputs in the tasks table.
    async with app_lifespan.engine.connect() as conn:
        task_query = (
            f'SELECT status, outputs FROM "{schema}".tasks WHERE task_id = :tid'
        )
        task_row = await DQLQuery(
            task_query, result_handler=ResultHandler.ONE_OR_NONE
        ).execute(conn, tid=str(db_task.task_id))

    assert task_row is not None
    status, outputs = task_row
    assert status == TaskStatusEnum.COMPLETED.value
    assert outputs is not None
    assert "export_id" in outputs
    assert outputs["export_id"] == export_id
    assert "n_tiles" in outputs
    assert "total_bytes" in outputs
