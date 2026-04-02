import pytest
import os
from sqlalchemy import text
from dynastore.tools.identifiers import generate_task_id, generate_id_hex
from dynastore.tasks.ingestion.ingestion_task import IngestionTask
from dynastore.modules.tasks.models import TaskPayload
from dynastore.modules.processes.models import ExecuteRequest
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction

@pytest.mark.asyncio
async def test_geojson_ingestion_with_lang_en(task_app_state, test_data_loader, data_id):
    """
    Reproduction test for GeoJSON ingestion with lang='en'.
    This should not fail with ValueError: Conflicting language parameters.
    """
    catalog_id = f"cat_lang_{data_id}_{generate_id_hex()[:6]}"
    collection_id = f"test_lang_en_{generate_id_hex()[:6]}"

    # Minimal GeoJSON snippet
    geojson_content = '{"type": "Feature","geometry": {"type": "Point","coordinates": [125.6, 10.1]},"properties": {"name": "Dinagat Islands"}}'
    
    # Create temp file
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as tmp:
        tmp.write(geojson_content)
        geojson_path = tmp.name

    try:
        catalogs: CatalogsProtocol = get_protocol(CatalogsProtocol)
        async with managed_transaction(task_app_state.engine) as conn:
            await catalogs.create_catalog(
                {"id": catalog_id, "title": catalog_id}, lang='en', db_resource=conn
            )

        # Prepare Task with explicit lang='en'
        task = IngestionTask(task_app_state)
        payload = TaskPayload(
            task_id=generate_task_id(),
            caller_id="test_user",
            inputs=ExecuteRequest(
                inputs={
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                    "ingestion_request": {
                        "asset": {"uri": geojson_path},
                        "source_srid": 4326,
                        "lang": "en", # This triggered the error before
                        "column_mapping": {
                            "external_id": "name",
                            "attributes_source_type": "all",
                        },
                    },
                }
            ),
        )

        # Execution - should SUCCEED now
        await task.run(payload)

        # Verification
        async with task_app_state.engine.connect() as conn:
            phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn)
            phys_table = await catalogs.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
            
            result = await conn.execute(text(f'SELECT count(*) FROM "{phys_schema}"."{phys_table}"'))
            assert result.scalar() == 1
            
    finally:
        if os.path.exists(geojson_path):
            os.remove(geojson_path)
