import pytest
import os
import asyncio
import logging
from unittest.mock import MagicMock, patch, AsyncMock
from dynastore.tools.identifiers import generate_task_id
from sqlalchemy import text

# Use the correct models for catalog
from dynastore.modules.catalog import models as catalog_models
from dynastore.modules.catalog.config_service import ConfigService
from dynastore.tasks.ingestion.ingestion_task import IngestionTask
from dynastore.modules.tasks.models import TaskPayload
from dynastore.modules.processes.models import ExecuteRequest
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.storage.driver_config import DriverRecordsPostgresqlConfig
from dynastore.tasks.ingestion.operations import ingestion_operation, IngestionOperationInterface

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def setup_ingestion_env(monkeypatch):
    """Ensure proxy module is enabled for ingestion tests."""
    monkeypatch.setenv("DYNASTORE_MODULES", "db_config,db,catalog,gcp,stats,proxy")

@pytest.mark.asyncio
async def test_operation_sequential_execution(task_app_state, test_data_loader, data_id):
    """
    Verifies that pre-operations are executed sequentially and can modify the asset context.
    """
    catalog_id = f"cat_op_{data_id}"
    collection_id = "test_op_coll"
    geojson_path = os.path.join(os.path.dirname(__file__), "data", "points.geojson")

    catalogs = get_protocol(CatalogsProtocol)
    configs = get_protocol(ConfigsProtocol)
    # Drop existing schema if needed
    async with managed_transaction(task_app_state.engine) as conn:
        phys_schema_before = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn, allow_missing=True)
        if phys_schema_before:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{phys_schema_before}" CASCADE'))

    # Create catalog and collection without passing conn, ensuring they commit their own transactions
    await catalogs.create_catalog(catalog_models.Catalog(id=catalog_id, title=catalog_id))
    col_def = catalog_models.Collection(
        id=collection_id,
        title=collection_id,
        extent=catalog_models.Extent(
            spatial=catalog_models.SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=catalog_models.TemporalExtent(interval=[[None, None]])
        )
    )
    await catalogs.create_collection(catalog_id, col_def)
    
    # Explicitly disable partitioning via ConfigManager so ingestion respects it
    pg_plugin_id = DriverRecordsPostgresqlConfig._plugin_id
    config = await configs.get_config(pg_plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    if config.partitioning:
         config.partitioning.enabled = False
    await configs.set_config(pg_plugin_id, config, catalog_id=catalog_id, collection_id=collection_id)
    
    # Drop the table that was eagerly created with default config (partitioned)
    # to force IngestionTask to recreate it with the new unpartitioned config.
    async with managed_transaction(task_app_state.engine) as conn:
        phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await catalogs.resolve_physical_table(catalog_id, collection_id, db_resource=conn)
        if phys_schema and phys_table:
             await conn.execute(text(f'DROP TABLE IF EXISTS "{phys_schema}"."{phys_table}" CASCADE'))

    task = IngestionTask(task_app_state)
    
    # Define operations locally to ensure they are registered and available
    @ingestion_operation
    class OpX(IngestionOperationInterface):
        async def pre_op(self, catalog, collection, asset):
            asset.metadata["opX_called"] = True
            return asset
        async def post_op(self, **kwargs): pass

    @ingestion_operation
    class OpY(IngestionOperationInterface):
        async def pre_op(self, catalog, collection, asset):
            if asset.metadata.get("opX_called"):
                asset.metadata["sequential_ok"] = True
            return asset
        async def post_op(self, **kwargs): pass

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
                    "column_mapping": {
                        "external_id": "id",
                        "attributes_source_type": "all"
                    },
                    "pre_operations": {
                        "OpX": {},
                        "OpY": {}
                    }
                }
            }
        )
    )

    await task.run(payload)

    async with task_app_state.engine.connect() as conn:
        # Retrieve the physical table name that was generated during ingestion/creation
        # Retrieve physical names for the raw query
        catalogs = get_protocol(CatalogsProtocol)
        phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await catalogs.resolve_physical_table(catalog_id, collection_id, db_resource=conn)

        # Execute raw count check (Variables are clean strings, manual quotes here are correct)
        result = await conn.execute(text(f'SELECT count(*) FROM "{phys_schema}"."{phys_table}"'))
        assert result.scalar() == 2

@pytest.mark.asyncio
async def test_gcp_content_type_inspector_integration(task_app_state, data_id):
    """
    Tests ContentTypeInspectorOperation integration by mocking GCP.
    """
    from dynastore.modules.gcp.gcp_ingestion_operations import ContentTypeInspectorOperation
    from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest, IngestionAsset
    
    # Mock GCP Module and Storage
    mock_gcp = MagicMock()
    mock_storage = MagicMock()
    mock_blob = MagicMock()
    
    mock_gcp.get_storage_client.return_value = mock_storage
    mock_storage.bucket.return_value.blob.return_value = mock_blob
    mock_blob.download_as_bytes.return_value = b"some data with latin-1: \xe9"

    with patch("dynastore.modules.gcp.gcp_ingestion_operations.get_protocol", return_value=mock_gcp), \
         patch("dynastore.modules.get_protocol", return_value=mock_gcp), \
         patch("dynastore.modules.gcp.gcp_ingestion_operations.from_bytes") as mock_from_bytes, \
         patch("dynastore.modules.concurrency.run_in_thread", side_effect=lambda f, *args, **kwargs: f(*args, **kwargs)):
        
        mock_best = MagicMock()
        mock_best.encoding = "latin-1"
        mock_best.confidence = 0.9
        
        mock_results = MagicMock()
        mock_results.best.return_value = mock_best
        mock_from_bytes.return_value = mock_results

        # Initialize Operation
        task_request = TaskIngestionRequest(
            asset={"uri": "gs://bucket/test.dbf"},
            column_mapping={"external_id": "id"}
        )
        
        op = ContentTypeInspectorOperation(
            engine=task_app_state.engine,
            task_id="test-task",
            task_request=task_request,
            config=None
        )
        
        asset = IngestionAsset(uri="gs://bucket/test.dbf")
        catalog = MagicMock(id="cat1")
        collection = MagicMock(id="coll1")
        
        await op.pre_op(catalog, collection, asset)
        
        assert task_request.encoding == "latin-1"

@pytest.mark.asyncio
async def test_gcp_asset_downloader_with_metadata(task_app_state, data_id):
    """
    Tests AssetDownloaderOperation with custom metadata and collection folder.
    """
    from dynastore.modules.gcp.gcp_ingestion_operations import AssetDownloaderOperation, AssetDownloaderConfig
    from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest, IngestionAsset
    
    # Mock GCP Module
    mock_gcp = MagicMock()
    mock_storage = MagicMock()
    mock_storage.bucket.return_value = MagicMock()
    
    mock_gcp.get_storage_client.return_value = mock_storage
    mock_gcp.get_or_create_bucket_for_catalog = AsyncMock(return_value="target-bucket")
    mock_gcp.get_collection_storage_path = AsyncMock(return_value="gs://target-bucket/collections/coll1/")
    
    with patch("dynastore.modules.gcp.gcp_ingestion_operations.get_protocol", return_value=mock_gcp), \
         patch("httpx.AsyncClient.get") as mock_get, \
         patch("dynastore.modules.concurrency.run_in_thread", side_effect=lambda f, *args, **kwargs: f(*args, **kwargs)):
        
        mock_response = MagicMock()
        mock_response.content = b"file content"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Config with metadata
        config = AssetDownloaderConfig(metadata={"user_metakey": "user_metavalue"})
        
        task_request = TaskIngestionRequest(
            asset={"uri": "https://example.com/data.zip"},
            column_mapping={"external_id": "id"}
        )
        
        op = AssetDownloaderOperation(
            engine=task_app_state.engine,
            task_id="test-task-dl-meta",
            task_request=task_request,
            config=config
        )
        
        asset = IngestionAsset(uri="https://example.com/data.zip")
        catalog = MagicMock(id="cat1")
        collection = MagicMock(id="coll1")
        
        await op.pre_op(catalog, collection, asset)
        
        # Verify metadata was applied
        assert asset.metadata["user_metakey"] == "user_metavalue"
        # Verify uri was updated to the collection-relative path
        assert asset.uri.startswith("gs://target-bucket/collections/coll1/ingestion-temp/")
        assert "data.zip" in asset.uri
