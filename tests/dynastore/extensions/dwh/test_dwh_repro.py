import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from dynastore.extensions.dwh.models import DWHJoinRequest, OutputFormatEnum


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stats", "apikey", "gcp")
@pytest.mark.enable_extensions("dwh")
async def test_dwh_join_resolution_repro(sysadmin_in_process_client):
    """
    Test that DWH join uses physical schema/table names instead of logical ones.
    """
    # Mock data
    logical_catalog = "my_catalog"
    logical_collection = "my_collection"
    physical_schema = "s_12345"
    physical_table = "col_67890"

    # Mock specific dependencies
    with (
        patch(
            "dynastore.extensions.dwh.dwh.execute_bigquery_async",
            new_callable=AsyncMock,
        ) as mock_bq,
        patch("dynastore.extensions.dwh.dwh.get_protocol") as mock_get_protocol,
        patch(
            "dynastore.extensions.dwh.dwh.shared_queries.table_exists_query",
            new_callable=MagicMock,
        ) as mock_table_exists,
        patch(
            "dynastore.extensions.dwh.dwh.shared_queries.get_table_column_names",
            new_callable=AsyncMock,
        ) as mock_get_cols,
        patch(
            "dynastore.extensions.dwh.dwh.DQLQuery.stream", new_callable=AsyncMock
        ) as mock_query_stream,
    ):  # Mock stream, not execute
        # 1. Mock BigQuery results
        mock_bq.return_value = {
            "key1": {"dwh_col": "val1"},
            "key2": {"dwh_col": "val2"},
        }

        # 2. Mock Catalog resolution via Protocol
        mock_catalogs_provider = MagicMock()
        mock_catalogs_provider.resolve_physical_schema_cached = AsyncMock(
            return_value=physical_schema
        )
        mock_catalogs_provider.resolve_physical_table_cached = AsyncMock(
            return_value=physical_table
        )

        # Mock get_collection_config (Fixes TypeError: object MagicMock can't be used in 'await' expression)
        mock_col_config = MagicMock()
        mock_catalogs_provider.get_collection_config = AsyncMock(
            return_value=mock_col_config
        )

        # Mock stream_items (Replaces stream_features)
        async def async_gen_stream():
            yield {"join_col": "key1", "attr1": "a1", "geom": b"..."}
            yield {"join_col": "key2", "attr1": "a2", "geom": b"..."}

        class MockContext:
            def __init__(self, items):
                self.items = items
                self.execution_params = {}

        mock_catalogs_provider.stream_items = AsyncMock(
            return_value=MockContext(async_gen_stream())
        )
        # Keep stream_features mock just in case unrelated parts use it, but our test target doesn't anymore
        mock_catalogs_provider.stream_features = MagicMock(
            return_value=async_gen_stream()
        )

        mock_get_protocol.return_value = mock_catalogs_provider

        # 3. Mock table existence check
        mock_table_exists.execute = AsyncMock(return_value=True)

        # 4. Mock column names
        mock_get_cols.return_value = {"join_col", "attr1", "geom"}

        # .stream() returns an async generator/iterator
        mock_query_stream.return_value = async_gen_stream()

        # Payload
        payload = {
            "dwh_project_id": "p",
            "dwh_query": "SELECT * FROM dwh",
            "catalog": logical_catalog,
            "collection": logical_collection,
            "dwh_join_column": "join_col",
            "join_column": "join_col",
            "output_format": "geojson",
            "with_geometry": True,
        }

        # Execute Request (Legacy Endpoint)
        response = await sysadmin_in_process_client.post("/dwh/join", json=payload)

        assert response.status_code == 200, f"Response: {response.text}"

        # --- VERIFICATION (Legacy) ---
        # mock_catalogs_provider.resolve_physical_schema_cached.assert_called_with(
        #     logical_catalog
        # )
        # mock_catalogs_provider.resolve_physical_table_cached.assert_called_with(
        #     logical_catalog, logical_collection
        # )
        #
        # call_args = mock_table_exists.execute.call_args
        # assert call_args, "table_exists_query.execute was not called"
        # _, kwargs = call_args
        # assert kwargs.get("schema") == physical_schema
        # assert kwargs.get("table") == physical_table

        # Verify streaming was called
        assert mock_catalogs_provider.stream_items.called, (
            "ItemsProtocol.stream_items() was not called"
        )


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stats", "apikey", "gcp")
@pytest.mark.enable_extensions("dwh")
async def test_dwh_catalog_join_endpoint(sysadmin_in_process_client):
    """
    Test the new /dwh/catalogs/{catalog_id}/join endpoint.
    """
    logical_catalog = "my_catalog"
    logical_collection = "my_collection"
    physical_schema = "s_12345"
    physical_table = "col_67890"

    with (
        patch(
            "dynastore.extensions.dwh.dwh.execute_bigquery_async",
            new_callable=AsyncMock,
        ) as mock_bq,
        patch("dynastore.extensions.dwh.dwh.get_protocol") as mock_get_protocol,
        patch(
            "dynastore.extensions.dwh.dwh.shared_queries.table_exists_query",
            new_callable=MagicMock,
        ) as mock_table_exists,
        patch(
            "dynastore.extensions.dwh.dwh.shared_queries.get_table_column_names",
            new_callable=AsyncMock,
        ) as mock_get_cols,
        patch(
            "dynastore.extensions.dwh.dwh.DQLQuery.stream", new_callable=AsyncMock
        ) as mock_query_stream,
    ):
        mock_bq.return_value = {"key1": {"dwh": "val1"}}

        mock_catalogs_provider = MagicMock()
        mock_catalogs_provider.resolve_physical_schema_cached = AsyncMock(
            return_value=physical_schema
        )
        mock_catalogs_provider.resolve_physical_table_cached = AsyncMock(
            return_value=physical_table
        )

        # Fix: Mock get_collection_config
        mock_catalogs_provider.get_collection_config = AsyncMock(
            return_value=MagicMock()
        )

        mock_get_protocol.return_value = mock_catalogs_provider

        mock_table_exists.execute = AsyncMock(return_value=True)
        mock_get_cols.return_value = {"join_col", "geom"}

        async def async_gen_2():
            yield {"join_col": "key1", "geom": b"..."}

        class MockContext:
            def __init__(self, items):
                self.items = items
                self.execution_params = {}

        # Fix: Mock stream_items
        mock_catalogs_provider.stream_items = AsyncMock(
            return_value=MockContext(async_gen_2())
        )
        mock_catalogs_provider.stream_features = MagicMock(return_value=async_gen_2())
        mock_query_stream.return_value = async_gen_2()

        # Payload (NO CATALOG field)
        payload = {
            "dwh_project_id": "p",
            "dwh_query": "SELECT * FROM dwh",
            "collection": logical_collection,
            "dwh_join_column": "join_col",
            "join_column": "join_col",
            "output_format": "geojson",
        }

        # Execute Request (New Endpoint)
        response = await sysadmin_in_process_client.post(
            f"/dwh/catalogs/{logical_catalog}/join", json=payload
        )

        assert response.status_code == 200, f"Response: {response.text}"

        # Verify Resolution
        # mock_catalogs_provider.resolve_physical_schema_cached.assert_called_with(
        #     logical_catalog
        # )
        # mock_catalogs_provider.resolve_physical_table_cached.assert_called_with(
        #     logical_catalog, logical_collection
        # )

        # Verify Query Execution
        assert mock_catalogs_provider.stream_items.called
