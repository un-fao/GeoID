import pytest
from unittest.mock import MagicMock, AsyncMock, patch


def _make_feature(join_col_val, extra=None):
    """Return a mock Feature with .properties, .id, .type, .geometry."""
    f = MagicMock()
    f.id = join_col_val
    f.type = "Feature"
    f.geometry = None
    props = {"join_col": join_col_val}
    if extra:
        props.update(extra)
    f.properties = props
    return f


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stats", "iam", "gcp")
@pytest.mark.enable_extensions("dwh")
async def test_dwh_join_resolution_repro(sysadmin_in_process_client):
    """
    Test that DWH join endpoint calls stream_items and returns 200.
    """
    logical_catalog = "my_catalog"
    logical_collection = "my_collection"

    feat1 = _make_feature("key1", {"attr1": "a1"})
    feat2 = _make_feature("key2", {"attr1": "a2"})

    async def async_gen_stream():
        yield feat1
        yield feat2

    class MockContext:
        def __init__(self, items):
            self.items = items
            self.execution_params = {}

    mock_catalogs_provider = MagicMock()
    mock_catalogs_provider.stream_items = AsyncMock(
        return_value=MockContext(async_gen_stream())
    )

    with (
        patch(
            "dynastore.extensions.dwh.dwh.execute_bigquery_async",
            new_callable=AsyncMock,
            return_value={
                "key1": {"dwh_col": "val1"},
                "key2": {"dwh_col": "val2"},
            },
        ),
        patch(
            "dynastore.extensions.dwh.dwh.get_protocol",
            return_value=mock_catalogs_provider,
        ),
    ):
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

        response = await sysadmin_in_process_client.post("/dwh/join", json=payload)
        assert response.status_code == 200, f"Response: {response.text}"
        assert mock_catalogs_provider.stream_items.called, (
            "stream_items() was not called"
        )


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stats", "iam", "gcp")
@pytest.mark.enable_extensions("dwh")
async def test_dwh_catalog_join_endpoint(sysadmin_in_process_client):
    """
    Test the /dwh/catalogs/{catalog_id}/join endpoint.
    """
    logical_catalog = "my_catalog"
    logical_collection = "my_collection"

    feat1 = _make_feature("key1")

    async def async_gen_2():
        yield feat1

    class MockContext:
        def __init__(self, items):
            self.items = items
            self.execution_params = {}

    mock_catalogs_provider = MagicMock()
    mock_catalogs_provider.stream_items = AsyncMock(
        return_value=MockContext(async_gen_2())
    )

    with (
        patch(
            "dynastore.extensions.dwh.dwh.execute_bigquery_async",
            new_callable=AsyncMock,
            return_value={"key1": {"dwh": "val1"}},
        ),
        patch(
            "dynastore.extensions.dwh.dwh.get_protocol",
            return_value=mock_catalogs_provider,
        ),
    ):
        payload = {
            "dwh_project_id": "p",
            "dwh_query": "SELECT * FROM dwh",
            "collection": logical_collection,
            "dwh_join_column": "join_col",
            "join_column": "join_col",
            "output_format": "geojson",
        }

        response = await sysadmin_in_process_client.post(
            f"/dwh/catalogs/{logical_catalog}/join", json=payload
        )
        assert response.status_code == 200, f"Response: {response.text}"
        assert mock_catalogs_provider.stream_items.called
