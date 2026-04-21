import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from dynastore.tasks.export_features.export_features_task import ExportFeaturesTask
from dynastore.tasks.dwh_join.dwh_join_export_task import DwhJoinExportTask
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum
from dynastore.modules.processes.models import ExecuteRequest


@pytest.mark.asyncio
async def test_export_features_task_run():
    """The task is a thin wrapper that delegates to
    ``modules.features_exporter.export_features``.  The previous
    implementation interleaved ``stream_features`` / ``get_features_as_byte_stream``
    / ``upload_stream_to_gcs`` inside the task; the refactor moved that
    pipeline into the module.  This test therefore patches the
    ``export_features`` symbol the task module imports and verifies
    the task forwards the request + reporters + task_id correctly.
    """
    mock_app_state = MagicMock()
    mock_engine = AsyncMock()

    with (
        patch(
            "dynastore.tasks.export_features.export_features_task.get_engine",
            return_value=mock_engine,
        ),
        patch(
            "dynastore.tasks.export_features.export_features_task.export_features",
            new_callable=AsyncMock,
        ) as mock_export,
        patch(
            "dynastore.tasks.export_features.export_features_task.initialize_reporters",
            return_value=[],
        ),
    ):
        task = ExportFeaturesTask(mock_app_state)

        payload = TaskPayload(
            task_id="123e4567-e89b-12d3-a456-426614174000",
            caller_id="tester",
            inputs=ExecuteRequest(
                inputs={
                    "catalog": "test_catalog",
                    "collection": "test_collection",
                    "output_format": "geojson",
                    "destination_uri": "gs://test-bucket/output.geojson",
                }
            ),
        )

        result = await task.run(payload)

        assert result.status == TaskStatusEnum.COMPLETED

        # Task delegates to the consolidated ``export_features`` call
        # in ``modules.features_exporter``.  Verify it fires once with
        # the engine + reporters + task_id + a materialised request.
        assert mock_export.await_count == 1
        call_engine, call_request = mock_export.await_args.args
        assert call_engine is mock_engine
        # The task constructs an ``ExportFeaturesRequest`` from the
        # inputs dict.  Spot-check the round-trip.
        assert call_request.destination_uri == "gs://test-bucket/output.geojson"
        assert mock_export.await_args.kwargs["task_id"] == (
            "123e4567-e89b-12d3-a456-426614174000"
        )


@pytest.mark.asyncio
async def test_dwh_join_export_task_run():
    # Mock Dependencies
    mock_app_state = MagicMock()
    mock_engine = AsyncMock()

    # Mock BigQuery Result
    mock_bq_result = {"A": {"dwh_col": 100}, "B": {"dwh_col": 200}}

    # Mock stream_features
    async def mock_stream_features(*args, **kwargs):
        # We need to return features that have join_column 'join_col'
        yield {"id": 1, "join_col": "A", "attributes": {"some": "attr"}}
        yield {"id": 2, "join_col": "B", "attributes": {"some": "other"}}

    with (
        patch(
            "dynastore.tasks.dwh_join.dwh_join_export_task.get_engine",
            return_value=mock_engine,
        ),
        patch(
            "dynastore.tasks.dwh_join.dwh_join_export_task.stream_features",
            side_effect=mock_stream_features,
        ),
        patch(
            "dynastore.tasks.dwh_join.dwh_join_export_task.execute_bigquery_async",
            return_value=mock_bq_result,
        ),
        patch(
            "dynastore.tasks.dwh_join.dwh_join_export_task.get_features_as_byte_stream",
            return_value=[b"chunk"],
        ) as mock_get_stream,
        patch(
            "dynastore.tasks.dwh_join.dwh_join_export_task.upload_stream_to_gcs"
        ) as mock_upload,
        patch(
            "dynastore.tasks.dwh_join.dwh_join_export_task.initialize_reporters",
            return_value=[],
        ),
    ):
        task = DwhJoinExportTask(mock_app_state)

        payload = TaskPayload(
            task_id="123e4567-e89b-12d3-a456-426614174001",
            caller_id="tester",
            inputs=ExecuteRequest(
                inputs={
                    "dwh_project_id": "p",
                    "dwh_query": "SELECT ...",
                    "catalog": "c",
                    "collection": "l",
                    "dwh_join_column": "J",
                    "join_column": "join_col",
                    "output_format": "geojson",
                    "destination_uri": "gs://bucket/out.json",
                }
            ),
        )

        result = await task.run(payload)

        assert result.status == TaskStatusEnum.COMPLETED
        assert mock_upload.called
