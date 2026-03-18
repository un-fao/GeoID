import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from dynastore.tasks.export_features.export_features_task import ExportFeaturesTask
from dynastore.tasks.dwh_join.dwh_join_export_task import DwhJoinExportTask
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum
from dynastore.modules.processes.models import ExecuteRequest


@pytest.mark.asyncio
async def test_export_features_task_run():
    # Mock App State and Engine
    mock_app_state = MagicMock()
    mock_engine = AsyncMock()

    # Mock stream_features (async generator)
    async def mock_stream_features(*args, **kwargs):
        yield {
            "id": 1,
            "properties": {"some": "attr"},
            "geometry": {"type": "Point", "coordinates": [0, 0]},
        }
        yield {
            "id": 2,
            "properties": {"some": "other"},
            "geometry": {"type": "Point", "coordinates": [1, 1]},
        }

    # Mock dependencies
    # We patch where they are used in the task module
    with (
        patch(
            "dynastore.tasks.export_features.export_features_task.get_engine",
            return_value=mock_engine,
        ),
        patch(
            "dynastore.tasks.export_features.export_features_task.stream_features",
            side_effect=mock_stream_features,
        ) as mock_stream_fn,
        patch(
            "dynastore.tasks.export_features.export_features_task.get_features_as_byte_stream",
            return_value=[b"chunk1", b"chunk2"],
        ) as mock_get_stream,
        patch(
            "dynastore.tasks.export_features.export_features_task.upload_stream_to_gcs"
        ) as mock_upload,
        patch(
            "dynastore.tasks.export_features.export_features_task.initialize_reporters",
            return_value=[],
        ),
    ):
        # Instantiate Task
        task = ExportFeaturesTask(mock_app_state)

        # Payload
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

        # Run Task
        result = await task.run(payload)

        # Assertions
        assert result.status == TaskStatusEnum.COMPLETED

        # Verify stream_features called (verifies import of new module works)
        assert mock_stream_fn.called

        # Verify get_features_as_byte_stream was called
        assert mock_get_stream.called

        # Verify Upload to GCS was called
        assert mock_upload.called
        assert mock_upload.call_count == 1
        call_args = mock_upload.call_args[1]
        assert call_args["destination_uri"] == "gs://test-bucket/output.geojson"


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
