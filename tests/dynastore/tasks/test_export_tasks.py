"""``ExportFeaturesTask.run`` server-owned output contract.

The export tasks no longer accept a client ``destination_uri``: per OGC API -
Processes the server owns result storage. The task derives a per-job key in the
catalog's own bucket (via ``result_message.server_output_uri``), passes it to
the consolidated ``modules.features_exporter.export_features`` pipeline, and
returns the artifact as a 7-day signed URL in the job message.

(The ``dwh_join`` export's end-to-end behaviour is covered by
``unit/test_dwh_join_export_enrich.py``.)
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.modules.processes.models import ExecuteRequest
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum
from dynastore.tasks.export_features.export_features_task import (
    EXPORT_FEATURES_PROCESS_DEFINITION,
    ExportFeaturesTask,
)

_TASK_MOD = "dynastore.tasks.export_features.export_features_task"
_RESOLVED_URI = (
    "gs://cat-bucket/processes/outputs/export_features/job-1/test_collection.geojson"
)
_SIGNED_URL = "https://storage.googleapis.com/signed?token=abc&X-Goog-Expires=604800"


@pytest.mark.asyncio
async def test_export_features_task_run_server_owned_output_and_signed_url():
    mock_engine = AsyncMock()
    # format_map drives content-type/extension; stub it so the test doesn't
    # depend on the registered formatter table.
    fmt = MagicMock()
    fmt.get.return_value = {"media_type": "application/geo+json", "extension": "geojson"}

    with (
        patch(f"{_TASK_MOD}.get_engine", return_value=mock_engine),
        patch(f"{_TASK_MOD}.initialize_reporters", return_value=[]),
        patch(f"{_TASK_MOD}.format_map", fmt),
        patch(f"{_TASK_MOD}.export_features", new_callable=AsyncMock) as mock_export,
        patch("dynastore.tasks.result_message.server_output_uri",
              new_callable=AsyncMock, return_value=_RESOLVED_URI),
        patch("dynastore.tasks.result_message.signed_result_url",
              new_callable=AsyncMock, return_value=_SIGNED_URL),
    ):
        task = ExportFeaturesTask(MagicMock())
        payload = TaskPayload(
            task_id="123e4567-e89b-12d3-a456-426614174000",
            caller_id="tester",
            inputs=ExecuteRequest(
                inputs={
                    "catalog": "test_catalog",
                    "collection": "test_collection",
                    "output_format": "geojson",
                    # NOTE: no destination_uri — the server derives it.
                }
            ),
        )
        result = await task.run(payload)

    # The signed URL is the job message; status is COMPLETED.
    assert result is not None
    assert result.status == TaskStatusEnum.COMPLETED
    assert result.message == _SIGNED_URL

    # The pipeline was handed the server-derived destination, not a client one.
    assert mock_export.await_count == 1
    assert mock_export.await_args.kwargs["destination_uri"] == _RESOLVED_URI
    assert mock_export.await_args.kwargs["task_id"] == (
        "123e4567-e89b-12d3-a456-426614174000"
    )


def test_export_features_request_has_no_destination_uri_field():
    """The output location is server-owned, so it is not a request field."""
    from dynastore.modules.features_exporter.models import ExportFeaturesRequest

    assert "destination_uri" not in ExportFeaturesRequest.model_fields


def test_export_features_definition_id():
    assert EXPORT_FEATURES_PROCESS_DEFINITION.id == "export_features"
