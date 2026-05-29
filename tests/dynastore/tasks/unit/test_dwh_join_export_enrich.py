#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Regression test for the DWH-join export task's feature-merge path.

Incident: the async OGC-Process job ``dwh_join`` failed at runtime with
``'Feature' object has no attribute 'get'``. The producer hand-rolled a
dict-style merge (``feature.get(join_column)`` / ``feature["attributes"] =
...``) but ``stream_features`` yields ``Feature`` *objects*, not dicts — so the
join blew up before a single row was written. The synchronous /dwh/join
endpoint never hit this because it merges via the shared ``enrich_features``
helper, which reads ``feature.properties``.

The fix routes the async task through that same ``enrich_features`` helper.
This test pins the contract by driving ``DwhJoinExportTask.run()`` end-to-end
with *real* ``Feature`` objects (which genuinely have no ``.get`` method) and
the real producer→queue→consumer thread bridge, mocking only I/O. With the old
hand-rolled merge it raises ``AttributeError``; with the fix the DWH columns
land in ``feature.properties`` and unmatched features are dropped (inner join).
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.ogc import Feature

_TASK_MOD = "dynastore.tasks.dwh_join.dwh_join_export_task"


def _payload(inputs: dict) -> SimpleNamespace:
    """Minimal stand-in for ``TaskPayload[ExecuteRequest]``.

    The task only reads ``payload.task_id`` and ``payload.inputs.inputs``.
    """
    return SimpleNamespace(
        task_id="019e7547-2dba-76ca-a53d-7f5264d05e47",
        inputs=SimpleNamespace(inputs=inputs),
    )


@pytest.mark.asyncio
async def test_dwh_join_export_merges_feature_objects_via_properties():
    """Feature objects flow through the real enrich_features merge and land
    with DWH columns merged into ``properties`` — no ``.get`` on Feature."""
    from dynastore.tasks.dwh_join.dwh_join_export_task import DwhJoinExportTask

    # Real Feature objects: BaseModel has no ``.get`` — the old code would raise.
    matched = Feature(
        type="Feature",
        id="key1",
        geometry=None,
        properties={"join_col": "key1", "name": "Abruzzi"},
    )
    unmatched = Feature(
        type="Feature",
        id="key2",
        geometry=None,
        properties={"join_col": "key2", "name": "NoDwhRow"},
    )

    def fake_stream_features(config, db_resource, hints=frozenset()):
        async def _gen():
            yield matched
            yield unmatched

        return _gen()

    written: list = []

    def fake_byte_stream(features, output_format, target_srid=4326, encoding="utf-8"):
        # The consumer drains this iterator inside the worker thread; capture
        # exactly what reaches the writer, then return a trivial byte stream.
        written.extend(list(features))
        return iter([b"{}"])

    inputs = {
        "dwh_project_id": "p",
        "dwh_query": "SELECT 1",
        "catalog": "cat",
        "collection": "col",
        "dwh_join_column": "join_col",
        "join_column": "join_col",
        "with_geometry": True,
        "properties": ["*"],
        "stats": [],
        "system": [],
        "output_format": "geojson",
        "destination_uri": "gs://bucket/out.json",
    }

    with (
        patch(f"{_TASK_MOD}.get_engine", return_value=MagicMock()),
        patch(f"{_TASK_MOD}.initialize_reporters", return_value=[]),
        patch(
            f"{_TASK_MOD}.execute_bigquery_async",
            new_callable=AsyncMock,
            return_value={"key1": {"dwh_col": "val1"}},  # only key1 has a DWH row
        ),
        patch(
            f"{_TASK_MOD}.resolve_category_field_names",
            new_callable=AsyncMock,
            return_value=["join_col", "name"],
        ),
        patch(f"{_TASK_MOD}.stream_features", side_effect=fake_stream_features),
        patch(f"{_TASK_MOD}.get_features_as_byte_stream", side_effect=fake_byte_stream),
        patch(f"{_TASK_MOD}.upload_stream_to_gcs") as mock_upload,
    ):
        task = DwhJoinExportTask(app_state=MagicMock())
        # Must not raise AttributeError("'Feature' object has no attribute 'get'").
        await task.run(_payload(inputs))

    # Inner join: only the matched feature is written; the unmatched is dropped.
    assert len(written) == 1, f"expected 1 joined feature, got {len(written)}"
    joined = written[0]
    # The merged result is still a Feature with DWH columns in .properties.
    assert isinstance(joined, Feature)
    assert joined.properties["dwh_col"] == "val1"
    assert joined.properties["name"] == "Abruzzi"  # base PG property preserved
    assert joined.id == "key1"
    mock_upload.assert_called_once()


@pytest.mark.asyncio
async def test_dwh_join_export_empty_join_writes_nothing():
    """No DWH rows → producer yields nothing → writer sees an empty stream and
    the task still completes cleanly (no crash, upload still invoked once)."""
    from dynastore.tasks.dwh_join.dwh_join_export_task import DwhJoinExportTask

    def fake_stream_features(config, db_resource, hints=frozenset()):
        async def _gen():
            yield Feature(type="Feature", id="k", geometry=None, properties={"join_col": "k"})

        return _gen()

    written: list = []

    def fake_byte_stream(features, output_format, target_srid=4326, encoding="utf-8"):
        written.extend(list(features))
        return iter([b"{}"])

    inputs = {
        "dwh_project_id": "p",
        "dwh_query": "SELECT 1",
        "catalog": "cat",
        "collection": "col",
        "dwh_join_column": "join_col",
        "join_column": "join_col",
        "output_format": "geojson",
        "destination_uri": "gs://bucket/out.json",
    }

    with (
        patch(f"{_TASK_MOD}.get_engine", return_value=MagicMock()),
        patch(f"{_TASK_MOD}.initialize_reporters", return_value=[]),
        patch(
            f"{_TASK_MOD}.execute_bigquery_async",
            new_callable=AsyncMock,
            return_value={},  # empty DWH result
        ),
        patch(
            f"{_TASK_MOD}.resolve_category_field_names",
            new_callable=AsyncMock,
            return_value=["join_col"],
        ),
        patch(f"{_TASK_MOD}.stream_features", side_effect=fake_stream_features),
        patch(f"{_TASK_MOD}.get_features_as_byte_stream", side_effect=fake_byte_stream),
        patch(f"{_TASK_MOD}.upload_stream_to_gcs"),
    ):
        task = DwhJoinExportTask(app_state=MagicMock())
        await task.run(_payload(inputs))

    assert written == [], "no features should be written when DWH returns no rows"
