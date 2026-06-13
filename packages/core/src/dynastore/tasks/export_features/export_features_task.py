#    Copyright 2026 FAO
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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Thin task wrapper around ``modules.features_exporter.service.export_features``.

The heavy lifting (feature streaming, byte-stream formatting, GCS upload)
lives in ``dynastore.modules.features_exporter`` so the same pipeline can be
called from a sync REST endpoint, an async in-process background task, or a
Cloud Run Job worker — only the request envelope and reporter plumbing differ.
"""

import logging
from typing import Optional

# Hard runtime dep — see modules/elasticsearch/module.py for rationale.
# Forces entry-point load to fail on services without ``shapely`` (transitively
# required by the ``geospatial_core`` extra in worker_task_export_features) so
# the CapabilityMap doesn't list this task as claimable on services lacking it.
import shapely  # noqa: F401

from dynastore.extensions.tools.formatters import format_map
from dynastore.modules.features_exporter import export_features
from dynastore.modules.processes.models import ExecuteRequest, Process, StatusInfo
from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks import result_message
from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.tools import initialize_reporters
from dynastore.tools.protocol_helpers import get_engine

from .definition import EXPORT_FEATURES_PROCESS_DEFINITION
from .models import ExportFeaturesRequest

logger = logging.getLogger(__name__)


class ExportFeaturesTask(TaskProtocol[Process, TaskPayload[ExecuteRequest], Optional[StatusInfo]]):
    priority: int = 100

    @staticmethod
    def get_definition() -> Process:
        return EXPORT_FEATURES_PROCESS_DEFINITION

    def __init__(self, app_state: object):
        self.app_state = app_state

    async def run(self, payload: TaskPayload[ExecuteRequest]) -> Optional[StatusInfo]:
        task_id = payload.task_id
        engine = get_engine()
        if engine is None:
            raise RuntimeError("No database engine available.")

        inputs = dict(payload.inputs.inputs)
        # OGC dispatcher injects `catalog_id` / `collection_id` from the URL
        # path, but ExportFeaturesRequest's field names are `catalog` /
        # `collection` (kept compatible with sync REST callers that already
        # use the short names). Bridge the convention here so the model
        # validates without breaking either caller.
        if "catalog" not in inputs and "catalog_id" in inputs:
            inputs["catalog"] = inputs.pop("catalog_id")
        if "collection" not in inputs and "collection_id" in inputs:
            inputs["collection"] = inputs.pop("collection_id")
        request = ExportFeaturesRequest(**inputs)

        reporters = initialize_reporters(
            engine=engine,
            task_id=str(task_id),
            task_request=request,
            reporting_config=request.reporting,
        )

        # Server-owned result storage (OGC API - Processes): derive a per-job
        # key in the catalog's own bucket; the location is never client-supplied.
        formatter = format_map.get(request.output_format)
        if formatter is None:
            raise RuntimeError(f"No formatter registered for {request.output_format}")
        extension = formatter.get("extension") or request.output_format.value
        filename = f"{request.collection}.{extension}"
        output_uri = await result_message.server_output_uri(
            request.catalog,
            EXPORT_FEATURES_PROCESS_DEFINITION.id,
            str(task_id),
            filename,
        )

        await export_features(
            engine,
            request,
            destination_uri=output_uri,
            reporters=reporters,
            task_id=str(task_id),
        )

        # Return the artifact as a time-limited signed URL (OGC output by ref).
        result_url = await result_message.signed_result_url(output_uri)
        return result_message.completed(task_id, message=result_url)
