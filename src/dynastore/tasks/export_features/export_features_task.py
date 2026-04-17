import logging
import asyncio
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.processes.models import (
    Process,
    ProcessOutput,
    ProcessScope,
    ExecuteRequest,
    StatusInfo,
    JobControlOptions,
    TransmissionMode,
)
from dynastore.modules.processes.schema_gen import pydantic_to_process_inputs
from dynastore.tasks.tools import initialize_reporters
from dynastore.tools.async_utils import SyncQueueIterator
from dynastore.tools.file_io import get_features_as_byte_stream
from dynastore.modules.gcp.tools.bucket import upload_stream_to_gcs
from dynastore.modules.tools.features import FeatureStreamConfig, stream_features
from dynastore.models.shared_models import OutputFormatEnum
from dynastore.modules.concurrency import get_concurrency_backend
from dynastore.extensions.tools.formatters import format_map

logger = logging.getLogger(__name__)


class ExportFeaturesRequest(BaseModel):
    """Request model for exporting features from a collection."""
    
    catalog: str = Field(..., description="Catalog ID")
    collection: str = Field(..., description="Collection ID")
    output_format: OutputFormatEnum = Field(..., description="Output file format")
    destination_uri: str = Field(..., description="GCS URI for output (gs://...)")
    encoding: str = Field(default="utf-8", description="Character encoding for output")
    cql_filter: Optional[str] = Field(None, description="CQL2/ECQL filter expression")
    property_names: Optional[List[str]] = Field(None, description="List of properties to include (None = all)")
    limit: Optional[int] = Field(None, description="Maximum number of features to export")
    offset: Optional[int] = Field(None, description="Number of features to skip")
    target_srid: Optional[int] = Field(None, description="Target SRID for geometry transformation")
    reporting: Optional[dict] = Field(None, description="Reporter configuration")


# Auto-generate process definition from Pydantic model
EXPORT_FEATURES_PROCESS_DEFINITION = Process(
    id="export-features",
    version="1.0.0",
    title="Export Features",
    description="Exports features from a collection to a file in Cloud Storage with optional CQL filtering and property projection.",
    scopes=[ProcessScope.COLLECTION],
    inputs=pydantic_to_process_inputs(ExportFeaturesRequest),
    outputs={
        "result": ProcessOutput.model_validate({"title": "Result", "schema": {"type": "object"}})
    },
    jobControlOptions=[JobControlOptions.ASYNC_EXECUTE],
    outputTransmission=[TransmissionMode.VALUE]
)
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

        try:
            inputs = payload.inputs.inputs
            request = ExportFeaturesRequest(**inputs)
            
            logger.info(
                f"Starting ExportFeaturesTask {task_id} for {request.catalog}:{request.collection}"
            )

            # Initialize reporters
            reporters = initialize_reporters(
                engine=engine,
                task_id=str(task_id),
                task_request=request,
                reporting_config=request.reporting
            )

            # Notify started
            for r in reporters:
                await r.task_started(
                    str(task_id),
                    request.collection,
                    request.catalog,
                    request.destination_uri
                )

            # --- Producer: Stream features from DB ---
            queue = asyncio.Queue(maxsize=1000)
            
            async def producer():
                try:
                    # Configure feature stream
                    stream_config = FeatureStreamConfig(
                        catalog=request.catalog,
                        collection=request.collection,
                        cql_filter=request.cql_filter,
                        property_names=request.property_names,
                        limit=request.limit,
                        offset=request.offset,
                        include_geometry=True,
                        target_srid=request.target_srid
                    )
                    
                    # Stream features and push to queue
                    async for feature in stream_features(stream_config, engine):
                        await queue.put(feature)
                        
                except Exception as e:
                    logger.error(f"Producer failed: {e}", exc_info=True)
                    raise e
                finally:
                    await queue.put(None)  # Sentinel to signal end of stream

            # --- Consumer: Sync file writing in thread ---
            def blocking_export_logic(iterator):
                # 1. Get the byte stream
                byte_stream = get_features_as_byte_stream(
                    features=iterator,
                    output_format=request.output_format,
                    target_srid=request.target_srid or 4326,
                    encoding=request.encoding
                )
                
                # 2. Upload to storage
                formatter = format_map.get(request.output_format)
                if formatter is None:
                    raise RuntimeError(f"No formatter registered for {request.output_format}")
                upload_stream_to_gcs(
                    byte_stream=byte_stream,
                    destination_uri=request.destination_uri,
                    content_type=formatter["media_type"]
                )
            
            # Start producer task
            producer_task = asyncio.create_task(producer())
            
            # Start consumer in thread
            loop = asyncio.get_running_loop()
            sync_iterator = SyncQueueIterator(queue, loop)
            
            logger.info("Starting blocking export logic in threadpool...")
            run_in_threadpool = get_concurrency_backend()
            await run_in_threadpool(blocking_export_logic, sync_iterator)
            
            # Ensure producer finished cleanly
            await producer_task

            # Notify finished
            for r in reporters:
                await r.task_finished("SUCCESS")

            logger.info(f"ExportFeaturesTask {task_id} completed successfully.")

        except Exception as e:
            logger.error(f"ExportFeaturesTask {task_id} failed: {e}", exc_info=True)
            if 'reporters' in locals():
                for r in reporters:
                    await r.task_finished("FAILED", error_message=str(e))
            raise e

        return StatusInfo(
            jobID=task_id,
            status=TaskStatusEnum.COMPLETED,
            message="Export completed",
            links=[]
        )
