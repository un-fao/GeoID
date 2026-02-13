#    Copyright 2025 FAO
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

import logging
import json
from unittest import result
from unittest import result
import uuid
from typing import List, Union, Any, Optional
from pydantic import ValidationError
from fastapi import (APIRouter, BackgroundTasks, Depends, HTTPException,
                     Request, Response, status, Body)
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions import dynastore_extension, protocols
from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.tools.security import get_principal
from dynastore.modules.db_config.tools import get_any_engine
from dynastore.tasks import get_definitions_by_type

import dynastore.modules.processes.processes_module as processes_module
from dynastore.modules.tasks import runners, tasks_module
from dynastore.modules.tasks.models import (Task, TaskExecutionMode, TaskStatusEnum, RunnerContext)
from dynastore.modules.processes import models
from dynastore.modules.apikey.models import Principal, SYSTEM_USER_ID
from dynastore.modules.apikey.models import Principal, SYSTEM_USER_ID



logger = logging.getLogger(__name__)

router: APIRouter = APIRouter(prefix="/processes", tags=["OGC API - Processes"])

@router.get("/processes", response_model=models.ProcessList)
async def list_processes(request: Request):
    """Lists all available processes."""
    process_definitions = get_definitions_by_type(models.Process)
    process_summaries = []

    for process in process_definitions:
        # Create the "self" link pointing to the detailed description
        process_url = str(request.url_for('get_process_description', process_id=process.id))
        self_link = models.Link(href=process_url, rel="self", type="application/json", title="Detailed process description")

        # Convert the process object to a dictionary, add the links,
        # and then validate the complete dictionary into a ProcessSummary.
        process_dict = process.model_dump()
        process_dict['links'] = [self_link]
        summary = models.ProcessSummary.model_validate(process_dict)

        process_summaries.append(summary)

    links = [models.Link(href=str(request.url), rel="self", type="application/json")]
    return models.ProcessList(processes=process_summaries, links=links)

@router.get("/processes/{process_id}", response_model=models.Process)
async def get_process_description(process_id: str):
    """Gets a detailed description of a single process."""
    process = next((p for p in get_definitions_by_type(models.Process) if p.id == process_id), None)
    if not process:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Process '{process_id}' not found.")
    return process

@router.post(
    "/processes/{process_id}/execution",
    status_code=status.HTTP_201_CREATED,
    response_model=Union[models.StatusInfo, Any], # The response can be a status or a direct result
)
async def execute_process(
    process_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    execution_request: models.ExecuteRequest = Body(
        ...,
        examples=[
            {
                "inputs": {
                    "asset_id": "target-asset-id",
                    "catalog_id": "target-catalog-id", 
                    "collection_id": "target-collection-id",
                    "asset_uri": "gs://bucket/path.tif",
                    "asset_type": "RASTER",
                    "asset_metadata": {"custom_field": "value"}
                },
                "response": "document"
            },
            {
                "inputs": {
                    "catalog_id": "target-catalog-id",
                    "collection_id": "target-collection-id",
                    "ingestion_request": {
                        "asset": {"uri": "gs://bucket/data.csv"},
                        "source_srid": 4326,
                        "column_mapping": {
                            "external_id": "id",
                            "attributes_source_type": "all"
                        },
                        "format": "csv"
                    }
                }
            }
        ],
        description="Execution inputs. See process definition for details."
    ),
    principal: Optional[Principal] = Depends(get_principal)
):
    """Executes a process, creating a new job (task)."""
    caller_id = principal.identifier if principal else SYSTEM_USER_ID
    engine = get_any_engine(request.app.state)

    # Determine preferred mode from 'Prefer' header
    preferred_mode = None
    prefer_header = request.headers.get("Prefer")
    if prefer_header:
        if "respond-async" in prefer_header:
            preferred_mode = models.JobControlOptions.ASYNC_EXECUTE
        elif "wait=" in prefer_header:
            preferred_mode = models.JobControlOptions.SYNC_EXECUTE

    try:
        result = await processes_module.execute_process(
            process_id=process_id,
            execution_request=execution_request,
            engine=engine,
            caller_id=caller_id,
            preferred_mode=preferred_mode,
            background_tasks=background_tasks
        )
    except (ValidationError, ValueError) as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except NotImplementedError as e:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=str(e))
    except Exception as e:
        logger.error(f"Execution of process '{process_id}' failed: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Process execution failed: {e}")

    return _handle_execution_result(result, request)

@router.post(
    "/catalogs/{catalog_id}/processes/{process_id}/execution",
    status_code=status.HTTP_201_CREATED,
    response_model=Union[models.StatusInfo, Any]
)
async def execute_process_catalog(
    catalog_id: str,
    process_id: str,
    execution_request: models.ExecuteRequest,
    request: Request,
    background_tasks: BackgroundTasks,

    principal: Optional[Principal] = Depends(get_principal)
):
    """Executes a process specialized for a catalog."""
    caller_id = principal.identifier if principal else SYSTEM_USER_ID
    engine = get_any_engine(request.app.state)
    preferred_mode = _get_preferred_mode(request)

    try:
        result = await processes_module.execute_process(
            process_id=process_id,
            execution_request=execution_request,
            engine=engine,
            caller_id=caller_id,
            preferred_mode=preferred_mode,
            background_tasks=background_tasks,
            catalog_id=catalog_id
        )
    except Exception as e:
        _handle_execution_exception(process_id, e)

    return _handle_execution_result(result, request)

@router.post(
    "/catalogs/{catalog_id}/collections/{collection_id}/processes/{process_id}/execution",
    status_code=status.HTTP_201_CREATED,
    response_model=Union[models.StatusInfo, Any]
)
async def execute_process_collection(
    catalog_id: str,
    collection_id: str,
    process_id: str,
    execution_request: models.ExecuteRequest,
    request: Request,
    background_tasks: BackgroundTasks,

    principal: Optional[Principal] = Depends(get_principal)
):
    """Executes a process specialized for a collection."""
    caller_id = principal.identifier if principal else SYSTEM_USER_ID
    engine = get_any_engine(request.app.state)
    preferred_mode = _get_preferred_mode(request)
    result = None
    try:
        result = await processes_module.execute_process(
            process_id=process_id,
            execution_request=execution_request,
            engine=engine,
            caller_id=caller_id,
            preferred_mode=preferred_mode,
            background_tasks=background_tasks,
            catalog_id=catalog_id,
            collection_id=collection_id
        )
    except Exception as e:
        _handle_execution_exception(process_id, e)

    return _handle_execution_result(result, request)

def _get_preferred_mode(request: Request):
    prefer_header = request.headers.get("Prefer")
    if prefer_header:
        if "respond-async" in prefer_header:
            return models.JobControlOptions.ASYNC_EXECUTE
        elif "wait=" in prefer_header:
            return models.JobControlOptions.SYNC_EXECUTE
    return None

def _handle_execution_exception(process_id: str, e: Exception):
    if isinstance(e, (ValidationError, ValueError)):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    if isinstance(e, NotImplementedError):
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=str(e))
    logger.error(f"Execution of process '{process_id}' failed: {e}", exc_info=True)
    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Process execution failed: {e}")

def _handle_execution_result(result: Task | models.StatusInfo, request: Request):
    if isinstance(result, Task):
        # ASYNC_EXECUTE: The runner returned a new task object.
        # Respond with 201 Created and a Location header.
        
        # Determine specialized URL if context is available
        path_params = request.scope.get("path_params", {})
        catalog_id = path_params.get("catalog_id")
        collection_id = path_params.get("collection_id")
        
        if catalog_id and collection_id:
            job_status_url = request.url_for('get_job_status_collection', catalog_id=catalog_id, collection_id=collection_id, job_id=str(result.task_id))
        elif catalog_id:
            job_status_url = request.url_for('get_job_status_catalog', catalog_id=catalog_id, job_id=str(result.task_id))
        else:
            job_status_url = request.url_for('get_job_status', job_id=str(result.task_id))

        status_info = _task_to_status_info(result, request)

        return Response(
            content=status_info.model_dump_json(by_alias=True),
            status_code=status.HTTP_201_CREATED,
            headers={"Location": str(job_status_url)},
            media_type="application/json"
        )
    else:
        # SYNC_EXECUTE: The runner returned the final result directly.
        # Respond with 200 OK and the result as the body.
        if result is None:
            return Response(content="", status_code=status.HTTP_200_OK)
        elif isinstance(result, dict) or isinstance(result, list):
            content = json.dumps(result)
        elif hasattr(result, 'model_dump_json'):
            content = result.model_dump_json(by_alias=True)
        else:
            content = str(result)

        accept_header = request.headers.get("Accept", "application/json")
        if "application/json" in accept_header or "*/*" in accept_header or "application/*" in accept_header:
            return Response(content=content, status_code=status.HTTP_200_OK, media_type="application/json")
        elif "text/plain" in accept_header:
            return Response(content=content, status_code=status.HTTP_200_OK, media_type="text/plain")
        else:
            # If the requested media type is not supported, return 406.
            raise HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE, detail=f"Requested media type '{accept_header}' not supported for synchronous process results.")

async def _get_job_internal(job_id: uuid.UUID, catalog_id: str, conn: AsyncConnection):
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.modules import get_protocol
    catalogs = get_protocol(CatalogsProtocol)
    schema = await catalogs.resolve_physical_schema(catalog_id, conn)
    task = await tasks_module.get_task(conn, job_id, schema=schema)
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found in schema '{schema}'.")
    return task

@router.get("/catalogs/{catalog_id}/jobs/{job_id}", response_model=models.StatusInfo, name="get_job_status_catalog")
async def get_job_status_catalog(catalog_id: str, job_id: uuid.UUID, request: Request, conn: AsyncConnection = Depends(get_async_connection)):
    """Gets the status of a specific job (Catalog context)."""
    task = await _get_job_internal(job_id, catalog_id, conn)
    return _task_to_status_info(task, request)

@router.get("/catalogs/{catalog_id}/jobs/{job_id}/results", name="get_job_results_catalog")
async def get_job_results_catalog(catalog_id: str, job_id: uuid.UUID, conn: AsyncConnection = Depends(get_async_connection)):
    """Gets the results of a completed job (Catalog context)."""
    task = await _get_job_internal(job_id, catalog_id, conn)
    return _handle_job_results(task, job_id)

def _handle_job_results(task: Task, job_id: uuid.UUID):
    if task.status == TaskStatusEnum.FAILED:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' failed and has no results. See status for error.")
    if task.status != TaskStatusEnum.COMPLETED:
        raise HTTPException(status_code=status.HTTP_202_ACCEPTED, detail=f"Job '{job_id}' is not complete. Current status: {task.status}")
    return task.outputs or {}

@dynastore_extension
class ProcessesService(protocols.ExtensionProtocol):
    """
    Implements the OGC API - Processes standard.
    - Dynamically discovers registered Tasks that expose a process definition.
    - Uses the 'tasks' module to manage jobs (task executions).
    """
    router = router

def _task_to_status_info(task: Task, request: Request) -> models.StatusInfo:
    """Helper to convert a DynaStore Task model to an OGC StatusInfo model."""
    job_id = task.task_id
    
    # Determine the context for links
    path_params = request.scope.get("path_params", {})
    catalog_id = path_params.get("catalog_id")
    collection_id = path_params.get("collection_id")

    if catalog_id and collection_id:
        status_url = str(request.url_for('get_job_status_collection', catalog_id=catalog_id, collection_id=collection_id, job_id=str(job_id)))
    elif catalog_id:
        status_url = str(request.url_for('get_job_status_catalog', catalog_id=catalog_id, job_id=str(job_id)))
    else:
        status_url = str(request.url_for('get_job_status', job_id=str(job_id)))
    
    links: List[models.Link] = [
        models.Link(href=status_url, rel="self", type="application/json", title="This document"),
    ]
    # If the job is complete and has outputs, add a link to the results endpoint.
    if task.status == TaskStatusEnum.COMPLETED and task.outputs is not None:
        if catalog_id and collection_id:
            results_url = str(request.url_for('get_job_results_collection', catalog_id=catalog_id, collection_id=collection_id, job_id=str(job_id)))
        elif catalog_id:
            results_url = str(request.url_for('get_job_results_catalog', catalog_id=catalog_id, job_id=str(job_id)))
        else:
            results_url = str(request.url_for('get_job_results', job_id=str(job_id)))
        links.append(models.Link(href=results_url, rel="results", type="application/json", title="Job results"))

    return models.StatusInfo(
        jobID=job_id,
        status=task.status,
        message=task.error_message or f"Task is {task.status.lower()}",
        progress=task.progress,
        created=task.timestamp,
        updated=task.finished_at or task.started_at or task.timestamp,
        links=links
    )