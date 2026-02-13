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
from typing import Any, Optional
from pydantic import ValidationError

from dynastore.modules.db_config.query_executor import DbEngine
from dynastore.modules import get_protocol
from dynastore.modules.tasks import runners
from dynastore.modules.tasks.models import (Task, TaskExecutionMode, RunnerContext)
from dynastore.modules.processes import models
from dynastore.modules.apikey.models import SYSTEM_USER_ID
from dynastore.tasks import get_definitions_by_type
from dynastore.models.protocols import CatalogsProtocol, DatabaseProtocol
from dynastore.models.auth import AuthorizationProtocol, Principal, Action

logger = logging.getLogger(__name__)

async def execute_process(
    process_id: str,
    execution_request: models.ExecuteRequest,
    engine: DbEngine,
    caller_id: str = SYSTEM_USER_ID,
    preferred_mode: Optional[models.JobControlOptions] = None,
    background_tasks: Optional[Any] = None,
    catalog_id: Optional[str] = None,
    collection_id: Optional[str] = None,
) -> Any:
    """
    Core logic for executing a process. This function is decoupled from the API layer.
    It finds the appropriate process definition, validates inputs, determines the
    execution mode, and delegates to the highest-priority available runner.
    """
    # 1. Find the requested process definition.
    process = next((p for p in get_definitions_by_type(models.Process) if p.id == process_id), None)
    if not process:
        raise ValueError(f"Process '{process_id}' not found.")

    # 2. Perform pre-flight validation of inputs against the process definition's schema.
    for input_name, input_def in process.inputs.items():
        if input_def.schema_:
            try:
                from jsonschema import validate
                payload = execution_request.inputs.get(input_name)
                
                # Skip validation if the input is not provided. 
                #jsonschema.validate() would fail on None if the schema expects an object.
                if payload is None:
                    continue

                validate(instance=payload, schema=input_def.schema_)
                logger.info(f"Input '{input_name}' for process '{process_id}' passed pre-flight validation.")
            except Exception as e:
                # Catch jsonschema.ValidationError which might be raised by validate()
                # We use lazy import to avoid heavy dependencies at load time.
                try:
                    from jsonschema.exceptions import ValidationError as JsonschemaValidationError
                    is_jsonschema_error = isinstance(e, JsonschemaValidationError)
                except ImportError:
                    is_jsonschema_error = False

                if is_jsonschema_error or isinstance(e, (ValidationError, ValueError)):
                    logger.warning(f"Validation failed for input '{input_name}': {e}")
                    raise ValueError(f"Invalid input for '{input_name}': {e}")
                
                # Re-raise unexpected exceptions
                raise e

    candidate_modes = []

    # 3. Determine the execution mode.
    chosen_mode = None
    # Honor client preference if it's supported by the process.
    if preferred_mode and preferred_mode in process.jobControlOptions:
        candidate_modes.append(preferred_mode)
    
    # Add the process's default supported options as fallback.
    for option in process.jobControlOptions:
        if option not in candidate_modes and option in [models.JobControlOptions.ASYNC_EXECUTE, models.JobControlOptions.SYNC_EXECUTE]:
            candidate_modes.append(option)
            
    # Iterate through candidates and pick the first one with available runners.
    for mode in candidate_modes:
        execution_mode_check = None
        if mode == models.JobControlOptions.ASYNC_EXECUTE:
            execution_mode_check = TaskExecutionMode.ASYNCHRONOUS
        elif mode == models.JobControlOptions.SYNC_EXECUTE:
            execution_mode_check = TaskExecutionMode.SYNCHRONOUS
        
        if execution_mode_check and runners.get_runners(execution_mode_check):
            chosen_mode = mode
            logger.info(f"Selected execution mode '{mode}' for process '{process_id}' (runners available).")
            break
        else:
            logger.debug(f"Skipping execution mode '{mode}' for process '{process_id}': No runners available.")

    if not chosen_mode:
        raise NotImplementedError(f"No available execution mode for process '{process_id}'. Supported modes: {process.jobControlOptions}, but no runners are registered for them.")

    # Map OGC JobControlOptions to the internal TaskExecutionMode
    if chosen_mode == models.JobControlOptions.ASYNC_EXECUTE:
        execution_mode = TaskExecutionMode.ASYNCHRONOUS
    elif chosen_mode == models.JobControlOptions.SYNC_EXECUTE:
        execution_mode = TaskExecutionMode.SYNCHRONOUS
    else:
        raise NotImplementedError(f"Execution mode '{chosen_mode}' is not supported by any runner.")

    # 4. Get the prioritized list of runners for the chosen mode.
    available_runners = runners.get_runners(execution_mode)
    if not available_runners:
        raise NotImplementedError(f"No runners registered for execution mode '{execution_mode.value}'.")

    # 3. Determine execution mode (sync vs async).
    execution_mode = TaskExecutionMode.ASYNCHRONOUS if preferred_mode == models.JobControlOptions.ASYNC_EXECUTE else TaskExecutionMode.SYNCHRONOUS
    
    # 5. Authorization check (if AuthorizationProtocol is available)
    auth_protocol = get_protocol(AuthorizationProtocol)
    if auth_protocol:
        # Create a principal for the caller
        principal = Principal(user_id=caller_id)
        
        # Check if the caller has permission to execute processes in this catalog
        resource_id = f"catalog:{catalog_id}" if catalog_id else "system"
        if collection_id:
            resource_id = f"{resource_id}:collection:{collection_id}"
        
        is_authorized = await auth_protocol.is_authorized(
            principal=principal,
            action=Action.EXECUTE,
            resource_id=resource_id
        )
        if not is_authorized:
            from fastapi import HTTPException
            raise HTTPException(status_code=403, detail=f"User '{caller_id}' is not authorized to execute process '{process_id}' on resource '{resource_id}'.")

    # 6. Execute using the prioritized runners.
    result = None
    last_error = None
    
    # Resolve the db schema for the context
    db_schema = "public"
    catalog_protocol = get_protocol(CatalogsProtocol)
    if catalog_protocol and catalog_id:
        db_schema = await catalog_protocol.resolve_physical_schema(catalog_id, engine)

    # Build the RunnerContext with catalog and collection info.
    context = RunnerContext(
        engine=engine,
        task_type=process_id,
        caller_id=caller_id,
        inputs=execution_request.model_dump(),
        db_schema=db_schema or "public",
        extra_context={"background_tasks": background_tasks}
    )

    for runner in available_runners:
        try:
            logger.info(f"Attempting execution using runner '{runner.__class__.__name__}'...")
            result = await runner.run(context)
            if result is not None:
                break
        except Exception as e:
            last_error = e
            logger.warning(f"Runner '{runner.__class__.__name__}' failed for process '{process_id}': {e}")
            continue # Try the next runner

    if result is None:
        # If the loop completes and result is still None, all runners failed.
        raise RuntimeError(f"All available runners for mode '{execution_mode.value}' failed to execute for process '{process_id}'. Last error: {last_error}")

    return result