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
from typing import Dict, Any, Optional, List, Annotated
from dynastore.modules import get_module_instance_by_class, get_protocol
from fastapi import FastAPI, APIRouter, HTTPException, status, Body, Path, Query, Request, BackgroundTasks
from pydantic import UUID4, BaseModel, Field, AliasChoices, ConfigDict
from dynastore.extensions import dynastore_extension
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols import AssetsProtocol
from dynastore.extensions.tools.exception_handlers import handle_exception
from dynastore.extensions.tools.security import get_principal
from dynastore.models.auth import Principal
from fastapi import Depends
from dynastore.modules.catalog.catalog_module import CatalogModule
from dynastore.modules.catalog.asset_manager import AssetManager, Asset, AssetBase, AssetFilter, AssetUpdate
from dynastore.modules.catalog.asset_tasks_spi import AssetTasksSPI
from dynastore.modules.processes.models import ExecuteRequest, StatusInfo, JobControlOptions
from contextlib import asynccontextmanager
import uuid

logger = logging.getLogger(__name__)
_assets: AssetsProtocol

class SearchQuery(BaseModel):
    """Payload for advanced asset searching."""
    model_config = ConfigDict(extra='allow', populate_by_name=True)

    filters: List[AssetFilter] = Field(default_factory=list, description="List of granular filters to apply.")
    collection_id: Annotated[Optional[str], Field(None, description="Optional scope to a specific collection.")]
    limit: int = Field(10, ge=1, le=100)
    offset: int = Field(0, ge=0)

@dynastore_extension
class AssetService(ExtensionProtocol):
    """
    Asset Service Extension.
    Exposes API endpoints for managing assets across catalogs and collections.
    """
    router: APIRouter = APIRouter(prefix="/assets", tags=["Assets"])

    def __init__(self, app: FastAPI):
        self.app = app

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        global _assets
        _assets = get_protocol(AssetsProtocol)
        if not _assets:
            raise RuntimeError("AssetService requires an AssetsProtocol implementation (like CatalogModule) to be active.")
        yield

    # =============================================================================
    #  CATALOG LEVEL OPERATIONS
    # =============================================================================

    @router.get("/catalogs/{catalog_id}", summary="List Catalog Assets")
    async def list_catalog_assets(
        catalog_id: str = Path(..., description="The catalog ID"),
        limit: int = Query(10, ge=1, le=100), 
        offset: int = Query(0, ge=0)
    ):
        """Returns a list of assets associated directly with the catalog (no collection)."""
        return await _assets.list_assets(catalog_id=catalog_id, collection_id=None, limit=limit, offset=offset)

    @router.post("/catalogs/{catalog_id}", response_model=Asset, status_code=status.HTTP_201_CREATED, summary="Create Catalog Asset")
    async def create_catalog_asset(
        asset_in: AssetBase,
        catalog_id: str = Path(..., description="The catalog ID")
    ):
        """Creates a new asset at the catalog level."""
        try:
            return await _assets.create_asset(catalog_id=catalog_id, asset=asset_in, collection_id=None)
        except Exception as e:
            raise handle_exception(e, resource_name="Asset", resource_id=f"{catalog_id}:{asset_in.id}", operation="Catalog asset creation")

    @router.delete("/catalogs/{catalog_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete All Catalog Assets")
    async def delete_catalog_assets(
        catalog_id: str = Path(..., description="The catalog ID"),
        force: bool = Query(False, description="True for Hard Delete, False for Soft Delete"),
        propagate: bool = Query(False, description="Whether to propagate deletion to linked features")
    ):
        """Removes all assets belonging to the catalog."""
        await _assets.delete_assets(catalog_id=catalog_id, hard=force, propagate=propagate)
    
    # =============================================================================
    #  CATALOG ASSET (SINGLE)
    # =============================================================================

    @router.get("/catalogs/{catalog_id}/assets/{asset_id}", response_model=Asset, summary="Get Catalog Asset by ID")
    async def get_catalog_asset(
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID")
    ):
        """Retrieves details for a specific asset in the catalog."""
        asset = await _assets.get_asset(catalog_id=catalog_id, asset_id=asset_id, collection_id=None)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        return asset

    @router.put("/catalogs/{catalog_id}/assets/{asset_id}", response_model=Asset, summary="Update Catalog Asset metadata")
    async def update_catalog_asset(
        asset_in: AssetUpdate,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID")
    ):
        """Updates the metadata of an existing catalog asset. All other fields are immutable."""
        try:
            return await _assets.update_asset(catalog_id=catalog_id, asset_id=asset_id, asset_update=asset_in)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @router.delete("/catalogs/{catalog_id}/assets/{asset_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete Catalog Asset by ID")
    async def delete_catalog_asset_by_id(
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
        force: bool = Query(False, description="True for Hard Delete, False for Soft Delete"),
        propagate: bool = Query(False, description="Whether to propagate deletion to linked features")
    ):
        """Deletes a specific asset by ID."""
        success = await _assets.delete_assets(catalog_id=catalog_id, asset_id=asset_id, hard=force, propagate=propagate)
        if success == 0:
            raise HTTPException(status_code=404, detail="Asset not found")

    # =============================================================================
    #  COLLECTION LEVEL OPERATIONS
    # =============================================================================

    @router.get("/catalogs/{catalog_id}/collections/{collection_id}", summary="List Collection Assets")
    async def list_collection_assets(
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        limit: int = Query(10, ge=1, le=100), 
        offset: int = Query(0, ge=0)
    ):
        """Returns a list of assets associated with a specific collection."""
        return await _assets.list_assets(catalog_id=catalog_id, collection_id=collection_id, limit=limit, offset=offset)

    @router.post("/catalogs/{catalog_id}/collections/{collection_id}", response_model=Asset, status_code=status.HTTP_201_CREATED, summary="Create Collection Asset")
    async def create_collection_asset(
        asset_in: AssetBase,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID")
    ):
        """Creates a new asset associated with a specific collection."""
        # Ensure the asset object has the correct collection linkage if passed, or override
        # We rely on arguments passed to create_asset, but asset_in might have conflicting info?
        # AssetBase doesn't have collection_id, so it's fine.
        try:
            return await _assets.create_asset(catalog_id=catalog_id, asset=asset_in, collection_id=collection_id)
        except Exception as e:
            raise handle_exception(e, resource_name="Asset", resource_id=f"{catalog_id}:{collection_id}:{asset_in.id}", operation="Collection asset creation")
    
    @router.delete("/catalogs/{catalog_id}/collections/{collection_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete All Collection Assets")
    async def delete_collection_assets(
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        force: bool = Query(False, description="True for Hard Delete, False for Soft Delete"),
        propagate: bool = Query(False, description="Whether to propagate deletion to linked features")
    ):
        """Removes all assets belonging to the specified collection."""
        await _assets.delete_assets(catalog_id=catalog_id, collection_id=collection_id, hard=force, propagate=propagate)

    # =============================================================================
    #  COLLECTION ASSET (SINGLE)
    # =============================================================================
    
    @router.get("/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}", response_model=Asset, summary="Get Collection Asset by ID")
    async def get_collection_asset(
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID")
    ):
        """Retrieves details for a specific asset in a collection."""
        asset = await _assets.get_asset(catalog_id=catalog_id, asset_id=asset_id, collection_id=collection_id)
        if not asset:
             raise HTTPException(status_code=404, detail="Asset not found in this collection")
        return asset

    @router.put("/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}", response_model=Asset, summary="Update Collection Asset metadata")
    async def update_collection_asset(
        asset_in: AssetUpdate,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID")
    ):
        """Updates the metadata of an existing collection asset. All other fields are immutable."""
        try:
            return await _assets.update_asset(catalog_id=catalog_id, asset_id=asset_id, asset_update=asset_in, collection_id=collection_id)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    @router.delete("/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete Collection Asset by ID")
    async def delete_collection_asset_by_id(
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
        force: bool = Query(False, description="True for Hard Delete, False for Soft Delete"),
        propagate: bool = Query(False, description="Whether to propagate deletion to linked features")
    ):
        """Deletes a specific asset by ID."""
        success = await _assets.delete_assets(catalog_id=catalog_id, asset_id=asset_id, collection_id=collection_id, hard=force, propagate=propagate)
        if success == 0:
            raise HTTPException(status_code=404, detail="Asset not found")


    # =============================================================================
    #  SEARCH
    # =============================================================================

    @router.post("/catalogs/{catalog_id}/search", response_model=List[Asset], summary="Advanced Asset Search")
    async def advanced_search(
        catalog_id: str = Path(..., description="The catalog ID"),
        query: SearchQuery = Body(...)
    ):
        """
        Granular POST-based search using the advanced query builder.
        """
        try:
            return await _assets.advanced_search(
                catalog_id=catalog_id,
                filters=query.filters,
                collection_id=query.collection_id,
                limit=query.limit,
                offset=query.offset
            )
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    # =============================================================================
    #  ASSET TASKS (SPI)
    # =============================================================================

    @staticmethod
    async def _get_spi_instance(config: Any, app_state: object) -> Optional[AssetTasksSPI]:
        """
        Resolves an AssetTasksSPI instance from a TaskConfig.
        If the instance is not already created, it attempts a lightweight instantiation
        if the task class implements the SPI.
        """
        # 1. Check if already instantiated
        if config.instance and isinstance(config.instance, AssetTasksSPI):
            return config.instance
        
        # 2. If not instantiated, check if the class implements the SPI and is not a placeholder
        cls = config.cls
        if cls and not getattr(cls, 'is_placeholder', False):
            try:
                # Use issubclass for explicit inheritance or Protocol check
                if issubclass(cls, AssetTasksSPI):
                    logger.debug(f"Task class '{config.name}' implements AssetTasksSPI. Attempting on-demand instantiation.")
                    # Instantiate (mimicking dynastore.tasks.manage_tasks logic)
                    if cls.__init__ is not object.__init__:
                        import inspect
                        sig = inspect.signature(cls.__init__)
                        if 'app_state' in sig.parameters:
                             instance = cls(app_state=app_state)
                        else:
                             instance = cls()
                    else:
                        instance = cls()
                    
                    # Note: We don't cache this back to config.instance here to avoid
                    # side-effects on the global task registry, but we return it for use.
                    return instance
            except Exception as e:
                logger.debug(f"Could not check or instantiate task class '{config.name}' for SPI: {e}")
        
        return None

    @staticmethod
    async def _get_available_tasks_for_asset(asset: Asset, app_state: object) -> List[Any]:
        from dynastore.tasks import get_all_task_configs
        configs = get_all_task_configs()
        
        available = []
        logger.info(f"Finding tasks for asset {asset.asset_id} (type: {asset.asset_type})...")
        
        for name, config in configs.items():
            logger.debug(f"Checking task: {name} (module: {config.module_name})")
            
            # Use the robust instance resolver
            instance = await AssetService._get_spi_instance(config, app_state)
            
            if instance:
                try:
                    logger.debug(f"Calling can_run_on_asset for task '{name}'...")
                    if await instance.can_run_on_asset(asset):
                        logger.info(f"Task '{name}' can run on asset {asset.asset_id}.")
                        if config.process_definition:
                             available.append(config.process_definition)
                        else:
                             # Fallback: try to get it from the instance if missing in config
                             if hasattr(instance, 'get_process_definition'):
                                 available.append(instance.get_process_definition())
                             else:
                                 logger.warning(f"Task '{name}' matched but has no process definition.")
                    else:
                        logger.debug(f"Task '{name}' cannot run on asset {asset.asset_id}.")
                except Exception as e:
                    logger.error(f"Error checking SPI for task '{config.name}': {e}", exc_info=True)
            else:
                 logger.debug(f"Task '{name}' does not implement AssetTasksSPI implementation (instance: {config.instance is not None}).")

        return available

    @router.get("/catalogs/{catalog_id}/assets/{asset_id}/tasks", summary="List Available Tasks for Catalog Asset")
    async def list_catalog_asset_tasks(
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID")
    ):
        """Lists tasks that can be executed on this catalog-level asset."""
        try:
            if _assets is None:
                logger.error("_assets is None! Extension not initialized properly?")
                raise HTTPException(status_code=500, detail="AssetService not initialized properly")
                
            asset = await _assets.get_asset(catalog_id=catalog_id, asset_id=asset_id)
            if not asset: raise HTTPException(status_code=404, detail="Asset not found")
            return await AssetService._get_available_tasks_for_asset(asset, request.app.state)
        except HTTPException:
            raise
        except Exception as e:
            logger.critical(f"UNHANDLED EXCEPTION in list_catalog_asset_tasks: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")

    @router.get("/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}/tasks", summary="List Available Tasks for Collection Asset")
    async def list_collection_asset_tasks(
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID")
    ):
        """Lists tasks that can be executed on this collection-level asset."""
        asset = await _assets.get_asset(catalog_id=catalog_id, asset_id=asset_id, collection_id=collection_id)
        if not asset: raise HTTPException(status_code=404, detail="Asset not found")
        return await AssetService._get_available_tasks_for_asset(asset, request.app.state)

    @router.post("/catalogs/{catalog_id}/assets/{asset_id}/tasks/{task_id}/execute", status_code=status.HTTP_201_CREATED, summary="Execute Task on Catalog Asset")
    async def execute_catalog_asset_task(
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
        task_id: str = Path(..., description="The task (process) ID", examples=["gdal"]),
        principal: Optional[Principal] = Depends(get_principal),
        mode: Optional[JobControlOptions] = Query(None, description="Execution mode preference."),
        execution_request: ExecuteRequest = Body(
            ...,
            examples=[{
                "inputs": {
                    "asset_metadata": {"description": "Custom override"},
                    "other_param": "value"
                }
            }],
            description="Task execution parameters. Use 'inputs' to pass arguments or override metadata."
        )
    ):
        """Triggers a background task on the specified catalog asset."""
        asset = await _assets.get_asset(catalog_id=catalog_id, asset_id=asset_id)
        if not asset: raise HTTPException(status_code=404, detail="Asset not found")
        
        caller_id_val = principal.id if principal else "nouser"
        return await AssetService._execute_asset_task(request, background_tasks, asset, task_id, execution_request, str(caller_id_val), mode)

    @router.post("/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}/tasks/{task_id}/execute", status_code=status.HTTP_201_CREATED, summary="Execute Task on Collection Asset")
    async def execute_collection_asset_task(
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
        task_id: str = Path(..., description="The task (process) ID", examples=["ingestion"]),
        principal: Optional[Principal] = Depends(get_principal),
        mode: Optional[JobControlOptions] = Query(None, description="Execution mode preference."),
        execution_request: ExecuteRequest = Body(
            ..., 
            examples=[{
                "inputs": {
                    "catalog_id": "target_catalog_id",
                    "collection_id": "target_collection_id",
                    "ingestion_request": {
                        "asset": {"asset_id": "asset_id_example"},
                        "column_mapping": {
                            "external_id": "station_id",
                            "csv_lat_column": "latitude",
                            "csv_lon_column": "longitude"
                        }
                    }
                }
            }], 
            description="Task execution parameters. Use 'inputs' to pass arguments or override metadata."
        )
    ):
        """Triggers a background task on the specified collection asset."""
        asset = await _assets.get_asset(catalog_id=catalog_id, asset_id=asset_id, collection_id=collection_id)
        if not asset: raise HTTPException(status_code=404, detail="Asset not found")
        
        caller_id_val = principal.id if principal else "nouser"
        return await AssetService._execute_asset_task(request, background_tasks, asset, task_id, execution_request, str(caller_id_val), mode)

    @staticmethod
    async def _execute_asset_task(request: Request, background_tasks: BackgroundTasks, asset: Asset, task_id: str, execution_request: ExecuteRequest, caller_id: str, mode: Optional[JobControlOptions] = None):
        # We use the processes_module to execute the process
        import dynastore.modules.processes.processes_module as processes_module
        from dynastore.modules.db_config.tools import get_any_engine
        
        # We might need to inject the asset into the execution request inputs if the task expects it
        # Requirement: "each task through the SPI may accept or not the asset as input for the process"
        # We'll let the user provide it in 'inputs', or we can automatically inject it if it's not there?
        # Most likely, the runner of the process will be triggered.
        
        engine = get_any_engine(request.app.state)
        
        # SPI Adapter Layer: try to get the task instance to call its specialized adapter if available.
        from dynastore.tasks import get_all_task_configs
        configs = get_all_task_configs()
        config = configs.get(task_id)
        
        task_instance = None
        if config:
            task_instance = await AssetService._get_spi_instance(config, request.app.state)
        
        logger.debug(f"Executing asset task '{task_id}' on asset '{asset.asset_id}'. SPI check starting...")
        
        if task_instance:
            # 1. Initial Guard: Check if the task can run on this specific asset
            logger.debug(f"Task '{task_id}' implements AssetTasksSPI. Checking compatibility...")
            if not await task_instance.can_run_on_asset(asset):
                logger.warning(f"Task '{task_id}' rejected asset '{asset.asset_id}' via can_run_on_asset.")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, 
                    detail=f"Task '{task_id}' is not compatible with the selected asset (type: {asset.asset_type})."
                )
            
            # 2. Standard Metadata Injection: Always provide a baseline context
            logger.debug(f"Performing standard metadata injection for task '{task_id}'.")
            
            # Always attach the asset object to the execution request
            # execution_request.asset = asset # REMOVED: asset object is no longer part of the DTO
            
            inputs = execution_request.inputs or {}
            if isinstance(inputs, dict):
                inputs.setdefault("asset_uri", str(asset.uri))
                inputs.setdefault("asset_id", asset.asset_id)
                # inputs.setdefault("asset_code", asset.asset_id) # Backwards compat
                inputs.setdefault("catalog_id", asset.catalog_id)
                inputs.setdefault("collection_id", asset.collection_id)
                inputs.setdefault("asset_type", asset.asset_type.value)
                execution_request.inputs = inputs

            # 3. Adaptation Layer: Let the SPI specialized logic further adapt or map fields
            logger.debug(f"Task '{task_id}' implements AssetTasksSPI. Adapting request...")
            execution_request = await task_instance.get_execution_request(asset, execution_request)
        else:
             logger.warning(f"Task instance for '{task_id}' not found or doesn't implement SPI. Injection logic skipped.")

        try:
            # We assume 'task_id' here refers to the OGC Process ID
            result = await processes_module.execute_process(
                process_id=task_id,
                execution_request=execution_request,
                engine=engine,
                caller_id=caller_id,
                preferred_mode=mode,
                background_tasks=background_tasks
            )
            # Handle result (Task object for async)
            from dynastore.modules.tasks.models import Task
            if isinstance(result, Task):
                # Return 201 with Location (we'd need to link to jobs which is in processes extension)
                # For now, let's return a basic status info
                return {
                    "jobID": str(result.task_id),
                    "status": result.status,
                    "message": "Task created successfully"
                }
            return result
        except Exception as e:
            logger.exception(f"Asset task execution failed: {e}")
            raise HTTPException(status_code=400, detail=str(e))
