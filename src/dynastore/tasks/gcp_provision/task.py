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
from typing import Any, Dict

from pydantic import BaseModel
from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.models import (
    RunnerContext,
    TaskPayload,
)

from dynastore.modules import get_protocol
from dynastore.models.protocols import (
    StorageProtocol,
    EventingProtocol,
    CatalogsProtocol,
)

logger = logging.getLogger(__name__)

def _get_catalog_protocol() -> CatalogsProtocol:
    protocol = get_protocol(CatalogsProtocol)
    if not protocol:
        raise RuntimeError("CatalogsProtocol not available")
    return protocol

def _get_storage_protocol() -> StorageProtocol:
    protocol = get_protocol(StorageProtocol)
    if not protocol:
        raise RuntimeError("StorageProtocol not available - GCP module not loaded")
    return protocol

class GcpProvisionInputs(BaseModel):
    catalog_id: str

class ProvisioningTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for creating and configuring a GCS bucket for a catalog.
    
    Idempotent: checks if resources exist before creating.
    If it fails after max retries, the janitor will move it to DEAD_LETTER
    and the catalog will remain in 'provisioning' or 'failed' state.
    """
    task_type = "gcp_provision_catalog"

    async def run(self, payload: TaskPayload[GcpProvisionInputs]) -> Dict[str, Any]:
        try:
            catalog_id = payload.inputs.catalog_id
            if not catalog_id:
                raise ValueError("Missing 'catalog_id' in task inputs")

            logger.info(f"GcpProvisionCatalogTask: Provisioning resources for catalog '{catalog_id}'...")

            # 1. Resolve storage protocol (GCP)
            storage = _get_storage_protocol()
            
            # 2. Setup the bucket and eventing idempotently
            if hasattr(storage, "setup_catalog_gcp_resources"):
                # Using the native GCP module method which provisions both bucket and eventing
                bucket_name, _ = await storage.setup_catalog_gcp_resources(catalog_id)
            else:
                # Fallback for mocked storage
                bucket_name = await storage.ensure_storage_for_catalog(catalog_id)
                eventing = get_protocol(EventingProtocol)
                if eventing:
                    await eventing.setup_catalog_eventing(catalog_id)

            logger.info(f"GcpProvisionCatalogTask: Bucket '{bucket_name}' ensured for catalog '{catalog_id}'.")

            # 4. Mark catalog as ready
            catalogs = _get_catalog_protocol()
            await catalogs.update_provisioning_status(catalog_id, "ready")
            
            logger.info(f"GcpProvisionCatalogTask: Catalog '{catalog_id}' marked as READY.")
            
            return {
                "catalog_id": catalog_id,
                "bucket_name": bucket_name,
                "status": "ready"
            }
        except Exception as e:
            logger.error(f"CRITICAL: GcpProvisionCatalogTask FAILED for {payload.inputs.catalog_id}: {e}", exc_info=True)
            raise

class GcpDestroyCatalogTask(TaskProtocol):
    priority: int = 100
    """
    Durable task for tearing down GCS resources associated with a catalog.
    
    Idempotent: safe to run multiple times even if resources are already gone.
    """
    task_type = "gcp_destroy_catalog"

    async def run(self, payload: TaskPayload[GcpProvisionInputs]) -> Dict[str, Any]:
        catalog_id = payload.inputs.catalog_id
        if not catalog_id:
            raise ValueError("Missing 'catalog_id' in task inputs")

        logger.info(f"GcpDestroyCatalogTask: Tearing down resources for catalog '{catalog_id}'...")

        # 1. Teardown eventing (optional)
        eventing = get_protocol(EventingProtocol)
        if eventing:
            try:
                await eventing.teardown_catalog_notifications(catalog_id)
                logger.info(f"GcpDestroyCatalogTask: Eventing removed for catalog '{catalog_id}'.")
            except Exception as e:
                logger.warning(f"GcpDestroyCatalogTask: Failed to teardown eventing for '{catalog_id}': {e}")

        # 2. Delete/cleanup storage
        try:
            storage = _get_storage_protocol()
            await storage.delete_catalog_bucket(catalog_id)
            logger.info(f"GcpDestroyCatalogTask: Bucket resources for '{catalog_id}' deleted.")
        except Exception as e:
            logger.warning(f"GcpDestroyCatalogTask: Failed to delete storage for '{catalog_id}': {e}")

        logger.info(f"GcpDestroyCatalogTask: Cleanup completed for catalog '{catalog_id}'.")
        
        return {
            "catalog_id": catalog_id,
            "status": "destroyed"
        }
