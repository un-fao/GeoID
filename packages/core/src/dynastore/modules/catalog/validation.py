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

from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
import logging
from typing import Set, List
from dynastore.modules.db_config.exceptions import InternalValidationError
from dynastore.modules.db_config.query_executor import DbResource

logger = logging.getLogger(__name__)

async def get_valid_properties(conn: DbResource, catalog_id: str, collection_id: str) -> Set[str]:
    """
    Returns a set of all valid property names for a collection,
    including physical columns and schema-defined attributes.
    """
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation
    from dynastore.models.protocols.storage_driver import Capability

    # 1. Resolve the driver — required for both introspection and config below.
    # (Previously this was inside the introspection try/except, so a get_driver
    # failure was swallowed and then re-surfaced as a confusing NameError on the
    # `driver.get_driver_config` call below.)
    driver = await get_driver(Operation.READ, catalog_id, collection_id)

    # 2. Physical columns via driver introspection (best-effort).
    physical_columns: Set[str] = set()
    if hasattr(driver, "capabilities") and Capability.INTROSPECTION in driver.capabilities:
        try:
            schema_info = await driver.introspect_schema(
                catalog_id, collection_id, db_resource=conn
            )
            physical_columns = {entry.name for entry in schema_info} if schema_info else set()
        except Exception as e:
            logger.warning(
                "Schema introspection failed for %s.%s; proceeding without physical columns: %s",
                catalog_id,
                collection_id,
                e,
            )

    # 3. Get driver config (sidecars, partitioning, etc.)
    config = await driver.get_driver_config(catalog_id, collection_id, db_resource=conn)

    schema_properties = set()
    if config:
        # 2a. Legacy Attribute Schema
        if hasattr(config, "attribute_schema") and config.attribute_schema:
            schema_properties.update({entry.name for entry in config.attribute_schema})

        # 2b. Sidecar Fields
        if driver_sidecars(config):
            from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
            for sc_config in driver_sidecars(config):
                if not sc_config.enabled:
                    continue
                try:
                    # Instantiate sidecar to access dynamic field definitions
                    sidecar = SidecarRegistry.get_sidecar(sc_config)
                    if sidecar is None:
                        continue
                    field_defs = sidecar.get_field_definitions()
                    schema_properties.update(field_defs.keys())
                except Exception as e:
                    logger.warning(f"Failed to get field definitions from sidecar {type(sc_config).__name__}: {e}")
        
    # Combine them
    return physical_columns.union(schema_properties)

async def validate_filter_properties(conn: DbResource, catalog_id: str, collection_id: str, property_names: List[str]):
    """
    Validates that all provided property names are valid for the collection.
    Raises HTTPException 400 if any property is unknown.
    """
    valid_props = await get_valid_properties(conn, catalog_id, collection_id)
    
    unknown_props = [p for p in property_names if p not in valid_props]
    if unknown_props:
        logger.warning(f"Unknown properties filtered for {catalog_id}.{collection_id}: {unknown_props}")
        raise InternalValidationError(
            f"Unknown filter properties: {', '.join(unknown_props)}. Valid properties are: {', '.join(sorted(valid_props))}"
        )
