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

"""
Multi-driver storage abstraction layer.

Public API::

    from dynastore.modules.storage import (
        get_driver, resolve_drivers, Operation, FailurePolicy,
    )

    driver = await get_driver("READ", catalog_id, collection_id, hint="search")
    async for entity in driver.read_entities(catalog_id, collection_id):
        ...

Storage routing is controlled by ``RoutingPluginConfig``
(``plugin_id = "routing"``) via the existing config API.
Per-driver settings are in ``DriverPluginConfig`` subclasses
(``plugin_id = "driver:<driver_id>"``).
"""

from dynastore.models.protocols.storage_driver import (
    Capability,
    CollectionStorageDriverProtocol,
    ReadOnlyDriverMixin,
    StorageLocationResolver,
)
from dynastore.modules.storage.driver_config import (
    CollectionDriverConfig,
    CollectionWritePolicy,
    AssetDriverConfig,
    DriverCapability,
    DriverPluginConfig,
    PG_DRIVER_PLUGIN_ID,
    WRITE_POLICY_PLUGIN_ID,
    PostgresCollectionDriverConfig,
    WriteConflictPolicy,
    get_pg_collection_config,
)
from dynastore.modules.storage.driver_enricher import DriverMetadataEnricher
from dynastore.modules.storage.errors import ReadOnlyDriverError, SoftDeleteNotSupportedError
from dynastore.modules.storage.router import (
    ResolvedDriver,
    get_asset_driver,
    get_asset_write_drivers,
    get_driver,
    get_write_drivers,
    resolve_drivers,
)
from dynastore.modules.storage.routing_config import (
    AssetRoutingPluginConfig,
    FailurePolicy,
    Operation,
    OperationDriverEntry,
    RoutingPluginConfig,
)

__all__ = [
    # Protocol
    "CollectionStorageDriverProtocol",
    "StorageLocationResolver",
    "Capability",
    "ReadOnlyDriverMixin",
    # Driver configs
    "DriverPluginConfig",
    "CollectionDriverConfig",
    "AssetDriverConfig",
    "DriverCapability",
    # Write policy
    "CollectionWritePolicy",
    "WriteConflictPolicy",
    "WRITE_POLICY_PLUGIN_ID",
    # Enricher
    "DriverMetadataEnricher",
    # Routing configs
    "RoutingPluginConfig",
    "AssetRoutingPluginConfig",
    "OperationDriverEntry",
    "FailurePolicy",
    "Operation",
    # Router
    "resolve_drivers",
    "ResolvedDriver",
    "get_driver",
    "get_write_drivers",
    "get_asset_driver",
    "get_asset_write_drivers",
    # Errors
    "ReadOnlyDriverError",
    "SoftDeleteNotSupportedError",
]
