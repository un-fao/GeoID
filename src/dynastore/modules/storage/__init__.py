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
        get_driver, Capability, ReadHint,
    )

    driver = await get_driver(catalog_id, collection_id, hint=ReadHint.SEARCH)
    async for entity in driver.read_entities(catalog_id, collection_id):
        ...

Storage routing configuration is now part of ``CollectionPluginConfig``
in ``dynastore.modules.catalog.catalog_config``.
"""

from dynastore.models.protocols.storage_driver import (
    Capability,
    CollectionStorageDriverProtocol,
    ReadOnlyDriverMixin,
    StorageLocationResolver,
)
from dynastore.modules.storage.config import DriverRef
from dynastore.modules.storage.errors import ReadOnlyDriverError, SoftDeleteNotSupportedError
from dynastore.modules.storage.hints import ReadHint, register_hint, get_registered_hints
from dynastore.modules.storage.location import (
    StorageLocationConfig,
    StorageLocationConfigRegistry,
)
from dynastore.modules.storage.router import get_driver

__all__ = [
    # Protocol
    "CollectionStorageDriverProtocol",
    "StorageLocationResolver",
    "Capability",
    "ReadOnlyDriverMixin",
    # Config
    "DriverRef",
    # Hints
    "ReadHint",
    "register_hint",
    "get_registered_hints",
    # Location
    "StorageLocationConfig",
    "StorageLocationConfigRegistry",
    # Errors
    "ReadOnlyDriverError",
    "SoftDeleteNotSupportedError",
    # Router
    "get_driver",
]
