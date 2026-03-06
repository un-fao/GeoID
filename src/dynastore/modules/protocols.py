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

# dynastore/modules/protocols.py

import logging
from typing import Protocol, AsyncGenerator, Any, Optional, runtime_checkable
from contextlib import asynccontextmanager
from dynastore.models.auth import Principal
from dynastore.tools.plugin import ProtocolPlugin

logger = logging.getLogger(__name__)

# Re-exporting for backward compatibility if needed, or simply cleaning up.
# The core protocols are now in dynastore.models.auth


class HasConfigManager(Protocol):
    """
    Protocol for any component (Module, Extension, Task) that provides access 
    to the centralized Configuration Manager.
    """
    def get_config_manager(self) -> Any:
        """
        Returns the configuration manager instance (typically ConfigManager).
        The return type is Any to avoid circular imports with the concrete class.
        """
        ...

    def get_protocol(self, protocol_type: Any) -> Any:
        """
        Discovers and caches a protocol implementation at the instance level.
        Provides a standardized way for modules and extensions to access other protocols.
        """
        if not hasattr(self, "_protocol_cache"):
            self._protocol_cache = {}
        if protocol_type not in self._protocol_cache:
            from dynastore.tools.discovery import get_protocol
            instance = get_protocol(protocol_type)
            if not instance:
                logger.debug(
                    f"Protocol {protocol_type.__name__ if hasattr(protocol_type, '__name__') else protocol_type} not found at this stage."
                )
                return None
            self._protocol_cache[protocol_type] = instance
        return self._protocol_cache[protocol_type]


class ModuleProtocol(ProtocolPlugin[object], HasConfigManager):
    """
    Defines the contract for a DynaStore foundational Module.

    Modules are core, reusable services that provide foundational capabilities
    (like database connections or configuration management) to the rest of the
    application. They are independent of FastAPI.

    Each Module is a ``ProtocolPlugin[object]``, meaning:
    - Its ``lifespan(app_state: object)`` is the single lifecycle hook.
    - Its ``priority`` is compared *only* against other modules (same category).
    - ``is_available()`` controls whether it is returned by protocol discovery.

    ---
    Example:
    ```python
    class MyModule(ModuleProtocol):

        @asynccontextmanager
        async def lifespan(self, app_state: object):
            app_state.my_service = MyService()
            try:
                yield
            finally:
                del app_state.my_service
    ```
    """

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the module.
        """
        return getattr(cls, "_registered_name", "unregistered_module")
