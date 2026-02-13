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

from typing import Protocol, AsyncGenerator, Any, Optional, runtime_checkable
from contextlib import asynccontextmanager
from dynastore.models.auth import Principal

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

class ModuleProtocol(HasConfigManager, Protocol):
    """
    Defines the contract for a DynaStore foundational Module.

    Modules are core, reusable services that provide foundational capabilities
    (like database connections or configuration management) to the rest of the
    application. They are independent of FastAPI.

    ---
    Conventional Members:
    The system checks for the presence of these members at runtime.

    - `__init__(self, app_state: object)`: A module can optionally define a
      constructor. If the constructor accepts arguments, the module loader
      will pass the shared `app_state` object to it.

    - `lifespan(self, app_state: object)`: This is the primary entrypoint for a
      module. It's an async context manager for managing resources, called
      during application startup and shutdown.
    ---

    Example:
    ```python
    @dynastore_module
    class MyModule:
        # This module has no __init__ because it's stateless at creation.

        @asynccontextmanager
        async def lifespan(self, app_state: object):
            print("MyModule is starting up...")
            # Setup resources and attach them to the state
            app_state.my_service = MyService()
            try:
                yield
            finally:
                # Clean up resources
                print("MyModule is shutting down.")
                del app_state.my_service
    ```
    """

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        """A context manager to manage the module's lifecycle."""
        yield

    _registered_name: str = "unregistered_module" # Default value, will be set by decorator
    priority: int = 0  # Default priority

    def is_available(self) -> bool:
        """
        Returns whether the module is currently available to provide its protocol capability.
        Used by the discovery mechanism for prioritized fallbacks.
        """
        return True

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the module.
        This class method reads the name set by the @dynastore_module decorator.
        """
        return cls._registered_name
