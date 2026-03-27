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

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Protocol, Type, TypeVar

from dynastore.tools.plugin import ProtocolPlugin

if TYPE_CHECKING:
    from dynastore.models.protocols.config_registry import ConfigRegistryProtocol

logger = logging.getLogger(__name__)

_P = TypeVar("_P")


class HasConfigService(Protocol):
    """
    Protocol for any component (Module, Extension, Task) that provides access
    to the centralized Configuration Manager.
    """

    def get_config_service(self) -> Optional[ConfigRegistryProtocol]:
        """Returns the configuration manager instance."""
        ...

    def get_protocol(self, protocol_type: Type[_P]) -> Optional[_P]:
        """
        Discovers and caches a protocol implementation at the instance level.
        Provides a standardized way for modules and extensions to access other protocols.
        """
        cache: dict[type, Any] = getattr(self, "_protocol_cache", {})
        if not cache:
            object.__setattr__(self, "_protocol_cache", cache)
        if protocol_type not in cache:
            from dynastore.tools.discovery import get_protocol
            instance = get_protocol(protocol_type)
            if not instance:
                logger.debug(
                    f"Protocol {getattr(protocol_type, '__name__', protocol_type)} not found at this stage."
                )
                return None
            cache[protocol_type] = instance
        return cache[protocol_type]


class ModuleProtocol(ProtocolPlugin[object], HasConfigService):
    """
    Defines the contract for a DynaStore foundational Module.

    Modules are core, reusable services that provide foundational capabilities
    (like database connections or configuration management) to the rest of the
    application. They are independent of FastAPI.

    Each Module is a ``ProtocolPlugin[object]``, meaning:
    - Its ``lifespan(app_state: object)`` is the single lifecycle hook.
    - Its ``priority`` is compared *only* against other modules (same category).
    - ``is_available()`` controls whether it is returned by protocol discovery.

    Subclasses are **automatically registered** in ``_DYNASTORE_MODULES`` when
    their defining Python module is imported (no decorator required).

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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-register every concrete subclass when it is first defined."""
        super().__init_subclass__(**kwargs)
        # Skip abstract classes (those that still have unimplemented abstract methods)
        if getattr(cls, '__abstractmethods__', None):
            return
        # Defer import to avoid circular imports at module load time
        try:
            from dynastore.modules import _register_module
            _register_module(cls)
        except Exception as e:
            logger.debug(f"ModuleProtocol.__init_subclass__: skipping auto-registration of {cls.__name__}: {e}")

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the module.
        """
        return getattr(cls, "_registered_name", "unregistered_module")
