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

# dynastore/modules/__init__.py

import logging
import pkgutil
import importlib
import os
import inspect
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Type, TypeVar, cast, List, Optional
from pathlib import Path

from .protocols import ModuleProtocol
from dynastore.tools.discovery import get_protocol, get_protocols
from dynastore.tools.env import load_component_dotenv

logger = logging.getLogger(__name__)

T_Module = TypeVar("T_Module", bound=ModuleProtocol)

_DYNASTORE_MODULES: Dict[str, "ModuleConfig"] = {}

@dataclass
class ModuleConfig:
    cls: Type[ModuleProtocol]
    instance: ModuleProtocol | None = None

def _register_module(cls: Type[T_Module], registration_name: Optional[str] = None) -> Type[T_Module]:
    """
    Internal helper to register a module class.
    """
    if registration_name is None:
        try:
            parts = cls.__module__.split('.')
            idx = parts.index('modules')
            registration_name = parts[idx + 1]
        except (ValueError, IndexError):
            registration_name = cls.__name__

    if registration_name in _DYNASTORE_MODULES:
        # Avoid redundant registration if same class
        if _DYNASTORE_MODULES[registration_name].cls == cls:
            return cls
        logger.warning(f"Module '{registration_name}' is already registered with a different class. Overwriting.")
    
    _DYNASTORE_MODULES[registration_name] = ModuleConfig(cls=cls)
    cls._registered_name = registration_name
    logger.info(f"Registered module: {cls.__name__} (as '{registration_name}')")
    return cls

def get_module_instance(name: str) -> ModuleProtocol | None:
    """Retrieves the singleton instance of a registered module by name."""
    import warnings
    warnings.warn(
        f"get_module_instance('{name}') is deprecated. Use get_protocol(...) instead for better decoupling.",
        DeprecationWarning,
        stacklevel=2
    )
    config = _DYNASTORE_MODULES.get(name)
    return config.instance if config else None

def get_module_instance_by_class(cls: Type[T_Module]) -> T_Module | None:
    """
    Retrieves the singleton instance of a registered module by its class type.
    This provides better type hinting than get_module_instance(name).
    """
    import warnings
    warnings.warn(
        f"get_module_instance_by_class({cls.__name__}) is deprecated. Use get_protocol(...) instead for better decoupling.",
        DeprecationWarning,
        stacklevel=2
    )
    for name, config in _DYNASTORE_MODULES.items():
        if config.instance and isinstance(config.instance, cls):
            logger.warning(f"DEBUG: get_module_instance_by_class({cls.__name__}) found instance of {type(config.instance).__name__} registered as '{name}' at {id(config.instance)}")
            # We can safely cast here because we've confirmed the class type.
            return cast(T_Module, config.instance)
    logger.warning(f"DEBUG: get_module_instance_by_class({cls.__name__}) found NOTHING")
    return None


def discover_modules(include_only: Optional[List[str]] = None):
    """
    Discovers all foundational modules dynamically using PEP-517 entry points
    under the 'dynastore.modules' group.
    """
    if include_only is None:
        scope = os.getenv("SCOPE")
        if scope:
            include_only = [s.strip() for s in scope.split(",")]

    logger.info("--- [modules] Discovering components via entry points... ---")
    from dynastore.tools.discovery import discover_and_load_plugins
    
    # Discovery now returns uninstantiated classes based purely on entry points
    classes = discover_and_load_plugins("dynastore.modules", include_only=include_only)
    
    # Populate _DYNASTORE_MODULES with the discovered classes
    for name, cls in classes.items():
        _DYNASTORE_MODULES[name] = ModuleConfig(cls=cls)
            
    logger.info(f"--- DISCOVERED MODULES: {list(_DYNASTORE_MODULES.keys())} ---")


def _get_ordered_modules() -> List[str]:
    """
    Returns modules sorted by their ``priority`` class attribute (ascending).
    A lower priority value means the module is started earlier.
    Modules without a ``priority`` attribute default to 100.
    """
    def _priority(name: str) -> int:
        config = _DYNASTORE_MODULES.get(name)
        if config is None:
            return 100
        return getattr(config.cls, "priority", 100)

    return sorted(_DYNASTORE_MODULES.keys(), key=_priority)

def instantiate_modules(app_state: object, include_only: Optional[List[str]] = None):
    """
    Instantiates all discovered modules and attaches them to the app_state.
    This is separated from the main lifespan to allow for early instantiation
    before the full application startup.
    """
    available_modules = list(_DYNASTORE_MODULES.keys())

    # If filtering is requested (e.g. for tests), apply it
    if include_only is not None:
        target_names = {name.lower().replace("_", "-") for name in include_only}
        available_modules = [
            name for name in available_modules 
            if name.lower().replace("_", "-") in target_names
        ]

    # Sort by priority so foundational modules (lower priority value) come first
    def _priority(name: str) -> int:
        config = _DYNASTORE_MODULES.get(name)
        if config is None:
            return 100
        return getattr(config.cls, "priority", 100)

    ordered_modules = sorted(available_modules, key=_priority)
    
    logger.info(f"Instantiating modules in order: {ordered_modules}")
    for module_name in ordered_modules:
        config = _DYNASTORE_MODULES.get(module_name)
        if not config:
            continue
        
        cls = config.cls
        load_component_dotenv(cls)
        try:
            sig = inspect.signature(cls)
            instance = cls(app_state=app_state) if 'app_state' in sig.parameters else cls()
            config.instance = instance

            # Register in the central protocol discovery registry
            from dynastore.tools.discovery import register_plugin
            register_plugin(instance)

            logger.info(f"Instantiated module '{module_name}' ({cls.__name__})")
        except Exception:
            logger.error(f"CRITICAL: Failed during __init__ of module '{module_name}'. It will be unavailable.", exc_info=True)
            config.instance = None


@asynccontextmanager
async def lifespan(app_state: object):
    """
    Manages the combined lifecycle of all registered modules, ensuring they are
    started and stopped in the correct dependency order.
    """
    ordered_modules = _get_ordered_modules()

    if not ordered_modules:
        yield
        return

    configs_to_load = [
        _DYNASTORE_MODULES[name] for name in ordered_modules if name in _DYNASTORE_MODULES
    ]

    # The AsyncExitStack is the key to reverse-order shutdown.
    # As `enter_async_context` is called on each module's lifespan, its exit
    # logic is pushed onto a stack. When this `async with` block concludes
    # (on application shutdown), the stack is unwound (LIFO), guaranteeing
    # that the last module started is the first one shut down.
    async with AsyncExitStack() as stack:
        for config in configs_to_load:
            # If instantiation failed, the instance will be None.
            if not config.instance:
                logger.warning(f"Skipping lifespan for module '{config.cls.__name__}' as it was not instantiated correctly.")
                continue

            logger.warning(f"DEBUG: Entering lifespan for module: {config.cls.__name__}")
            if isinstance(config.instance, ModuleProtocol):
                # The lifespan context manager will handle async initializations.
                try:
                    lifespan_manager = config.instance.lifespan(app_state)
                    await stack.enter_async_context(lifespan_manager)
                    logger.warning(f"DEBUG: Lifespan for module '{config.cls.__name__}' entered successfully.")
                except Exception as e:
                    logger.error(f"Failed to enter lifespan for module '{config.cls.__name__}'", exc_info=True)
                    # Low-priority modules (< 20) are foundational — abort hard on failure
                    if getattr(config.cls, "priority", 100) < 20:
                        raise RuntimeError(f"CRITICAL: Foundational module '{config.cls.__name__}' failed during startup. Aborting.") from e

        
        yield
        
        # Wait for all background tasks before shutting down modules
        # This prevents closing GCP clients while tasks are still running.
        try:
            from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
            await lifecycle_registry.wait_for_all_tasks()
        except ImportError:
            pass
    
    logger.info("Application shutting down. Exiting module lifespans in reverse order.")
