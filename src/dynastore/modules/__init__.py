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
from typing import Dict, Type, TypeVar, cast, List, Optional
from dotenv import load_dotenv
from pathlib import Path

from .protocols import ModuleProtocol
from dynastore.tools.discovery import get_protocol, get_protocols

logger = logging.getLogger(__name__)

T_Module = TypeVar("T_Module", bound=ModuleProtocol)

_DYNASTORE_MODULES: Dict[str, "ModuleConfig"] = {}

@dataclass
class ModuleConfig:
    cls: Type[ModuleProtocol]
    instance: ModuleProtocol | None = None

def dynastore_module(cls: Type[T_Module]) -> Type[T_Module]:
    """A decorator to register a class as a DynaStore Module."""
    try:
        # Robustly determine the module name by finding the segment after 'modules'
        parts = cls.__module__.split('.')
        idx = parts.index('modules')
        registration_name = parts[idx + 1]
    except (ValueError, IndexError):
        logger.error(f"Could not determine module folder name for module '{cls.__name__}' from '{cls.__module__}'. Falling back to class name.")
        registration_name = cls.__name__


    if registration_name in _DYNASTORE_MODULES:
        logger.warning(f"Module '{registration_name}' is already registered. Overwriting.")
    
    # The registration logic remains the same.
    _DYNASTORE_MODULES[registration_name] = ModuleConfig(cls=cls)
    
    # Set the registered name on the class itself.
    cls._registered_name = registration_name
    logger.info(f"Discovered module: {cls.__name__} (registered as '{registration_name}')")

    # Return the original class, preserving its type.
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


def discover_modules(enabled_modules: Optional[List[str]] = None):
    """
    Discovers modules by dynamically importing sub-packages, loading .env files
    in a hierarchical order: root first, then module-specific (overriding).
    
    Args:
        enabled_modules: Optional list of module names to enabled. 
                         If provided, overrides DYNASTORE_MODULES env var.
    """
    # 1. Load the root .env file first, without overriding existing env vars.
    # This assumes a project structure like: /path/to/project/dynastore/modules
    project_root = Path(__file__).resolve().parents[2]
    root_dotenv_path = project_root / '.env'
    if root_dotenv_path.exists():
        load_dotenv(dotenv_path=root_dotenv_path, override=False)
        logger.info(f"Loaded base environment variables from '{root_dotenv_path}'")
    else:
        logger.warning(f"Root .env file not found at '{root_dotenv_path}'.")

    package_path = os.path.dirname(__file__)
    package_name = __name__

    if enabled_modules is None:
        enabled_modules_str = os.getenv("DYNASTORE_MODULES", "")
        if enabled_modules_str.strip() == "*":
            enabled_modules = [
                item for item in os.listdir(package_path)
                if os.path.isdir(os.path.join(package_path, item)) and not item.startswith('__')
            ]
            logger.info(f"Wildcard '*' detected for DYNASTORE_MODULES. Discovered: {enabled_modules}")
        else:
            enabled_modules = [m.strip() for m in enabled_modules_str.split(',') if m.strip()]


    if not enabled_modules:
        logger.warning("DYNASTORE_MODULES is not set. No foundational modules will be loaded.")
        return

    logger.info(f"Attempting to load enabled modules in order: {enabled_modules}")

    # 2. Iterate through modules in the specified order.
    for module_name in enabled_modules:
        module_path = os.path.join(package_path, module_name)
        if not os.path.isdir(module_path):
            logger.warning(f"Enabled module '{module_name}' not found at path '{module_path}'. Skipping.")
            continue

        try:
            # 3. Load module-specific .env file, OVERRIDING previous values.
            module_dotenv_path = os.path.join(module_path, '.env')
            if os.path.exists(module_dotenv_path):
                load_dotenv(dotenv_path=module_dotenv_path, override=True)
                logger.debug(f"Loaded and override environment variables from '{module_dotenv_path}'")

            # 4. Filter by requirements.txt if present
            from dynastore.tools.dependencies import check_requirements
            requirements_path = os.path.join(module_path, 'requirements.txt')
            if not check_requirements(requirements_path):
                logger.warning(f"Skipping module '{module_name}' due to unsatisfied requirements in '{requirements_path}'.")
                continue

            # 5. Import the Python modules to trigger registration.
            sub_package_name = f"{package_name}.{module_name}"
            for _, sub_module_name, _ in pkgutil.iter_modules([module_path]):
                full_module_path = f"{sub_package_name}.{sub_module_name}"
                try:
                    importlib.import_module(full_module_path)
                except Exception:
                     logger.error(f"Failed to import submodule '{full_module_path}'", exc_info=True)
        except Exception:
            logger.error(f"Failed to load module '{module_name}'", exc_info=True)


FOUNDATIONAL_MODULES = ["db_config", "db"]

def _get_ordered_modules(enabled_modules: Optional[List[str]] = None) -> List[str]:
    """
    Determines the ordered list of modules to load based on environment variables
    or an explicit list, ensuring foundational modules come first.
    """
    if enabled_modules is None:
        enabled_modules_str = os.getenv("DYNASTORE_MODULES", "")
        if enabled_modules_str.strip() == "*":
            # Start with foundational modules, then add the rest alphabetically
            available_modules = sorted(_DYNASTORE_MODULES.keys())
            ordered_modules = [m for m in FOUNDATIONAL_MODULES if m in available_modules]
            ordered_modules.extend([m for m in available_modules if m not in FOUNDATIONAL_MODULES])
        else:
            ordered_modules = [m.strip() for m in enabled_modules_str.split(',') if m.strip()]
    else:
        ordered_modules = enabled_modules
    
    # Re-verify foundational order even if explicit list is provided?
    # Usually, we trust the explicit list, but for safety in CI we might want to ensure it.
    # For now, let's just make it easier to get the right list.
    return ordered_modules

def instantiate_modules(app_state: object, enabled_modules: Optional[List[str]] = None):
    """
    Instantiates all discovered modules and attaches them to the app_state.
    This is separated from the main lifespan to allow for early instantiation
    before the full application startup.
    """
    ordered_modules = _get_ordered_modules(enabled_modules)
    
    logger.info(f"Instantiating modules in order: {ordered_modules}")
    for module_name in ordered_modules:
        config = _DYNASTORE_MODULES.get(module_name)
        if not config:
            continue
        
        cls = config.cls
        try:
            sig = inspect.signature(cls)
            config.instance = cls(app_state=app_state) if 'app_state' in sig.parameters else cls()
            logger.warning(f"DEBUG: Instantiated module '{module_name}' ({cls.__name__}) at {id(config.instance)}")
        except Exception:
            logger.error(f"CRITICAL: Failed during __init__ of module '{module_name}'. It will be unavailable.", exc_info=True)
            config.instance = None


@asynccontextmanager
async def lifespan(app_state: object, enabled_modules: Optional[List[str]] = None):
    """
    Manages the combined lifecycle of all registered modules, ensuring they are
    started and stopped in the correct dependency order.
    """
    ordered_modules = _get_ordered_modules(enabled_modules)

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

            # Automatic Provider Initialization
            # This discovers and initializes any Provider descriptors on the module instance.
            try:
                from dynastore.tools.discovery import initialize_providers
                from dynastore.models.protocols import DatabaseProtocol
                
                # Resolve DB engine if available (best effort)
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None
                await initialize_providers(config.instance, app_state, db_resource=engine, stack=stack)
                logger.debug(f"Providers initialized for module '{config.cls.__name__}'")
            except Exception:
                 logger.error(f"Failed to initialize providers for module '{config.cls.__name__}'", exc_info=True)
                 # We continue, as the module might not have providers or might handle it manually (though legacy)

            if hasattr(config.instance, "lifespan"):
                # The lifespan context manager will handle async initializations.
                try:
                    lifespan_manager = config.instance.lifespan(app_state)
                    await stack.enter_async_context(lifespan_manager)
                    logger.info(f"Lifespan for module '{config.cls.__name__}' entered successfully.")
                except Exception as e:
                    logger.error(f"Failed to enter lifespan for module '{config.cls.__name__}'", exc_info=True)
                    # Foundational modules MUST succeed, or we abort early to prevent cascading noise
                    if any(foundational in config.cls.__module__ for foundational in FOUNDATIONAL_MODULES):
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
