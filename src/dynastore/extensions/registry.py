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

# dynastore/extensions/registry.py

import logging
from typing import Dict, Type, TypeVar, Optional, List, cast
from dataclasses import dataclass
import warnings

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.tools.env import load_component_dotenv

logger = logging.getLogger(__name__)

T_Extension = TypeVar("T_Extension", bound=ExtensionProtocol)

@dataclass
class ExtensionConfig:
    cls: Type[ExtensionProtocol]
    instance: ExtensionProtocol | None = None

_DYNASTORE_EXTENSIONS: Dict[str, ExtensionConfig] = {}


def _register_extension(cls: Type[T_Extension], registration_name: Optional[str] = None) -> Type[T_Extension]:
    """
    Internal helper to register an extension class.
    """
    if registration_name is None:
        try:
            parts = cls.__module__.split('.')
            idx = parts.index('extensions')
            registration_name = parts[idx + 1]
        except (ValueError, IndexError):
            registration_name = cls.__name__

    if registration_name in _DYNASTORE_EXTENSIONS:
        if _DYNASTORE_EXTENSIONS[registration_name].cls == cls:
            return cls
        logger.warning(f"Extension '{registration_name}' is already registered. Overwriting.")
        
    _DYNASTORE_EXTENSIONS[registration_name] = ExtensionConfig(cls=cls)
    cls._registered_name = registration_name
    logger.info(f"Registered extension: {cls.__name__} (as '{registration_name}')")
    return cls

def dynastore_extension(cls: Type[T_Extension]) -> Type[T_Extension]:
    """A decorator to register a class as a DynaStore Extension."""
    return _register_extension(cls)

def get_extension_instance(name: str) -> ExtensionProtocol | None:
    """Retrieves the singleton instance of a registered extension by name."""
    warnings.warn(
        f"get_extension_instance('{name}') is deprecated. Use get_protocol(...) instead for better decoupling.",
        DeprecationWarning,
        stacklevel=2
    )
    config = _DYNASTORE_EXTENSIONS.get(name)
    return config.instance if config else None

def get_extension_instance_by_class(cls: Type[T_Extension]) -> T_Extension | None:
    """
    Retrieves the singleton instance of a registered extension by its class type.
    This provides better type hinting than get_extension_instance(name).
    """
    warnings.warn(
        f"get_extension_instance_by_class({cls.__name__}) is deprecated. Use get_protocol(...) instead for better decoupling.",
        DeprecationWarning,
        stacklevel=2
    )
    for _, config in _DYNASTORE_EXTENSIONS.items():
        if config.instance and isinstance(config.instance, cls):
            return cast(T_Extension, config.instance)
    return None

def discover_extensions(include_only: Optional[List[str]] = None):
    """
    Discovers extensions using PEP-517 entry points defined in pyproject.toml
    under the "dynastore.extensions" group.
    """
    if include_only is None:
        import os
        scope = os.getenv("SCOPE")
        if scope:
            include_only = [s.strip() for s in scope.split(",")]

    logger.info("--- [extensions] Discovering components via entry points... ---")
    # Discovery returns uninstantiated classes based purely on entry points
    from dynastore.tools.discovery import discover_and_load_plugins
    classes = discover_and_load_plugins("dynastore.extensions", include_only=include_only)
    
    # Populate _DYNASTORE_EXTENSIONS directly from discovered classes
    for name, cls in classes.items():
        _DYNASTORE_EXTENSIONS[name] = ExtensionConfig(cls=cls)
 
    logger.info(f"--- DISCOVERED EXTENSIONS: {list(_DYNASTORE_EXTENSIONS.keys())} ---")

def instantiate_extensions(app: object, include_only: Optional[List[str]] = None):
    """
    Instantiates all discovered extensions.
    """
    extensions_to_load = _DYNASTORE_EXTENSIONS.keys()

    # If filtering is requested (e.g. for tests), apply it
    if include_only is not None:
        target_names = {name.lower().replace("_", "-") for name in include_only}
        extensions_to_load = [
            name for name in extensions_to_load 
            if name.lower().replace("_", "-") in target_names
        ]
    
    # Sort extensions by priority if defined
    def get_priority(name):
        config = _DYNASTORE_EXTENSIONS.get(name)
        return getattr(config.cls, "priority", 0) if config else 0
    
    extensions_to_load = sorted(extensions_to_load, key=get_priority)

    logger.info(f"Attempting to instantiate enabled extension modules: {list(extensions_to_load)}")
    ordered_configs = []
    for extension_name in extensions_to_load:
        config = _DYNASTORE_EXTENSIONS.get(extension_name)
        if not config:
            logger.warning(f"Extension '{extension_name}' is enabled but not discovered. Skipping instantiation.")
            continue
            
        cls = config.cls
        load_component_dotenv(cls)
        try:
            import inspect
            sig = inspect.signature(cls)
            if "app" in sig.parameters:
                instance = cls(app=app)
            elif "app_state" in sig.parameters:
                instance = cls(app_state=getattr(app, 'state', app))
            else:
                instance = cls()
            
            config.instance = instance
            
            # Register in central registry
            from dynastore.tools.discovery import register_plugin
            register_plugin(instance)
            
            logger.info(f"Instantiated Extension: '{extension_name}' ({cls.__name__})")
            if config.instance:
                ordered_configs.append(config)
        except Exception as e:
            logger.error(f"Failed to instantiate extension '{extension_name}': {e}", exc_info=True)
            config.instance = None

    # Attach ordered configs to app state
    if hasattr(app, "state"):
        app.state.ordered_configs = ordered_configs
        logger.info(f"Attached {len(ordered_configs)} ordered extension configs to app.state")

def apply_app_configurations(app: object):
    """
    Applies configurations to the FastAPI app for all successfully instantiated extensions.
    """
    for extension_name, config in _DYNASTORE_EXTENSIONS.items():
        if config.instance:
            try:
                config.instance.configure_app(app)
            except Exception:
                logger.error(f"Failed to configure app for extension '{extension_name}'", exc_info=True)
