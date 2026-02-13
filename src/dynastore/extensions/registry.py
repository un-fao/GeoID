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

import importlib
import inspect
import logging
import os
import pkgutil
from dataclasses import dataclass
from typing import Dict, List, Tuple, Type, TypeVar, cast, Optional

from dotenv import load_dotenv
from fastapi import FastAPI

from .protocols import ExtensionProtocol

logger = logging.getLogger(__name__)

# --- Central Extension Registry ---
_DYNASTORE_EXTENSIONS: Dict[str, "ExtensionConfig"] = {}

T_Extension = TypeVar("T_Extension", bound=ExtensionProtocol)

@dataclass
class ExtensionConfig:
    """A data structure to hold the configuration for a discovered extension."""
    cls: Type[ExtensionProtocol]
    instance: ExtensionProtocol | None = None

def dynastore_extension(cls: Type[T_Extension]) -> Type[T_Extension]:
    """
    A decorator to register a class as a DynaStore Web Extension.
    """
    try:
        # Robustly determine the module name by finding the segment after 'extensions'
        parts = cls.__module__.split('.')
        idx = parts.index('extensions')
        registration_name = parts[idx + 1]
    except (ValueError, IndexError):
        logger.error(f"Could not determine module folder name for extension '{cls.__name__}' from '{cls.__module__}'. Falling back to class name.")
        registration_name = cls.__name__

    if registration_name in _DYNASTORE_EXTENSIONS:
        logger.warning(f"Web extension '{registration_name}' is already registered. Overwriting.")
    
    _DYNASTORE_EXTENSIONS[registration_name] = ExtensionConfig(cls=cls)
    
    cls._registered_name = registration_name
    logger.info(f"Discovered web extension class: {cls.__name__} (registered as '{registration_name}')")
    return cls

def get_extension_instance(name: str) -> ExtensionProtocol | None:
    config = _DYNASTORE_EXTENSIONS.get(name)
    return config.instance if config else None

def get_extension_instance_by_class(cls: Type[T_Extension]) -> T_Extension | None:
    for config in _DYNASTORE_EXTENSIONS.values():
        if config.instance and isinstance(config.instance, cls):
            return cast(T_Extension, config.instance)
    return None


def discover_extensions(enabled_extensions: Optional[List[str]] = None, enabled_modules: Optional[List[str]] = None):
    # Ensure foundational modules are discovered/loaded first
    # This is critical for extensions that import modules at the top level
    from dynastore import modules
    modules.discover_modules(enabled_modules)

    package_path = os.path.dirname(__file__)
    package_name = __name__.rsplit('.', 1)[0] # Handle if this is a subpackage
    logger.info(f"Discovering web extensions in '{package_path}'...")
    
    if enabled_extensions is None:
        enabled_modules_str = os.getenv("DYNASTORE_EXTENSION_MODULES", "")
        if enabled_modules_str.strip() == "*":
            enabled_modules = [
                item for item in os.listdir(package_path)
                if os.path.isdir(os.path.join(package_path, item)) and not item.startswith('__')
            ]
            logger.info(f"Wildcard '*' detected for DYNASTORE_EXTENSION_MODULES. Discovered: {enabled_modules}")
        else:
            enabled_modules = [m.strip() for m in enabled_modules_str.split(',') if m.strip()]
    else:
        enabled_modules = enabled_extensions

    if not enabled_modules:
        logger.warning("DYNASTORE_EXTENSION_MODULES is not set. No extensions will be discovered.")
        return

    logger.info(f"Attempting to load enabled extension modules: {enabled_modules}")
    
    base_dotenv_path = os.path.join(package_path, '.env')
    if os.path.exists(base_dotenv_path):
        load_dotenv(dotenv_path=base_dotenv_path, override=True)

    for module_name in enabled_modules:
        module_path = os.path.join(package_path, module_name)
        if not os.path.isdir(module_path):
            logger.warning(f"Enabled module '{module_name}' not found at path '{module_path}'. Skipping.")
            continue
            
        try:
            module_dotenv_path = os.path.join(module_path, '.env')
            if os.path.exists(module_dotenv_path):
                load_dotenv(dotenv_path=module_dotenv_path, override=True)

            # 4. Filter by requirements.txt if present
            from dynastore.tools.dependencies import check_requirements
            requirements_path = os.path.join(module_path, 'requirements.txt')
            if not check_requirements(requirements_path):
                logger.warning(f"Skipping extension '{module_name}' due to unsatisfied requirements in '{requirements_path}'.")
                continue

            sub_package_name = f"{package_name}.{module_name}"
            # Because discover_extensions is usually called from __init__, package_name might be 'dynastore.extensions'
            # We need to import the module 'dynastore.extensions.{module_name}'
            
            # Simple recursive importer for the extension folder
            for _, sub_module_name, _ in pkgutil.iter_modules([module_path]):
                full_module_path = f"{sub_package_name}.{sub_module_name}"
                try:
                    try:
                        importlib.import_module(full_module_path)
                    except ImportError:
                        # Fallback for when package_name calculation is tricky based on invocation
                        importlib.import_module(f"dynastore.extensions.{module_name}.{sub_module_name}")
                except Exception:
                     logger.error(f"Failed to import extension submodule '{sub_module_name}' from '{module_name}'", exc_info=True)

        except Exception:
            logger.error(f"Failed to load extension module '{module_name}'", exc_info=True)

def get_ordered_configs(enabled_extensions: Optional[List[str]] = None) -> Tuple[List[ExtensionConfig], List[str]]:
    if enabled_extensions is None:
        enabled_modules_str = os.getenv("DYNASTORE_EXTENSION_MODULES", "")
        if enabled_modules_str.strip() == "*":
            # Note: We still need to order them logically. For now, we use the registered order.
            def _get_module(cfg):
                parts = cfg.cls.__module__.split('.')
                try:
                    idx = parts.index('extensions')
                    return parts[idx + 1]
                except (ValueError, IndexError):
                    return parts[2] if len(parts) > 2 else 'unknown'

            ordered_modules = list(set(_get_module(config) for config in _DYNASTORE_EXTENSIONS.values()))
        else:
            ordered_modules = [m.strip() for m in enabled_modules_str.split(',') if m.strip()]
    else:
        ordered_modules = enabled_extensions
    
    module_map: Dict[str, List[ExtensionConfig]] = {}
    for config in _DYNASTORE_EXTENSIONS.values():
        parts = config.cls.__module__.split('.')
        try:
            idx = parts.index('extensions')
            module_name = parts[idx + 1]
        except (ValueError, IndexError):
            module_name = parts[2] if len(parts) > 2 else 'unknown'
        module_map.setdefault(module_name, []).append(config)

    ordered_configs: List[ExtensionConfig] = []
    for module_name in ordered_modules:
        if module_name in module_map:
            ordered_configs.extend(module_map[module_name])

    return ordered_configs, ordered_modules

def instantiate_extensions(app: FastAPI, enabled_extensions: Optional[List[str]] = None):
    configs, ordered_modules = get_ordered_configs(enabled_extensions)
    app.state.ordered_configs = configs
    app.state.ordered_modules = ordered_modules

    logger.info("--- Instantiating all web extensions ---")
    for config in configs:
        if config.instance: continue
        name, cls = config.cls.__name__, config.cls
        try:
            sig = inspect.signature(cls.__init__)
            config.instance = cls(app=app) if 'app' in sig.parameters else cls()
            logger.info(f"Singleton for extension '{name}' created successfully.")
        except Exception as e:
            logger.error(f"CRITICAL: Failed during __init__ of extension '{name}': {str(e)}. It will be unavailable.", exc_info=True)
            config.instance = None