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

# dynastore/tasks/__init__.py

import logging
import pkgutil
import importlib
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Dict, Any, Type, List, Generic, Optional
from typing import TypeVar

from .protocols import TaskProtocol, ProcessTaskProtocol
from dynastore.modules.processes.models import Process
from dynastore import modules

logger = logging.getLogger(__name__)

DefinitionType = TypeVar('DefinitionType')
PayloadType = TypeVar('PayloadType')
ReturnType = TypeVar('ReturnType')

@dataclass
class TaskConfig(Generic[DefinitionType, PayloadType, ReturnType]):
    cls: Type[TaskProtocol[DefinitionType, PayloadType, ReturnType]]
    module_name: str
    name: str
    process_definition: DefinitionType | None = None
    instance: TaskProtocol[DefinitionType, PayloadType, ReturnType] | None = None

_DYNASTORE_TASKS: Dict[str, TaskConfig] = {}

def dynastore_task(cls: Optional[Type[TaskProtocol]] = None, *, registration_name: Optional[str] = None):
    """
    A decorator to register a class as a DynaStore Background Task.
    It robustly determines the task's parent module for correct ordering.
    Can be used as @dynastore_task or @dynastore_task(registration_name='foo').
    """
    def _register(target_cls):
        # If a registration name is provided, always use it.
        # Otherwise, derive it from the class's module.
        reg_name = registration_name
        
        if reg_name is None:
            # Robustly determine the module name by finding the segment after 'tasks'
            parts = target_cls.__module__.split('.')
            try:
                idx = parts.index('tasks')
                reg_name = parts[idx + 1]
            except (ValueError, IndexError):
                logger.warning(f"Could not determine task module from '{target_cls.__module__}'. Falling back to class name.")
                reg_name = str(target_cls.__name__)

        try:
             # Always try to derive the actual module folder name for TaskConfig
             parts = target_cls.__module__.split('.')
             idx = parts.index('tasks')
             derived_module_name = parts[idx + 1]
        except (ValueError, IndexError):
             derived_module_name = reg_name
             
        module_name = derived_module_name
        
        # Check if the task exposes an OGC Process definition and cache it.
        process_def = None
        if hasattr(target_cls, 'get_process_definition'):
            process_def = target_cls.get_process_definition()
            logger.info(f"Task '{reg_name}' exposes an OGC Process definition.")

        # Check for existing placeholder and upgrade if necessary
        existing_config = _DYNASTORE_TASKS.get(reg_name)
        if existing_config:
            if getattr(existing_config.cls, 'is_placeholder', False):
                 logger.info(f"Upgrading placeholder task '{reg_name}' to full implementation.")
                 existing_config.cls = target_cls
                 existing_config.process_definition = process_def or existing_config.process_definition
                 existing_config.module_name = module_name # Update module name in case placeholder guess was wrong
                 _DYNASTORE_TASKS[reg_name] = existing_config
            elif getattr(target_cls, 'is_placeholder', False):
                 logger.info(f"Skipping registration of placeholder task '{reg_name}' because a full implementation already exists.")
                 return target_cls
            else:
                 logger.warning(f"Task '{reg_name}' is already registered as full implementation. Overwriting.")
                 _DYNASTORE_TASKS[reg_name] = TaskConfig(cls=target_cls, module_name=module_name, name=reg_name, process_definition=process_def)
        else:
            _DYNASTORE_TASKS[reg_name] = TaskConfig(cls=target_cls, module_name=module_name, name=reg_name, process_definition=process_def)
            
        logger.info(f"Discovered task class: {target_cls.__name__} (registered as '{reg_name}' in module '{module_name}')")
        
        target_cls._registered_name = reg_name
        return target_cls

    if cls is None:
        return _register
    else:
        return _register(cls)


def get_task_instance(name: str) -> TaskProtocol | None:
    config = _DYNASTORE_TASKS.get(name)
    return config.instance if config else None

def get_all_task_configs() -> Dict[str, TaskConfig]:
    """Returns the complete registry of discovered task configurations."""
    return _DYNASTORE_TASKS

T = TypeVar('T')

def get_definitions_by_type(def_type: Type[T]) -> List[T]:
    """
    Returns a list of all cached task definitions that are instances of a specific type.
    This is a type-safe way for consumers to get only the definitions they care about.
    """
    definitions: List[T] = []
    for config in _DYNASTORE_TASKS.values():
        if config.process_definition is not None and isinstance(config.process_definition, def_type):
            definitions.append(config.process_definition)
    return definitions

def discover_tasks(enabled_tasks: List[str] | None = None, enabled_modules: List[str] | None = None):
    """
    Discovers tasks by selectively importing sub-packages with a two-stage approach:
    1. Definition (Lightweight): Look for `definition.py` and register a placeholder logic.
    2. Implementation (Heavy): Look for the main task definition using pkgutil.
    
    This allows OGC Process definitions to be discovered even if heavy dependencies are missing.

    Args:
        enabled_tasks: List of tasks to enable.
        enabled_modules: List of foundational modules to enable (dependencies).
    """
    # Ensure foundational modules are discovered/loaded first
    modules.discover_modules(enabled_modules)

    package_path = os.path.dirname(__file__)
    package_name = __name__
    
    ordered_task_modules = []
    if enabled_tasks is not None:
        ordered_task_modules = [m.strip() for m in enabled_tasks if m.strip()]
        logger.info(f"Discovering specific tasks from provided list mapping: {ordered_task_modules}")
    else:
        enabled_modules_str = os.getenv("DYNASTORE_TASK_MODULES", "")
        if enabled_modules_str.strip() == "*":
            # Sort for predictable discovery if wildcard
            modules_found = sorted([
                item for item in os.listdir(package_path)
                if os.path.isdir(os.path.join(package_path, item)) and not item.startswith('__')
            ])
            ordered_task_modules = modules_found
            logger.info(f"Wildcard '*' detected for DYNASTORE_TASK_MODULES. Discovered (alphabetical): {ordered_task_modules}")
        else:
            ordered_task_modules = [m.strip() for m in enabled_modules_str.split(',') if m.strip()]
            logger.info(f"Discovering tasks from DYNASTORE_TASK_MODULES (in order): {ordered_task_modules}")

    if not ordered_task_modules:
        logger.warning("No task modules specified for discovery.")
        return

    base_dotenv_path = os.path.join(package_path, '.env')
    if os.path.exists(base_dotenv_path):
        load_dotenv(dotenv_path=base_dotenv_path, override=True)
        
    for module_name in ordered_task_modules:
        module_path = os.path.join(package_path, module_name)
        if not os.path.isdir(module_path):
            logger.warning(f"Enabled module '{module_name}' not found at path '{module_path}'. Skipping.")
            continue
        
        full_package_name = f"{package_name}.{module_name}"

        # --- STAGE 1: LIGHTWEIGHT DEFINITION DISCOVERY ---
        try:
            definition_module_path = f"{full_package_name}.definition"
            # Try to import just the definition module
            def_mod = importlib.import_module(definition_module_path)
            
            # Look for a Process object (by convention often *_PROCESS_DEFINITION or similar)
            found_definition = None
            for attr_name in dir(def_mod):
                attr = getattr(def_mod, attr_name)
                # We assume if it's an instance of Process, it's THE definition (or one of them)
                # This logic could be refined to look for specific variable names if needed.
                if isinstance(attr, Process):
                    found_definition = attr
                    break
            
            if found_definition:
                # Register Placeholder
                class DefinitionPlaceholderTask(ProcessTaskProtocol):
                    is_placeholder = True
                    @staticmethod
                    def get_process_definition():
                        return found_definition
                
                # Register immediately. If implementation loads later, it will upgrade this.
                if module_name not in _DYNASTORE_TASKS:
                    _DYNASTORE_TASKS[module_name] = TaskConfig(
                         cls=DefinitionPlaceholderTask,
                         module_name=module_name,
                         name=module_name,
                         process_definition=found_definition
                    )
                    logger.info(f"Stage 1: Registered definition placeholder for '{module_name}'.")

        except ImportError as e:
            # It's fine if definition.py doesn't exist. Not all tasks are OGC Processes.
            logger.debug(f"Stage 1: Could not definition for '{module_name}' (ImportError): {e}")
            pass
        except Exception as e:
            logger.warning(f"Stage 1: Failed to load definition for '{module_name}': {e}", exc_info=True)


        # --- STAGE 2: HEAVY IMPLEMENTATION DISCOVERY ---
        try:
            # First, try to import the package itself to trigger its __init__
            try:
                importlib.import_module(full_package_name)
                logger.debug(f"Stage 2: Successfully imported task package '{full_package_name}'.")
            except Exception as e:
                logger.debug(f"Stage 2: Task package '{full_package_name}' could not be imported directly: {e}")

            # Standard pkgutil walk to find the implementation which should have @dynastore_task
            found_submodules = []
            logger.info(f"Stage 2: Scanning module path '{module_path}' for submodules...")
            
            # Fallback check: if directory exists and iteration yields nothing, log it.
            submodules_iter = list(pkgutil.iter_modules([module_path]))
            if not submodules_iter:
                logger.info(f"Stage 2: No implementation submodules found via pkgutil in '{module_path}'. Files: {os.listdir(module_path)}")
                
            for _, sub_module_name, _ in submodules_iter:
                if sub_module_name == 'definition':
                    continue
                full_module_path = f"{full_package_name}.{sub_module_name}"
                logger.info(f"Stage 2: Importing implementation submodule '{full_module_path}'")
                importlib.import_module(full_module_path)
                found_submodules.append(sub_module_name)
            
            if found_submodules:
                logger.info(f"Stage 2: Discovered submodules for '{module_name}': {found_submodules}")
            else:
                logger.debug(f"Stage 2: No implementation submodules discovered via pkgutil for '{module_name}' at '{module_path}'.")
        except Exception as e:
            # If implementation fails, we check if we at least got the definition
            if module_name in _DYNASTORE_TASKS and getattr(_DYNASTORE_TASKS[module_name].cls, 'is_placeholder', False):
                 logger.warning(f"Stage 2: Failed to import implementation for '{module_name}' (missing dependencies?): {e}. "
                                "Task definition is available via placeholder.")
            else:
                 logger.warning(f"Failed to import from task module '{module_name}': {str(e)}.", exc_info=True)


@asynccontextmanager
async def manage_tasks(app_state: object, enabled_tasks: Optional[List[str]] = None):
    if enabled_tasks is None:
        enabled_modules_str = os.getenv("DYNASTORE_TASK_MODULES", "")
        if enabled_modules_str.strip() == "*":
            ordered_modules = list(set(config.module_name for config in _DYNASTORE_TASKS.values()))
        else:
            ordered_modules = [m.strip() for m in enabled_modules_str.split(',') if m.strip()]
    else:
        ordered_modules = enabled_tasks
    if not ordered_modules:
        logger.warning("No task modules enabled.")
        yield
        return

    module_map: Dict[str, List[TaskConfig]] = {}
    for config in _DYNASTORE_TASKS.values():
        module_map.setdefault(config.module_name, []).append(config)
    
    found_modules = set(module_map.keys())
    requested_modules = set(ordered_modules)
    missing_modules = requested_modules - found_modules
    if missing_modules:
        # Check if we should warn? Some requested modules might only be placeholders
        # which are in module_map. If they are missing from module_map entirely, that's a problem.
        logger.warning(f"Requested task modules not found or empty: {', '.join(missing_modules)}")

    configs: List[TaskConfig] = []
    for module_name in ordered_modules:
        if module_name in module_map:
            configs.extend(module_map[module_name])

    # --- PHASE 0: SETUP ALL RUNNERS ---
    logger.info("--- Phase 0: Setting up all registered task runners ---")
    from dynastore.modules.tasks.runners import get_all_runners_with_setup
    all_runners_to_setup = get_all_runners_with_setup()
    logger.info(f"Found {len(all_runners_to_setup)} runners with setup hooks.")
    for priority, runner_instance in all_runners_to_setup:
        try:
            await runner_instance.setup(app_state)
            logger.info(f"Runner '{runner_instance.__class__.__name__}' (priority {priority}) setup complete.")
        except Exception:
            logger.error(f"CRITICAL: Failed during setup of runner '{runner_instance.__class__.__name__}'.", exc_info=True)
    
    # RE-POPULATE: After Phase 0, we might have new dynamically registered tasks.
    # We MUST refresh our local copy of 'configs' to include them.
    # Rebuild module_map to include dynamically registered tasks
    module_map = {}
    for config in _DYNASTORE_TASKS.values():
        module_map.setdefault(config.module_name, []).append(config)
    
    configs = [] 
    for module_name in ordered_modules:
        if module_name in module_map:
            configs.extend(module_map[module_name])

    logger.info(f"Activating {len(configs)} tasks in order: {ordered_modules}")

    # --- PHASE 1: INSTANTIATE ALL SINGLETONS ---
    logger.info("--- Phase 1: Creating all task singletons ---")
    for config in configs:
        name, cls = config.cls.__name__, config.cls
        
        if getattr(cls, 'is_placeholder', False):
             logger.info(f"Skipping instantiation of placeholder task '{name}'. It provides definition only.")
             config.instance = None
             continue

        try:
            # Check if __init__ takes arguments (like app_state)
            if cls.__init__ is not object.__init__:
                import inspect
                sig = inspect.signature(cls.__init__)
                if 'app_state' in sig.parameters:
                    config.instance = cls(app_state=app_state)
                else:
                    config.instance = cls()
            else:
                config.instance = cls() # ALWAYS create an instance
            logger.info(f"Singleton for task '{name}' created successfully.")
        except Exception:
            logger.error(f"CRITICAL: Failed during __init__ of task '{name}'. It will be unavailable.", exc_info=True)
            config.instance = None
    
    # --- PHASE 2: EXECUTE ALL STARTUP HOOKS ---
    logger.info("--- Phase 2: Executing all task startup hooks ---")
    for config in configs:
        if config.instance is None:
            # Quietly skip if placeholder, else warn
            if not getattr(config.cls, 'is_placeholder', False):
                logger.warning(f"Skipping startup for '{config.cls.__name__}' because its instance is None.")
            continue
        
        name = config.cls.__name__
        if hasattr(config.instance, "startup"):
            try:
                await config.instance.startup()
                logger.info(f"Startup for task '{name}' completed successfully.")
            except Exception:
                logger.error(f"CRITICAL: Startup for task '{name}' failed.", exc_info=True)

    try:
        yield
    except Exception:
        logger.error("Error during tasks lifecycle management.", exc_info=True)
    finally:
        logger.info("Shutting down tasks...")
        for config in reversed(configs):
            if config.instance and hasattr(config.instance, "shutdown"):
                name = config.cls.__name__
                try:
                    await config.instance.shutdown()
                    logger.info(f"Task '{name}' shut down successfully.")
                except Exception:
                    logger.error(f"Task '{name}' failed during shutdown.", exc_info=True)
