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

from .protocols import TaskProtocol
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
    type: str = "task"
    definition: Any | None = None
    instance: TaskProtocol[DefinitionType, PayloadType, ReturnType] | None = None

_DYNASTORE_TASKS: Dict[str, TaskConfig] = {}

def _register_task(target_cls: Type[TaskProtocol], registration_name: Optional[str] = None, type: str = "task"):
    """Internal helper to register a task class."""
    reg_name = registration_name
    
    if reg_name is None:
        if hasattr(target_cls, 'task_type'):
            reg_name = getattr(target_cls, 'task_type')
        else:
            parts = target_cls.__module__.split('.')
            try:
                idx = parts.index('tasks')
                reg_name = parts[idx + 1]
            except (ValueError, IndexError):
                logger.warning(f"Could not determine task module from '{target_cls.__module__}'. Falling back to class name.")
                reg_name = str(target_cls.__name__)

    try:
         parts = target_cls.__module__.split('.')
         idx = parts.index('tasks')
         derived_module_name = parts[idx + 1]
    except (ValueError, IndexError):
         derived_module_name = reg_name
         
    module_name = derived_module_name
    
    task_def = None
    if hasattr(target_cls, 'get_definition'):
        try:
            task_def = target_cls.get_definition()
        except Exception as e:
            logger.debug(f"Could not extract task definition from {target_cls.__name__}: {e}")

    existing_config = _DYNASTORE_TASKS.get(reg_name)
    if existing_config:
        if existing_config.cls == target_cls:
            # Idempotent: already registered with this exact implementation
            return target_cls
            
        if getattr(existing_config.cls, 'is_placeholder', False):
             logger.info(f"Upgrading placeholder task '{reg_name}' to full implementation.")
             existing_config.cls = target_cls
             existing_config.definition = task_def or existing_config.definition
             existing_config.module_name = module_name
             _DYNASTORE_TASKS[reg_name] = existing_config
        elif getattr(target_cls, 'is_placeholder', False):
             return target_cls
        else:
             # Real implementation overwrite
             logger.info(f"Overwriting task '{reg_name}' with new implementation.")
             _DYNASTORE_TASKS[reg_name] = TaskConfig(cls=target_cls, module_name=module_name, name=reg_name, definition=task_def)
    else:
        _DYNASTORE_TASKS[reg_name] = TaskConfig(
            cls=target_cls,
            module_name=module_name,
            name=reg_name,
            type=type,
            definition=task_def,
            instance=None
        )
    logger.info(f"Discovered task class: {target_cls.__name__} (registered as '{reg_name}' in module '{module_name}')")
    target_cls._registered_name = reg_name
    return target_cls


def get_task_instance(name: str) -> TaskProtocol | None:
    config = _DYNASTORE_TASKS.get(name)
    return config.instance if config else None

def get_all_task_configs() -> Dict[str, TaskConfig]:
    """Returns the complete registry of discovered task configurations."""
    return _DYNASTORE_TASKS

def get_loaded_task_types() -> set:
    """Returns the set of task_type strings registered on this instance.
    
    Used by the dispatcher and wait_for_all_tasks to scope their behavior
    to only the task types this service instance can execute.
    In a distributed deployment (Cloud Run, Docker Compose), each service
    loads a different subset via DYNASTORE_TASK_MODULES.
    """
    return set(_DYNASTORE_TASKS.keys())

def hydrate_task_payload(task_instance: TaskProtocol, raw_payload_dict: Dict[str, Any]) -> Any:
    """
    Introspects a task's `run` method and hydrates a raw dictionary into
     the expected TaskPayload[T] model.
    """
    from dynastore.modules.tasks.models import TaskPayload
    from pydantic import BaseModel

    run_method = getattr(task_instance, 'run', None)
    if not run_method:
        return TaskPayload.model_validate(raw_payload_dict)

    # Find the 'payload' annotation
    import inspect
    sig = inspect.signature(run_method)
    payload_param = sig.parameters.get('payload')
    
    if not payload_param or payload_param.annotation is inspect.Parameter.empty:
        return TaskPayload.model_validate(raw_payload_dict)

    payload_model = payload_param.annotation
    
    # If it's a TaskPayload[SomeModel], Pydantic v2 can validate the whole thing.
    try:
        if hasattr(payload_model, "model_validate"):
            return payload_model.model_validate(raw_payload_dict)
    except Exception as e:
        logger.warning(f"Failed to hydrate payload for {task_instance.__class__.__name__}: {e}")
    
    # Fallback to generic TaskPayload
    return TaskPayload.model_validate(raw_payload_dict)

T = TypeVar('T')

def get_definitions_by_type(def_type: Type[T]) -> List[T]:
    """
    Returns a list of all cached task definitions that are instances of a specific type.
    This is a type-safe way for consumers to get only the definitions they care about.
    """
    definitions: List[T] = []
    for config in _DYNASTORE_TASKS.values():
        if config.definition is not None and isinstance(config.definition, def_type):
            definitions.append(config.definition)
    return definitions

def discover_tasks(enabled_tasks: List[str] | None = None, enabled_modules: List[str] | None = None):
    """
    Discovers tasks by selectively importing sub-packages based on DYNASTORE_TASK_MODULES.
    Only enabled modules are inspected or imported to avoid dependency issues.
    
    Args:
        enabled_tasks: List of tasks to enable. Overrides DYNASTORE_TASK_MODULES.
        enabled_modules: List of foundational modules to enable (dependencies).
    """
    # Ensure foundational modules are discovered/loaded first
    modules.discover_modules(enabled_modules)

    package_path = os.path.dirname(__file__)
    package_name = __name__
    
    # 1. Determine enabled modules
    enabled_task_modules = []
    if enabled_tasks is not None:
        enabled_task_modules = [m.strip() for m in enabled_tasks if m.strip()]
    else:
        enabled_modules_str = os.getenv("DYNASTORE_TASK_MODULES", "")
        if enabled_modules_str.strip() == "*":
            # Sort for predictable discovery if wildcard
            enabled_task_modules = sorted([
                item for item in os.listdir(package_path)
                if os.path.isdir(os.path.join(package_path, item)) and not item.startswith('__')
            ])
            logger.info(f"Wildcard '*' detected for DYNASTORE_TASK_MODULES. Discovered: {enabled_task_modules}")
        else:
            enabled_task_modules = [m.strip() for m in enabled_modules_str.split(',') if m.strip()]

    if not enabled_task_modules:
        logger.warning("No task modules specified in DYNASTORE_TASK_MODULES. No tasks will be discovered.")
        return

    logger.info(f"Discovering tasks for enabled modules: {enabled_task_modules}")

    base_dotenv_path = os.path.join(package_path, '.env')
    if os.path.exists(base_dotenv_path):
        load_dotenv(dotenv_path=base_dotenv_path, override=True)
        
    for module_name in enabled_task_modules:
        module_path = os.path.join(package_path, module_name)
        if not os.path.isdir(module_path):
            logger.warning(f"Enabled module '{module_name}' not found at path '{module_path}'. Skipping.")
            continue
        
        full_package_name = f"{package_name}.{module_name}"

        # --- STAGE 1: LIGHTWEIGHT DEFINITION DISCOVERY ---
        definition_module_name = "definition"
        definition_file = os.path.join(module_path, f"{definition_module_name}.py")
        if os.path.exists(definition_file):
            try:
                # Import the definition module
                def_mod = importlib.import_module(f"{full_package_name}.{definition_module_name}")
                
                # Look for a task definition constant
                task_def = None
                for attr_name in dir(def_mod):
                    if attr_name.endswith("_PROCESS_DEFINITION"):
                        task_def = getattr(def_mod, attr_name)
                        break
                
                if task_def and module_name not in _DYNASTORE_TASKS:
                    # Register a placeholder
                    class PlaceholderTask:
                        is_placeholder = True
                        @staticmethod
                        def get_definition():
                            return task_def
                    
                    PlaceholderTask.__name__ = f"{module_name.capitalize()}Placeholder"
                    
                    _DYNASTORE_TASKS[module_name] = TaskConfig(
                        cls=PlaceholderTask,
                        module_name=module_name,
                        name=module_name,
                        definition=task_def
                    )
                    logger.info(f"Stage 1: Registered placeholder for '{module_name}'")
            except Exception as e:
                logger.warning(f"Stage 1: Failed to import definition for '{module_name}': {e}")

        # --- STAGE 2: HEAVY IMPLEMENTATION DISCOVERY ---
        try:
            # First, try to import the package itself to trigger its __init__
            try:
                importlib.import_module(full_package_name)
                logger.debug(f"Stage 2: Successfully imported task package '{full_package_name}'.")
            except Exception as e:
                logger.debug(f"Stage 2: Task package '{full_package_name}' could not be imported directly: {e}")

            # Standard pkgutil walk to find submodules which might have task implementations
            found_submodules = []
            import inspect
            for _, sub_module_name, _ in pkgutil.iter_modules([module_path]):
                if sub_module_name == 'definition':
                    continue
                full_module_path = f"{full_package_name}.{sub_module_name}"
                logger.info(f"Stage 2: Importing implementation submodule '{full_module_path}'")
                try:
                    target_module = importlib.import_module(full_module_path)
                    found_submodules.append(sub_module_name)
                    
                    # Auto-discovery: scan for classes with 'run' method
                    for name, obj in inspect.getmembers(target_module, inspect.isclass):
                        if hasattr(obj, 'run') and obj.__module__ == full_module_path:
                            # If the class has an explicit task_type, use it. 
                            # Otherwise fall back to the module name if it's a single-task module.
                            if hasattr(obj, 'task_type'):
                                _register_task(obj)
                            else:
                                _register_task(obj, registration_name=module_name)
                except ModuleNotFoundError as e:
                     logger.warning(f"Skipping task submodule '{full_module_path}' due to missing dependency: {e}")
                except Exception:
                     logger.error(f"Failed to import/scan task submodule '{full_module_path}'", exc_info=True)
            
            if found_submodules:
                logger.info(f"Stage 2: Discovered submodules for '{module_name}': {found_submodules}")
        except Exception as e:
            # If implementation fails, we check if we at least got the definition
            if module_name in _DYNASTORE_TASKS and getattr(_DYNASTORE_TASKS[module_name].cls, 'is_placeholder', False):
                 logger.warning(f"Stage 2: Failed to import implementation for '{module_name}' (missing dependencies?): {e}. "
                                "Task definition is available via placeholder.")
            else:
                 logger.warning(f"Failed to import from task module '{module_name}': {str(e)}.", exc_info=True)


@asynccontextmanager
async def manage_tasks(app_state: object, enabled_tasks: Optional[List[str]] = None):
    from contextlib import AsyncExitStack
    from dynastore.modules.tasks.runners import register_default_runners, get_all_runners_with_setup
    
    # Ensure default runners are ALWAYS registered, even after test resets.
    register_default_runners()

    async with AsyncExitStack() as stack:
        # --- PHASE 0: SETUP ALL RUNNERS ---
        logger.info("--- Phase 0: Setting up all registered task runners ---")
        all_runners_to_setup = get_all_runners_with_setup()
        logger.info(f"Found {len(all_runners_to_setup)} runners with setup hooks.")
        # Runners are sorted by priority (lowest first) by get_all_runners_with_setup/discovery system
        for priority, runner_instance in all_runners_to_setup:
            try:
                from dynastore.tools.plugin import ProtocolPlugin
                # Runners are ProtocolPlugins too, we manage their lifespan here as well
                await stack.enter_async_context(runner_instance.lifespan(app_state))
                # Legacy setup hook if still present
                if hasattr(runner_instance, "setup") and getattr(runner_instance.setup, "__func__", runner_instance.setup) is not ProtocolPlugin.lifespan:
                    await runner_instance.setup(app_state)
                logger.info(f"Runner '{runner_instance.__class__.__name__}' (priority {priority}) activated.")
            except Exception:
                logger.error(f"CRITICAL: Failed during activation of runner '{runner_instance.__class__.__name__}'.", exc_info=True)

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
        
        configs: List[TaskConfig] = []
        for module_name in ordered_modules:
            if module_name in module_map:
                configs.extend(module_map[module_name])

        # RE-POPULATE: After Phase 0, we might have new dynamically registered tasks.
        module_map = {}
        for config in _DYNASTORE_TASKS.values():
            module_map.setdefault(config.module_name, []).append(config)
        
        configs = [] 
        for module_name in ordered_modules:
            if module_name in module_map:
                configs.extend(module_map[module_name])

        # --- PHASE 1: INSTANTIATE ALL SINGLETONS ---
        logger.warning(f"DEBUG: --- Phase 1: Creating all task singletons for modules: {ordered_modules} ---")
        for config in configs:
            name, cls = config.cls.__name__, config.cls
            logger.warning(f"DEBUG: Considering task class '{name}' (registered as '{config.name}')")
            if getattr(cls, 'is_placeholder', False):
                 logger.warning(f"DEBUG: Task '{name}' is a placeholder. Skipping instantiation.")
                 continue

            try:
                # Instantiate
                if cls.__init__ is not object.__init__:
                    import inspect
                    sig = inspect.signature(cls.__init__)
                    if 'app_state' in sig.parameters:
                        config.instance = cls(app_state=app_state)
                    else:
                        config.instance = cls()
                else:
                    config.instance = cls()
                logger.warning(f"DEBUG: Singleton for task '{name}' (registered as '{config.name}') created successfully.")
            except Exception as e:
                logger.error(f"CRITICAL: Failed during __init__ of task '{name}': {e}", exc_info=True)
                config.instance = None

        # --- PHASE 2: SORT AND ENTER LIFESPANS ---
        # Sort by priority (lowest number first)
        configs.sort(key=lambda c: getattr(c.instance, "priority", getattr(c.cls, "priority", 100)))
        
        logger.info(f"Activating {len([c for c in configs if c.instance])} tasks...")
        for config in configs:
            if config.instance is None:
                continue
            
            name = config.cls.__name__
            try:
                if hasattr(config.instance, "lifespan"):
                    await stack.enter_async_context(config.instance.lifespan(app_state))
                logger.info(f"Task '{name}' (priority {getattr(config.instance, 'priority', 100)}) activated.")
            except Exception:
                logger.error(f"CRITICAL: Failed to enter lifespan for task '{name}'.", exc_info=True)

        try:
            yield
        except Exception:
            logger.error("Error during tasks lifecycle management.", exc_info=True)
            raise
        finally:
            logger.info("Shutting down tasks and runners via AsyncExitStack...")
