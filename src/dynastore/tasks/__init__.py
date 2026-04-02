import logging
import inspect
import os
from dataclasses import dataclass
from contextlib import asynccontextmanager, AsyncExitStack
from typing import Dict, Type, Any, Optional, List, cast, AsyncGenerator

from .protocols import TaskProtocol
from dynastore.models.tasks import TaskPayload
from dynastore.tools.env import load_component_dotenv

logger = logging.getLogger(__name__)

@dataclass
class TaskConfig:
    cls: Type[TaskProtocol]
    module_name: str
    name: str
    type: str = "task"
    definition: Any = None
    instance: TaskProtocol | None = None

_DYNASTORE_TASKS: Dict[str, TaskConfig] = {}

def get_task_config(task_type: str) -> Optional[TaskConfig]:
    """Look up task config by registration name or task_type class attribute."""
    config = _DYNASTORE_TASKS.get(task_type)
    if config:
        return config
    for cfg in _DYNASTORE_TASKS.values():
        if getattr(cfg.cls, "task_type", None) == task_type:
            return cfg
    return None

def get_all_task_configs() -> Dict[str, TaskConfig]:
    return _DYNASTORE_TASKS

def get_loaded_task_types() -> List[str]:
    """Returns a list of all task_type values from discovered task classes.

    These correspond to the task_type attribute on each TaskProtocol class,
    which is the value stored in the DB 'task_type' column — NOT the entry-point
    registration name (which may differ).
    """
    types: List[str] = []
    for name, config in _DYNASTORE_TASKS.items():
        task_type = getattr(config.cls, "task_type", None)
        types.append(task_type if task_type else name)
    return types

def get_definitions_by_type(target_type: Any) -> List[Any]:
    """
    Returns a list of task definitions (e.g. Process objects) that match the target type.
    """
    results = []
    for config in _DYNASTORE_TASKS.values():
        if config.definition and isinstance(config.definition, target_type):
            results.append(config.definition)
    return results

def get_task_definitions() -> List[Any]:
    """Returns all task definitions."""
    return [config.definition for config in _DYNASTORE_TASKS.values() if config.definition]

def discover_tasks(include_only: Optional[List[str]] = None):
    """
    Discovers tasks using PEP-517 entry points defined in pyproject.toml
    under the "dynastore.tasks" group.
    """
    if include_only is None:
        scope = os.getenv("SCOPE")
        if scope:
            include_only = [s.strip() for s in scope.split(",")]

    logger.info("--- [tasks] Discovering components via entry points... ---")
    
    # Discovery returns uninstantiated classes
    from dynastore.tools.discovery import discover_and_load_plugins
    classes = discover_and_load_plugins("dynastore.tasks", include_only=include_only)
    
    for name, cls in classes.items():
        # Use the task_type class attribute as key when available so the
        # registry key matches the value stored in the DB task_type column.
        key = getattr(cls, "task_type", None) or name

        # Extract some metadata from the class if possible
        definition = None
        if hasattr(cls, "get_definition"):
            definition = cls.get_definition() # type: ignore
        elif hasattr(cls, "get_process_definition"):
            definition = cls.get_process_definition() # type: ignore

        _DYNASTORE_TASKS[key] = TaskConfig(
            cls=cls,
            type="task",
            module_name=cls.__module__,
            name=key,
            definition=definition
        )
 
    logger.info(f"--- DISCOVERED TASKS: {list(_DYNASTORE_TASKS.keys())} ---")


def _register_task(target_cls: Type[TaskProtocol], registration_name: Optional[str] = None, type: str = "task"):
    if registration_name is None:
        # Prefer the explicit task_type class attribute (matches what gets stored
        # in the DB) so that each task class gets a unique key even when multiple
        # task classes live in the same Python package.
        task_type_attr = getattr(target_cls, "task_type", None)
        if task_type_attr:
            registration_name = task_type_attr
        else:
            try:
                parts = target_cls.__module__.split('.')
                idx = parts.index('tasks')
                registration_name = parts[idx + 1]
            except (ValueError, IndexError):
                registration_name = target_cls.__name__

    if registration_name in _DYNASTORE_TASKS:
        if _DYNASTORE_TASKS[registration_name].cls == target_cls:
            return target_cls
        logger.warning(f"Task '{registration_name}' is already registered. Overwriting.")

    # Extract some metadata from the class if possible
    definition = None
    if hasattr(target_cls, "get_definition"):
        definition = target_cls.get_definition() # type: ignore
    elif hasattr(target_cls, "get_process_definition"):
        definition = target_cls.get_process_definition() # type: ignore

    _DYNASTORE_TASKS[registration_name] = TaskConfig(
        cls=target_cls, 
        type=type,
        module_name=target_cls.__module__,
        name=registration_name,
        definition=definition
    )
    target_cls._registered_name = registration_name
    logger.info(f"Registered task: {target_cls.__name__} (as '{registration_name}')")
    return target_cls

def get_task_instance(name: str) -> TaskProtocol | None:
    """Look up a task by registration name OR by its task_type class attribute."""
    config = _DYNASTORE_TASKS.get(name)
    if not config:
        # Fallback: search by task_type attribute (DB task_type may differ from
        # the entry-point registration name).
        for cfg in _DYNASTORE_TASKS.values():
            if getattr(cfg.cls, "task_type", None) == name:
                config = cfg
                break
    if not config:
        return None

    if config.instance:
        return config.instance

    load_component_dotenv(config.cls)
    try:
        return config.cls()
    except Exception as e:
        logger.error(f"Failed to instantiate task '{name}': {e}")
        return None

def _extract_inputs_type(payload_hint):
    """Extract the InputsType T from a TaskPayload[T] type annotation.

    ``typing.get_args`` returns ``()`` for Pydantic v2 concrete generics
    because Pydantic creates real subclasses instead of ``typing`` aliases.
    Fall back to ``__pydantic_generic_metadata__['args']`` in that case.
    """
    from typing import get_args
    args = get_args(payload_hint)
    if args:
        return args[0]
    # Pydantic v2 concrete generic: TaskPayload[T] is a ModelMetaclass subclass
    meta = getattr(payload_hint, '__pydantic_generic_metadata__', None)
    if meta:
        pydantic_args = meta.get('args', ())
        if pydantic_args:
            return pydantic_args[0]
    return None


def hydrate_task_payload(task_instance: TaskProtocol, raw_payload: Dict[str, Any]) -> TaskPayload:
    """
    Hydrates a raw payload dictionary into a strongly-typed TaskPayload instance,
    using the type hints from the task's run method to determine the expected input type.
    """
    import inspect
    from typing import get_type_hints
    from pydantic import BaseModel

    # 1. Determine the expected input type from the task's run method
    input_model = None
    try:
        # Get type hints for the 'run' method
        hints = get_type_hints(task_instance.run)
        payload_hint = hints.get('payload')
        if payload_hint:
            input_model = _extract_inputs_type(payload_hint)
    except Exception as e:
        logger.debug(f"Failed to extract input model for task {task_instance.__class__.__name__}: {e}")

    # 2. Extract raw inputs
    inputs = raw_payload.get("inputs", {})
    
    # 3. Hydrate inputs if they are expected to be a Pydantic model
    if input_model and inspect.isclass(input_model) and issubclass(input_model, BaseModel) and isinstance(inputs, dict):
        try:
            inputs = input_model(**inputs)
        except Exception as e:
            logger.warning(f"Failed to hydrate inputs to {input_model.__name__} for task {task_instance.__class__.__name__}: {e}")
            # Fall back to raw inputs

    # 4. Construct the TaskPayload
    return TaskPayload(
        task_id=raw_payload["task_id"],
        caller_id=raw_payload["caller_id"],
        inputs=inputs,
        asset=raw_payload.get("asset")
    )

@asynccontextmanager
async def manage_tasks(app_state: object, include_only: Optional[List[str]] = None) -> AsyncGenerator[None, None]:
    """
    Async context manager that owns the full lifecycle of all enabled tasks.
    It instantiates each task, enters its lifespan, and yields control.
    On exit, it ensures all lifespans are exited in reverse order.
    """
    # Filter tasks if requested (primarily for tests)
    tasks_to_manage = _DYNASTORE_TASKS.items()
    if include_only is not None:
        target_names = {name.lower().replace("_", "-") for name in include_only}
        tasks_to_manage = [
            (name, config) for name, config in tasks_to_manage 
            if name.lower().replace("_", "-") in target_names
        ]

    # Sort tasks by priority (highest first)
    ordered_tasks = sorted(
        tasks_to_manage,
        key=lambda item: getattr(item[1].cls, "priority", 0),
        reverse=True,
    )

    async with AsyncExitStack() as stack:
        for name, config in ordered_tasks:
            load_component_dotenv(config.cls)
            try:
                # Instantiate
                import inspect
                sig = inspect.signature(config.cls.__init__)
                if "app_state" in sig.parameters:
                    instance = config.cls(app_state=app_state)
                else:
                    instance = config.cls()
                
                config.instance = instance
                
                # Register the task plugin in the central discovery registry
                from dynastore.tools.discovery import register_plugin
                register_plugin(instance)
                
                # Enter lifespan if implemented
                if hasattr(instance, "lifespan"):
                    logger.debug(f"Entering lifespan for task: {name}")
                    await stack.enter_async_context(instance.lifespan(app_state))
                    logger.debug(f"Lifespan for task '{name}' entered successfully.")
                else:
                    logger.debug(f"Task '{name}' does not implement lifespan. Skipping.")
            except Exception as e:
                logger.error(f"Failed to initialize task '{name}': {e}", exc_info=True)
                # If a task fails to initialize, we continue with others unless it's critical?
                # For now, we just log the error.

        yield
