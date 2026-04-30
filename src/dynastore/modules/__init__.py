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
from typing import Any, Dict, Type, TypeVar, List, Optional
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
    cls._registered_name = registration_name  # type: ignore[attr-defined]
    logger.info(f"Registered module: {cls.__name__} (as '{registration_name}')")
    return cls


def discover_modules():
    """Discover every ``dynastore.modules`` entry-point from installed packages.

    Identity is package metadata.  Entry-points whose module imports fail
    (because their optional deps weren't selected at ``pip install`` time)
    are gracefully skipped by :func:`discover_and_load_plugins`.

    Idempotent: if the same entry-point class was already discovered, the
    existing :class:`ModuleConfig` (and any attached ``instance``) is
    preserved.  This matters because multiple bootstrap paths call us
    (``main.py``, ``extensions/bootstrap.py``, ``tasks/bootstrap.py``); a
    naïve re-assignment would reset ``instance = None`` on every re-entry,
    making ``_DYNASTORE_MODULES[name].instance`` fall out of sync with
    ``_DYNASTORE_PLUGINS`` and breaking the ``get_protocol`` fallback path.
    """
    logger.info("--- [modules] Discovering components via entry points... ---")
    from dynastore.tools.discovery import discover_and_load_plugins

    for name, cls in discover_and_load_plugins("dynastore.modules").items():
        existing = _DYNASTORE_MODULES.get(name)
        if existing is not None and existing.cls is cls:
            # Same class already registered — keep the config (and its
            # instance, if any).  Avoids wiping the live instance on
            # repeated discover_modules() invocations from different
            # bootstrap callers.
            continue
        if existing is not None and existing.instance is not None:
            logger.warning(
                "Module '%s' discovered with a different class (%s → %s); "
                "discarding the live instance. This is almost certainly a "
                "packaging / entry-point duplication bug.",
                name, existing.cls.__name__, cls.__name__,
            )
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
    init_failures: List[str] = []
    for module_name in ordered_modules:
        config = _DYNASTORE_MODULES.get(module_name)
        if not config:
            continue

        # Idempotent: if this module was already instantiated in an earlier
        # bootstrap pass (e.g. main.py ran, then extensions/bootstrap.py
        # re-entered), reuse the live instance.  Rebuilding would create a
        # second GCPModule (etc.) whose __init__ might reinitialize sync
        # clients on the wrong event loop, and leave the registered-plugin
        # registry containing a stale reference.
        if config.instance is not None:
            logger.info(
                f"Module '{module_name}' already instantiated ({type(config.instance).__name__}) — reusing."
            )
            continue

        cls = config.cls
        load_component_dotenv(cls)
        try:
            sig = inspect.signature(cls)
            instance = cls(app_state=app_state) if 'app_state' in sig.parameters else cls()  # type: ignore[call-arg]
            config.instance = instance

            # Register in the central protocol discovery registry
            from dynastore.tools.discovery import register_plugin
            register_plugin(instance)

            logger.info(f"Instantiated module '{module_name}' ({cls.__name__})")
        except Exception as e:
            # Louder than the original error line: modules that fail here are
            # invisible at runtime (get_protocol returns None for every
            # protocol they were supposed to provide).  Track them so the
            # post-discovery audit below can summarise in one line — useful
            # on Cloud Run where a single log line is enough to filter on.
            init_failures.append(f"{module_name}: {type(e).__name__}: {e}")
            logger.error(
                f"CRITICAL: Failed during __init__ of module '{module_name}'. "
                f"It will be unavailable. Downstream code that calls "
                f"get_protocol(<ProtocolName>) will silently return None until "
                f"this is fixed.",
                exc_info=True,
            )
            config.instance = None

    # Post-sync-init audit — reports which modules survived __init__.
    # Runtime-protocol resolution is audited separately at the end of
    # ``lifespan()`` startup, because providers like ``CatalogService`` /
    # ``PostgresProxyStorage`` register inside their module's async
    # lifespan, not during sync ``__init__`` — probing them here is a
    # timing false-positive.
    try:
        live_modules = sorted(
            name for name, cfg in _DYNASTORE_MODULES.items() if cfg.instance
        )
        missing_modules = sorted(
            name for name in ordered_modules
            if _DYNASTORE_MODULES.get(name) and not _DYNASTORE_MODULES[name].instance
        )
        if init_failures:
            logger.critical(
                "Module __init__ left %d module(s) failed: %s — "
                "get_protocol() will return None for any protocol they were "
                "supposed to register. Check CRITICAL 'Failed during __init__' "
                "lines above.",
                len(init_failures), init_failures,
            )
        logger.info(
            "Module sync-init complete. live=%s missing=%s init_failures=%s",
            live_modules, missing_modules, init_failures,
        )
    except Exception as audit_err:  # pragma: no cover — diagnostic best-effort
        logger.error("Module sync-init audit failed: %s", audit_err)


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
                    # Wrong-SCOPE soft-skip: ModuleNotFoundError at lifespan
                    # entry means the deployment didn't pip-install the
                    # extras the module's runtime needs (e.g. `db` requires
                    # asyncpg, which is excluded by sync-only worker SCOPEs
                    # like worker_task_ingestion).  Treat the same as the
                    # __init__-time ImportError gate above: warn and skip,
                    # even for "foundational" priority<20 modules.  The
                    # protocol the missing module would have provided
                    # (e.g. DatabaseProtocol from DBService) is expected to
                    # come from a sibling registered for this SCOPE
                    # (e.g. DatastoreModule providing the sync engine).
                    if isinstance(e, ModuleNotFoundError):
                        logger.warning(
                            "Skipping lifespan for module '%s' — required "
                            "runtime dep missing (%s).  This is expected "
                            "when SCOPE excludes the module's extras; "
                            "downstream protocol consumers must come from "
                            "another registered module.",
                            config.cls.__name__, e,
                        )
                        continue
                    logger.error(f"Failed to enter lifespan for module '{config.cls.__name__}'", exc_info=True)
                    # Low-priority modules (< 20) are foundational — abort hard on failure
                    if getattr(config.cls, "priority", 100) < 20:
                        raise RuntimeError(f"CRITICAL: Foundational module '{config.cls.__name__}' failed during startup. Aborting.") from e

        # Runtime-protocol audit — runs after every module's async lifespan
        # has entered (so every register_plugin(svc) call has fired). The
        # probed set is the protocols downstream code is known to resolve
        # without a DB round-trip; missing here = real failure, not async-
        # init timing.
        try:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols import (
                StorageProtocol, DatabaseProtocol, CatalogsProtocol, TasksProtocol,
            )
            probed = {
                "Storage":  type(get_protocol(StorageProtocol)).__name__   if get_protocol(StorageProtocol)  else None,
                "Database": type(get_protocol(DatabaseProtocol)).__name__  if get_protocol(DatabaseProtocol) else None,
                "Catalogs": type(get_protocol(CatalogsProtocol)).__name__  if get_protocol(CatalogsProtocol) else None,
                "Tasks":    type(get_protocol(TasksProtocol)).__name__     if get_protocol(TasksProtocol)    else None,
            }
            if any(v is None for v in probed.values()):
                logger.critical(
                    "Lifespan startup left core protocols UNRESOLVED: %s — "
                    "tasks depending on these will fail at runtime. "
                    "protocol_resolvers=%s",
                    [k for k, v in probed.items() if v is None], probed,
                )
            else:
                logger.info("Runtime protocols resolved: %s", probed)
        except Exception as audit_err:  # pragma: no cover — diagnostic best-effort
            logger.error("Runtime-protocol audit failed: %s", audit_err)

        yield

        # Wait for all background tasks before shutting down modules
        # This prevents closing GCP clients while tasks are still running.
        try:
            from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
            await lifecycle_registry.wait_for_all_tasks()
        except ImportError:
            pass
    
    logger.info("Application shutting down. Exiting module lifespans in reverse order.")
