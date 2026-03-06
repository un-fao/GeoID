#    Copyright 2026 FAO
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

"""
Protocol discovery utilities.

Design notes
------------
* ``get_protocols(proto)`` returns all registered instances that satisfy
  ``isinstance(instance, proto)``, sorted by ``priority`` in ascending order.
* **Category isolation is implicit**: because ``get_protocols`` filters by an
  explicit protocol type, different protocol families never interfere.
  For example, ``get_protocols(StorageProtocol)`` only compares Storage
  providers against each other; ``get_protocols(ExtensionProtocol)`` only
  compares Extensions against Extensions.
* ``register_plugin`` accepts *any* object — no ``ProtocolPlugin`` inheritance
  required.  This eliminates boilerplate for internal services that implement
  a feature protocol but do not need a managed lifecycle.
* Priorities are evaluated in ascending order (lowest number = highest
  precedence, returned first).  The default priority is 100.
"""

import logging
from functools import lru_cache
from typing import (
    Any,
    List,
    Optional,
    Type,
    TypeVar,
    cast,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global plugin registry
# ---------------------------------------------------------------------------
# Any object registered here is searchable via get_protocol() / get_protocols().
# Modules, Extensions, Tasks and their inner services all end up here.
_DYNASTORE_PLUGINS: List[Any] = []


def register_plugin(instance: Any) -> None:
    """
    Registers any object so it can be discovered via
    ``get_protocol()`` / ``get_protocols()``.

    Safe to call multiple times with the same instance — duplicates are ignored.
    No ``ProtocolPlugin`` inheritance is required; any Python object works.
    """
    if instance not in _DYNASTORE_PLUGINS:
        _DYNASTORE_PLUGINS.append(instance)
        # Clear lru_cache so next lookup sees the new plugin
        get_protocol.cache_clear()
        get_protocols.cache_clear()


def unregister_plugin(instance: Any) -> None:
    """
    Removes a previously registered plugin.

    After unregistering, the plugin will no longer be returned by
    ``get_protocol()`` / ``get_protocols()``.
    """
    if instance in _DYNASTORE_PLUGINS:
        _DYNASTORE_PLUGINS.remove(instance)
        get_protocol.cache_clear()
        get_protocols.cache_clear()


# ---------------------------------------------------------------------------
# Protocol discovery
# ---------------------------------------------------------------------------

@lru_cache(maxsize=128)
def get_protocol(protocol: Type[T]) -> Optional[T]:
    """
    Returns the highest-priority available instance that implements *protocol*
    (i.e. the first element after sorting by ascending ``priority``).

    Returns ``None`` when no suitable provider is found.
    """
    instances = get_protocols(protocol)
    return instances[0] if instances else None


@lru_cache(maxsize=128)
def get_protocols(protocol: Type[T]) -> List[T]:
    """
    Returns all available instances that implement *protocol*, sorted by
    ``priority`` in ascending order (lower number = higher precedence).

    Category isolation is implicit: only instances that pass
    ``isinstance(instance, protocol)`` are included, so different protocol
    families never interfere with each other's priorities.

    Only returns instances where ``is_available()`` returns ``True`` (when
    the method exists).
    """
    discovered: List[T] = []
    seen = set()

    def _accept(obj: Any) -> bool:
        obj_id = id(obj)
        if obj_id in seen:
            return False
        if not isinstance(obj, protocol):
            return False
        if hasattr(obj, "is_available") and not obj.is_available():
            return False
        seen.add(obj_id)
        return True

    # 1. Dynamically registered plugins (explicit registrations)
    for instance in _DYNASTORE_PLUGINS:
        if _accept(instance):
            discovered.append(cast(T, instance))

    # 2. Modules
    from dynastore.modules import _DYNASTORE_MODULES
    for config in _DYNASTORE_MODULES.values():
        if config.instance and _accept(config.instance):
            discovered.append(cast(T, config.instance))

    # 3. Extensions
    try:
        from dynastore.extensions.registry import _DYNASTORE_EXTENSIONS
        for config in _DYNASTORE_EXTENSIONS.values():
            if config.instance and _accept(config.instance):
                discovered.append(cast(T, config.instance))
    except (ImportError, ModuleNotFoundError):
        # Extensions are optional (e.g. not available in task-worker images)
        pass

    # 4. Tasks
    from dynastore.tasks import _DYNASTORE_TASKS
    for config in _DYNASTORE_TASKS.values():
        if config.instance and _accept(config.instance):
            discovered.append(cast(T, config.instance))

    # Sort by priority ascending (lower number = higher precedence).
    # Each call to get_protocols() is already scoped to a single protocol type,
    # so priorities from different protocol families never interfere.
    discovered.sort(key=lambda x: getattr(x, "priority", 100))

    return discovered
