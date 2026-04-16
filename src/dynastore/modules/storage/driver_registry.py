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

"""Process-wide driver registry (L0 cache).

``DriverRegistry`` is a module-level singleton that maps ``driver_class_name``
→ driver instance for both collection and asset drivers.  It is built once on
first access and never rebuilt at runtime (drivers are registered at startup
and are immutable thereafter).

Motivation: the hot resolution path in ``router.resolve_drivers`` previously
called ``get_protocols()`` and built a fresh ``{name: driver}`` dict on every
request.  While ``get_protocols`` itself is ``lru_cache``'d, allocating a new
dict per request adds unnecessary GC pressure at 1000 r/s.

Usage::

    from dynastore.modules.storage.driver_registry import DriverRegistry

    driver = DriverRegistry.get_collection("CollectionPostgresqlDriver")
    asset  = DriverRegistry.get_asset("AssetPostgresqlDriver")

Cache invalidation::

    DriverRegistry.clear()   # on (un)register_plugin events; forces rebuild
"""

import threading
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from dynastore.models.protocols.storage_driver import CollectionItemsStore
    from dynastore.models.protocols.asset_driver import AssetStore


class DriverRegistry:
    """Process-wide singleton mapping driver_class_name → driver instance."""

    _collection_index: Optional[Dict[str, "CollectionItemsStore"]] = None
    _asset_index: Optional[Dict[str, "AssetStore"]] = None
    _lock = threading.Lock()

    @classmethod
    def _build_collection(cls) -> "Dict[str, CollectionItemsStore]":
        from dynastore.models.protocols.storage_driver import CollectionItemsStore
        from dynastore.tools.discovery import get_protocols
        return {type(d).__name__: d for d in get_protocols(CollectionItemsStore)}

    @classmethod
    def _build_asset(cls) -> "Dict[str, AssetStore]":
        from dynastore.models.protocols.asset_driver import AssetStore
        from dynastore.tools.discovery import get_protocols
        return {type(d).__name__: d for d in get_protocols(AssetStore)}

    @classmethod
    def _ensure_collection(cls) -> "Dict[str, CollectionItemsStore]":
        if cls._collection_index is None:
            with cls._lock:
                if cls._collection_index is None:
                    cls._collection_index = cls._build_collection()
        return cls._collection_index  # type: ignore[return-value]

    @classmethod
    def _ensure_asset(cls) -> "Dict[str, AssetStore]":
        if cls._asset_index is None:
            with cls._lock:
                if cls._asset_index is None:
                    cls._asset_index = cls._build_asset()
        return cls._asset_index  # type: ignore[return-value]

    @classmethod
    def get_collection(
        cls, driver_id: str
    ) -> "Optional[CollectionItemsStore]":
        """Look up a collection driver by class name.  O(1)."""
        return cls._ensure_collection().get(driver_id)

    @classmethod
    def get_asset(cls, driver_id: str) -> "Optional[AssetStore]":
        """Look up an asset driver by class name.  O(1)."""
        return cls._ensure_asset().get(driver_id)

    @classmethod
    def collection_index(cls) -> "Dict[str, CollectionItemsStore]":
        """Full collection driver index (read-only view)."""
        return cls._ensure_collection()

    @classmethod
    def asset_index(cls) -> "Dict[str, AssetStore]":
        """Full asset driver index (read-only view)."""
        return cls._ensure_asset()

    @classmethod
    def clear(cls) -> None:
        """Force rebuild on next access.

        Call after ``(un)register_plugin`` events or ``router.clear_cache()``.
        """
        with cls._lock:
            cls._collection_index = None
            cls._asset_index = None
