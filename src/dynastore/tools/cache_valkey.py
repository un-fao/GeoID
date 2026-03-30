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

"""
Valkey (Redis-compatible) async cache backend for cross-instance consistency.

Implements ``CacheBackend`` + ``LockableCacheBackend`` protocols.
Serialization uses msgpack with ExtType handlers for Pydantic models,
datetime, Enum, and UUID.

Registered by ``CacheModule`` when ``VALKEY_URL`` is set.
Falls back to ``LocalAsyncCacheBackend`` (priority=1000) when unavailable.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

import msgpack

from dynastore.models.protocols.cache import CacheStats

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Valkey INFO section → field mapping (used by ValkeyCacheBackend.info())
# ---------------------------------------------------------------------------
# Maps flat INFO field names to their logical section so callers can use
# info["server"]["redis_version"], info["memory"]["used_memory_human"], etc.
_INFO_FIELD_SECTION: dict = {
    # server section
    "redis_version": "server", "redis_git_sha1": "server", "redis_git_dirty": "server",
    "redis_build_id": "server", "redis_mode": "server", "os": "server",
    "arch_bits": "server", "monotonic_clock": "server", "multiplexing_api": "server",
    "atomicvar_api": "server", "gcc_version": "server", "process_id": "server",
    "server_time_usec": "server", "uptime_in_seconds": "server",
    "uptime_in_days": "server", "hz": "server", "configured_hz": "server",
    "aof_rewrites": "server", "executable": "server", "config_file": "server",
    # clients section
    "connected_clients": "clients", "cluster_connections": "clients",
    "maxclients": "clients", "client_recent_max_input_buffer": "clients",
    "client_recent_max_output_buffer": "clients",
    # memory section
    "used_memory": "memory", "used_memory_human": "memory",
    "used_memory_rss": "memory", "used_memory_rss_human": "memory",
    "used_memory_peak": "memory", "used_memory_peak_human": "memory",
    "used_memory_peak_perc": "memory", "used_memory_overhead": "memory",
    "used_memory_startup": "memory", "used_memory_dataset": "memory",
    "used_memory_dataset_perc": "memory", "allocator_allocated": "memory",
    "allocator_active": "memory", "allocator_resident": "memory",
    "total_system_memory": "memory", "total_system_memory_human": "memory",
    "used_memory_lua": "memory", "used_memory_vm_eval": "memory",
    "used_memory_lua_human": "memory", "used_memory_scripts_eval": "memory",
    "number_of_cached_scripts": "memory", "number_of_functions": "memory",
    "number_of_libraries": "memory", "used_memory_vm_functions": "memory",
    "used_memory_vm_total": "memory", "used_memory_vm_total_human": "memory",
    "used_memory_functions": "memory", "used_memory_scripts": "memory",
    "used_memory_scripts_human": "memory", "maxmemory": "memory",
    "maxmemory_human": "memory", "maxmemory_policy": "memory",
    # stats section
    "total_connections_received": "stats", "total_commands_processed": "stats",
    "instantaneous_ops_per_sec": "stats", "total_net_input_bytes": "stats",
    "total_net_output_bytes": "stats", "total_net_repl_input_bytes": "stats",
    "total_net_repl_output_bytes": "stats", "rejected_connections": "stats",
    "expired_keys": "stats", "evicted_keys": "stats", "keyspace_hits": "stats",
    "keyspace_misses": "stats",
    # replication section
    "role": "replication", "connected_slaves": "replication",
    "master_failover_state": "replication", "master_replid": "replication",
    "master_repl_offset": "replication", "repl_backlog_active": "replication",
    "repl_backlog_size": "replication",
}

# ---------------------------------------------------------------------------
#  MsgPack ExtType codes for non-primitive types
# ---------------------------------------------------------------------------
_EXT_DATETIME = 1
_EXT_ENUM = 2
_EXT_UUID = 3
_EXT_PYDANTIC = 10


def _msgpack_default(obj: Any) -> msgpack.ExtType:
    """Custom msgpack serializer for non-primitive types."""
    if isinstance(obj, datetime):
        return msgpack.ExtType(_EXT_DATETIME, obj.isoformat().encode("utf-8"))
    if isinstance(obj, UUID):
        return msgpack.ExtType(_EXT_UUID, obj.hex.encode("utf-8"))
    if isinstance(obj, Enum):
        return msgpack.ExtType(
            _EXT_ENUM,
            msgpack.packb(obj.value, use_bin_type=True),
        )
    if hasattr(obj, "model_dump_json"):
        # Pydantic model: store fully-qualified class name + JSON dump
        cls = type(obj)
        fqn = f"{cls.__module__}.{cls.__qualname__}".encode("utf-8")
        json_bytes = obj.model_dump_json().encode("utf-8")
        return msgpack.ExtType(_EXT_PYDANTIC, fqn + b"\x00" + json_bytes)
    raise TypeError(f"Object of type {type(obj).__name__} is not msgpack-serializable")


def _msgpack_ext_hook(code: int, data: bytes) -> Any:
    """Custom msgpack deserializer for ExtType values."""
    if code == _EXT_DATETIME:
        return datetime.fromisoformat(data.decode("utf-8"))
    if code == _EXT_UUID:
        return UUID(data.decode("utf-8"))
    if code == _EXT_ENUM:
        return msgpack.unpackb(data, raw=False)
    if code == _EXT_PYDANTIC:
        sep = data.index(b"\x00")
        fqn = data[:sep].decode("utf-8")
        json_bytes = data[sep + 1:]
        module_path, _, class_name = fqn.rpartition(".")
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        return cls.model_validate_json(json_bytes)
    return msgpack.ExtType(code, data)


def _serialize(value: Any) -> bytes:
    """Serialize a value to msgpack bytes."""
    return msgpack.packb(value, default=_msgpack_default, use_bin_type=True)


def _deserialize(data: bytes) -> Any:
    """Deserialize msgpack bytes to a value."""
    return msgpack.unpackb(data, ext_hook=_msgpack_ext_hook, raw=False)


# ---------------------------------------------------------------------------
#  ValkeyCacheBackend
# ---------------------------------------------------------------------------


class ValkeyCacheBackend:
    """Shared Valkey cache backend for cross-instance consistency.

    - ``priority = 100`` — wins over ``LocalAsyncCacheBackend`` (1000)
    - Key prefix ``ds:`` isolates Dynastore in shared Valkey instances
    - Implements ``CacheBackend`` + ``LockableCacheBackend`` protocols
    """

    def __init__(self, url: str, key_prefix: str = "ds:") -> None:
        import valkey.asyncio as avalkey

        self._pool = avalkey.ConnectionPool.from_url(url, decode_responses=False)
        self._client = avalkey.Valkey(connection_pool=self._pool)
        self._prefix = key_prefix
        self._stats = CacheStats(maxsize=0)
        self._locks: Dict[str, asyncio.Lock] = {}

    @property
    def name(self) -> str:
        return "valkey"

    @property
    def priority(self) -> int:
        return 100

    def _key(self, key: str) -> str:
        """Prefix a cache key for Valkey namespace isolation."""
        return f"{self._prefix}{key}"

    async def get(self, key: str) -> Optional[bytes]:
        raw = await self._client.get(self._key(key))
        if raw is None:
            return None
        return _deserialize(raw)

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        serialized = _serialize(value)
        kwargs: Dict[str, Any] = {}
        if exist is True:
            kwargs["xx"] = True
        if exist is False:
            kwargs["nx"] = True
        if ttl is not None:
            # Use millisecond precision for sub-second TTLs
            kwargs["px"] = int(ttl * 1000)
        result = await self._client.set(self._key(key), serialized, **kwargs)
        return bool(result)

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        if key is not None:
            return bool(await self._client.unlink(self._key(key)))
        if namespace is not None:
            # SCAN + UNLINK by prefix pattern
            # Cache keys use "|" as separator (from _make_cache_key)
            pattern = f"{self._prefix}{namespace}|*"
            count = 0
            cursor: int = 0
            while True:
                cursor, keys = await self._client.scan(
                    cursor, match=pattern, count=200
                )
                if keys:
                    await self._client.unlink(*keys)
                    count += len(keys)
                if cursor == 0:
                    break
            return count > 0
        if tags is not None:
            return False  # Tag-based invalidation not supported
        return False

    async def exists(self, key: str) -> bool:
        return bool(await self._client.exists(self._key(key)))

    async def get_lock(self, key: str) -> asyncio.Lock:
        """Per-process asyncio lock for stampede protection.

        Not a distributed lock — sufficient for single-instance stampede
        prevention. Cross-instance stampede is acceptable (rare, bounded).
        """
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def ping(self) -> bool:
        """Health check — verify Valkey connectivity."""
        return bool(await self._client.ping())

    async def info(self) -> dict:
        """Return server info sections (server, memory, stats, replication).

        The valkey INFO command returns a flat string; the client parses it
        into a dict keyed by section name, each value being a dict of fields.
        """
        raw = await self._client.info("all")
        # valkey.asyncio returns a flat dict of all fields; group into sections
        # by matching known prefixes so callers can use info["server"]["redis_version"]
        sections: dict = {}
        for field, value in raw.items():
            # Fields like "redis_version", "used_memory_human", etc. are flat
            # in the raw dict — group them by their logical INFO section
            section = _INFO_FIELD_SECTION.get(field, "misc")
            sections.setdefault(section, {})[field] = value
        return sections

    async def close(self) -> None:
        """Shut down connection pool cleanly."""
        await self._client.aclose()
        await self._pool.aclose()
