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

"""Valkey cache backend — TCP keepalives + fast-fail read timeout (#655).

valkey-py ships with ``socket_keepalive=False``. On Cloud Run that's a
trap: Cloud NAT silently drops the established-connection mapping after
long idle, so the next cache op hits a dead socket and ``ValkeyCluster``
pays a full re-initialisation. TCP keepalives prevent that dead-socket
condition — that's the real fix, mirroring the DB pool keepalive parity.

The read timeout (``socket_timeout``) is deliberately *short*. A cache
must fail fast: every op is wrapped so a timeout returns a miss and the
caller falls through to the source (sub-second), and three consecutive
failures trip the circuit breaker that drops Valkey from rotation. A
short timeout makes both happen quickly; a long one just convoys
requests behind a backend that's already unhealthy.
"""

from __future__ import annotations

import socket

import pytest

from dynastore.modules.cache.cache_config import CachePluginConfig
from dynastore.tools.cache_valkey import ValkeyCacheBackend


# --------------------------------------------------------------------------
# CachePluginConfig — new tunables
# --------------------------------------------------------------------------


def test_cache_config_exposes_socket_timeout_and_keepalive_defaults():
    cfg = CachePluginConfig()
    # Read timeout is short by design — a cache fails fast to the source
    # rather than blocking the hot path on an unhealthy backend. 2s is
    # ~hundreds of times a healthy same-VPC op: generous headroom, never a
    # multi-second stall.
    assert cfg.socket_timeout_seconds == 2.0
    # Idle window sits well under Cloud NAT's ~1200s established-conn timeout,
    # matching the DB pool keepalive parity (#655).
    assert cfg.tcp_keepalive_idle_seconds == 300
    assert cfg.tcp_keepalive_interval_seconds == 30
    assert cfg.tcp_keepalive_count == 5


# --------------------------------------------------------------------------
# ValkeyCacheBackend — kwargs reaching the connection pool
# --------------------------------------------------------------------------


def test_backend_passes_socket_timeout_and_keepalive_to_pool():
    backend = ValkeyCacheBackend(
        url="valkey://localhost:6379",
        socket_connect_timeout=10.0,
        socket_timeout=2.0,
        tcp_keepalive_idle=300,
        tcp_keepalive_interval=30,
        tcp_keepalive_count=5,
    )
    kwargs = backend._pool.connection_kwargs

    assert kwargs["socket_connect_timeout"] == 10.0
    assert kwargs["socket_timeout"] == 2.0
    assert kwargs["socket_keepalive"] is True

    # socket_keepalive_options uses platform socket constants; on Linux
    # (Cloud Run) all three must be populated.
    if hasattr(socket, "TCP_KEEPIDLE"):
        opts = kwargs["socket_keepalive_options"]
        assert opts[socket.TCP_KEEPIDLE] == 300
        assert opts[socket.TCP_KEEPINTVL] == 30
        assert opts[socket.TCP_KEEPCNT] == 5


def test_backend_omits_keepalive_when_no_tunables_supplied():
    # Backwards-compatible: a backend built without keepalive args must not
    # force socket_keepalive on (preserves the old behaviour for callers
    # that haven't been updated).
    backend = ValkeyCacheBackend(url="valkey://localhost:6379")
    kwargs = backend._pool.connection_kwargs
    assert "socket_keepalive" not in kwargs
    assert "socket_timeout" not in kwargs


# --------------------------------------------------------------------------
# Missing-extra handling — wrong-SCOPE deployments
# --------------------------------------------------------------------------


def test_backend_raises_modulenotfounderror_when_deps_absent(monkeypatch):
    """When the ``module_cache`` extra isn't installed, ``__init__`` must
    raise ``ModuleNotFoundError`` (a subclass of ``ImportError``) so the
    module loader's wrong-SCOPE soft-skip catches it instead of the worker
    crashing on a bare ``ImportError``."""
    import dynastore.tools.cache_valkey as cv

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", False)
    monkeypatch.setattr(cv, "_CACHE_DEPS_ERR", ImportError("No module named 'msgpack'"))

    with pytest.raises(ModuleNotFoundError) as exc:
        ValkeyCacheBackend(url="valkey://localhost:6379")
    assert "module_cache" in str(exc.value)
    # Subclass relationship keeps every existing `except ImportError` working.
    assert isinstance(exc.value, ImportError)
