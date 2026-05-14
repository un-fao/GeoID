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

"""Unit tests for Valkey backend socket_timeout + TCP keepalive wiring.

Regression cover for the review-env cluster read-timeout incident: valkey-py
hard-defaults socket_timeout to 5s and socket_keepalive to False, so an idle
Cloud Run -> Memorystore socket is dropped by Cloud NAT and a cold topology
fetch can exceed 5s. The ValkeyEngineConfig tunables must reach the
connection pool kwargs.
"""

from __future__ import annotations

import socket

import pytest

from dynastore.modules.db_config.engine_config import ValkeyEngineConfig
from dynastore.tools.cache_valkey import (
    ValkeyCacheBackend,
    _build_keepalive_options,
    build_valkey_client,
)


# --------------------------------------------------------------------------
# ValkeyEngineConfig — connection tunables (moved from CachePluginConfig)
# --------------------------------------------------------------------------


def test_valkey_engine_config_exposes_socket_timeout_and_keepalive_defaults():
    cfg = ValkeyEngineConfig()
    # Read timeout is short by design — a cache fails fast to the source
    # rather than blocking the hot path on an unhealthy backend. 5s is
    # generous headroom for a healthy same-VPC op.
    assert cfg.socket_timeout_seconds == 5.0
    assert cfg.socket_connect_timeout_seconds == 3.0
    # Idle window sits well under Cloud NAT's ~1200s established-conn timeout,
    # matching the DB pool keepalive parity (#655).
    assert cfg.tcp_keepalive_idle_seconds == 300
    assert cfg.tcp_keepalive_interval_seconds == 30
    assert cfg.tcp_keepalive_count == 5


def test_build_keepalive_options_maps_available_constants() -> None:
    """_build_keepalive_options emits only the socket constants the OS exposes."""
    opts = _build_keepalive_options(300, 30, 5)
    if hasattr(socket, "TCP_KEEPIDLE"):
        assert opts[socket.TCP_KEEPIDLE] == 300
    if hasattr(socket, "TCP_KEEPINTVL"):
        assert opts[socket.TCP_KEEPINTVL] == 30
    if hasattr(socket, "TCP_KEEPCNT"):
        assert opts[socket.TCP_KEEPCNT] == 5
    # Nothing supplied -> empty mapping (no spurious keepalive opts).
    assert _build_keepalive_options(None, None, None) == {}


def test_build_valkey_client_wires_socket_timeout_and_keepalives() -> None:
    """build_valkey_client forwards the tunables to the connection pool kwargs."""
    client, pool = build_valkey_client(
        url="valkey://localhost:6379",
        socket_connect_timeout=3.0,
        socket_timeout=5.0,
        tcp_keepalive_idle=300,
        tcp_keepalive_interval=30,
        tcp_keepalive_count=5,
    )
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert pool_kwargs["socket_timeout"] == 5.0
    assert pool_kwargs["socket_connect_timeout"] == 3.0
    assert pool_kwargs["socket_keepalive"] is True
    assert pool_kwargs["socket_keepalive_options"]  # non-empty on Linux/macOS


def test_build_valkey_client_omits_keepalives_when_no_tunables_supplied() -> None:
    """Backwards-compat: no keepalive kwargs leak in when nothing is passed."""
    client, pool = build_valkey_client(url="valkey://localhost:6379")
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert "socket_timeout" not in pool_kwargs
    assert "socket_connect_timeout" not in pool_kwargs
    assert "socket_keepalive" not in pool_kwargs
    assert "socket_keepalive_options" not in pool_kwargs
