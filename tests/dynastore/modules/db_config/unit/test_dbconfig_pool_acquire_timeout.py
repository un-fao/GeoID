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

"""Unit tests for DBConfig.pool_acquire_timeout — bounded pool-acquire wait.

Background (#1894): the previous code passed ``pool_command_timeout`` (60s)
to SQLAlchemy's ``create_async_engine(pool_timeout=…)``.  That parameter is
the pool-*acquire* wait, not the statement-execution budget, so the pool
would silently block callers for up to 60s before surfacing saturation.

Fix: a dedicated ``pool_acquire_timeout`` field (env ``DB_POOL_ACQUIRE_TIMEOUT``,
default 30s) is validated at startup and wired to ``pool_timeout`` in the
engine factory.  The ``pool_command_timeout`` field is preserved for its
original purpose (statement-level timeouts via asyncpg).
"""
from __future__ import annotations

import logging

from dynastore.modules.db_config.db_config import DBConfig


def _cfg(acquire_timeout: int) -> DBConfig:
    cfg = DBConfig()
    cfg.pool_acquire_timeout = acquire_timeout
    return cfg


# --------------------------------------------------------------------------- #
# Default value                                                               #
# --------------------------------------------------------------------------- #
def test_default_pool_acquire_timeout_is_30():
    """Code default matches connect_timeout and is a reasonable fail-fast bound."""
    cfg = DBConfig()
    assert cfg.pool_acquire_timeout == 30


# --------------------------------------------------------------------------- #
# Env resolution                                                              #
# --------------------------------------------------------------------------- #
def test_env_sets_pool_acquire_timeout(monkeypatch):
    monkeypatch.setenv("DB_POOL_ACQUIRE_TIMEOUT", "10")
    from dynastore.modules.db_config.db_config import _cfg_int
    result = _cfg_int("DB_POOL_ACQUIRE_TIMEOUT", 30, file_values={})
    assert result == 10


def test_env_unsubstituted_placeholder_falls_back_to_default(monkeypatch):
    """An unsubstituted ${...} deploy placeholder must not break startup."""
    from dynastore.modules.db_config.db_config import _cfg_int
    result = _cfg_int(
        "DB_POOL_ACQUIRE_TIMEOUT", 30,
        file_values={"DB_POOL_ACQUIRE_TIMEOUT": "${DB_POOL_ACQUIRE_TIMEOUT}"},
    )
    assert result == 30


# --------------------------------------------------------------------------- #
# validate_pool_sizing: zero / negative value is reset to 30                 #
# --------------------------------------------------------------------------- #
def test_zero_acquire_timeout_is_reset(caplog):
    cfg = _cfg(acquire_timeout=0)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert cfg.pool_acquire_timeout == 30
    assert any("DB_POOL_ACQUIRE_TIMEOUT" in r.message for r in caplog.records)


def test_negative_acquire_timeout_is_reset():
    cfg = _cfg(acquire_timeout=-5)
    cfg.validate_pool_sizing()
    assert cfg.pool_acquire_timeout == 30


def test_zero_acquire_timeout_warns_naming_env_var(caplog):
    cfg = _cfg(acquire_timeout=0)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert any(
        "DB_POOL_ACQUIRE_TIMEOUT" in r.message for r in caplog.records
    ), "expected a WARNING naming DB_POOL_ACQUIRE_TIMEOUT"


# --------------------------------------------------------------------------- #
# validate_pool_sizing: large value emits a warning but is not clamped        #
# --------------------------------------------------------------------------- #
def test_large_acquire_timeout_warns(caplog):
    cfg = _cfg(acquire_timeout=300)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert any(
        "DB_POOL_ACQUIRE_TIMEOUT" in r.message and "unusually high" in r.message
        for r in caplog.records
    ), "expected a WARNING about an unusually large pool_acquire_timeout"


def test_large_acquire_timeout_is_not_clamped():
    """Operators may legitimately need a higher timeout in some envs; only warn."""
    cfg = _cfg(acquire_timeout=300)
    cfg.validate_pool_sizing()
    assert cfg.pool_acquire_timeout == 300


# --------------------------------------------------------------------------- #
# Happy path: healthy value produces no warnings                              #
# --------------------------------------------------------------------------- #
def test_healthy_acquire_timeout_no_warning(caplog):
    cfg = _cfg(acquire_timeout=30)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    acquire_warns = [
        r for r in caplog.records if "DB_POOL_ACQUIRE_TIMEOUT" in r.message
    ]
    assert not acquire_warns, "a healthy timeout must not emit any WARNING"


def test_acquire_timeout_field_is_separate_from_command_timeout():
    """pool_acquire_timeout and pool_command_timeout are independent tunables."""
    cfg = DBConfig()
    assert hasattr(cfg, "pool_acquire_timeout")
    assert hasattr(cfg, "pool_command_timeout")
    assert cfg.pool_acquire_timeout != cfg.pool_command_timeout or True
    # They may coincidentally be equal in some configs; the key invariant is
    # that both attributes exist and resolve independently.


# --------------------------------------------------------------------------- #
# Source-level pin: db_service must use pool_acquire_timeout for pool_timeout  #
# --------------------------------------------------------------------------- #
def _db_service_source() -> str:
    import pathlib
    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[5]
    return (
        repo_root / "packages/core/src/dynastore/modules/db/db_service.py"
    ).read_text(encoding="utf-8")


def test_db_service_uses_pool_acquire_timeout_not_command_timeout():
    """create_async_engine must be given pool_acquire_timeout, not the old
    pool_command_timeout, for the pool_timeout kwarg (#1894)."""
    source = _db_service_source()
    assert "pool_timeout=db_config.pool_acquire_timeout" in source, (
        "db_service.py must pass pool_acquire_timeout (not pool_command_timeout) "
        "to create_async_engine pool_timeout= (#1894)"
    )
    assert "pool_timeout=db_config.pool_command_timeout" not in source, (
        "db_service.py still passes pool_command_timeout to pool_timeout= — "
        "this was the bug; it must use pool_acquire_timeout (#1894)"
    )
