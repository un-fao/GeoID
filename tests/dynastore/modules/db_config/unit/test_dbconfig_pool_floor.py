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

"""Regression test: ``DBConfig.validate_pool_sizing`` guards a too-small pool.

Background (dynastore #320): the review environment overrode
``DB_POOL_MIN_SIZE=2`` (code default is 5). With only two base connections,
3+ concurrent requests exhausted the overflow and queued until they hit the
60s QueuePool timeout ("QueuePool limit of size 2 overflow 3 reached,
connection timed out, timeout 60.00"), cascading into engine-snapshot retry
exhaustion and sustained 100% memory / container restarts.

Fix: a startup ``validate_pool_sizing`` that WARNs (naming the offending env
var and the QueuePool-timeout risk) and clamps ``pool_min_size`` /
``pool_max_size`` up to a safe floor, so a misconfigured tiny value can never
silently strangle the service. A healthy config must be left untouched.
"""
from __future__ import annotations

import logging

from dynastore.modules.db_config.db_config import (
    SAFE_POOL_MIN_FLOOR,
    SAFE_POOL_MIN_OVERFLOW,
    DBConfig,
)


def _cfg(min_size: int, max_size: int) -> DBConfig:
    cfg = DBConfig()
    # Instance attributes shadow the env-driven class attributes.
    cfg.pool_min_size = min_size
    cfg.pool_max_size = max_size
    return cfg


def test_healthy_config_is_left_unchanged(caplog):
    cfg = _cfg(min_size=5, max_size=100)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert cfg.pool_min_size == 5
    assert cfg.pool_max_size == 100
    assert caplog.records == []


def test_too_low_min_size_is_clamped_to_floor():
    # The incident's exact override: DB_POOL_MIN_SIZE=2.
    cfg = _cfg(min_size=2, max_size=100)
    cfg.validate_pool_sizing()
    assert cfg.pool_min_size == SAFE_POOL_MIN_FLOOR
    assert cfg.pool_max_size == 100


def test_too_low_min_size_warns_and_names_env_var(caplog):
    cfg = _cfg(min_size=2, max_size=100)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert any(
        "DB_POOL_MIN_SIZE" in rec.message and "QueuePool" in rec.message
        for rec in caplog.records
    ), "expected a WARNING naming DB_POOL_MIN_SIZE and the QueuePool risk"


def test_too_low_max_size_is_clamped_to_leave_overflow():
    # A tiny max no longer clamps to *exactly* the base floor — that produced
    # max_overflow == 0 (size-5 overflow-0), a rigid pool that deadlocks under
    # concurrent startup load. The base is floored to SAFE_POOL_MIN_FLOOR and
    # the max is lifted to leave at least SAFE_POOL_MIN_OVERFLOW of burst.
    cfg = _cfg(min_size=1, max_size=2)
    cfg.validate_pool_sizing()
    assert cfg.pool_min_size == SAFE_POOL_MIN_FLOOR
    assert cfg.pool_max_size == SAFE_POOL_MIN_FLOOR + SAFE_POOL_MIN_OVERFLOW
    assert cfg.pool_max_overflow == SAFE_POOL_MIN_OVERFLOW


def test_too_low_max_size_warns_and_names_env_var(caplog):
    # Healthy min, but the total is capped tiny by a small max.
    cfg = _cfg(min_size=5, max_size=3)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert any(
        "DB_POOL_MAX_SIZE" in rec.message and "QueuePool" in rec.message
        for rec in caplog.records
    ), "expected a WARNING naming DB_POOL_MAX_SIZE and the QueuePool risk"
    assert cfg.pool_max_size == SAFE_POOL_MIN_FLOOR + SAFE_POOL_MIN_OVERFLOW


def test_floor_collapse_never_yields_zero_overflow():
    # The exact dev incident: auth/tools right-sized to DB_POOL_MIN_SIZE=1 /
    # DB_POOL_MAX_SIZE=3. Both clamp up to the base floor (5), which without an
    # overflow guard would leave max_overflow == 0 and deadlock startup. The
    # pool must retain real burst headroom.
    cfg = _cfg(min_size=1, max_size=3)
    cfg.validate_pool_sizing()
    assert cfg.pool_max_overflow == SAFE_POOL_MIN_OVERFLOW
    assert cfg.pool_max_overflow > 0


def test_clamped_pool_yields_safe_overflow():
    # After clamping, the derived overflow must stay valid (>= 0, the #320
    # negative-overflow clamp) AND carry the minimum burst headroom so the pool
    # can grow past its base under load.
    cfg = _cfg(min_size=2, max_size=2)
    cfg.validate_pool_sizing()
    assert cfg.pool_max_overflow >= SAFE_POOL_MIN_OVERFLOW
