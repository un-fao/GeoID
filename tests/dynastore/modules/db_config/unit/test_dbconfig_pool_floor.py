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

Background (dynastore #320): the review environment timed out under concurrent
load ("QueuePool limit of size 2 overflow 3 reached, connection timed out,
timeout 60.00"), cascading into engine-snapshot retry exhaustion and sustained
100% memory / container restarts. The failure was a *total-capacity* starvation
— size 2 + overflow 3 = 5 connections could not serve the concurrency — not a
shortage of persistent base connections.

Fix (dynastore #392 refinement): ``validate_pool_sizing`` floors the *total
capacity* a pool can open at once (``pool_size + max_overflow``, the thing #320
starved) and leaves the persistent base to the operator down to a floor of 1.
Forcing a large base only inflates the idle footprint a deployment pins on the
shared Postgres (``base × workers × MIN_SCALE``) without adding burst room. The
guard WARNs (naming the offending env var and the QueuePool risk) and clamps up
to the safe floors; a healthy config is left untouched.
"""
from __future__ import annotations

import logging

from dynastore.modules.db_config.db_config import (
    SAFE_POOL_MIN_FLOOR,
    SAFE_POOL_MIN_OVERFLOW,
    SAFE_POOL_TOTAL_FLOOR,
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


def test_small_base_is_respected_not_inflated(caplog):
    # The #320 override DB_POOL_MIN_SIZE=2 is now a legitimate deployment choice:
    # the base is no longer forced up to a large floor (which only inflated the
    # idle footprint, #392). With a healthy max the small base is left intact
    # and emits no warning — burst safety comes from the total, not the base.
    cfg = _cfg(min_size=2, max_size=100)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert cfg.pool_min_size == 2
    assert cfg.pool_max_size == 100
    assert caplog.records == []


def test_zero_base_is_clamped_to_minimum(caplog):
    # A 0/negative base is a misconfig, not an intentional small value — a pool
    # must hold at least one connection.
    cfg = _cfg(min_size=0, max_size=100)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert cfg.pool_min_size == SAFE_POOL_MIN_FLOOR
    assert any(
        "DB_POOL_MIN_SIZE" in rec.message for rec in caplog.records
    ), "expected a WARNING naming DB_POOL_MIN_SIZE"


def test_too_low_max_size_is_clamped_to_total_floor():
    # The dev auth/tools right-sizing (1/3). The small base stays, but the max
    # is lifted to the total-capacity floor so the pool can burst — the size-5
    # overflow-0 rigid pool that deadlocked startup can no longer form.
    cfg = _cfg(min_size=1, max_size=3)
    cfg.validate_pool_sizing()
    assert cfg.pool_min_size == 1
    assert cfg.pool_max_size == SAFE_POOL_TOTAL_FLOOR
    assert cfg.pool_max_overflow == SAFE_POOL_TOTAL_FLOOR - 1


def test_too_low_max_size_warns_and_names_env_var(caplog):
    cfg = _cfg(min_size=5, max_size=3)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert any(
        "DB_POOL_MAX_SIZE" in rec.message and "QueuePool" in rec.message
        for rec in caplog.records
    ), "expected a WARNING naming DB_POOL_MAX_SIZE and the QueuePool risk"
    assert cfg.pool_max_size == SAFE_POOL_TOTAL_FLOOR


def test_floor_collapse_never_yields_zero_overflow():
    # The exact dev incident: auth/tools right-sized to 1/3. The total floor
    # alone already guarantees burst, so overflow must be strictly positive.
    cfg = _cfg(min_size=1, max_size=3)
    cfg.validate_pool_sizing()
    assert cfg.pool_max_overflow >= SAFE_POOL_MIN_OVERFLOW
    assert cfg.pool_max_overflow > 0


def test_overflow_gap_guard_lifts_max_above_total_floor():
    # When the base sits near the (already floored) max, the gap guard lifts the
    # max further so a minimum burst headroom remains above the base. base 8 +
    # SAFE_POOL_MIN_OVERFLOW exceeds the total floor of 10, so max -> 13.
    cfg = _cfg(min_size=8, max_size=8)
    cfg.validate_pool_sizing()
    assert cfg.pool_max_size == 8 + SAFE_POOL_MIN_OVERFLOW
    assert cfg.pool_max_overflow == SAFE_POOL_MIN_OVERFLOW


def test_clamped_pool_yields_safe_overflow():
    # After clamping, the derived overflow must stay valid (>= 0, the #320
    # negative-overflow clamp) AND carry the minimum burst headroom so the pool
    # can grow past its base under load.
    cfg = _cfg(min_size=2, max_size=2)
    cfg.validate_pool_sizing()
    assert cfg.pool_max_overflow >= SAFE_POOL_MIN_OVERFLOW
