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


def test_too_low_max_size_is_clamped_to_floor():
    cfg = _cfg(min_size=1, max_size=2)
    cfg.validate_pool_sizing()
    assert cfg.pool_min_size == SAFE_POOL_MIN_FLOOR
    assert cfg.pool_max_size == SAFE_POOL_TOTAL_FLOOR


def test_too_low_max_size_warns_and_names_env_var(caplog):
    # Healthy min, but the total is capped tiny by a small max.
    cfg = _cfg(min_size=5, max_size=3)
    with caplog.at_level(logging.WARNING):
        cfg.validate_pool_sizing()
    assert any(
        "DB_POOL_MAX_SIZE" in rec.message and "QueuePool" in rec.message
        for rec in caplog.records
    ), "expected a WARNING naming DB_POOL_MAX_SIZE and the QueuePool risk"
    assert cfg.pool_max_size == SAFE_POOL_TOTAL_FLOOR


def test_clamped_pool_yields_non_negative_overflow():
    # After clamping, the derived overflow must stay valid (>= 0). This ties
    # the floor guard back to the existing negative-overflow clamp (#320).
    cfg = _cfg(min_size=2, max_size=2)
    cfg.validate_pool_sizing()
    assert cfg.pool_max_overflow >= 0
