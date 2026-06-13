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

"""Unit coverage for ``CircuitBreaker`` — the per-indexer breaker the
``IndexDispatcher`` consults before each sync attempt.

State machine: CLOSED → OPEN (after threshold consecutive failures)
→ HALF_OPEN (after cooldown elapsed) → CLOSED on success / OPEN on failure.
"""
from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from dynastore.modules.storage.circuit_breaker import CircuitBreaker


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


class TestConstructorValidation:
    def test_default_thresholds(self):
        cb = CircuitBreaker()
        # No state for unknown indexer — closed by default.
        assert cb.is_open("nobody") is False
        assert cb.state_of("nobody") == "CLOSED"

    def test_failure_threshold_must_be_positive(self):
        with pytest.raises(ValueError):
            CircuitBreaker(failure_threshold=0)

    def test_cooldown_must_be_positive(self):
        with pytest.raises(ValueError):
            CircuitBreaker(cooldown_seconds=0)


# ---------------------------------------------------------------------------
# Failure / success accounting
# ---------------------------------------------------------------------------


class TestFailureAccounting:
    def test_below_threshold_stays_closed(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure("es")
        cb.record_failure("es")
        assert cb.state_of("es") == "CLOSED"
        assert cb.is_open("es") is False

    def test_threshold_reached_opens(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure("es")
        cb.record_failure("es")
        cb.record_failure("es")
        assert cb.state_of("es") == "OPEN"
        assert cb.is_open("es") is True

    def test_success_resets_counter(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure("es")
        cb.record_failure("es")
        cb.record_success("es")
        # Two more failures shouldn't open — counter was reset.
        cb.record_failure("es")
        cb.record_failure("es")
        assert cb.state_of("es") == "CLOSED"

    def test_per_indexer_isolation(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure("es_public")
        cb.record_failure("es_public")
        # The other indexer is independent.
        assert cb.is_open("es_public") is True
        assert cb.is_open("es_private") is False


# ---------------------------------------------------------------------------
# OPEN → HALF_OPEN cooldown logic — test by mocking time.monotonic
# ---------------------------------------------------------------------------


class TestCooldownAndHalfOpen:
    def test_open_within_cooldown_stays_open(self):
        with patch("dynastore.modules.storage.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 1000.0
            cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=30)
            cb.record_failure("es")
            cb.record_failure("es")
            # Still inside the 30 s window.
            mock_time.monotonic.return_value = 1010.0
            assert cb.is_open("es") is True
            assert cb.state_of("es") == "OPEN"

    def test_cooldown_elapsed_transitions_to_half_open(self):
        with patch("dynastore.modules.storage.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 1000.0
            cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=30)
            cb.record_failure("es")
            cb.record_failure("es")
            # Past cooldown.
            mock_time.monotonic.return_value = 1031.0
            assert cb.is_open("es") is False  # probe allowed
            assert cb.state_of("es") == "HALF_OPEN"

    def test_half_open_probe_success_closes_breaker(self):
        with patch("dynastore.modules.storage.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 1000.0
            cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=30)
            cb.record_failure("es")
            cb.record_failure("es")
            mock_time.monotonic.return_value = 1031.0
            cb.is_open("es")  # advances to HALF_OPEN
            cb.record_success("es")
            assert cb.state_of("es") == "CLOSED"
            assert cb.is_open("es") is False

    def test_half_open_probe_failure_reopens(self):
        with patch("dynastore.modules.storage.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 1000.0
            cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=30)
            cb.record_failure("es")
            cb.record_failure("es")
            mock_time.monotonic.return_value = 1031.0
            cb.is_open("es")  # advances to HALF_OPEN
            cb.record_failure("es")  # probe fails
            assert cb.state_of("es") == "OPEN"
            # Within new cooldown — still open.
            mock_time.monotonic.return_value = 1040.0
            assert cb.is_open("es") is True


# ---------------------------------------------------------------------------
# Operator surface — snapshot, reset
# ---------------------------------------------------------------------------


class TestOperatorSurface:
    def test_snapshot_returns_per_indexer_state(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure("es_a")
        cb.record_failure("es_a")
        cb.record_failure("es_b")
        snap = cb.snapshot()
        assert "es_a" in snap and "es_b" in snap
        assert snap["es_a"]["state"] == "OPEN"
        assert snap["es_a"]["consecutive_failures"] == 2
        assert snap["es_b"]["state"] == "CLOSED"
        assert snap["es_b"]["consecutive_failures"] == 1

    def test_reset_single_indexer(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure("es_a")
        cb.record_failure("es_a")
        cb.record_failure("es_b")
        cb.record_failure("es_b")
        cb.reset("es_a")
        assert cb.state_of("es_a") == "CLOSED"
        assert cb.state_of("es_b") == "OPEN"

    def test_reset_all(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure("es_a")
        cb.record_failure("es_a")
        cb.record_failure("es_b")
        cb.record_failure("es_b")
        cb.reset()
        assert cb.state_of("es_a") == "CLOSED"
        assert cb.state_of("es_b") == "CLOSED"

    def test_state_of_does_not_mutate(self):
        """state_of is read-only — the OPEN→HALF_OPEN transition only
        happens via is_open, which the dispatcher consults on the hot
        path. Operator inspection (admin endpoints) must not flip state."""
        with patch("dynastore.modules.storage.circuit_breaker.time") as mock_time:
            mock_time.monotonic.return_value = 1000.0
            cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=30)
            cb.record_failure("es")
            cb.record_failure("es")
            mock_time.monotonic.return_value = 1031.0
            # state_of called past the cooldown — should still report OPEN.
            assert cb.state_of("es") == "OPEN"
            # Now is_open() is what advances the state machine.
            assert cb.is_open("es") is False
            assert cb.state_of("es") == "HALF_OPEN"


# ---------------------------------------------------------------------------
# Thread safety smoke test
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_record_failure_is_consistent(self):
        cb = CircuitBreaker(failure_threshold=100)
        # 10 threads, 10 failures each → 100 total → exactly at threshold.

        def worker():
            for _ in range(10):
                cb.record_failure("es")

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        snap = cb.snapshot()
        assert snap["es"]["consecutive_failures"] == 100
        assert snap["es"]["state"] == "OPEN"
