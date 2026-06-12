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

"""DB-free regression tests for the shared durable-work primitives (#1807).

The durable library's extraction contract is *byte-equivalence*: lock-key
integers, channel strings, and backoff delays must be exactly what the
subsystems computed before the move. These tests pin:

* golden lock-key values (a derivation change would silently re-key every
  advisory lock in the deployment — two leaders during a rolling deploy);
* alias identity at the historical import sites;
* the Python backoff mirror against the SQL curve semantics;
* channel-name constants against the plpgsql trigger literals that emit
  them (the DDL can't import Python constants, so source text is checked).
"""

import inspect

import pytest

from dynastore.durable.backoff import DEFAULT_BACKOFF_SECONDS, backoff_delay_seconds
from dynastore.durable.locks import stable_lock_key_signed64, stable_lock_key_uint63
from dynastore.durable.notify import (
    EVENTS_CHANNEL,
    NEW_TASK_QUEUED,
    TASK_STATUS_CHANGED,
    outbox_channel,
)


# ---------------------------------------------------------------------------
# Lock-key derivations — golden values
# ---------------------------------------------------------------------------


class TestLockKeyDerivations:
    def test_signed64_golden_values(self):
        # Pinned outputs of the sha256 derivation. If these change, every
        # pg_advisory_lock keyed through locking_tools is silently re-keyed.
        assert stable_lock_key_signed64("events_consumer_lock") == -921663935371928212
        assert stable_lock_key_signed64("dynastore.startup") == -1423723793760060476

    def test_signed64_may_be_negative(self):
        # The signed64 derivation uses the full signed bigint range.
        assert stable_lock_key_signed64("events_consumer_lock") < 0

    def test_uint63_golden_values(self):
        # Pinned outputs of the blake2b derivation used by the dispatcher.
        assert (
            stable_lock_key_uint63("dynastore.mandatory.backstop")
            == 3726089538419417805
        )
        assert (
            stable_lock_key_uint63("dynastore.capability", "gdal")
            == 3575639418821100772
        )

    def test_uint63_is_non_negative_63_bit(self):
        for key in ("a", "dynastore.mandatory.backstop", "x" * 200):
            v = stable_lock_key_uint63(key)
            assert 0 <= v <= 0x7FFFFFFFFFFFFFFF

    def test_uint63_part_boundaries_are_significant(self):
        # NUL-joined parts: ("ab","c") and ("a","bc") must not collide.
        assert stable_lock_key_uint63("ab", "c") != stable_lock_key_uint63("a", "bc")

    def test_the_two_derivations_differ(self):
        # They are intentionally distinct algorithms; a refactor that merges
        # them would re-key one family of locks mid-rollout.
        key = "same-logical-key"
        assert stable_lock_key_signed64(key) != stable_lock_key_uint63(key)

    def test_locking_tools_alias_is_the_shared_function(self):
        from dynastore.modules.db_config import locking_tools

        assert locking_tools._get_stable_lock_id is stable_lock_key_signed64

    def test_dispatcher_alias_is_the_shared_function(self):
        from dynastore.modules.tasks import dispatcher

        assert dispatcher._stable_advisory_lock_key is stable_lock_key_uint63


# ---------------------------------------------------------------------------
# Backoff curve
# ---------------------------------------------------------------------------


class TestBackoffCurve:
    def test_curve_values_pinned(self):
        # 1s, 5s, 30s, 5min, 30min — the storage_outbox retry curve.
        assert DEFAULT_BACKOFF_SECONDS == [1, 5, 30, 300, 1800]

    def test_delay_indexes_curve_zero_based(self):
        assert backoff_delay_seconds(0) == 1
        assert backoff_delay_seconds(1) == 5
        assert backoff_delay_seconds(2) == 30
        assert backoff_delay_seconds(3) == 300

    def test_delay_clamps_to_last_entry(self):
        # Mirrors the SQL CASE in PgOutboxStore.mark_retry: attempts at or
        # beyond the last index reuse the final (plateau) entry.
        assert backoff_delay_seconds(4) == 1800
        assert backoff_delay_seconds(5) == 1800
        assert backoff_delay_seconds(1000) == 1800

    def test_delay_rejects_negative_attempts(self):
        with pytest.raises(ValueError):
            backoff_delay_seconds(-1)

    def test_pg_outbox_uses_the_shared_curve(self):
        from dynastore.modules.storage import pg_outbox

        assert pg_outbox._BACKOFF_SECONDS is DEFAULT_BACKOFF_SECONDS


# ---------------------------------------------------------------------------
# Notify channels — constants, re-exports, and plpgsql cross-checks
# ---------------------------------------------------------------------------


class TestNotifyChannels:
    def test_channel_names_pinned(self):
        assert NEW_TASK_QUEUED == "new_task_queued"
        assert TASK_STATUS_CHANGED == "task_status_changed"
        assert EVENTS_CHANNEL == "dynastore_events_channel"

    def test_queue_module_reexports_same_objects(self):
        from dynastore.modules.tasks import queue

        assert queue.NEW_TASK_QUEUED is NEW_TASK_QUEUED
        assert queue.TASK_STATUS_CHANGED is TASK_STATUS_CHANGED
        assert queue.EVENTS_CHANNEL is EVENTS_CHANNEL

    def test_event_channel_enum_matches_constant(self):
        from dynastore.modules.events.models import EventChannel

        assert EventChannel.EVENTS.value == EVENTS_CHANNEL

    def test_outbox_channel_builder(self):
        assert outbox_channel("es", "tenant_a") == "outbox_es_tenant_a"

    def test_outbox_channel_rejects_hostile_identifiers(self):
        with pytest.raises(Exception):
            outbox_channel("es", 'x"; DROP TABLE y; --')

    def test_tasks_trigger_ddl_emits_new_task_queued(self):
        # The plpgsql trigger can't import the constant; pin the literal so a
        # rename here without a DDL change fails the suite instead of
        # shipping a dispatcher that listens on a dead channel.
        from dynastore.modules.tasks import tasks_module

        src = inspect.getsource(tasks_module)
        assert f"pg_notify('{NEW_TASK_QUEUED}'" in src

    def test_events_trigger_ddl_emits_events_channel(self):
        from dynastore.modules.events import events_module

        src = inspect.getsource(events_module)
        assert f"pg_notify('{EVENTS_CHANNEL}'" in src

    def test_outbox_trigger_ddl_matches_builder_shape(self):
        # Trigger builds 'outbox_' || NEW.driver_id || '_' || cat; the
        # Python builder must produce the same prefix/joiner shape.
        from dynastore.modules.storage import outbox_ddl

        src = inspect.getsource(outbox_ddl)
        assert "'outbox_' || NEW.driver_id || '_' || cat" in src
        assert outbox_channel("d", "c") == "outbox_" + "d" + "_" + "c"


# ---------------------------------------------------------------------------
# Election surface
# ---------------------------------------------------------------------------


class TestElectionSurface:
    def test_canonical_exports_resolve_to_hardened_implementations(self):
        from dynastore.durable import pg_advisory_leadership, run_leader_loop
        from dynastore.modules.db_config import locking_tools
        from dynastore.tools import async_utils

        assert pg_advisory_leadership is locking_tools.pg_advisory_leadership
        assert run_leader_loop is async_utils.run_leader_loop
