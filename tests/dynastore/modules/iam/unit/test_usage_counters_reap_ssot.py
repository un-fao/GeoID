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

"""Pin the SQL SSOT for ``usage_counters`` expiry reaping.

Gap #6 of issue #800: ``PostgresUsageCounter.reap_expired`` and the
out-of-process reaper used to embed two copies of the same
``DELETE … WHERE expires_at IS NOT NULL AND expires_at < NOW()`` clause.
If a grace period or any other predicate was later introduced on one
side and not the other, lifetime quotas would silently stop being reaped
(or windowed rows would be reaped early). These tests pin the shared
constant. The out-of-process reaper is now the leader-elected
``MaintenanceSupervisor`` IAM prune (it replaced the former plpgsql +
pg_cron job, deleted in #1911 / #1927).
"""

from __future__ import annotations


def test_postgres_driver_uses_shared_reap_sql() -> None:
    """The ``_REAP_EXPIRED`` query template in the PG driver must be the
    same string as ``REAP_EXPIRED_USAGE_COUNTERS_SQL`` — otherwise the
    SSOT is a fiction and drift is back on the table."""
    from dynastore.modules.iam.iam_queries import (
        REAP_EXPIRED_USAGE_COUNTERS_SQL,
    )
    from dynastore.modules.iam.usage_counter_pg import _REAP_EXPIRED

    assert _REAP_EXPIRED.template == REAP_EXPIRED_USAGE_COUNTERS_SQL


def test_reap_sql_targets_only_expired_windowed_rows() -> None:
    """Lifetime counters (``expires_at IS NULL``) must NOT be deleted by
    the nightly prune — they represent installed quotas tied to a
    policy + principal and live until the principal hits the limit or
    an admin issues an explicit reset."""
    from dynastore.modules.iam.iam_queries import (
        REAP_EXPIRED_USAGE_COUNTERS_SQL,
    )

    sql = REAP_EXPIRED_USAGE_COUNTERS_SQL.lower()
    assert "delete from" in sql
    assert "usage_counters" in sql
    assert "expires_at is not null" in sql
    assert "expires_at < now()" in sql


def test_supervisor_prune_consumes_shared_reap_predicate() -> None:
    """The ``MaintenanceSupervisor`` IAM prune — the canonical out-of-process
    reaper since #1911 / #1927 — must consume the SSOT WHERE predicate, not a
    hand-typed copy. Guards against re-inlining the windowed-expiry clause."""
    import inspect

    from dynastore.modules.catalog import maintenance_supervisor

    src = inspect.getsource(maintenance_supervisor)
    assert "REAP_EXPIRED_USAGE_COUNTERS_WHERE" in src, (
        "supervisor IAM prune must reference the SSOT WHERE constant; "
        "found no import / usage in maintenance_supervisor.py"
    )
    # And the predicate must NOT be re-typed as a literal — that's the
    # drift pattern we want to prevent.
    assert "expires_at IS NOT NULL AND expires_at < NOW()" not in src, (
        "found inline copy of the reap WHERE clause in "
        "maintenance_supervisor.py — use REAP_EXPIRED_USAGE_COUNTERS_WHERE"
    )
