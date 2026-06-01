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

"""Bucket math + protocol-shape tests for the usage-counter foundation.

Live PG behavior (atomic upsert, CAS) is covered by the integration test
suite added with PR-A4 when the policy condition handlers consume the
driver. This module is hermetic: no DB, no Valkey.
"""

from datetime import datetime, timezone

from dynastore.models.protocols.usage_counter import UsageCounterProtocol
from dynastore.modules.iam.usage_counter_bucket import (
    LIFETIME_BUCKET as _LIFETIME_BUCKET,
    bucket_for as _bucket_for,
    expires_for as _expires_for,
)
from dynastore.modules.iam.usage_counter_pg import (
    _INCR_IF_BELOW,
    PostgresUsageCounter,
)


class TestBucketMath:
    def test_lifetime_bucket_constant_is_epoch_utc(self):
        assert _LIFETIME_BUCKET == datetime.fromtimestamp(0, tz=timezone.utc)

    def test_no_window_returns_lifetime_bucket(self):
        assert _bucket_for(None) == _LIFETIME_BUCKET
        assert _bucket_for(0) == _LIFETIME_BUCKET
        assert _bucket_for(-30) == _LIFETIME_BUCKET

    def test_window_aligns_to_floor(self):
        # 12:34:56 with a 60s window → 12:34:00
        now = datetime(2026, 5, 15, 12, 34, 56, tzinfo=timezone.utc)
        bucket = _bucket_for(60, now=now)
        assert bucket == datetime(2026, 5, 15, 12, 34, 0, tzinfo=timezone.utc)

    def test_window_aligns_within_same_bucket(self):
        # Two timestamps in the same 60s window must hash to the same bucket
        a = datetime(2026, 5, 15, 12, 34, 1, tzinfo=timezone.utc)
        b = datetime(2026, 5, 15, 12, 34, 59, tzinfo=timezone.utc)
        assert _bucket_for(60, now=a) == _bucket_for(60, now=b)

    def test_window_rolls_over_at_boundary(self):
        edge_before = datetime(2026, 5, 15, 12, 34, 59, tzinfo=timezone.utc)
        edge_after = datetime(2026, 5, 15, 12, 35, 0, tzinfo=timezone.utc)
        assert _bucket_for(60, now=edge_before) != _bucket_for(60, now=edge_after)

    def test_expires_for_lifetime_is_none(self):
        assert _expires_for(_LIFETIME_BUCKET, None) is None
        assert _expires_for(_LIFETIME_BUCKET, 0) is None

    def test_expires_for_rate_window_is_two_windows_past_bucket(self):
        bucket = datetime(2026, 5, 15, 12, 34, 0, tzinfo=timezone.utc)
        # 60s window → expires_at = bucket + 120s grace
        exp = _expires_for(bucket, 60)
        assert exp == datetime(2026, 5, 15, 12, 36, 0, tzinfo=timezone.utc)


class TestProtocolConformance:
    def test_postgres_driver_satisfies_protocol(self):
        # `runtime_checkable` Protocol — structural check on the class.
        # Skips ``__init__`` (which touches the engine) by inspecting
        # the bound methods on the class itself.
        for method in (
            "incr",
            "get",
            "incr_if_below",
            "reset",
            "reap_expired",
        ):
            assert callable(getattr(PostgresUsageCounter, method)), method
        assert hasattr(PostgresUsageCounter, "name")
        assert hasattr(PostgresUsageCounter, "priority")

    def test_protocol_is_runtime_checkable(self):
        # Guard against accidentally dropping @runtime_checkable —
        # downstream code uses isinstance(driver, UsageCounterProtocol).
        assert getattr(UsageCounterProtocol, "_is_runtime_protocol", False) is True


def test_incr_if_below_signature_is_keyword_only():
    # Pure signature shape check — exercises the keyword-only contract
    # the policy condition handlers depend on. Live atomicity is covered
    # by integration tests once handlers wire up in PR-A4.
    import inspect

    sig = inspect.signature(PostgresUsageCounter.incr_if_below)
    params = sig.parameters
    assert "policy_id" in params
    assert "principal_key" in params
    assert "limit" in params
    assert params["window_seconds"].kind == inspect.Parameter.KEYWORD_ONLY
    assert params["amount"].kind == inspect.Parameter.KEYWORD_ONLY


class TestIncrIfBelowSqlShape:
    """Static SQL-shape guards for the CAS query.

    Live atomicity (the actual upsert path against a running Postgres)
    is covered by the PR-A4 integration tests. The checks here pin the
    structural properties that are easy to break without noticing:
    both branches of the upsert (insert and update) must respect the
    cap predicate.
    """

    def _sql(self) -> str:
        # ``BaseQuery.template`` exposes the pre-``{schema}``-substitution
        # SQL string. Whitespace is collapsed so the shape checks below
        # are robust to incidental reformatting.
        return " ".join(_INCR_IF_BELOW.template.split())

    def test_insert_path_gated_by_limit_predicate(self):
        # The very first hit on a bucket must reject when amount > limit.
        # A plain ``INSERT … VALUES`` would bypass the cap because
        # Postgres' ``ON CONFLICT DO UPDATE … WHERE`` predicate only
        # applies to the update branch. The fix uses ``INSERT … SELECT
        # … WHERE :amount <= :limit`` so the insert branch is gated too.
        # Both operands are CAST to bigint — see the type-anchoring guard
        # below for why the bare comparison cannot be used.
        sql = self._sql().lower()
        assert "cast(:amount as bigint) <= cast(:limit as bigint)" in sql, (
            "INSERT branch must be gated by amount <= limit so the "
            "first hit cannot bypass the cap"
        )
        # Sanity: the form used for the gate is INSERT…SELECT, not
        # INSERT…VALUES.
        assert "insert into" in sql
        assert "select :policy_id" in sql

    def test_update_path_gated_by_running_total_predicate(self):
        sql = self._sql().lower()
        # ``u.count + excluded.count <= :limit`` blocks the increment
        # once a row exists and the sum would exceed the cap. ``:limit``
        # is CAST to bigint to stay consistent with its other use site.
        assert "u.count + excluded.count <= cast(:limit as bigint)" in sql

    def test_cap_predicate_is_type_anchored_for_asyncpg(self):
        # Regression guard for #1709. The insert-branch gate compares two
        # free bind parameters (``:amount`` and ``:limit``) with no column
        # to anchor their type. Without an explicit CAST, asyncpg cannot
        # decide whether ``<=`` resolves to the integer or text operator
        # and raises ``AmbiguousParameterError`` at prepare time, so every
        # quota check (rate_limit / max_count) fails. Both operands must be
        # cast to bigint (the ``count`` column type).
        sql = self._sql().lower()
        assert "cast(:amount as bigint)" in sql
        assert "cast(:limit as bigint)" in sql
        # The pre-fix bare comparison must never come back.
        assert ":amount <= :limit" not in sql

    def test_returns_count_and_allowed(self):
        sql = self._sql().lower()
        assert "as count" in sql
        assert "as allowed" in sql
