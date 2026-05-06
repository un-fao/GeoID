"""Pin the partition-key-aware policy storage invariants.

`PostgresPolicyStorage.update_policy` previously did GET-then-DELETE-then-INSERT
with both queries unfiltered by ``partition_key``. Under multi-service boot
the same default policy IDs ping-pong between partition_keys, the unfiltered
DELETE wiped every partition's copy of that id, and concurrent reads saw
Deny-by-Default — the IAM-outage class of bugs.

The fix:

* ``GET_POLICY`` and ``DELETE_POLICY`` SQL filter by ``(id, partition_key)``,
  matching the table's PRIMARY KEY.
* ``update_policy`` does a single ``UPSERT_POLICY`` (``ON CONFLICT (id,
  partition_key) DO UPDATE``) — no GET, no DELETE.

These tests pin those invariants without standing up Postgres.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.iam.models import Policy
from dynastore.modules.iam.postgres_policy_storage import (
    DELETE_POLICY,
    GET_POLICY,
    PostgresPolicyStorage,
    UPSERT_POLICY,
)


def _mk_policy(pid: str, partition_key: str = "global") -> Policy:
    return Policy(
        id=pid,
        version="1.0",
        description="t",
        effect="ALLOW",
        actions=["*"],
        resources=[".*"],
        conditions=[],
        partition_key=partition_key,
    )


def test_get_policy_sql_filters_by_partition_key() -> None:
    """GET_POLICY must include ``partition_key`` in its WHERE clause —
    looking up by id alone leaks rows across tenants when the same id exists
    in multiple partitions."""
    sql = GET_POLICY.template.lower()
    assert "where id = :id" in sql
    assert "partition_key = :partition_key" in sql


def test_delete_policy_sql_filters_by_partition_key() -> None:
    """DELETE_POLICY must include ``partition_key`` in its WHERE clause —
    admin deletes must never cascade across partitions."""
    sql = DELETE_POLICY.template.lower()
    assert "where id = :id" in sql
    assert "partition_key = :partition_key" in sql


@pytest.mark.asyncio
async def test_update_policy_does_not_issue_delete() -> None:
    """``update_policy`` must call only UPSERT_POLICY — never GET_POLICY,
    never DELETE_POLICY. The pre-fix GET-then-DELETE-then-INSERT shape is
    the source of the cross-partition wipe."""
    storage = PostgresPolicyStorage.__new__(PostgresPolicyStorage)
    storage.engine = MagicMock()

    policy = _mk_policy("public_access", partition_key="global")

    upsert_mock = AsyncMock(return_value=policy)
    get_mock = AsyncMock(return_value=None)
    delete_mock = AsyncMock(return_value=0)

    fake_db = MagicMock()
    mt_cm = MagicMock()
    mt_cm.__aenter__ = AsyncMock(return_value=fake_db)
    mt_cm.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.postgres_policy_storage.managed_transaction",
        return_value=mt_cm,
    ), patch.object(UPSERT_POLICY, "execute", upsert_mock), patch.object(
        GET_POLICY, "execute", get_mock
    ), patch.object(DELETE_POLICY, "execute", delete_mock):
        result = await storage.update_policy(policy)

    assert result is policy
    assert upsert_mock.await_count == 1, "update_policy must UPSERT once"
    assert get_mock.await_count == 0, "update_policy must NOT issue GET_POLICY"
    assert delete_mock.await_count == 0, "update_policy must NOT issue DELETE_POLICY"

    assert upsert_mock.await_args is not None
    upsert_kwargs = upsert_mock.await_args.kwargs
    assert upsert_kwargs["id"] == "public_access"
    assert upsert_kwargs["partition_key"] == "global"
    # actions are serialized through Policy's normalisation; we only assert
    # that they reach the upsert as a JSON string, not the exact form.
    json.loads(upsert_kwargs["actions"])


@pytest.mark.asyncio
async def test_get_policy_passes_partition_key_to_query() -> None:
    """``PostgresPolicyStorage.get_policy`` must thread ``partition_key``
    into the SQL parameters so the partition filter actually fires."""
    storage = PostgresPolicyStorage.__new__(PostgresPolicyStorage)
    storage.engine = MagicMock()

    get_mock = AsyncMock(return_value=None)
    fake_db = MagicMock()
    mt_cm = MagicMock()
    mt_cm.__aenter__ = AsyncMock(return_value=fake_db)
    mt_cm.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.postgres_policy_storage.managed_transaction",
        return_value=mt_cm,
    ), patch.object(GET_POLICY, "execute", get_mock):
        await storage.get_policy("public_access", partition_key="_system_")

    assert get_mock.await_count == 1
    assert get_mock.await_args is not None
    kwargs = get_mock.await_args.kwargs
    assert kwargs["id"] == "public_access"
    assert kwargs["partition_key"] == "_system_"


@pytest.mark.asyncio
async def test_delete_policy_passes_partition_key_to_query() -> None:
    """``PostgresPolicyStorage.delete_policy`` must thread ``partition_key``
    so admin deletes never cascade across partitions."""
    storage = PostgresPolicyStorage.__new__(PostgresPolicyStorage)
    storage.engine = MagicMock()

    delete_mock = AsyncMock(return_value=1)
    fake_db = MagicMock()
    mt_cm = MagicMock()
    mt_cm.__aenter__ = AsyncMock(return_value=fake_db)
    mt_cm.__aexit__ = AsyncMock(return_value=None)

    with patch(
        "dynastore.modules.iam.postgres_policy_storage.managed_transaction",
        return_value=mt_cm,
    ), patch.object(DELETE_POLICY, "execute", delete_mock):
        result = await storage.delete_policy("public_access", partition_key="_system_")

    assert result is True
    assert delete_mock.await_args is not None
    kwargs = delete_mock.await_args.kwargs
    assert kwargs["id"] == "public_access"
    assert kwargs["partition_key"] == "_system_"


def test_default_partition_key_is_global() -> None:
    """Backward-compat: callers that don't pass ``partition_key`` get
    ``"global"`` so existing un-tenanted code paths keep working."""
    import inspect

    sig_get = inspect.signature(PostgresPolicyStorage.get_policy)
    sig_del = inspect.signature(PostgresPolicyStorage.delete_policy)

    assert sig_get.parameters["partition_key"].default == "global"
    assert sig_del.parameters["partition_key"].default == "global"
