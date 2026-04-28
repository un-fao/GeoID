"""Platform alias helpers — ensure_public_alias_exists, add_/remove_index_to_public_alias.

These helpers wrap a small set of OpenSearch alias operations behind
defensive logging. Tests mock the ES client (``_get_client``) and the
prefix lookup (``_get_prefix``) so they exercise the helper logic
without an ES cluster.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.elasticsearch.aliases import (
    add_index_to_public_alias,
    ensure_public_alias_exists,
    remove_index_from_public_alias,
)


def _mock_es_client(exists_alias_returns: bool = False, raise_on: str | None = None):
    """Build an AsyncMock simulating the relevant ES indices subset."""
    es = AsyncMock()
    if raise_on == "exists_alias":
        es.indices.exists_alias = AsyncMock(side_effect=RuntimeError("boom"))
    else:
        es.indices.exists_alias = AsyncMock(return_value=exists_alias_returns)
    if raise_on == "update_aliases":
        es.indices.update_aliases = AsyncMock(side_effect=RuntimeError("boom"))
    else:
        es.indices.update_aliases = AsyncMock()
    return es


@pytest.mark.asyncio
async def test_ensure_public_alias_exists_no_op_when_alias_present():
    es = _mock_es_client(exists_alias_returns=True)
    with patch(
        "dynastore.modules.elasticsearch.aliases._get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.aliases._get_prefix", return_value="dynastore",
    ):
        await ensure_public_alias_exists()
    es.indices.exists_alias.assert_awaited_once_with(name="dynastore-items")
    es.indices.update_aliases.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_public_alias_exists_skips_when_client_none():
    """ES client not initialised → debug log, no calls."""
    with patch(
        "dynastore.modules.elasticsearch.aliases._get_client", return_value=None,
    ):
        await ensure_public_alias_exists()  # must not raise


@pytest.mark.asyncio
async def test_ensure_public_alias_exists_swallows_check_failure():
    """exists_alias raising → swallowed (logged), no propagation."""
    es = _mock_es_client(raise_on="exists_alias")
    with patch(
        "dynastore.modules.elasticsearch.aliases._get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.aliases._get_prefix", return_value="dynastore",
    ):
        await ensure_public_alias_exists()  # must not raise


@pytest.mark.asyncio
async def test_add_index_to_public_alias_issues_correct_action():
    es = _mock_es_client()
    with patch(
        "dynastore.modules.elasticsearch.aliases._get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.aliases._get_prefix", return_value="dynastore",
    ):
        await add_index_to_public_alias("dynastore-tenantA-items")
    es.indices.update_aliases.assert_awaited_once()
    body = es.indices.update_aliases.call_args.kwargs["body"]
    assert body == {
        "actions": [{"add": {
            "index": "dynastore-tenantA-items",
            "alias": "dynastore-items",
        }}]
    }


@pytest.mark.asyncio
async def test_add_index_to_public_alias_swallows_es_failure():
    """Failed alias add is logged but does not propagate."""
    es = _mock_es_client(raise_on="update_aliases")
    with patch(
        "dynastore.modules.elasticsearch.aliases._get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.aliases._get_prefix", return_value="dynastore",
    ):
        await add_index_to_public_alias("dynastore-tenantA-items")  # must not raise


@pytest.mark.asyncio
async def test_remove_index_from_public_alias_issues_correct_action():
    es = _mock_es_client()
    with patch(
        "dynastore.modules.elasticsearch.aliases._get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.aliases._get_prefix", return_value="dynastore",
    ):
        await remove_index_from_public_alias("dynastore-tenantA-items")
    body = es.indices.update_aliases.call_args.kwargs["body"]
    assert body == {
        "actions": [{"remove": {
            "index": "dynastore-tenantA-items",
            "alias": "dynastore-items",
        }}]
    }


@pytest.mark.asyncio
async def test_remove_index_from_public_alias_swallows_failure():
    """Removing a non-member is silently absorbed (debug log only)."""
    es = _mock_es_client(raise_on="update_aliases")
    with patch(
        "dynastore.modules.elasticsearch.aliases._get_client", return_value=es,
    ), patch(
        "dynastore.modules.elasticsearch.aliases._get_prefix", return_value="dynastore",
    ):
        await remove_index_from_public_alias("dynastore-tenantA-items")  # must not raise
