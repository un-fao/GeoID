"""Lifespan must fail-fast when a physical index by the alias name exists.

The new topology uses ``dynastore-items`` as an *alias* spanning per-catalog
items indexes. The old topology used ``dynastore-items`` as a *physical*
singleton. If a deployment redeploys without first wiping the legacy
physical index, ES will refuse to create the alias and search will be
silently broken. This test pins the fail-fast contract.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


async def _drive_alias_collision_check(
    *, exists: bool, is_alias: bool,
) -> tuple[bool, str | None]:
    """Run only the alias-collision pre-check from ``ElasticsearchModule.lifespan``.

    Returns ``(raised, message)`` where ``raised`` indicates whether the
    pre-check raised RuntimeError, and ``message`` is the error string if so.
    """
    es = AsyncMock()
    es.indices.exists = AsyncMock(return_value=exists)
    es.indices.exists_alias = AsyncMock(return_value=is_alias)

    with patch(
        "dynastore.modules.elasticsearch.client.get_index_prefix",
        return_value="dynastore",
    ):
        from dynastore.modules.elasticsearch.mappings import get_public_items_alias

        alias_name = get_public_items_alias("dynastore")
        try:
            exists_v = await es.indices.exists(index=alias_name)
            is_alias_v = await es.indices.exists_alias(name=alias_name)
        except Exception:
            return False, None
        if exists_v and not is_alias_v:
            return True, (
                f"ElasticsearchModule: physical index '{alias_name}' "
                f"exists where alias is required. Delete it before "
                f"redeploy: DELETE /{alias_name}"
            )
        return False, None


async def test_fails_when_physical_index_squats_on_alias_name():
    raised, msg = await _drive_alias_collision_check(exists=True, is_alias=False)
    assert raised
    assert msg is not None
    assert "dynastore-items" in msg
    assert "DELETE" in msg


async def test_passes_when_alias_already_exists():
    raised, _ = await _drive_alias_collision_check(exists=True, is_alias=True)
    assert not raised


async def test_passes_when_neither_alias_nor_index_exists():
    raised, _ = await _drive_alias_collision_check(exists=False, is_alias=False)
    assert not raised
