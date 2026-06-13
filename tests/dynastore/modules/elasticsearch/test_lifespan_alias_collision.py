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
