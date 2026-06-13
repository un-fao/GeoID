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

"""``resolve_category_field_names`` — three-category projection resolver.

The resolver expands ``["*"]`` per category from the collection's categorized
field universe (delegated to the driver/sidecar config via
``ItemsProtocol.get_categorized_fields``), validates explicit names against the
right category (with a cross-category diagnostic), always includes the join
column, and returns a flat, de-duplicated, order-preserving name list. Geometry
is handled by the caller and intentionally absent here.
"""

from typing import FrozenSet
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tools.field_categories import resolve_category_field_names

_PATCH_TARGET = "dynastore.modules.tools.field_categories.get_protocol"


def _mock_items_svc(
    sys_universe: FrozenSet[str],
    stats_universe: FrozenSet[str],
    props_universe: FrozenSet[str],
):
    svc = MagicMock()
    svc.get_categorized_fields = AsyncMock(
        return_value=(sys_universe, stats_universe, props_universe)
    )
    return svc


@pytest.mark.asyncio
async def test_none_categories_returns_only_join_column():
    svc = _mock_items_svc(
        frozenset({"geoid", "external_id", "validity"}),
        frozenset({"area_ha", "h3_lvl5"}),
        frozenset({"code", "region"}),
    )
    with patch(_PATCH_TARGET, return_value=svc):
        result = await resolve_category_field_names(
            "cat1", "col1", join_column="geoid"
        )
    assert result == ["geoid"]


@pytest.mark.asyncio
async def test_wildcard_expands_all_categories_without_duplicates():
    svc = _mock_items_svc(
        frozenset({"geoid", "external_id"}),
        frozenset({"area_ha"}),
        frozenset({"code", "region"}),
    )
    with patch(_PATCH_TARGET, return_value=svc):
        result = await resolve_category_field_names(
            "cat1", "col1",
            properties=["*"], stats=["*"], system=["*"], join_column="geoid",
        )
    for expected in ("code", "region", "area_ha", "geoid", "external_id"):
        assert expected in result
    assert len(result) == len(set(result))


@pytest.mark.asyncio
async def test_explicit_valid_names_resolve():
    svc = _mock_items_svc(
        frozenset({"geoid", "external_id"}),
        frozenset({"area_ha", "h3_lvl5"}),
        frozenset({"code", "region"}),
    )
    with patch(_PATCH_TARGET, return_value=svc):
        result = await resolve_category_field_names(
            "cat1", "col1",
            properties=["code"], stats=["area_ha"], system=["geoid"],
            join_column="geoid",
        )
    assert {"code", "area_ha", "geoid"} <= set(result)
    assert result.count("geoid") == 1


@pytest.mark.asyncio
async def test_unknown_field_raises_with_available_list():
    svc = _mock_items_svc(frozenset({"geoid"}), frozenset({"area_ha"}), frozenset({"code"}))
    with patch(_PATCH_TARGET, return_value=svc):
        with pytest.raises(ValueError, match="Unknown properties field"):
            await resolve_category_field_names(
                "cat1", "col1", properties=["nonexistent_prop"]
            )


@pytest.mark.asyncio
async def test_cross_category_misrouting_names_the_right_category():
    svc = _mock_items_svc(
        frozenset({"geoid", "external_id"}), frozenset({"area_ha"}), frozenset({"code"})
    )
    with patch(_PATCH_TARGET, return_value=svc):
        with pytest.raises(ValueError, match="belongs to the 'system' category"):
            await resolve_category_field_names(
                "cat1", "col1", properties=["geoid"]
            )


@pytest.mark.asyncio
async def test_join_column_always_included():
    svc = _mock_items_svc(frozenset({"geoid"}), frozenset(), frozenset({"code"}))
    with patch(_PATCH_TARGET, return_value=svc):
        result = await resolve_category_field_names(
            "cat1", "col1", join_column="user_id"
        )
    assert result == ["user_id"]


@pytest.mark.asyncio
async def test_join_column_not_duplicated():
    svc = _mock_items_svc(frozenset({"geoid", "external_id"}), frozenset(), frozenset())
    with patch(_PATCH_TARGET, return_value=svc):
        result = await resolve_category_field_names(
            "cat1", "col1", system=["geoid"], join_column="geoid"
        )
    assert result.count("geoid") == 1


@pytest.mark.asyncio
async def test_all_none_no_join_column_returns_empty():
    svc = _mock_items_svc(frozenset({"geoid"}), frozenset({"area_ha"}), frozenset({"code"}))
    with patch(_PATCH_TARGET, return_value=svc):
        result = await resolve_category_field_names("cat1", "col1")
    assert result == []


@pytest.mark.asyncio
async def test_protocol_not_registered_raises_runtime_error():
    with patch(_PATCH_TARGET, return_value=None):
        with pytest.raises(RuntimeError, match="ItemsProtocol is not registered"):
            await resolve_category_field_names("cat1", "col1", properties=["code"])


@pytest.mark.asyncio
async def test_order_is_properties_then_stats_then_system():
    svc = _mock_items_svc(frozenset({"geoid"}), frozenset({"area_ha"}), frozenset({"code"}))
    with patch(_PATCH_TARGET, return_value=svc):
        result = await resolve_category_field_names(
            "cat1", "col1", properties=["code"], stats=["area_ha"], system=["geoid"]
        )
    assert result.index("code") < result.index("area_ha") < result.index("geoid")
