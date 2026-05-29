#    Copyright 2025 FAO
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

"""Resolve three-category field lists (properties / stats / system) into a flat,
de-duplicated, order-preserving sequence of field names to project.

The three categories mirror the platform's system/stats/properties taxonomy:
- *system*     — identity + lifecycle fields (SYSTEM_FIELD_KEYS).
- *stats*      — computed/sidecar fields (area, perimeter, geohash, h3, …).
- *properties* — user-facing attribute fields (feature.properties).

This is the single call-site used by both the DWH join endpoint and the DWH
join export task. It delegates storage knowledge entirely to the collection
config and its sidecars, so the same request works whether a field is JSONB,
columnar, or a system column.
"""

import logging
from typing import Any, FrozenSet, List, Optional, Tuple

from dynastore.models.protocols import ItemsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

_WILDCARD = "*"


def _expand_category(
    requested: Optional[List[str]],
    universe: FrozenSet[str],
    category_name: str,
    other_universes: Optional[List[Tuple[str, FrozenSet[str]]]] = None,
) -> List[str]:
    """Expand one category list against its universe.

    - ``None``       → empty (caller does not want this category).
    - ``["*"]``      → all names in *universe* (sorted for determinism).
    - explicit list  → each name validated against *universe*.

    When ``other_universes`` is provided and an explicit name exists in another
    category, a cross-category diagnostic is raised instead of the generic
    "Unknown field" error.

    Raises ``ValueError`` with a human-readable message when an explicit name
    is not in *universe*.
    """
    if requested is None:
        return []
    if _WILDCARD in requested:
        return sorted(universe)
    result: List[str] = []
    for name in requested:
        if name not in universe:
            cross_cat: Optional[str] = None
            if other_universes:
                for other_cat, other_u in other_universes:
                    if name in other_u:
                        cross_cat = other_cat
                        break
            if cross_cat is not None:
                raise ValueError(
                    f"Field '{name}' was requested in the '{category_name}' list but "
                    f"belongs to the '{cross_cat}' category. Move it to the correct list."
                )
            raise ValueError(
                f"Unknown {category_name} field: '{name}'. "
                f"Available {category_name} fields: {sorted(universe)}"
            )
        result.append(name)
    return result


async def resolve_category_field_names(
    catalog_id: str,
    collection_id: str,
    *,
    properties: Optional[List[str]] = None,
    stats: Optional[List[str]] = None,
    system: Optional[List[str]] = None,
    join_column: Optional[str] = None,
    db_resource: Optional[Any] = None,
) -> List[str]:
    """Resolve the three category lists to a flat, de-duplicated, order-preserving
    list of field names to project.

    Each category supports ``["*"]`` (expand all available names) or an explicit
    list (validated against that category's universe). ``None`` means the caller
    does not want that category.

    *join_column* is always appended if given and not already present.

    Geometry handling is intentionally left to the caller; this function returns
    only non-geometry field names.

    Raises:
        ValueError: when an explicit name is not in its declared category, or
            when it belongs to a different category (a cross-category mis-routing
            produces a descriptive message).
        RuntimeError: when ``ItemsProtocol`` is not registered.
    """
    items_svc = get_protocol(ItemsProtocol)
    if items_svc is None:
        raise RuntimeError(
            "ItemsProtocol is not registered; cannot resolve category field names."
        )

    sys_universe: FrozenSet[str]
    stats_universe: FrozenSet[str]
    props_universe: FrozenSet[str]
    sys_universe, stats_universe, props_universe = await items_svc.get_categorized_fields(
        catalog_id, collection_id, db_resource=db_resource
    )

    props_names = _expand_category(
        properties, props_universe, "properties",
        other_universes=[("stats", stats_universe), ("system", sys_universe)],
    )
    stats_names = _expand_category(
        stats, stats_universe, "stats",
        other_universes=[("properties", props_universe), ("system", sys_universe)],
    )
    sys_names = _expand_category(
        system, sys_universe, "system",
        other_universes=[("properties", props_universe), ("stats", stats_universe)],
    )

    # De-duplicate while preserving order: properties first, then stats, system.
    seen: set[str] = set()
    result: List[str] = []
    for name in (*props_names, *stats_names, *sys_names):
        if name not in seen:
            seen.add(name)
            result.append(name)

    if join_column and join_column not in seen:
        result.append(join_column)

    return result
