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

"""
Elasticsearch index mapping + naming for the private items driver.

Driver-private — not imported by the platform :mod:`modules.elasticsearch.mappings`,
which holds only platform-wide STAC mappings (catalog/collection/item/asset).

Stores the full feature (geometry + properties + external_id) in a single
index per tenant (catalog). Access is gated by the DENY policy applied
when the private driver is active. Docs that would exceed the ES 10MB
per-doc limit are shrunk by ``simplify_to_fit``
(:mod:`dynastore.tools.geometry_simplify`), which records a
``simplification_factor`` and ``simplification_mode`` on the stored doc.
"""

from __future__ import annotations

import os
from typing import Any, Dict


# Private items index uses `dynamic: false` at the top level, so only the
# `properties` subtree grows dynamically. The default 1500 is tighter than
# the public items ceiling (2000) but still absorbs the realistic
# tenant-attribute fan-out without hitting the ES default of 1000 on
# re-ingestion. Operator-tunable via `ES_PRIVATE_ITEMS_TOTAL_FIELDS_LIMIT`.


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def get_private_items_index_settings() -> Dict[str, Any]:
    """Settings dict for the per-catalog private items index."""
    return {
        "index.mapping.total_fields.limit": _int_env(
            "ES_PRIVATE_ITEMS_TOTAL_FIELDS_LIMIT", 1500,
        ),
    }


TENANT_FEATURE_MAPPING: Dict[str, Any] = {
    "dynamic": False,  # reject unknown top-level fields (typos, smuggling)
    "properties": {
        "geoid":                 {"type": "keyword"},
        "catalog_id":            {"type": "keyword"},
        "collection_id":         {"type": "keyword"},
        "external_id":           {"type": "keyword"},
        "geometry":              {"type": "geo_shape"},
        "bbox":                  {"type": "float"},
        "simplification_factor": {"type": "float"},
        "simplification_mode":   {"type": "keyword"},
        # Tenant attributes live under a dynamic sub-tree so new fields
        # are indexed without mapping updates.
        "properties":            {"type": "object", "dynamic": True},
    },
}


def get_private_index_name(prefix: str, catalog_id: str) -> str:
    """Per-catalog private items index. Owned by the private items
    driver; the platform never references this naming. Catalog-first
    naming mirrors ``get_tenant_items_index`` so all per-catalog indexes
    cluster lexicographically.
    """
    return f"{prefix}-{catalog_id}-private-items"
