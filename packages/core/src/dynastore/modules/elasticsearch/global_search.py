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

"""Cross-catalog item search over the platform public items alias.

The platform-wide alias (:func:`mappings.get_public_items_alias`,
``{prefix}-items``) spans every per-catalog public items index — each tenant
index is enrolled on its first write (``aliases.add_index_to_public_alias``).
This module is the discovery counterpart: one structural query against the
alias answers "which items match, across every catalog" without touching
per-catalog routing.

Scope and access model:

* Only the **public** driver indices are aliased here. The tenant-private
  driver maintains its own internal aliases and is deliberately out of
  scope — a cross-catalog search never sees private items.
* Catalog attribution per hit comes from the concrete index name each hit
  resolves to (``{prefix}-{catalog_id}-items``); the public item doc itself
  carries no ``catalog_id`` field (see ``build_items_query`` notes).

Callers (the STAC ``/search`` root route) map a ``None`` return — no ES
client configured — to HTTP 501, and an absent alias (fresh platform, no
writes yet) to an empty result page.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)


@dataclass
class GlobalSearchHit:
    """One cross-catalog hit: the owning catalog and the wire-shaped item."""

    catalog_id: Optional[str]
    item: Dict[str, Any]


@dataclass
class GlobalSearchPage:
    """A page of cross-catalog hits plus the exact total match count."""

    hits: List[GlobalSearchHit] = field(default_factory=list)
    total: int = 0


def catalog_id_from_index(index_name: str, prefix: str) -> Optional[str]:
    """Extract the catalog id from a per-catalog public items index name.

    The naming contract is owned by ``mappings.get_tenant_items_index``
    (``{prefix}-{catalog_id}-items``); prefix and suffix are fixed, so the
    catalog id is whatever sits between them — including ids that themselves
    contain dashes. Returns ``None`` for names outside the contract (e.g. a
    foreign index manually added to the alias).
    """
    head = f"{prefix}-"
    tail = "-items"
    if not (index_name.startswith(head) and index_name.endswith(tail)):
        return None
    cat = index_name[len(head):-len(tail)]
    return cat or None


async def search_public_items(
    *,
    ids: Optional[List[str]] = None,
    collections: Optional[List[str]] = None,
    bbox: Optional[Sequence[float]] = None,
    intersects: Optional[Dict[str, Any]] = None,
    datetime: Optional[str] = None,
    sortby: Optional[List[str]] = None,
    limit: int = 10,
    offset: int = 0,
    es_client: Any = None,
) -> Optional[GlobalSearchPage]:
    """Run one structural item search across the platform public alias.

    Returns ``None`` when no ES client is configured (the deployment has no
    platform search backend); an empty page when the alias does not exist
    yet (no public item has ever been written). ``es_client`` is injectable
    for tests; production callers leave it unset and the module client is
    resolved.
    """
    from dynastore.modules.elasticsearch.client import (
        get_client,
        get_index_prefix,
    )
    from dynastore.modules.elasticsearch.items_query import (
        PUBLIC_ENVELOPE_FIELDS,
        build_items_query,
        parse_sort,
    )
    from dynastore.modules.elasticsearch.items_projection import (
        unproject_item_from_es,
    )
    from dynastore.modules.elasticsearch.mappings import get_public_items_alias

    es = es_client if es_client is not None else get_client()
    if es is None:
        return None

    prefix = get_index_prefix()
    alias = get_public_items_alias(prefix)

    query = build_items_query(
        ids=ids,
        collections=collections,
        bbox=bbox,
        intersects=intersects,
        datetime=datetime,
        fields=PUBLIC_ENVELOPE_FIELDS,
    )
    body: Dict[str, Any] = {
        "query": query,
        "from": offset,
        "size": limit,
        "track_total_hits": True,
    }
    if sortby:
        field_clauses: List[Dict[str, Any]] = []
        for entry in sortby:
            for clause in parse_sort(entry):
                if "_score" not in clause:
                    field_clauses.append(clause)
        if field_clauses:
            body["sort"] = field_clauses + [{"_score": {"order": "desc"}}]

    try:
        resp = await es.search(index=alias, body=body)
    except Exception as exc:  # noqa: BLE001 — backend-specific error classes
        # A fresh platform has no alias until the first public item write;
        # treat the 404 as "nothing indexed yet", not an error.
        if getattr(exc, "status_code", None) == 404:
            logger.debug(
                "global_search: alias '%s' absent (no public items yet)", alias
            )
            return GlobalSearchPage()
        raise

    raw_hits = resp.get("hits", {})
    total = int(raw_hits.get("total", {}).get("value", 0))
    page = GlobalSearchPage(total=total)
    for h in raw_hits.get("hits", []):
        cat = catalog_id_from_index(h.get("_index", ""), prefix)
        page.hits.append(
            GlobalSearchHit(
                catalog_id=cat,
                item=unproject_item_from_es(h.get("_source", {})),
            )
        )
    return page
