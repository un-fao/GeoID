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

"""Audience ConditionHandlers — read the audience PluginConfigs to decide
whether anonymous traffic should be allowed for a given catalog or
collection.

Migrated from the geoid extension as part of the dynamic-policy
foundation (#286). The handler logic is unchanged in semantics:

* ``catalog_lookup_public_allowed`` (``CatalogLookupAudienceHandler``)
  returns True iff ``CatalogLookupAudience.is_public=True`` for
  ``ctx.catalog_id``. Used by operator policies that open a lookup
  surface (e.g. ``/search`` gated by ``lookup_only_search``).
* ``collection_write_anonymous_allowed``
  (``CollectionWriteAudienceHandler``) returns True iff
  ``CollectionWriteAudience.allow_anonymous_create=True`` for
  ``(ctx.catalog_id, parsed collection_id)``. Used by operator policies
  that open anonymous-write surfaces (STAC and OGC Features items POST).

Both fail closed on every uncertainty: missing ``ctx.catalog_id``,
unresolvable ``collection_id``, ``ConfigsProtocol`` not registered,
``get_config`` raising, or the resolved policy not being the expected
type.

The handlers register automatically into ``ConditionRegistry`` via the
core IAM module's startup path (see ``conditions.ConditionRegistry``),
so operators do not need to enable an extension to use them.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

from dynastore.modules.iam.conditions import ConditionHandler, EvaluationContext
from dynastore.modules.iam.audience_configs import (
    CatalogLookupAudience,
    CollectionWriteAudience,
)
from dynastore.tools.discovery import get_protocol

_COLLECTION_PATH_RE = re.compile(r"/catalogs/[^/]+/collections/(?P<col>[^/]+)(?:/|$)")


def _resolve_collection_id(ctx: EvaluationContext) -> Optional[str]:
    """Return collection_id from ``ctx.extras['collection_id']`` if set,
    otherwise parse it out of ``ctx.path``.

    Mirrors the path-extraction the IAM middleware does for catalog_id
    today; the middleware doesn't yet populate collection_id on its own.
    Covers both STAC and OGC-Features collection-scoped paths since
    both share the ``.../catalogs/{cat}/collections/{col}/...`` shape.
    """
    extras = getattr(ctx, "extras", None) or {}
    col = extras.get("collection_id")
    if col:
        return col
    path = getattr(ctx, "path", "") or ""
    m = _COLLECTION_PATH_RE.search(path)
    return m.group("col") if m else None


class CatalogLookupAudienceHandler(ConditionHandler):
    """Allow when ``CatalogLookupAudience.is_public=True`` for the
    request's catalog.

    Fails closed on every uncertainty: missing ``ctx.catalog_id``,
    ``ConfigsProtocol`` not registered, ``get_config`` raises, or the
    resolved policy is not a ``CatalogLookupAudience`` instance.
    """

    @property
    def type(self) -> str:
        return "catalog_lookup_public_allowed"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        from dynastore.models.protocols.configs import ConfigsProtocol

        catalog_id = ctx.catalog_id
        if not catalog_id:
            return False
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return False
        try:
            policy = await configs.get_config(CatalogLookupAudience, catalog_id=catalog_id)
        except Exception:
            return False
        return isinstance(policy, CatalogLookupAudience) and bool(policy.is_public)


class CollectionWriteAudienceHandler(ConditionHandler):
    """Allow when ``CollectionWriteAudience.allow_anonymous_create=True``
    for the request's ``(catalog_id, collection_id)``.

    The collection_id resolves from ``ctx.extras['collection_id']`` if a
    future middleware populates it, else from a regex parse of
    ``ctx.path`` — the same shape applies to STAC and OGC-Features
    collection-scoped routes, so a single handler covers both POST
    surfaces (no per-extension policy duplication).

    Fails closed on missing catalog_id, missing collection_id, missing
    ConfigsProtocol, or any error.
    """

    @property
    def type(self) -> str:
        return "collection_write_anonymous_allowed"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        from dynastore.models.protocols.configs import ConfigsProtocol

        catalog_id = ctx.catalog_id
        if not catalog_id:
            return False
        collection_id = _resolve_collection_id(ctx)
        if not collection_id:
            return False
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return False
        try:
            policy = await configs.get_config(
                CollectionWriteAudience,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
        except Exception:
            return False
        return isinstance(policy, CollectionWriteAudience) and bool(
            policy.allow_anonymous_create
        )
