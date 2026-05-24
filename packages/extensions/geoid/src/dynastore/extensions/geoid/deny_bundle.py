"""Catalog-wide deny-all-except-lookup policy bundle generator (#284).

Given a ``catalog_id``, :func:`build_lookup_only_deny_bundle` emits the full
catalog-scoped policy bundle for the lookup-only-anonymous-write demo:

* **DENY-all** for every OGC + platform service prefix and their
  catalog-scoped variants ``/{service}/catalogs/{catalog_id}/...`` —
  ``/stac``, ``/features``, ``/coverages``, ``/records``, ``/tiles``,
  ``/processes``, ``/edr``, ``/maps``, ``/styles``, ``/dggs``, ``/consys``,
  ``/moving_features``.
* **ALLOW carve-outs** for the routes the use case needs:
    * ``POST /features/catalogs/{cat}/collections/(coll1|coll2)/items``
      gated by the ``collection_write_anonymous_allowed`` condition (B4).
    * ``GET|POST /search`` and ``/search/catalogs/{cat}`` gated by the
      ``lookup_only_search`` condition (B1).

The returned objects are :class:`~dynastore.models.auth.Policy` instances
whose field shape matches the ``PolicyCreate`` body that
``POST /admin/policies?catalog_id=...`` accepts (id, description, actions,
resources, effect, priority, conditions). Push them as-is via
``Policy.model_dump`` or feed them to ``PermissionProtocol.create_policy``.

Deny-precedence note: the ALLOW carve-outs ride a higher ``priority`` than
the DENY baseline so the opened routes surface while the rest of the
catalog stays denied. On equal priority DENY wins, so the priority gap is
load-bearing.

The condition handlers ``lookup_only_search`` (B1) and
``collection_write_anonymous_allowed`` (B4) are referenced by type name
only — this generator is config-driven and does not require those handlers
to be implemented to produce a valid bundle.
"""
from __future__ import annotations

import re
from typing import List, Sequence

from dynastore.models.auth import Condition, Policy

__all__ = [
    "DEFAULT_DENY_SERVICE_PREFIXES",
    "build_lookup_only_deny_bundle",
]

#: Every OGC + platform service prefix locked down by the DENY baseline.
#: ``consys`` is the connected-systems route prefix; ``moving_features`` the
#: OGC Moving Features prefix. Parameterised so callers can extend/trim the
#: surface without editing the generator.
DEFAULT_DENY_SERVICE_PREFIXES: tuple[str, ...] = (
    "stac",
    "features",
    "coverages",
    "records",
    "tiles",
    "processes",
    "edr",
    "maps",
    "styles",
    "dggs",
    "consys",
    "moving_features",
)

# Priority of the ALLOW carve-outs. Must exceed the DENY baseline so the
# opened routes win the deny-precedence tie-break.
_DENY_PRIORITY = 0
_ALLOW_PRIORITY = 100

# All HTTP verbs — the DENY baseline blocks every method on the locked
# service prefixes.
_ALL_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


def _deny_resources(prefix: str, catalog_id: str) -> List[str]:
    """Resource regexes for one service prefix.

    Covers both the bare service surface (``/{prefix}`` and anything below
    it) and the explicit catalog-scoped variant
    ``/{prefix}/catalogs/{catalog_id}/...`` so the DENY holds regardless of
    which routing shape the request uses.
    """
    cat = re.escape(catalog_id)
    return [
        rf"/{prefix}(/.*)?",
        rf"/{prefix}/catalogs/{cat}(/.*)?",
    ]


def build_lookup_only_deny_bundle(
    catalog_id: str,
    *,
    write_collections: Sequence[str] = (),
    service_prefixes: Sequence[str] = DEFAULT_DENY_SERVICE_PREFIXES,
    id_prefix: str = "lookup_only",
) -> List[Policy]:
    """Build the catalog-scoped deny-all-except-lookup policy bundle.

    Args:
        catalog_id: The catalog the bundle locks down. All resource regexes
            and policy ids are scoped to it.
        write_collections: Collection ids that may receive anonymous item
            writes via ``POST /features/.../collections/{coll}/items``,
            gated by ``collection_write_anonymous_allowed``. When empty no
            write carve-out is emitted.
        service_prefixes: Service prefixes to DENY. Defaults to the full
            OGC + platform set (:data:`DEFAULT_DENY_SERVICE_PREFIXES`).
        id_prefix: Prefix for generated policy ids, keeping a catalog's
            bundle namespaced and idempotent to re-push.

    Returns:
        A list of :class:`Policy` objects: one DENY per service prefix, an
        ALLOW lookup-search carve-out, and (if ``write_collections``) an
        ALLOW write carve-out. Catalog-scoped throughout.
    """
    if not catalog_id:
        raise ValueError("catalog_id is required")

    cat = re.escape(catalog_id)
    policies: List[Policy] = []

    # 1. DENY baseline — one policy per service prefix.
    for prefix in service_prefixes:
        policies.append(
            Policy(
                id=f"{id_prefix}_{catalog_id}_deny_{prefix}",
                description=(
                    f"DENY all access to /{prefix} (and "
                    f"/{prefix}/catalogs/{catalog_id}/...) for catalog "
                    f"{catalog_id} — lookup-only baseline."
                ),
                actions=_ALL_METHODS,
                resources=_deny_resources(prefix, catalog_id),
                effect="DENY",
                priority=_DENY_PRIORITY,
            )
        )

    # 2. ALLOW carve-out — lookup-only search on /search and
    #    /search/catalogs/{cat}, gated by lookup_only_search (B1).
    policies.append(
        Policy(
            id=f"{id_prefix}_{catalog_id}_allow_lookup_search",
            description=(
                "ALLOW GET|POST /search and /search/catalogs/"
                f"{catalog_id} gated by lookup_only_search — anonymous "
                "callers may resolve a single record by lookup field only."
            ),
            actions=["GET", "POST"],
            resources=[
                r"/search(/.*)?",
                rf"/search/catalogs/{cat}(/.*)?",
            ],
            effect="ALLOW",
            priority=_ALLOW_PRIORITY,
            conditions=[Condition(type="lookup_only_search")],
        )
    )

    # 3. ALLOW carve-out — anonymous item write on the named collections,
    #    gated by collection_write_anonymous_allowed (B4).
    if write_collections:
        coll_group = "|".join(re.escape(c) for c in write_collections)
        policies.append(
            Policy(
                id=f"{id_prefix}_{catalog_id}_allow_collection_write",
                description=(
                    "ALLOW POST /features/catalogs/"
                    f"{catalog_id}/collections/({coll_group})/items gated by "
                    "collection_write_anonymous_allowed — anonymous create "
                    "on the opted-in intake collections only."
                ),
                actions=["POST"],
                resources=[
                    rf"/features/catalogs/{cat}/collections/({coll_group})/items"
                ],
                effect="ALLOW",
                priority=_ALLOW_PRIORITY,
                conditions=[Condition(type="collection_write_anonymous_allowed")],
            )
        )

    return policies
