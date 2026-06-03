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
Build a tenant-feature doc shaped for the canonical ES envelope.

Driver-private helper used by :class:`PrivateEntityTransformer`,
:class:`ItemsElasticsearchPrivateDriver` (write_entities, index, index_bulk),
and the ``PrivateIndexTask`` runner. Lives in the private subpackage so the
platform never imports it.

Post-#1800: ``build_tenant_feature_doc`` now produces the same canonical
envelope as the public items driver (``id``=geoid, ``properties`` user-only,
``stats`` / ``system`` containers). Private differs from public ONLY by
target index/alias. The old flat shape (``geoid`` at root, no stats/system)
is no longer emitted.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def build_tenant_feature_doc(
    item: Any,
    *,
    catalog_id: str,
    collection_id: str,
    external_id: Any = None,
    asset_id: Any = None,
    known_fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a canonical-envelope doc from a Feature/dict or
    :class:`~dynastore.modules.catalog.canonical_index_read.CanonicalIndexInput`.

    Accepts:

    * A :class:`~dynastore.modules.catalog.canonical_index_read.CanonicalIndexInput`
      instance — the preferred path when the caller already has a batched PG
      read result. Delegates directly to
      :func:`~dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc`.

    * A Feature pydantic model, a STAC item dict, or a GeoJSON Feature dict —
      adapts to a minimal ``row`` and calls ``build_canonical_index_doc``.
      Stats and system sections will be empty when sidecar data is unavailable
      through this path; the canonical shape is preserved.

    The returned doc uses the canonical envelope:
    ``id``=geoid, ``catalog_id``, ``collection_id``, ``external_id``,
    ``_external_id`` tracker, ``geometry``, ``bbox``, ``properties``
    (user-only, no SYSTEM_FIELD_KEYS), ``stats`` (sidecar-derived, when
    available), ``system`` (SYSTEM_FIELD_KEYS present in the row).

    ``known_fields`` is passed to the underlying projection so undeclared
    property keys route to ``properties.extras`` when a strict overlay is
    active. Pass ``{}`` or omit for legacy fully-dynamic mode.

    ``asset_id`` is the ingestion-context asset identity (mirrors the
    public driver's ``_asset_id`` tracking field). It is projected into the
    canonical ``asset_id`` identity field.
    """
    from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc

    # --- CanonicalIndexInput fast path ---
    # When the caller already has the PG-fetched canonical input (e.g. the
    # test invariant and future callers that perform the batch read), delegate
    # straight to the canonical builder.
    try:
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput
        if isinstance(item, CanonicalIndexInput):
            return build_canonical_index_doc(
                item.row,
                resolved_sidecars=item.resolved_sidecars,
                known_fields=known_fields or {},
                catalog_id=catalog_id,
                collection_id=collection_id,
                geometry=item.geometry,
                bbox=item.bbox,
                user_properties=item.user_properties,
                access=item.access,
            )
    except ImportError:
        pass

    # --- Feature / dict path ---
    # Extract raw src dict from whatever the caller supplied.
    # The CanonicalIndexInput path returned early above; item is a Feature,
    # STAC dict, or plain dict at this point.  We cast to Any to avoid
    # pyright narrowing confusion from the CanonicalIndexInput isinstance
    # check inside the try/except above.
    _item_any: Any = item
    src: Dict[str, Any]
    if hasattr(_item_any, "model_dump"):
        src = _item_any.model_dump(by_alias=True, exclude_none=True)
    elif isinstance(_item_any, dict):
        src = _item_any
    else:
        src = dict(_item_any)

    geoid = src.get("id") or src.get("geoid")
    raw_props = src.get("properties") or {}

    # Exclude SYSTEM_FIELD_KEYS from user_properties — they live in the
    # system container on the canonical doc.
    from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS
    _sys_keys = frozenset(SYSTEM_FIELD_KEYS)
    user_props = {k: v for k, v in raw_props.items() if k not in _sys_keys}

    # Resolve external_id: explicit arg wins; fall back to source's
    # _external_id tracker or properties.external_id.
    ext = (
        external_id
        if external_id is not None
        else (src.get("_external_id") or raw_props.get("external_id"))
    )

    # Resolve asset_id: explicit arg wins; fall back to source's _asset_id.
    aid = asset_id if asset_id is not None else src.get("_asset_id")

    # Build a minimal raw row that carries the identity fields the canonical
    # builder needs for the ``system`` section.  Sidecar-columnar stats values
    # are also passed when they appear at the top level (e.g. after a
    # map_row_to_feature call that keeps them flat).
    row: Dict[str, Any] = {"geoid": geoid}
    if ext is not None:
        row["external_id"] = str(ext)
    if aid is not None:
        row["asset_id"] = str(aid)
    # Carry any SYSTEM_FIELD_KEY columns that surfaced on the raw src dict
    # (e.g. bulk-reindex reads that keep geometry_hash / validity flat).
    for sk in _sys_keys:
        if sk in src and sk != "geoid":
            row[sk] = src[sk]
        elif sk in raw_props:
            row[sk] = raw_props[sk]

    geom = src.get("geometry")
    bbox_val = src.get("bbox")

    return build_canonical_index_doc(
        row,
        resolved_sidecars=[],
        known_fields=known_fields or {},
        catalog_id=catalog_id,
        collection_id=collection_id,
        geometry=geom if isinstance(geom, dict) else None,
        bbox=list(bbox_val) if bbox_val is not None else None,
        user_properties=user_props or None,
        access=None,
    )
