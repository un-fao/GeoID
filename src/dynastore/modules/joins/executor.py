"""Execute a JoinRequest: stream primary features + merge secondary lookup.

Driver-agnostic: the executor takes a primary-stream callable and an
already-materialized secondary lookup dict, so callers can wire in any
data source (DynaStore items via ItemsProtocol, BigQuery via Phase 4a's
CollectionBigQueryDriver, ad-hoc test fixtures, etc.).
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Dict

from dynastore.models.ogc import Feature
from dynastore.modules.joins.models import JoinRequest

PrimaryStream = Callable[..., AsyncIterator[Feature]]


async def run_join(
    request: JoinRequest,
    *,
    primary_stream: AsyncIterator[Feature],
    secondary_index: Dict[Any, Dict[str, Any]],
) -> AsyncIterator[Feature]:
    """Execute the join.

    Args:
        request: Validated JoinRequest.
        primary_stream: Async iterator over primary collection features.
        secondary_index: ``{join_value: row_dict}`` mapping prepared by
            the caller (typically by exhausting the secondary driver's
            stream and indexing on ``request.join.secondary_column``).

    Yields features with secondary properties merged into
    ``feature.properties`` (when ``request.join.enrichment is True``);
    otherwise just yields matching features unchanged.
    """
    join_col = request.join.primary_column
    enrich = request.join.enrichment
    proj = request.projection
    paging = request.paging
    yielded = 0
    skipped = 0

    async for feat in primary_stream:
        props = feat.properties or {}
        key = props.get(join_col)
        if key is None:
            continue
        match = secondary_index.get(key)
        if match is None:
            continue  # inner-join semantics by default — no LEFT JOIN at PR-1

        if paging is not None and skipped < paging.offset:
            skipped += 1
            continue

        merged = {**props, **match} if enrich else dict(props)
        if proj.attributes is not None:
            # Drop primary attributes the caller didn't ask for, but ALWAYS
            # keep the join key so client output stays self-describing.
            keep = set(proj.attributes) | {join_col}
            merged = {k: v for k, v in merged.items() if k in keep}

        yield Feature(
            type="Feature",
            id=feat.id,
            geometry=feat.geometry if proj.with_geometry else None,
            properties=merged,
        )
        yielded += 1
        if paging is not None and yielded >= paging.limit:
            return


async def index_secondary(
    secondary_stream: AsyncIterator[Feature],
    *,
    secondary_column: str,
) -> Dict[Any, Dict[str, Any]]:
    """Drain a secondary feature stream into a {key: properties} dict.

    Used by the joins service to pre-materialize the secondary side
    before invoking ``run_join``. Streaming both sides simultaneously
    is a future optimization; the dict-lookup approach is proven by the
    existing /dwh path.
    """
    out: Dict[Any, Dict[str, Any]] = {}
    async for feat in secondary_stream:
        props = feat.properties or {}
        key = props.get(secondary_column)
        if key is not None:
            out[key] = dict(props)
    return out
