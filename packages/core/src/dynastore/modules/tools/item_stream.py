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

"""
Driver-agnostic item stream normalization utilities.

The PG read path leaves columns selected outside the declared attribute schema
in the Feature's model_extra (extra='allow'), while the Elasticsearch canonical
path puts them in feature.properties directly. This mismatch caused every
feature to appear property-less on the PG path for consumers that join on
feature.properties, silently dropping all rows (see #1818).

This module provides:
  normalize_feature_attributes — lift model_extra keys into feature.properties,
                                  making the Feature layout uniform across drivers.
  stream_normalized_items      — the shared stream boundary: call stream_items
                                  and normalize each Feature before yielding.
"""

from typing import AsyncIterator, FrozenSet, TYPE_CHECKING

from dynastore.models.ogc import Feature

if TYPE_CHECKING:
    from dynastore.models.protocols import ItemsProtocol  # noqa: F401
    from dynastore.models.protocols.configs import ConfigsProtocol  # noqa: F401
    from dynastore.models.driver_context import DriverContext  # noqa: F401
    from dynastore.models.query_builder import QueryRequest  # noqa: F401
    from dynastore.modules.storage.hints import Hint  # noqa: F401
    from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType  # noqa: F401


def normalize_feature_attributes(feature: Feature) -> Feature:
    """Lift pydantic model_extra keys into feature.properties so consumers can
    join on feature.properties regardless of the backing driver.

    The PG read path leaves columns selected outside the declared attribute
    schema in the Feature's model_extra (extra='allow'); the Elasticsearch
    canonical path populates properties directly. This merges both into
    properties (existing properties wins on key collision) and clears the
    lifted extras so they are not also emitted at the top level. See #1818.

    Mutates and returns the same feature. Idempotent: a feature with no
    model_extra is returned unchanged.
    """
    extra = getattr(feature, "model_extra", None)
    if not extra:
        return feature
    props = feature.properties if feature.properties else {}
    feature.properties = {**extra, **props}
    if feature.__pydantic_extra__:
        feature.__pydantic_extra__.clear()
    return feature


async def stream_normalized_items(
    items_svc: "ItemsProtocol",
    catalog_id: str,
    collection_id: str,
    request: "QueryRequest",
    *,
    config: "ConfigsProtocol | None" = None,
    ctx: "DriverContext | None" = None,
    consumer: "ConsumerType | None" = None,
    hints: "FrozenSet[Hint]" = frozenset(),
) -> AsyncIterator[Feature]:
    """Stream via ItemsProtocol.stream_items and yield normalize_feature_attributes(f)
    for each feature. The single point where dwh join, OGC joins and feature
    export become blind to whether the store is PostgreSQL or Elasticsearch.

    See #1818: normalization ensures model_extra keys from the PG driver are
    visible in feature.properties for all downstream join and export consumers.
    """
    response = await items_svc.stream_items(
        catalog_id=catalog_id,
        collection_id=collection_id,
        request=request,
        config=config,
        ctx=ctx,
        consumer=consumer,
        hints=hints,
    )
    async for feature in response.items:
        yield normalize_feature_attributes(feature)
