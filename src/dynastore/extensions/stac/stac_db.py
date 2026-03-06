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

from dynastore.models.protocols import ItemsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.stac.stac_config import StacPluginConfig
import logging

logger = logging.getLogger(__name__)

from typing import Any, Dict, List, Optional, Tuple


async def get_stac_items_paginated(
    conn: Any,
    catalog_id: str,
    collection_id: str,
    limit: int,
    offset: int,
    stac_config: Optional[StacPluginConfig] = None,
) -> Tuple[list, int]:
    """
    Fetches a paginated list of items for STAC using the optimized CatalogsProtocol.
    Geometries are simplified at the database level based on the StacPluginConfig.
    """
    # 1. Resolve Items Service
    items_svc = get_protocol(ItemsProtocol)
    if not items_svc:
        raise RuntimeError("ItemsProtocol not available")

    # 2. Determine Simplification
    # Note: GeometrySidecar now handles simplification via 'simplification' param
    simplification = 0.0001
    if stac_config and stac_config.simplification:
        simplification = stac_config.simplification.default_tolerance
        # We use a single tolerance for the whole request for consistency with unified builder

    # 3. Request Features via unified ItemService (using stream for raw rows)
    # We use stream_features with as_geojson=False to get raw dictionaries.
    # This preserves 'validity' range objects and sidecar columns needed for STAC processing.

    params = {
        "limit": limit,
        "offset": offset,
        "include_total_count": True,
        "simplification": simplification,
        "geom_format": "WKB",  # pystac expect WKB or GeoJSON
        "select_columns": [
            "transaction_time",  # Exposed by AttributesSidecar as h.transaction_time
            "bbox_xmin",
            "bbox_ymin",
            "bbox_xmax",
            "bbox_ymax",  # Delegated to GeometrySidecar
        ],
    }

    stream = await items_svc.stream_features(
        conn,
        catalog_id=catalog_id,
        collection_id=collection_id,
        col_config=None,  # Will be resolved by service if None
        params=params,
        as_geojson=True, # RETURN FEATURE MODELS
        lang="*", # Let stac_generator handle specific language selection
    )

    if stream is None:
        return [], 0

    processed_result = [feature async for feature in stream]

    # 4. Get Total Count
    # We'll use get_features_count for total_count
    total_count = await items_svc.get_features_count(
        conn, catalog_id=catalog_id, collection_id=collection_id, params=params
    )

    return processed_result, total_count
