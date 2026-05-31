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
#    Contact: copyright@fao.org - http://fa.org/contact-us/terms/en/

# dynastore/extensions/wfs/wfs_db.py
import logging
from typing import Dict, Optional
from sqlalchemy.ext.asyncio import AsyncConnection

from .tools import canonical_data_type_to_xsd
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import (
    ItemsProtocol,
)

logger = logging.getLogger(__name__)


async def introspect_feature_type_schema(
    conn: AsyncConnection,
    catalog_id: str,  # Logical ID
    collection_id: str,  # Logical ID
) -> Optional[Dict[str, str]]:
    """
    Introspects a collection to determine its full schema for a DescribeFeatureType response.
    Leverages ItemsProtocol to aggregate fields from all sidecars consistently.
    """
    items_svc = get_protocol(ItemsProtocol)
    if not items_svc:
        logger.error("ItemsProtocol NOT found. WFS introspection failed.")
        return None

    # Fetch full field definitions including sidecars
    field_definitions = await items_svc.get_collection_fields(
        catalog_id=catalog_id, collection_id=collection_id, db_resource=conn
    )

    if not field_definitions:
        logger.warning(
            f"DescribeFeatureType requested for empty or non-existent collection: {catalog_id}:{collection_id}"
        )
        return None

    # Map each canonical ``data_type`` straight to its WFS/GML XSD type
    # (geometry included — handled inside the mapper).
    feature_schema = {}
    for name, definition in field_definitions.items():
        feature_schema[name] = canonical_data_type_to_xsd(definition.data_type)

    # Ensure standardized WFS ID field is present
    if "id" not in feature_schema:
        feature_schema["id"] = "xs:string"

    return feature_schema
