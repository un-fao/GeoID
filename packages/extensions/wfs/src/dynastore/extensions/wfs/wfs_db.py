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
from datetime import datetime
import logging
from typing import Dict, Any, Optional, List, Tuple
from sqlalchemy import (
    text,
    Dialect,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    JSON,
)
from sqlalchemy.dialects.postgresql import UUID  # Use specific type for UUID
from pydantic import create_model, StrictInt, StrictFloat, StrictBool, StrictStr
from sqlalchemy.ext.asyncio import AsyncConnection
from dynastore.modules.db_config import shared_queries
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    DbResource,
)
from geoalchemy2 import Geometry

from .tools import (
    get_xsd_type_from_python_type,
    get_xsd_type_from_attribute_schema_type,
)
import uuid
from pydantic import BaseModel
from dynastore.modules.tools.cql import parse_cql_filter
from ...tools.features import FeatureCollection
from ...tools.features import FeatureProperties
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import (
    ItemsProtocol,
    CatalogsProtocol,
    CollectionsProtocol,
)

logger = logging.getLogger(__name__)


def _map_protocol_type_to_sqlalchemy(proto_type: str) -> Any:
    """Map a canonical ``data_type`` (see ``dynastore.models.field_types``) to a
    SQLAlchemy type for WFS DescribeFeatureType / GML XSD generation.

    Inputs are the canonical vocabulary only — there is no legacy alias layer.
    ``date``/``time``/``timestamp`` all surface as ``xs:dateTime`` (GML has no
    finer split here); ``binary`` surfaces as text (base64 in the GML body).
    """
    pt = (proto_type or "").lower()
    if pt.startswith("geometry"):
        return Geometry
    return {
        "string": String,
        "uuid": UUID,
        "integer": Integer,
        "bigint": Integer,
        "double": Float,
        "numeric": Float,
        "boolean": Boolean,
        "date": DateTime,
        "time": DateTime,
        "timestamp": DateTime,
        "binary": String,
        "jsonb": JSON,
    }.get(pt, String)  # Fallback


# 1. New query to get both column name and data type
# _get_table_columns_query was moved to dynastore.modules.db_config.tools

# 1. New query to get both column name and data type
# _get_table_columns_query was moved to dynastore.modules.db_config.tools

# _map_pg_type_to_sqlalchemy_type was moved to dynastore.modules.db_config.tools

# _get_dynamic_field_mapping was removed in favor of ItemService.get_collection_fields



_get_first_attributes_json_query = DQLQuery(
    "SELECT attributes FROM {schema}.{table} WHERE attributes IS NOT NULL AND attributes != '{{}}'::jsonb LIMIT 1;",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)


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

    # Map proto-types to XSD types for WFS GML
    feature_schema = {}
    for name, definition in field_definitions.items():
        # Special case for geometry
        if definition.data_type.lower() == "geometry":
            feature_schema[name] = "gml:GeometryPropertyType"
        else:
            feature_schema[name] = get_xsd_type_from_python_type(
                _map_protocol_type_to_sqlalchemy(definition.data_type)
            )

    # Ensure standardized WFS ID field is present
    if "id" not in feature_schema:
        feature_schema["id"] = "xs:string"

    return feature_schema
