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

# dynastore/modules/styles/db.py

import uuid
import logging
from typing import List, Optional
import json

from sqlalchemy import text

from dynastore.modules.db_config.query_executor import DbResource, DQLQuery, ResultHandler, DDLQuery
from .models import Style, StyleCreate, StyleUpdate, Link, StyleSheet

logger = logging.getLogger(__name__)

# --- Query Definitions ---
# By defining queries as reusable objects, we centralize SQL logic and
# adhere to the project's Query Executor Pattern for consistency and safety.

_create_style_query = DQLQuery(
    """
    INSERT INTO styles.styles (catalog_id, collection_id, style_id, title, description, keywords, stylesheets)
    VALUES (:catalog_id, :collection_id, :style_id, :title, :description, :keywords, :stylesheets)
    RETURNING *;
    """,
    # Return a dictionary to allow key-based access in the enrichment function.
    result_handler=ResultHandler.ONE_DICT
)

_get_style_by_id_query = DQLQuery(
    "SELECT * FROM styles.styles WHERE catalog_id = :catalog_id AND style_id = :style_id;",
    result_handler=ResultHandler.ONE_DICT
)

_get_style_by_id_and_collection_query = DQLQuery(
    "SELECT * FROM styles.styles WHERE catalog_id = :catalog_id AND collection_id = :collection_id AND style_id = :style_id;",
    result_handler=ResultHandler.ONE_DICT,
)

_list_styles_query = DQLQuery(
    """
    SELECT * FROM styles.styles 
    WHERE catalog_id = :catalog_id AND collection_id = :collection_id
    ORDER BY style_id
    LIMIT :limit OFFSET :offset;
    """,
    result_handler=ResultHandler.ALL_DICTS
)

_delete_style_query = DQLQuery(
    "DELETE FROM styles.styles WHERE catalog_id = :catalog_id AND id = :style_uuid;",
    result_handler=ResultHandler.ROWCOUNT
)

# --- Helper Functions ---

def _enrich_style_from_row(row: dict, root_url: str = "") -> Optional[Style]:
    """
    Constructs a Style object from a database row, enriching it with dynamic links.
    This is necessary because links are not stored in the database.
    """
    if not row:
        return None
    
    base_path = f"{root_url}/styles/catalogs/{row['catalog_id']}/collections/{row['collection_id']}/styles/{row['style_id']}"
    
    # Enrich stylesheets with their specific links
    enriched_stylesheets = []
    for i, ss_data in enumerate(row.get('stylesheets', [])):
        ss_link = Link(href=f"{base_path}/styleSheet{i}", rel="stylesheet", type=ss_data.get('content', {}).get('format'))
        enriched_stylesheets.append(StyleSheet(content=ss_data['content'], link=ss_link))
    
    row['stylesheets'] = enriched_stylesheets
    row['links'] = [Link(href=base_path, rel="self", type="application/json")]
    return Style.model_validate(row)

# --- Public Functions ---

async def create_style(conn: DbResource, catalog_id: str, collection_id: str, style_data: StyleCreate) -> Optional[Style]:
    """Creates a new style record. Assumes partition management is handled by DB triggers."""
    style_dict = style_data.model_dump(exclude={"links"}) # links are not stored
    # The stylesheets field needs to be explicitly serialized to a JSON string.
    # We only store the 'content' part, as links are generated dynamically.
    style_dict['stylesheets'] = json.dumps([{'content': ss['content']} for ss in style_dict['stylesheets']])

    params = {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        **style_dict
    }
    raw_row = await _create_style_query.execute(conn, **params)
    return _enrich_style_from_row(raw_row)

async def get_style_by_id(conn: DbResource, catalog_id: str, style_id: str) -> Optional[Style]:
    """Retrieves a single style by its ID within a catalog partition."""
    raw_row = await _get_style_by_id_query.execute(conn, catalog_id=catalog_id, style_id=style_id)
    return _enrich_style_from_row(raw_row) if raw_row else None

async def get_style_by_id_and_collection(conn: DbResource, catalog_id: str, collection_id: str, style_id: str) -> Optional[Style]:
    """Retrieves a single style by its unique name within a collection."""
    raw_row = await _get_style_by_id_and_collection_query.execute(conn, catalog_id=catalog_id, collection_id=collection_id, style_id=style_id)
    return _enrich_style_from_row(raw_row) if raw_row else None

async def list_styles_for_collection(conn: DbResource, catalog_id: str, collection_id: str, limit: int = 100, offset: int = 0) -> List[Optional[Style]]:
    """Lists all styles associated with a specific collection."""
    raw_rows = await _list_styles_query.execute(conn, catalog_id=catalog_id, collection_id=collection_id, limit=limit, offset=offset)
    return [_enrich_style_from_row(row) for row in raw_rows]

async def update_style(conn: DbResource, catalog_id: str, style_id: str, style_data: StyleUpdate) -> Optional[Style]:
    """Updates an existing style record."""
    update_values = style_data.model_dump(exclude_unset=True)
    if not update_values:
        logger.warning("update_style called with no values to update. Returning current style.")
        return await get_style_by_id(conn, catalog_id, style_id)

    # Serialize stylesheets if present
    if 'stylesheets' in update_values:
        update_values['stylesheets'] = json.dumps([{'content': ss['content']} for ss in update_values['stylesheets']])

    async def _update_builder(db_resource, raw_params):
        """A builder function that constructs the UPDATE query dynamically."""
        # Use the passed-in raw_params which will contain the update_values
        set_clause_keys = [key for key in raw_params if key not in ('catalog_id', 'style_id')]
        set_clause = ", ".join([f'"{key}" = :{key}' for key in set_clause_keys])

        query_str = f"""
            UPDATE styles.styles SET {set_clause}, updated_at = NOW()
            WHERE catalog_id = :catalog_id AND id = :style_id
            RETURNING *;
        """
        return text(query_str), {**raw_params}

    # Use DDLQuery.from_builder to create a one-off executor for this dynamic query.
    update_executor = DQLQuery.from_builder(_update_builder, result_handler=ResultHandler.ONE)
    raw_row = await update_executor.execute(conn, catalog_id=catalog_id, style_id=style_id, **update_values)
    return _enrich_style_from_row(raw_row) if raw_row else None

async def delete_style(conn: DbResource, catalog_id: str, style_uuid: uuid.UUID) -> bool:
    """Deletes a style record from the database."""
    rows_affected = await _delete_style_query.execute(conn, catalog_id=catalog_id, style_uuid=style_uuid)
    return rows_affected > 0
