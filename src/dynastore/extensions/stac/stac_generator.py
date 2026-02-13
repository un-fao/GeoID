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

# dynastore/extensions/stac/stac_generator.py
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type, cast
from uuid import UUID

import pystac
from fastapi import HTTPException, Request, status
from shapely import wkb
from shapely.geometry import mapping, shape
from sqlalchemy.ext.asyncio import AsyncConnection
from dynastore.modules.db_config.tools import get_any_engine
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
import dynastore.modules.db_config.shared_queries as shared_queries
from dynastore.extensions.tools.url import (
    get_base_url, get_parent_url, get_root_url, get_url)
from dynastore.tools.geospatial import (GeometryProcessingError,
                                        calculate_spatial_indices,
                                        process_geometry)
from dynastore.modules.catalog.catalog_config import (CollectionPluginConfig,
                                                      GeometryStorageConfig)
from dynastore.modules.stac.stac_config import (STAC_PLUGIN_CONFIG_ID,
                                                   StacPluginConfig, HierarchyStrategy)
from dynastore.extensions.tools.conformance import (Conformance,
                                                    get_active_conformance)
from dynastore.modules.catalog.tools import prepare_item_for_db
from dynastore.models.localization import STAC_LANGUAGE_EXTENSION_URI, get_language_object, localize_dict
from .stac_models import stac_localize
from dynastore.models.protocols import ConfigsProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)

from . import stac_db, asset_factory
SUPPORTED_STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/datacube/v2.3.0/schema.json",
    "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
    STAC_LANGUAGE_EXTENSION_URI
]

async def create_root_catalog(request: Request, lang: str = "en") -> Dict[str, Any]:
    """Generates the root STAC Catalog."""
    base_url = get_url(request)
    root_catalog = pystac.Catalog(
        id="dynastore-stac-root",
        description="Root Catalog for all DynaStore Geospatial Data Catalogs.",
        title="DynaStore OGC STAC API",
    )

    # Dynamically inject the current server's conformance list
    root_catalog.extra_fields["conformsTo"] = get_active_conformance().conformsTo
    root_catalog.stac_extensions.append(STAC_LANGUAGE_EXTENSION_URI)

    # Note: Root catalog metadata is currently static/hardcoded in English.
    # In a full implementation, this could be localized if stored in DB.
    # For now, we manually inject the language block for consistency if requested.
    from dynastore.models.localization import get_language_object
    root_catalog.extra_fields["language"] = get_language_object(lang).model_dump(exclude_none=True)

    root_catalog.set_self_href(base_url)
    root_catalog.add_link(
        pystac.Link(
            rel="root", target=root_catalog.get_self_href(), title="Root Catalog"
        )
    )
    root_catalog.add_link(
        pystac.Link(rel="search", target=f"{base_url}/search", title="STAC Search")
    )
    root_catalog.add_link(
        pystac.Link(
            rel="conformance",
            target=f"{get_root_url(request)}/conformance",
            media_type="application/json",
            title="API Conformance",
        )
    )

    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc: raise RuntimeError("CatalogsProtocol not available")
    all_catalogs = await catalogs_svc.list_catalogs(lang=lang, limit=1000)
    for cat in all_catalogs:
        # Localize catalog summary for the link title
        catalog_id = cat.id
        child_href = f"{base_url}/catalogs/{catalog_id}"
        root_catalog.add_link(
            pystac.Link(
                rel="child",
                target=child_href,
                media_type="application/json",
                title=cat.title.resolve(lang) if cat.title else catalog_id,
            )
        )

    root_catalog.set_root(root_catalog)
    return root_catalog.to_dict()


async def create_catalog(request: Request, catalog_id: str, lang: str = "en") -> Dict[str, Any]:
    """Generates a STAC Catalog for a specific catalog ID."""
    base_url = get_url(request)
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc: raise RuntimeError("CatalogsProtocol not available")
    catalog_metadata_model = await catalogs_svc.get_catalog_model(catalog_id)
    if not catalog_metadata_model:
        return {}

    # Localize metadata before using it in PySTAC
    meta_dict, available_langs = stac_localize(catalog_metadata_model, lang)

    catalog = pystac.Catalog(
        id=catalog_id,
        description=meta_dict.get("description")
        or f"STAC Catalog for the '{catalog_id}' database schema.",
        title=meta_dict.get("title") or f"Catalog: {catalog_id}",
    )
    
    # Inject language metadata
    if "language" in meta_dict and STAC_LANGUAGE_EXTENSION_URI not in catalog.stac_extensions:
        catalog.stac_extensions.append(STAC_LANGUAGE_EXTENSION_URI)
    if "language" in meta_dict:
        catalog.extra_fields["language"] = meta_dict["language"]
    if "languages" in meta_dict:
        catalog.extra_fields["languages"] = meta_dict["languages"]

    # Merge localized extra metadata into catalog extra fields
    if "extra_metadata" in meta_dict and isinstance(meta_dict["extra_metadata"], dict):
        catalog.extra_fields.update(meta_dict["extra_metadata"])

    catalog.set_self_href(base_url)
    if lang != '*':
        catalog.get_links("self")[0].extra_fields["hreflang"] = lang

    root_link = pystac.Link(rel="root", target=get_parent_url(request, 2), title="Root Catalog")
    parent_link = pystac.Link(rel="parent", target=get_parent_url(request, 1), title="Parent Catalog")
    
    if lang != '*':
        root_link.extra_fields["hreflang"] = lang
        parent_link.extra_fields["hreflang"] = lang

    catalog.add_link(root_link)
    catalog.add_link(parent_link)

    # Add alternate links for other languages
    if lang != '*' and available_langs:
        for other_lang in sorted(available_langs - {lang}):
             alt_url = str(request.url.replace_query_params(lang=other_lang))
             lang_meta = get_language_object(other_lang)
             catalog.add_link(pystac.Link(
                 rel="alternate",
                 target=alt_url,
                 media_type="application/json",
                 title=lang_meta.name,
                 extra_fields={"hreflang": other_lang}
             ))

    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc: raise RuntimeError("CatalogsProtocol not available")
    collections = await catalogs_svc.list_collections(catalog_id, lang=lang, limit=1000)
    for coll in collections:
        # Localize collection summary for the link title
        collection_id = coll.id
        collection_href = f"{base_url}/collections/{collection_id}"
        catalog.add_link(
            pystac.Link(
                rel="child",
                target=collection_href,
                title=coll.title.resolve(lang) if coll.title else collection_id,
                media_type="application/json",
            )
        )
    catalog.set_root(catalog)
    return catalog.to_dict()


async def create_collections_catalog(request: Request, catalog_id: str, lang: str = "en") -> Dict[str, Any]:
    """Generates the collections list for a specific catalog."""
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc: raise RuntimeError("CatalogsProtocol not available")
    collections = await catalogs_svc.list_collections(catalog_id, lang=lang, limit=1000)
    
    stac_collections = []
    for coll in collections:
        stac_coll = await create_collection(request, catalog_id, coll.id, lang=lang)
        if stac_coll:
            stac_collections.append(stac_coll.to_dict())
        
    root_url = get_root_url(request)
    links = [
        {"rel": "self", "type": "application/json", "href": f"{root_url}/stac/catalogs/{catalog_id}/collections"},
        {"rel": "parent", "type": "application/json", "href": f"{root_url}/stac/catalogs/{catalog_id}"},
        {"rel": "root", "type": "application/json", "href": f"{root_url}/stac"}
    ]
    
    return {
        "collections": stac_collections,
        "links": links
    }


async def create_collection(
    request: Request, catalog_id: str, collection_id: str, lang: str = "en"
) -> Optional[pystac.Collection]:
    """Generates a full STAC Collection for a specific database table."""
    catalogs_svc = get_protocol(CatalogsProtocol)
    if not catalogs_svc: raise RuntimeError("CatalogsProtocol not available")
    metadata_model, layer_config = await asyncio.gather(
        catalogs_svc.get_collection_model(catalog_id, collection_id),
        catalogs_svc.get_collection_config(catalog_id, collection_id),
    )
    if not metadata_model:
        return None

    # Localize metadata
    meta_dict, available_langs = stac_localize(metadata_model, lang)

    # Fetch STAC-specific config to inject extensions like datacube
    config_manager = get_protocol(ConfigsProtocol)
    if not config_manager: raise RuntimeError("ConfigsProtocol not available")
    stac_config: StacPluginConfig = cast(StacPluginConfig, await config_manager.get_config(STAC_PLUGIN_CONFIG_ID, catalog_id, collection_id))

    # Correctly handle the Extent object and its attributes
    spatial_bbox = [0, 0, 0, 0]
    if metadata_model.extent and metadata_model.extent.spatial and metadata_model.extent.spatial.bbox:
        # The model stores bbox as a list of tuples, get the first one.
        spatial_bbox = metadata_model.extent.spatial.bbox[0]

    spatial_extent = pystac.SpatialExtent([list(spatial_bbox)])

    temporal_interval_dates = [None, None]
    if metadata_model.extent and metadata_model.extent.temporal and metadata_model.extent.temporal.interval:
        # The model stores interval as a list of tuples, get the first one.
        start_dt, end_dt = metadata_model.extent.temporal.interval[0]
        if start_dt and end_dt:
            temporal_interval_dates = [dt.replace(tzinfo=timezone.utc) for dt in (start_dt, end_dt)]

    temporal_extent = pystac.TemporalExtent(intervals=[temporal_interval_dates])
    extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)

    stac_extensions_to_add = [STAC_LANGUAGE_EXTENSION_URI]
    # Add extensions based on config
    if stac_config.cube_dimensions or stac_config.cube_variables:
        stac_extensions_to_add.append(SUPPORTED_STAC_EXTENSIONS[0])
    
    # Cast layer_config components to expected types to handle Immutable wrappers
    storage_config = None
    if layer_config:
        storage_config = cast(GeometryStorageConfig, layer_config.geometry_storage)
        if storage_config.target_srid != 4326:
            stac_extensions_to_add.append(SUPPORTED_STAC_EXTENSIONS[1])

    collection = pystac.Collection(
        id=collection_id,
        description=meta_dict.get("description") or f"Data from '{catalog_id}:{collection_id}'",
        extent=extent,
        title=meta_dict.get("title") or collection_id,
        stac_extensions=stac_extensions_to_add,
        keywords=meta_dict.get("keywords"),
        license=meta_dict.get("license") or "proprietary",
    )

    # Inject language metadata
    if "language" in meta_dict:
        collection.extra_fields["language"] = meta_dict["language"]
    if "languages" in meta_dict:
        collection.extra_fields["languages"] = meta_dict["languages"]

    # Merge localized extra metadata into collection extra fields
    if "extra_metadata" in meta_dict and isinstance(meta_dict["extra_metadata"], dict):
        collection.extra_fields.update(meta_dict["extra_metadata"])

    # Add datacube dimensions and variables from config
    if stac_config.cube_dimensions:
        collection.extra_fields["cube:dimensions"] = {k: v.model_dump(exclude_none=True) for k, v in stac_config.cube_dimensions.items()}
    
    if stac_config.cube_variables:
        collection.extra_fields["cube:variables"] = {k: v.model_dump(exclude_none=True) for k, v in stac_config.cube_variables.items()}

    if SUPPORTED_STAC_EXTENSIONS[1] in stac_extensions_to_add and storage_config:
        collection.extra_fields["proj:epsg"] = storage_config.target_srid
        collection.extra_fields["proj:wkt2"] = None

    # Set the root catalog to avoid pystac trying to resolve it via network
    root_catalog = pystac.Catalog(
        id="root",
        description="Root Catalog",
        href=get_root_url(request)
    )
    collection.set_root(root_catalog)

    base_url = get_url(request)
    collection.set_self_href(base_url)
    if lang != '*':
        collection.get_links("self")[0].extra_fields["hreflang"] = lang

    root_link = pystac.Link(rel="root", target=get_root_url(request), title="Root Catalog")
    parent_link = pystac.Link(rel="parent", target=get_parent_url(request, 1), title="Parent Catalog")
    items_link = pystac.Link(rel="items", target=f"{base_url}/items", media_type="application/geo+json", title="Items in this Collection")

    if lang != '*':
        root_link.extra_fields["hreflang"] = lang
        parent_link.extra_fields["hreflang"] = lang
        items_link.extra_fields["hreflang"] = lang

    collection.add_links([root_link, parent_link, items_link])

    # Add alternate links for other languages
    if lang != '*' and available_langs:
        for other_lang in sorted(available_langs - {lang}):
             alt_url = str(request.url.replace_query_params(lang=other_lang))
             lang_meta = get_language_object(other_lang)
             collection.add_link(pystac.Link(
                 rel="alternate",
                 target=alt_url,
                 media_type="application/json",
                 title=lang_meta.name,
                 extra_fields={"hreflang": other_lang}
             ))

    return collection


def _evaluate_condition(condition: str, properties: Dict[str, Any]) -> bool:
    """
    Evaluates a CQL2-TEXT condition string against feature properties.
    Uses the pygeofilter-based CQL parser for robust and standards-compliant evaluation.
    
    Args:
        condition: A CQL2-TEXT filter expression (e.g., "prop='value'", "prop IS NULL", "prop > 10")
        properties: Dictionary of feature properties to evaluate against
    
    Returns:
        True if the condition matches, False otherwise
    """
    if not condition:
        return True
    
    try:
        from dynastore.modules.tools.cql import parse_cql_filter, PYGEOFILTER_AVAILABLE
        from sqlalchemy import text
        from sqlalchemy.sql import column as sql_column
        
        if not PYGEOFILTER_AVAILABLE:
            # Fallback to simple equality check if pygeofilter is not available
            logger.warning("pygeofilter not available, using simple equality check")
            if "=" in condition:
                key, val = condition.split("=", 1)
                key = key.strip().lower()
                val = val.strip().replace("'", "").replace('"', "")
                return str(properties.get(key)) == val
            return False
        
        # Build field mapping for CQL parser - map property names to mock column accessors
        field_mapping = {}
        for prop_name in properties.keys():
            field_mapping[prop_name] = sql_column(prop_name)
        
        # Parse the CQL condition
        sql_where, params = parse_cql_filter(
            condition,
            field_mapping=field_mapping,
            valid_props=set(properties.keys()),
            parser_type='cql2'
        )
        
        if not sql_where:
            return True  # Empty filter means match all
        
        # Evaluate the condition in-memory by substituting values
        # This is a simple evaluator for basic conditions
        # For complex spatial predicates, this would need enhancement
        eval_expr = sql_where
        for param_name, param_value in params.items():
            # Replace bind parameters with actual values
            if isinstance(param_value, str):
                eval_expr = eval_expr.replace(f":{param_name}", f"'{param_value}'")
            else:
                eval_expr = eval_expr.replace(f":{param_name}", str(param_value))
        
        # Replace column references with property values
        for prop_name, prop_value in properties.items():
            # Handle NULL values
            if prop_value is None:
                eval_expr = eval_expr.replace(prop_name, "NULL")
            elif isinstance(prop_value, str):
                eval_expr = eval_expr.replace(prop_name, f"'{prop_value}'")
            else:
                eval_expr = eval_expr.replace(prop_name, str(prop_value))
        
        # Simple evaluation using Python's eval (safe for our controlled expressions)
        # Convert SQL operators to Python
        eval_expr = eval_expr.replace(" = ", " == ")
        eval_expr = eval_expr.replace(" AND ", " and ")
        eval_expr = eval_expr.replace(" OR ", " or ")
        eval_expr = eval_expr.replace(" NOT ", " not ")
        eval_expr = eval_expr.replace(" IS NULL", " is None")
        eval_expr = eval_expr.replace(" IS NOT NULL", " is not None")
        eval_expr = eval_expr.replace("NULL", "None")
        
        # Evaluate the expression
        result = eval(eval_expr)
        return bool(result)
        
    except Exception as e:
        logger.warning(f"Failed to evaluate hierarchy condition '{condition}': {e}")
        return False

def apply_hierarchy_links(
    item: pystac.Item,
    feature_properties: Dict[str, Any],
    asset_id: str,
    config: StacPluginConfig,
    catalog_id: str,
    collection_id: str,
    collection_url: str,
    source_collection_url: str,
    view_mode: str = "standard"
) -> pystac.Item:
    """
    Dynamically applies parent, child, and source links to a STAC Item based on configuration and View Mode.

    Args:
        item: The pystac.Item object to modify.
        feature_properties: The properties of the feature, used to evaluate hierarchy rules.
        asset_id: The ID linking this feature to its source file item.
        config: The StacPluginConfig for the collection.
        collection_url: The base URL of the current collection.
        source_collection_url: The base URL of the shadow `_sources` collection.
        view_mode: "standard" or "virtual".

    Returns:
        The modified pystac.Item with added links.
    """
    # 1. Virtual View Check
    if view_mode.startswith("virtual"):
        # The virtual URL structure: 
        # /stac/virtual/assets/{asset_id}/catalogs/{id}/collections/{coll_id}
        # /stac/virtual/hierarchy/{hierarchy_id}/catalogs/{id}/collections/{coll_id}

        # Extract root URL from the item's root catalog or derive from collection_url
        root_catalog = item.get_root()
        if root_catalog and hasattr(root_catalog, 'get_self_href'):
            root_href = root_catalog.get_self_href()
            if root_href:
                root_url = root_href
            else:
                # Fallback: derive from collection_url
                root_url = collection_url.rsplit('/catalogs/', 1)[0] if '/catalogs/' in collection_url else collection_url
        else:
            # Fallback: derive from collection_url
            root_url = collection_url.rsplit('/catalogs/', 1)[0] if '/catalogs/' in collection_url else collection_url
        
        if view_mode == "virtual-asset" and asset_id:
            virtual_coll_url = f"{root_url}/stac/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}"
            item.add_link(
                pystac.Link(rel="parent", target=virtual_coll_url, title=f"Source Asset: {asset_id}")
            )
            item.add_link(
                 pystac.Link(rel="collection", target=virtual_coll_url, title=f"Source Asset: {asset_id}")
            )
            return item

        if view_mode == "virtual-hierarchy":
            # For hierarchy, we need to know WHICH hierarchy_id we are in.
            # We can try to find the matching rule.
            for rule in config.hierarchy.rules.values():
                if _evaluate_condition(rule.condition, feature_properties):
                    # Link to the virtual collection for this hierarchy level
                    hier_coll_url = f"{root_url}/stac/virtual/hierarchy/{rule.hierarchy_id}/catalogs/{catalog_id}/collections/{collection_id}"
                    
                    # If this is a child level, we can scope it to the parent value
                    if rule.parent_code_property:
                        parent_val = feature_properties.get(rule.parent_code_property)
                        if parent_val:
                            hier_coll_url += f"?parent_value={parent_val}"
                    
                    item.add_link(
                        pystac.Link(rel="parent", target=hier_coll_url, title=f"Level: {rule.level_name or rule.hierarchy_id}")
                    )
                    item.add_link(
                        pystac.Link(rel="collection", target=hier_coll_url, title=f"Level: {rule.level_name or rule.hierarchy_id}")
                    )
                    break
            return item


    # Note: Asset lineage linking is now handled dynamically in asset_factory.

    # 3. Standard View: Dynamic Hierarchy Rules
    if config.hierarchy and config.hierarchy.enabled:
        for rule in config.hierarchy.rules:
            # Recursive Strategy
            if rule.strategy == HierarchyStrategy.RECURSIVE:
                 # Check if this item is a root in the recursive graph
                 is_root = False
                 if rule.root_condition:
                     is_root = _evaluate_condition(rule.root_condition, feature_properties)
                 elif rule.parent_code_field:
                     # Implicit root if parent field is null
                     parent_val = feature_properties.get(rule.parent_code_property)
                     is_root = parent_val is None
                
                 if is_root:
                     # It's a root item, so it links to the Collection (already handled by default)
                     # Or we can explicitly tag it.
                     pass 
                 else:
                     # It's a child node. Parent is the item with ID specified in parent_code_field
                     if rule.parent_code_field:
                         parent_code = feature_properties.get(rule.parent_code_property)
                         if parent_code:
                             parent_item_url = f"{collection_url}/items/{parent_code}"
                             item.add_link(pystac.Link(rel="parent", target=parent_item_url, media_type="application/geo+json"))

            # Fixed Strategy (Default)
            else: 
                # Evaluate condition to see if this rule applies to the current item
                if _evaluate_condition(rule.condition, feature_properties):
                    # Rule Matches: This item belongs to this level.
                    # Find its parent using parent_code_property
                    if rule.parent_code_property:
                        parent_code = feature_properties.get(rule.parent_code_property)
                        if parent_code:
                            parent_item_url = f"{collection_url}/items/{parent_code}"
                            item.add_link(pystac.Link(rel="parent", target=parent_item_url, media_type="application/geo+json"))
                    break  # Stop after the first matching rule for Fixed strategy

    return item


async def create_item_from_row(
    request: Request, catalog_id: str, collection_id: str, row: Dict, stac_config: StacPluginConfig, view_mode: str = "standard", lang: str = "en"
) -> Optional[pystac.Item]:
    """Generates a STAC Item from a single database row."""
    if not row:
        return None

    # Handle both SQLAlchemy Row objects (which have _mapping) and standard dicts.
    if hasattr(row, '_mapping'):
        row_dict = dict(row._mapping)
    else:
        row_dict = dict(row)
    
    # --- Localization of properties ---
    properties = row_dict.get("attributes", {})
    # Apply localization to properties dictionary
    localized_props, available_langs = localize_dict(properties, lang)
    properties = localized_props
    
    geom_dict = mapping(wkb.loads(row_dict["geom"])) if row_dict.get("geom") and isinstance(row_dict["geom"], bytes) else None
    bbox = [row_dict["bbox_xmin"], row_dict["bbox_ymin"], row_dict["bbox_xmax"], row_dict["bbox_ymax"]] if row_dict.get("bbox_xmax") is not None else None

    # Determine the primary datetime for the item
    item_dt = row_dict.get("valid_from") or row_dict.get("transaction_time")
    properties["start_datetime"] = item_dt.isoformat() if item_dt else None
    valid_to = row_dict.get("valid_to")
    properties["end_datetime"] = valid_to.isoformat() if valid_to and valid_to.year < 9999 else None

    if item_dt and item_dt.tzinfo is None:
        item_dt = item_dt.replace(tzinfo=timezone.utc)

    # Copy over non-attribute fields from the row to the properties dict
    # We exclude internal columns that are not part of the STAC Item representation.
    exclude_from_props = {
        "id", "geoid", "geom", "bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax", "attributes", 
        "valid_from", "valid_to", "catalog_id", "collection_id",
        "validity", "transaction_period", "was_geom_fixed", "processed_at", "recalculated_geom"
    }
    for key, value in row_dict.items():
        if key not in exclude_from_props:
            if isinstance(value, datetime): 
                value = value.isoformat()
            elif isinstance(value, UUID): 
                value = str(value)
            elif isinstance(value, bytes):
                # Should not happen for core columns after exclusions, but handles user binary data
                import base64
                value = base64.b64encode(value).decode('utf-8')
            elif hasattr(value, 'lower') and hasattr(value, 'upper') and not callable(value.lower):
                # Handle asyncpg.Range (e.g. transaction_period, though excluded above)
                lower = value.lower.isoformat() if isinstance(value.lower, datetime) else value.lower
                upper = value.upper.isoformat() if isinstance(value.upper, datetime) else value.upper
                value = [lower, upper]
            properties[key] = value

    logger.debug(f"STAC Item properties for {row_dict.get('geoid')}: {list(properties.keys())}")

    logger.debug(f"Creating STAC Item for row: geoid={row_dict.get('geoid')}, external_id={row_dict.get('external_id')}")
    item = pystac.Item(
        id=str(row_dict.get("external_id") or row_dict["geoid"]),
        geometry=geom_dict,
        bbox=bbox,
        datetime=item_dt,
        properties=properties,
        collection=collection_id
    )
    
    # Add Language Extension URI and metadata to the item
    if available_langs:
        from .stac_models import inject_stac_language_fields
        item_dict = item.to_dict()
        inject_stac_language_fields(item_dict, available_langs, lang)
        item.stac_extensions = item_dict.get('stac_extensions', item.stac_extensions)
        item.extra_fields.update({k: v for k, v in item_dict.items() if k in ['language', 'languages']})
        
    # Add hreflang to standard links
    if lang != '*':
        for link in item.links:
            if link.rel in ['self', 'root', 'parent', 'collection', 'items']:
                link.extra_fields["hreflang"] = lang

    # Add alternate links for other languages
    if lang != '*' and available_langs:
        for other_lang in sorted(available_langs - {lang}):
             alt_url = str(request.url.replace_query_params(lang=other_lang))
             lang_meta = get_language_object(other_lang)
             item.add_link(pystac.Link(
                 rel="alternate",
                 target=alt_url,
                 media_type="application/geo+json",
                 title=lang_meta.name,
                 extra_fields={"hreflang": other_lang}
             ))
    
    # Set the root catalog to avoid pystac trying to resolve it via network during serialization
    root_catalog = pystac.Catalog(
        id="root",
        description="Root Catalog",
        href=get_root_url(request)
    )
    item.set_root(root_catalog)

    root_url = get_root_url(request)
    collection_url = f"{root_url}/stac/catalogs/{catalog_id}/collections/{collection_id}"
    item.set_self_href(f"{collection_url}/items/{item.id}")
    item.add_link(pystac.Link(rel="root", target=f"{root_url}/stac", title="Root Catalog"))
    
    # Hierarchy and source links apply to all feature items.
    apply_hierarchy_links(
        item=item,
        feature_properties=item.properties,
        asset_id=row_dict.get("asset_id"),
        config=stac_config,
        catalog_id=catalog_id,
        collection_id=collection_id,
        collection_url=collection_url,
        source_collection_url="", # Deprecated
        view_mode=view_mode
    )

    # Add dynamic assets (e.g., OGC Features, Maps, Tiles, and Source File)
    asset_context = asset_factory.AssetContext(
        base_url=root_url,
        catalog_id=catalog_id,
        collection_id=collection_id,
        request=request,
        stac_config=stac_config,
        asset_id=row_dict.get("asset_id")
    )
    asset_factory.add_dynamic_assets(item, asset_context)

    return item


async def create_item_collection(
    request: Request,
    conn: AsyncConnection,
    schema: str,
    table: str,
    limit: int,
    offset: int,
    stac_config: StacPluginConfig,
    view_mode: str = "standard",
    catalog_id: str = None,
    collection_id: str = None,
    lang: str = "en"
) -> Dict[str, Any]:
    """Generates a STAC ItemCollection for a single collection."""
    # Ensure logical IDs are available for row-to-feature conversion
    catalog_id = catalog_id or schema
    collection_id = collection_id or table

    items_rows, item_count = await stac_db.get_stac_items_paginated(
        conn, catalog_id, collection_id, limit, offset, stac_config
    )

    stac_items_tasks = [
        create_item_from_row(request, catalog_id, collection_id, row, stac_config, view_mode=view_mode, lang=lang) for row in items_rows
    ]
    stac_items = await asyncio.gather(*stac_items_tasks)

    item_collection = pystac.ItemCollection(items=[item for item in stac_items if item])
    collection_dict = item_collection.to_dict()

    self_href = get_url(request, remove_qp=True)
    collection_dict.setdefault("links", []).append(
        {
            "rel": "self",
            "href": f"{self_href}?limit={limit}&offset={offset}",
            "type": "application/geo+json",
        }
    )

    if (offset + limit) < item_count:
        next_href = f"{self_href}?offset={offset + limit}&limit={limit}"
        collection_dict["links"].append(
            {
                "rel": "next",
                "href": next_href,
                "type": "application/geo+json",
                "title": "Next page",
            }
        )

    if offset > 0:
        prev_offset = max(0, offset - limit)
        prev_href = f"{self_href}?offset={prev_offset}&limit={limit}"
        collection_dict["links"].append(
            {
                "rel": "prev",
                "href": prev_href,
                "type": "application/geo+json",
                "title": "Previous page",
            }
        )

    collection_dict["numberMatched"] = item_count
    collection_dict["numberReturned"] = len(stac_items)
    return collection_dict


def create_empty_item_collection(request: Request, limit: int, offset: int) -> Dict[str, Any]:
    """Generates a valid but empty STAC ItemCollection."""
    item_collection = pystac.ItemCollection(items=[])
    collection_dict = item_collection.to_dict()
    
    self_href = get_url(request, remove_qp=True)
    collection_dict["links"] = [{
        "rel": "self",
        "href": f"{self_href}?limit={limit}&offset={offset}",
        "type": "application/geo+json",
        "title": "Self"
    }]
    collection_dict["numberMatched"] = 0
    collection_dict["numberReturned"] = 0
    return collection_dict


async def create_search_results_collection(
    request: Request, rows: List[Dict], total_count: int, limit: int, offset: int, stac_config: StacPluginConfig, lang: str = "en"
) -> Dict[str, Any]:
    """
    Generates a STAC ItemCollection from a cross-collection search result.
    This function is now a thin presentation layer over the generic search module.
    """


    # The core logic is to map each generic row from the search result to a STAC Item.
    # The `create_item_from_row` function is perfect for this task.
    stac_items_tasks = [
        create_item_from_row(request, row["catalog_id"], row["collection_id"], row, stac_config, lang=lang) # type: ignore
        for row in rows
    ]
    stac_items = await asyncio.gather(*stac_items_tasks)

    item_collection = pystac.ItemCollection(items=[item for item in stac_items if item])
    collection_dict = item_collection.to_dict()

    # Per STAC spec, POST search responses should not have next/prev links
    # but should provide context for the client to form the next request.
    # The 'context' object is deprecated in STAC API 1.0.0 for ItemCollection
    # in favor of numberMatched/numberReturned at the top level.
    # We will add them here.
    collection_dict["numberMatched"] = total_count
    collection_dict["numberReturned"] = len(stac_items)

    # Add a 'self' link to allow clients to repeat the search
    # (Note: GET request with parameters would be more standard here)
    self_href = get_url(request)
    collection_dict.setdefault("links", []).append(
        pystac.Link(
            rel="self", target=self_href, media_type="application/geo+json"
        ).to_dict()
    )

    return collection_dict


async def _process_stac_item_for_db(
    item: pystac.Item, layer_config: CollectionPluginConfig
) -> Dict[str, Any]:
    """Validates and prepares a STAC Item for database insertion."""
    if not item.geometry:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="STAC Item must have a valid geometry.",
        )

    try:
        # Pydantic/PySTAC Item.geometry is already a dict
        geom_dict = item.geometry

        # Ensure properties has explicit datetime fields derived from the Item
        props = item.properties.copy()
        if item.datetime:
            props['datetime'] = item.datetime.isoformat()

        # Call shared tool
        record = prepare_item_for_db(
            feature_geometry=geom_dict,
            feature_properties=props,
            layer_config=layer_config,
            external_id=item.id
        )
        return record

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )