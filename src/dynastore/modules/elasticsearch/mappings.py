"""
Elasticsearch index mappings for DynaStore STAC entities.

Design philosophy:
  - Minimal explicit mappings – only STAC v1.1.0 standard fields that ES cannot
    auto-detect correctly (geometry, dates, geopoints) are explicitly typed.
  - Dynamic templates handle everything else (multilingual text fields, hrefs,
    custom extension fields like 'eo:*', 'sat:*', 'proj:*', etc.) generically
    without requiring per-field configuration.
  - Items, collections, and catalogs can freely add STAC extension fields; they
    will be indexed by the catch-all dynamic templates automatically.
"""
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Dynamic templates – applied in order, first match wins.
# These handle the common patterns found in any STAC document.
# ---------------------------------------------------------------------------

DYNAMIC_TEMPLATES: List[Dict[str, Any]] = [
    # --- Multilingual text fields (stored as objects with lang-code keys) ---
    # title.en, title.fr, description.ar, etc. → text + .keyword for sorting
    {
        "titles": {
            "path_match": "*.title",
            "match_mapping_type": "string",
            "mapping": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
            },
        }
    },
    {
        "descriptions": {
            "path_match": "*.description",
            "match_mapping_type": "string",
            "mapping": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
            },
        }
    },
    # --- Keywords (text + keyword for aggregations) ---
    {
        "keywords": {
            "match": "keywords",
            "match_mapping_type": "string",
            "mapping": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
        }
    },
    # --- Suppress indexing of nested href/url fields (storage, not search) ---
    {
        "hrefs": {
            "match": "href",
            "mapping": {"type": "keyword", "index": False, "doc_values": False},
        }
    },
    # --- STAC extension: projection ---
    # proj:centroid → geo_point for distance queries
    {
        "proj_centroid": {
            "match": "proj:centroid",
            "mapping": {"type": "geo_point"},
        }
    },
    # proj:geometry / proj:projjson are complex objects, skip indexing
    {
        "proj_complex": {
            "match_pattern": "regex",
            "match": "proj:(projjson|geometry|bbox)",
            "mapping": {"type": "object", "enabled": False},
        }
    },
    # proj:epsg → integer
    {
        "proj_epsg": {
            "match": "proj:epsg",
            "mapping": {"type": "integer"},
        }
    },
    # --- Generic catch-all rules (must be last) ---
    # All other strings → keyword (good for filtering/aggregation)
    {
        "strings": {
            "match_mapping_type": "string",
            "mapping": {
                "type": "keyword",
                # .text sub-field for full-text search when needed
                "fields": {"text": {"type": "text", "analyzer": "standard"}},
            },
        }
    },
    # long integers → float for numeric range queries
    {
        "numerics": {
            "match_mapping_type": "long",
            "mapping": {"type": "float"},
        }
    },
]


# ---------------------------------------------------------------------------
# Explicit field mappings – only fields ES cannot auto-detect correctly.
# ---------------------------------------------------------------------------

# Fields common to all four entity types (item, collection, catalog, asset)
COMMON_PROPERTIES: Dict[str, Any] = {
    # STAC mandatory identifiers & type flags
    "id":           {"type": "keyword"},
    "catalog_id":   {"type": "keyword"},
    "collection_id": {"type": "keyword"},
    "type":         {"type": "keyword"},
    "stac_version": {"type": "keyword"},
    "stac_extensions": {"type": "keyword"},
    # Links array – not searched, only returned; suppress indexing
    "links":        {"type": "object", "enabled": False},
    # Assets object – suppressed at root; indexed separately in 'assets' index
    "assets":       {"type": "object", "enabled": False},
}

# STAC standard datetime fields shared across all entity types
STAC_DATETIME_FIELDS: Dict[str, Any] = {
    "properties": {
        "properties": {
            # STAC Item Common Metadata datetimes (RFC 3339)
            "datetime":       {"type": "date"},
            "start_datetime": {"type": "date"},
            "end_datetime":   {"type": "date"},
            "created":        {"type": "date"},
            "updated":        {"type": "date"},
            # All other item properties are handled by dynamic templates
        }
    }
}

# ---------------------------------------------------------------------------
# Index mappings per entity type
# ---------------------------------------------------------------------------

CATALOG_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": DYNAMIC_TEMPLATES,
    "numeric_detection": False,
    "properties": {
        **COMMON_PROPERTIES,
        # Catalog-level dates
        "created": {"type": "date"},
        "updated": {"type": "date"},
    },
}

COLLECTION_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": DYNAMIC_TEMPLATES,
    "numeric_detection": False,
    "properties": {
        **COMMON_PROPERTIES,
        "created": {"type": "date"},
        "updated": {"type": "date"},
        # Spatial/temporal extent – explicitly typed for geo queries
        "extent": {
            "properties": {
                "spatial": {
                    "properties": {
                        "bbox": {"type": "float"},
                    }
                },
                "temporal": {
                    "properties": {
                        "interval": {"type": "date_range"},
                    }
                },
            }
        },
    },
}

ITEM_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": DYNAMIC_TEMPLATES,
    "numeric_detection": False,
    "properties": {
        **COMMON_PROPERTIES,
        # STAC Item geometry (required, must be geo_shape)
        "geometry": {"type": "geo_shape"},
        # Bounding box
        "bbox": {"type": "float"},
        # All item properties are dynamic; only dates need explicit typing
        **STAC_DATETIME_FIELDS,
    },
}

ASSET_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": DYNAMIC_TEMPLATES,
    "numeric_detection": False,
    "properties": {
        "asset_id":      {"type": "keyword"},
        "catalog_id":    {"type": "keyword"},
        "collection_id": {"type": "keyword"},
        # Forward-compat for item-embedded asset promotion
        # (ItemAssetIndexer marker, no implementer in this PR).  Asset
        # docs sourced from item-embedded assets will populate item_id;
        # standalone catalog/collection assets leave it unset.
        "item_id":       {"type": "keyword"},
        "asset_type":    {"type": "keyword"},
        "uri":           {"type": "keyword", "index": False, "doc_values": False},
        "owned_by":      {"type": "keyword"},
        "created_at":    {"type": "date"},
        "deleted_at":    {"type": "date"},
        # metadata is dynamic — nested fields (e.g. metadata.license_id)
        # are auto-mapped by dynamic_templates (strings → keyword, longs → float)
        "metadata":      {"type": "object", "dynamic": True},
    },
}


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

MAPPINGS: Dict[str, Dict[str, Any]] = {
    "catalog":    CATALOG_MAPPING,
    "collection": COLLECTION_MAPPING,
    "item":       ITEM_MAPPING,
    "asset":      ASSET_MAPPING,
}


def get_mapping(entity_type: str) -> Dict[str, Any]:
    """Return the Elasticsearch mapping for the given STAC entity type."""
    return MAPPINGS.get(entity_type, ITEM_MAPPING)


def get_index_name(prefix: str, entity_type: str) -> str:
    """Return the index name for the given entity type and prefix."""
    return f"{prefix}-{entity_type}s"


def get_all_index_names(prefix: str) -> List[Dict[str, Any]]:
    """Return all index names with their mappings – useful for bootstrapping."""
    return [
        {"name": get_index_name(prefix, entity_type), "mapping": mapping}
        for entity_type, mapping in MAPPINGS.items()
    ]


# ---------------------------------------------------------------------------
# Tenant feature index mapping (formerly "obfuscated geoid index")
#
# Used when a catalog routes writes to the `elasticsearch_obfuscated` driver.
# Stores the full feature (geometry + properties + external_id) in a single
# index per tenant (catalog). Access is gated by:
#   - the DENY policy applied when ElasticsearchCatalogConfig.obfuscated=True
#   - the tenant-first search contract (requires catalog_id + (geoid OR
#     (external_id AND collection_id)) — see extensions/search).
#
# Docs that would exceed the ES 10MB per-doc limit are shrunk by the
# `simplify_to_fit` helper in tools/geometry_simplify.py, which records a
# `simplification_factor` and `simplification_mode` on the stored doc.
# ---------------------------------------------------------------------------

TENANT_FEATURE_MAPPING: Dict[str, Any] = {
    "dynamic": False,  # reject unknown top-level fields (typos, smuggling)
    "properties": {
        "geoid":                 {"type": "keyword"},
        "catalog_id":            {"type": "keyword"},
        "collection_id":         {"type": "keyword"},
        "external_id":           {"type": "keyword"},
        "geometry":              {"type": "geo_shape"},
        "bbox":                  {"type": "float"},
        "simplification_factor": {"type": "float"},
        "simplification_mode":   {"type": "keyword"},
        # Tenant attributes live under a dynamic sub-tree so new fields
        # are indexed without mapping updates.
        "properties":            {"type": "object", "dynamic": True},
    },
}

# Legacy alias — the 3-field mapping used by existing deployments. Referenced
# during bootstrap to detect indexes that need to be recreated with
# TENANT_FEATURE_MAPPING.
GEOID_OBFUSCATED_MAPPING_LEGACY: Dict[str, Any] = {
    "dynamic": False,
    "properties": {
        "geoid":         {"type": "keyword"},
        "catalog_id":    {"type": "keyword"},
        "collection_id": {"type": "keyword"},
    },
}


def get_obfuscated_index_name(prefix: str, catalog_id: str) -> str:
    """Return the tenant feature index name for a catalog.

    The physical name is kept as `{prefix}-geoid-{catalog_id}` for
    backward compatibility with existing deployments, even though the
    index now stores full features.
    """
    return f"{prefix}-geoid-{catalog_id}"


# Semantic alias so new call sites read clearly.
get_tenant_feature_index_name = get_obfuscated_index_name


def build_tenant_feature_doc(
    item: Any,
    *,
    catalog_id: str,
    collection_id: str,
    external_id: Any = None,
) -> Dict[str, Any]:
    """Build a `TENANT_FEATURE_MAPPING`-shaped doc from a Feature/dict.

    Accepts a Feature pydantic model, a STAC item dict, or a GeoJSON
    Feature dict. Pulls `geoid` from the item's `id`, `geometry` and
    `bbox` from the GeoJSON shape, and copies any non-internal
    `properties` (keys starting with `_` are skipped — those are
    SFEOS-internal tracking fields like `_external_id`).
    """
    if hasattr(item, "model_dump"):
        src = item.model_dump(by_alias=True, exclude_none=True)
    elif isinstance(item, dict):
        src = item
    else:
        src = dict(item)

    geoid = src.get("id") or src.get("geoid")
    raw_props = src.get("properties") or {}
    props = {k: v for k, v in raw_props.items() if not str(k).startswith("_")}

    doc: Dict[str, Any] = {
        "geoid": geoid,
        "catalog_id": catalog_id,
        "collection_id": collection_id,
    }

    ext = external_id if external_id is not None else src.get("_external_id")
    if ext is not None:
        doc["external_id"] = str(ext)

    geom = src.get("geometry")
    if geom is not None:
        doc["geometry"] = geom

    bbox = src.get("bbox")
    if bbox is not None:
        doc["bbox"] = list(bbox)

    if props:
        doc["properties"] = props

    return doc


def get_assets_index_name(prefix: str, catalog_id: str) -> str:
    """Return the name of the assets index for a catalog."""
    return f"{prefix}-assets-{catalog_id}"


def get_log_index_name(prefix: str) -> str:
    """Return the name of the logs index."""
    return f"{prefix}-logs"


LOG_MAPPING: Dict[str, Any] = {
    "dynamic": False,
    "properties": {
        "id": {"type": "keyword"},
        "catalog_id": {"type": "keyword"},
        "collection_id": {"type": "keyword"},
        "event_type": {"type": "keyword"},
        "level": {"type": "keyword"},
        "is_system": {"type": "boolean"},
        "message": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        "timestamp": {"type": "date"},
    },
}
