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


def get_tenant_items_index(prefix: str, catalog_id: str) -> str:
    """Per-tenant regular items index. Owned by the items ES driver."""
    return f"{prefix}-items-{catalog_id}"


def get_public_items_alias(prefix: str) -> str:
    """Platform-wide alias spanning all per-tenant regular items indexes.

    Used by OGC discovery search routes. Membership is managed by the
    items driver's `ensure_storage` (add) and routing-config
    apply-handler (remove on driver removal).
    """
    return f"{prefix}-items-public"


def get_assets_index_name(prefix: str, catalog_id: str) -> str:
    """Return the name of the assets index for a catalog."""
    return f"{prefix}-assets-{catalog_id}"


def get_collection_metadata_index(prefix: str, catalog_id: str) -> str:
    """Per-catalog collection-metadata index. Owned by ``CollectionElasticsearchDriver``."""
    return f"{prefix}_collection_metadata_{catalog_id}"


def get_collection_metadata_wildcard(prefix: str) -> str:
    """Wildcard pattern matching all per-catalog collection-metadata indexes."""
    return f"{prefix}_collection_metadata_*"


def get_search_index(
    prefix: str,
    entity_type: str,
    catalog_id: str | None = None,
) -> str:
    """Resolve the read-side index for the search service.

    Mirrors the writer-name helpers so search reads always target indexes
    that drivers actually populate. Distinct from ``get_index_name`` which
    returns the singleton bootstrap names used at lifespan time only.
    """
    if entity_type == "item":
        return (
            get_tenant_items_index(prefix, catalog_id)
            if catalog_id
            else get_public_items_alias(prefix)
        )
    if entity_type == "collection":
        return (
            get_collection_metadata_index(prefix, catalog_id)
            if catalog_id
            else get_collection_metadata_wildcard(prefix)
        )
    return get_index_name(prefix, entity_type)


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
