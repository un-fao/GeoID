# DynaStore STAC API Extension

Complete implementation of OGC STAC API 1.0.0 with advanced features including virtual collections, dynamic hierarchies, and aggregations.

**Router Prefix:** `/stac`  
**Conformance:** Core, Collections, Item Search, Transactions, Filter (CQL2), Fields, Sort, Context, Aggregation

---

## Table of Contents

### Getting Started
1. [Quick Start Guide](#quick-start-guide)
2. [API Endpoint Reference](#api-endpoint-reference)
3. [Error Codes](#error-codes)

### Core Concepts
4. [STAC Items & Collections](#stac-items--collections)
5. [Geometry & CRS Handling](#geometry--crs-handling)
6. [Lazy Table Creation](#lazy-table-creation)
7. [Multiple language](#multiple-languages)

### Configuration
8. [Collection Configuration](#collection-configuration)
   - [Storage Configuration](#storage-configuration)
   - [STAC Plugin Configuration](#stac-plugin-configuration)
   - [Datacube Extension](#datacube-extension)
   - [Asset Tracking](#asset-tracking)
   - [Hierarchy Configuration](#hierarchy-configuration)
   - [Aggregation Configuration](#aggregation-configuration)
   - [Simplification Settings](#simplification-settings)

### Querying & Search
9. [Item Search](#item-search)
   - [Spatial Filters](#spatial-filters)
   - [Temporal Filters](#temporal-filters)
   - [Attribute Filters](#attribute-filters)
   - [Pagination & Sorting](#pagination--sorting)
10. [Collection Search](#collection-search)

### Advanced Features
11. [Virtual Collections](#virtual-collections)
    - [Asset-Based Views](#asset-based-views)
    - [Hierarchy-Based Views](#hierarchy-based-views)
12. [Aggregations](#aggregations)
    - [Term Aggregation](#term-aggregation)
    - [Stats Aggregation](#stats-aggregation)
    - [Geohash Aggregation](#geohash-aggregation)
    - [Datetime Aggregation](#datetime-aggregation)
    - [Bbox Aggregation](#bbox-aggregation)
    - [Temporal Extent Aggregation](#temporal-extent-aggregation)

### Reference
13. [Complete Configuration Schema](#complete-configuration-schema)
14. [Reserved Names](#reserved-names)
15. [Performance Optimization](#performance-optimization)
16. [Troubleshooting](#troubleshooting)

---

## Quick Start Guide

### 1. Create a Catalog

Catalogs map to PostgreSQL schemas and organize collections.

```bash
curl -X POST http://localhost:8000/stac/catalogs \
  -H "Content-Type: application/json" \
  -d '{
    "id": "public",
    "title": "Public Catalog",
    "description": "Shared geospatial data"
  }'
```

### 2. Create a Collection

Collections map to database tables and contain STAC items.

```bash
curl -X POST http://localhost:8000/stac/catalogs/public/collections \
  -H "Content-Type: application/json" \
  -d '{
    "id": "roads",
    "title": "Road Network",
    "description": "Primary road infrastructure",
    "extent": {
      "spatial": {"bbox": [[-180, -90, 180, 90]]},
      "temporal": {"interval": [["2020-01-01T00:00:00Z", null]]}
    }
  }'
```

### 3. Configure Storage (Recommended)

Set geometry validation, spatial indices, and versioning behavior.

```bash
curl -X PUT http://localhost:8000/stac/catalogs/public/collections/roads/config \
  -H "Content-Type: application/json" \
  -d '{
    "versioning_behavior": "create_new_version",
    "geometry_storage": {
      "target_srid": 4326,
      "allowed_geometry_types": ["LineString", "MultiLineString"],
      "invalid_geom_policy": "attempt_fix",
      "target_dimension": "force_2d",
      "write_bbox": true
    },
    "h3_resolutions": [5, 8],
    "s2_resolutions": [10, 15]
  }'
```

### 4. Add STAC Items

Insert geospatial features as STAC items.

```bash
curl -X POST http://localhost:8000/stac/catalogs/public/collections/roads/items \
  -H "Content-Type: application/json" \
  -d '{
    "type": "Feature",
    "stac_version": "1.0.0",
    "id": "road-1",
    "geometry": {
      "type": "LineString",
      "coordinates": [[12.48, 41.89], [12.50, 41.90]]
    },
    "bbox": [12.48, 41.89, 12.50, 41.90],
    "properties": {
      "start_datetime": "2024-01-01T00:00:00Z",
      "end_datetime": null,
      "name": "Via Example",
      "surface": "paved",
      "lanes": 2
    }
  }'
```

### 5. Query Items

```bash
# List items with pagination
curl "http://localhost:8000/stac/catalogs/public/collections/roads/items?limit=20&offset=0"

# Get single item
curl "http://localhost:8000/stac/catalogs/public/collections/roads/items/road-1"
```

### 6. Search Across Collections

```bash
curl -X POST http://localhost:8000/stac/search \
  -H "Content-Type: application/json" \
  -d '{
    "catalog_id": "public",
    "collections": ["roads"],
    "bbox": [12.3, 41.8, 12.6, 42.0],
    "datetime": "2024-01-01T00:00:00Z/..",
    "filter": {
      "field": "surface",
      "operator": "eq",
      "value": "paved"
    },
    "limit": 10
  }'
```

### 7. Multiple Language

#### 7.1 Internationalization (i18n)

The STAC service supports multi-language metadata for Catalogs, Collections, and Items.

#### 7.2 Retrieving Localized Content

Clients can request metadata in a specific language using the lang query parameter or Accept-Language header.

Request: GET /stac/catalogs/my-catalog?lang=fr

Response: Returns the catalog metadata (title, description, etc.) localized in French.

Includes a language field describing the current language.

Includes a languages list of other available translations.

#### 7.3 Retrieving All Languages

To retrieve the full internationalized record (all available translations), use lang=*.

Request: GET /stac/catalogs/my-catalog?lang=*

Response: Returns fields as dictionaries mapping language codes to values.

Example: "title": {"en": "My Map", "fr": "Mon Carte"}

#### 7.4 Creating & Updating Content

The API supports two modes for creating or updating resources:

Localized Input (Simple):

Send standard STAC fields (strings).

Provide the lang parameter (e.g., lang=en).

The server automatically wraps the input into the correct language structure.

Example: 
POST /catalogs?lang=fr with {"title": "Mon Carte"} -> Saved as {"title": {"fr": "Mon Carte"}}.

#### Iernationalized Input (Advanced):

Send fields as dictionaries containing multiple languages.

Use lang=* (or omit to imply multi-language if the structure is detected).

Example: POST /catalogs with {"title": {"en": "Map", "es": "Mapa"}}.

#### 7.5 Removing a Language

To remove a specific language translation while keeping others, perform a PUT update with lang=* and omit the language key you wish to remove from the internationalized dictionary.

#### 7.6 Extra Metadata

Custom fields can be stored in the extra_metadata field. This field is also fully localizable.

Input: {"extra_metadata": {"custom_field": "value"}} with lang=en.

Storage: {"extra_metadata": {"en": {"custom_field": "value"}}}.

Retrieval: Automatically flattened into the STAC Item/Collection properties for the requested language.


---

## API Endpoint Reference

### Catalog Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/stac/` | Root STAC catalog |
| `GET` | `/stac/catalogs/{catalog_id}` | Get catalog metadata |
| `POST` | `/stac/catalogs` | Create catalog |
| `PUT` | `/stac/catalogs/{catalog_id}` | Update catalog |
| `DELETE` | `/stac/catalogs/{catalog_id}` | Delete catalog (logical) |

### Collection Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/stac/catalogs/{cat}/collections/{coll}` | Get collection |
| `POST` | `/stac/catalogs/{cat}/collections` | Create collection |
| `PUT` | `/stac/catalogs/{cat}/collections/{coll}` | Update collection |
| `DELETE` | `/stac/catalogs/{cat}/collections/{coll}` | Delete collection |
| `GET` | `/stac/catalogs/{cat}/collections/{coll}/config` | Get storage config |
| `PUT` | `/stac/catalogs/{cat}/collections/{coll}/config` | Set storage config (immutable) |

### Item Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/stac/catalogs/{cat}/collections/{coll}/items` | List items (paginated) |
| `GET` | `/stac/catalogs/{cat}/collections/{coll}/items/{id}` | Get single item |
| `POST` | `/stac/catalogs/{cat}/collections/{coll}/items` | Create item |
| `PUT` | `/stac/catalogs/{cat}/collections/{coll}/items/{id}` | Replace item |
| `DELETE` | `/stac/catalogs/{cat}/collections/{coll}/items/{id}` | Delete item |

### Search Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/stac/search` | Cross-collection item search |
| `POST` | `/stac/collections/search` | Collection search |
| `POST` | `/stac/catalogs/{cat}/collections/{coll}/aggregate` | Aggregation queries |

### Virtual Collection Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/stac/virtual/assets/catalogs/{cat}/collections/{coll}` | List assets as collections |
| `GET` | `/stac/virtual/assets/{asset}/catalogs/{cat}/collections/{coll}` | Asset collection view |
| `GET` | `/stac/virtual/assets/{asset}/catalogs/{cat}/collections/{coll}/items` | Items by asset |
| `GET` | `/stac/virtual/hierarchy/{hier}/catalogs/{cat}/collections/{coll}` | Hierarchy collection |
| `GET` | `/stac/virtual/hierarchy/{hier}/catalogs/{cat}/collections/{coll}/items` | Hierarchy items |
| `POST` | `/stac/virtual/hierarchy/{hier}/catalogs/{cat}/collections/{coll}/search` | Hierarchy search |

---

## Error Codes

| Code | Description |
|------|-------------|
| `400` | Invalid geometry, ID mismatch, bad filter, or invalid aggregation |
| `404` | Catalog, collection, item, or table not found |
| `409` | Configuration already set (immutable fields) |
| `500` | Unexpected server error |

---

## STAC Items & Collections

### Item Structure

STAC items follow the [STAC Item spec](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md):

```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "id": "unique-id",
  "geometry": {
    "type": "Point",
    "coordinates": [12.48, 41.89]
  },
  "bbox": [12.48, 41.89, 12.48, 41.89],
  "properties": {
    "datetime": "2024-01-01T00:00:00Z",
    "custom_field": "value"
  },
  "links": [...],
  "assets": {...}
}
```

### Collection Structure

```json
{
  "type": "Collection",
  "stac_version": "1.0.0",
  "id": "collection-id",
  "title": "Collection Title",
  "description": "Description",
  "license": "proprietary",
  "extent": {
    "spatial": {"bbox": [[-180, -90, 180, 90]]},
    "temporal": {"interval": [["2020-01-01T00:00:00Z", null]]}
  },
  "links": [...]
}
```

---

## Geometry & CRS Handling

- **Default CRS**: WGS84 (EPSG:4326)
- **Supported Geometry Types**: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon
- **Validation**: Configured per collection via `geometry_storage.invalid_geom_policy`
- **Transformation**: Automatic reprojection to `target_srid`
- **Dimension**: Force 2D or 3D via `target_dimension`

---

## Lazy Table Creation

Collections don't create physical database tables until the first item is inserted. This allows:
- Fast collection creation
- Schema-only metadata management
- Deferred storage configuration

**Behavior**:
- Empty collections return `{"type": "FeatureCollection", "features": []}`
- First insert triggers table creation with configured schema
- Subsequent inserts use existing table

---

## Collection Configuration

### Storage Configuration

Configure geometry handling, versioning, and spatial indices.

**Endpoint**: `PUT /stac/catalogs/{cat}/collections/{coll}/config`

```json
{
  "versioning_behavior": "create_new_version",
  "geometry_storage": {
    "target_srid": 4326,
    "allowed_geometry_types": ["Point", "Polygon"],
    "invalid_geom_policy": "attempt_fix",
    "target_dimension": "force_2d",
    "write_bbox": true
  },
  "h3_resolutions": [5, 8, 10],
  "s2_resolutions": [10, 15]
}
```

**Fields**:
- `versioning_behavior`: `create_new_version` | `update_in_place`
- `geometry_storage.target_srid`: Target SRID (e.g., 4326, 3857)
- `geometry_storage.allowed_geometry_types`: Restrict geometry types
- `geometry_storage.invalid_geom_policy`: `reject` | `attempt_fix` | `accept_as_is`
- `geometry_storage.target_dimension`: `force_2d` | `force_3d` | `preserve`
- `geometry_storage.write_bbox`: Auto-calculate bbox
- `h3_resolutions`: H3 index resolutions (0-15)
- `s2_resolutions`: S2 cell levels (0-30)

---

## STAC Plugin Configuration

Configure STAC-specific features per collection.

**Endpoint**: `PUT /config/catalogs/{cat}/collections/{coll}/plugins/stac`

### Core Options

```json
{
  "enabled": true,
  "enabled_extensions": ["datacube", "projection"],
  "summaries": {
    "gsd": [10, 20, 30],
    "eo:bands": [
      {"name": "B1", "common_name": "blue"},
      {"name": "B2", "common_name": "green"}
    ]
  },
  "navigation_links": [
    {
      "rel": "license",
      "href": "https://creativecommons.org/licenses/by/4.0/",
      "title": "CC BY 4.0"
    }
  ]
}
```

---

## Datacube Extension

Define multidimensional data structures.

### Dimensions

```json
"cube_dimensions": {
  "x": {
    "type": "spatial",
    "axis": "x",
    "extent": [-180, 180],
    "reference_system": 4326
  },
  "y": {
    "type": "spatial",
    "axis": "y",
    "extent": [-90, 90],
    "reference_system": 4326
  },
  "time": {
    "type": "temporal",
    "extent": ["2020-01-01T00:00:00Z", "2024-12-31T23:59:59Z"],
    "step": "P1D"
  },
  "band": {
    "type": "nominal",
    "values": ["B1", "B2", "B3", "B4"],
    "dynamic_source": {
      "type": "attribute_scan",
      "target_attribute": "band_name"
    }
  }
}
```

**Dimension Types**:
- `spatial`: X, Y, Z coordinates
- `temporal`: Time dimension
- `ordinal`: Ordered discrete values
- `nominal`: Unordered categorical values

**Dynamic Sources**:
- `attribute_scan`: Scan item properties
- `sql_query`: Custom SQL query
- `static`: Fixed values

### Variables

```json
"cube_variables": {
  "temperature": {
    "type": "data",
    "description": "Surface temperature",
    "unit": "°C",
    "dimensions": ["x", "y", "time"]
  },
  "ndvi": {
    "type": "data",
    "description": "Normalized Difference Vegetation Index",
    "unit": "index",
    "dimensions": ["x", "y", "time", "band"]
  }
}
```

---

## Asset Tracking

Track data lineage from source files to ingested items.

```json
"asset_tracking": {
  "enabled": true,
  "access_mode": "PROXY"
}
```

**Access Modes**:
- `PROXY`: Route through `/proxy/assets/{cat}/{asset}` (secure, authenticated)
- `DIRECT`: Expose raw storage URLs (S3, GCS, HTTP - requires public access)

**Behavior**:
- Ingested items get `derived_from` link
- Virtual endpoints expose asset-based views
- Asset metadata stored in `catalog.assets` table

**Example Item Link**:
```json
{
  "rel": "derived_from",
  "href": "http://localhost:8000/stac/virtual/assets/my_file.geojson/catalogs/public/collections/roads",
  "title": "Source: my_file.geojson"
}
```

---

## Hierarchy Configuration

Create dynamic hierarchical views of collections.

### FIXED Strategy

Predefined levels with explicit conditions.

```json
"hierarchy": {
  "enabled": true,
  "rules": {
    "countries": {
      "hierarchy_id": "countries",
      "strategy": "FIXED",
      "item_code_field": "iso_code",
      "level_name": "Country",
      "condition": "admin_level = '0'",
      "collection_title_template": "Countries",
      "collection_description_template": "Administrative Level 0"
    },
    "regions": {
      "hierarchy_id": "regions",
      "strategy": "FIXED",
      "parent_hierarchy_id": "countries",
      "item_code_field": "region_code",
      "parent_code_field": "iso_code",
      "level_name": "Region",
      "condition": "admin_level = '1'",
      "collection_title_template": "Regions of {parent_value}",
      "collection_description_template": "Administrative Level 1"
    }
  }
}
```

**Key Fields**:
- `hierarchy_id`: Unique identifier for this level
- `parent_hierarchy_id`: Links to parent level (for navigation)
- `item_code_field`: Property used as item ID at this level
- `parent_code_field`: Property linking item to parent
- `condition`: CQL2 filter defining level membership

### RECURSIVE Strategy

Self-referential parent-child relationships.

```json
"org_chart": {
  "hierarchy_id": "org_chart",
  "strategy": "RECURSIVE",
  "item_code_field": "employee_id",
  "parent_code_field": "manager_id",
  "root_condition": "manager_id IS NULL",
  "level_name": "Organization",
  "collection_title_template": "Organizational Hierarchy"
}
```

---

## Aggregation Configuration

Enable OGC STAC Aggregation Extension.

```json
"aggregations": {
  "enabled": true,
  "allow_custom": true,
  "max_aggregations_per_request": 5,
  "default_rules": [
    {
      "name": "by_country",
      "type": "term",
      "property": "properties.country_code",
      "limit": 20
    },
    {
      "name": "population_stats",
      "type": "stats",
      "property": "properties.population"
    },
    {
      "name": "spatial_density",
      "type": "geohash",
      "property": "geom",
      "precision": 5,
      "limit": 100
    },
    {
      "name": "temporal_distribution",
      "type": "datetime",
      "property": "properties.created_at",
      "interval": "1 month",
      "limit": 24
    }
  ]
}
```

---

## Simplification Settings

Dynamic geometry simplification for performance.

```json
"simplification": {
  "vertex_thresholds": {
    "100000": 0.5,
    "50000": 0.1,
    "10000": 0.01,
    "5000": 0.005,
    "1000": 0.001
  },
  "default_tolerance": 0.0001
}
```

**Behavior**: Geometries with more vertices than threshold are simplified using the specified tolerance.

---

## Item Search

Cross-collection search with spatial, temporal, and attribute filters.

**Endpoint**: `POST /stac/search`

### Basic Search

```json
{
  "catalog_id": "public",
  "collections": ["roads", "buildings"],
  "limit": 10,
  "offset": 0
}
```

---

## Spatial Filters

### Bounding Box

```json
{
  "bbox": [12.3, 41.8, 12.6, 42.0]
}
```

**Format**: `[minx, miny, maxx, maxy]` in WGS84  
**Behavior**: Returns items that **intersect** the box

### Intersects

```json
{
  "intersects": {
    "type": "Polygon",
    "coordinates": [[[12.47, 41.88], [12.52, 41.88], [12.52, 41.92], [12.47, 41.92], [12.47, 41.88]]]
  }
}
```

**Supported Types**: Point, LineString, Polygon, Multi*  
**Behavior**: Returns items whose geometry **intersects** the provided geometry

---

## Temporal Filters

### Datetime Intervals

```json
{
  "datetime": "2024-01-01T00:00:00Z/2024-12-31T23:59:59Z"
}
```

**Formats**:
- Single instant: `"2024-01-01T00:00:00Z"`
- Open start: `"../2024-12-31T23:59:59Z"`
- Open end: `"2024-01-01T00:00:00Z/.."`
- Closed interval: `"2024-01-01T00:00:00Z/2024-12-31T23:59:59Z"`

**Behavior**: Matches items where `datetime` or `[start_datetime, end_datetime]` intersects the query interval

---

## Attribute Filters

### Simple Filters

```json
{
  "filter": {
    "field": "surface",
    "operator": "eq",
    "value": "paved"
  }
}
```

**Operators**:

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equal | `"value": "paved"` |
| `neq` | Not equal | `"value": "closed"` |
| `lt` | Less than | `"value": 50` |
| `lte` | Less than or equal | `"value": 4` |
| `gt` | Greater than | `"value": 10` |
| `gte` | Greater than or equal | `"value": 100` |
| `like` | Pattern match (case-sensitive) | `"value": "%Main%"` |
| `ilike` | Pattern match (case-insensitive) | `"value": "%main%"` |

### Complex Filters

```json
{
  "filter": {
    "op": "and",
    "args": [
      {"field": "surface", "operator": "eq", "value": "paved"},
      {
        "op": "or",
        "args": [
          {"field": "lanes", "operator": "gte", "value": 4},
          {"field": "highway", "operator": "eq", "value": "motorway"}
        ]
      }
    ]
  }
}
```

**Logical Operators**: `and`, `or`, `not`

---

## Pagination & Sorting

### Pagination

```json
{
  "limit": 20,
  "offset": 0
}
```

- `limit`: 1-1000 (default: 10)
- `offset`: 0+ (default: 0)

### Sorting

```json
{
  "sortby": [
    {"field": "properties.created_at", "direction": "desc"},
    {"field": "properties.name", "direction": "asc"}
  ]
}
```

### Field Selection

```json
{
  "fields": {
    "include": ["id", "geometry", "properties.name"],
    "exclude": ["properties.internal_id"]
  }
}
```

---

## Collection Search

Search for collections by metadata.

**Endpoint**: `POST /stac/collections/search`

```json
{
  "catalog_id": "public",
  "ids": ["roads", "buildings"],
  "keywords": ["infrastructure"],
  "bbox": [12.0, 41.5, 12.8, 42.2],
  "datetime": "2024-01-01T00:00:00Z/..",
  "limit": 10,
  "offset": 0
}
```

---

## Virtual Collections

### Asset-Based Views

Browse collections through the lens of source files.

#### List Assets

```bash
GET /stac/virtual/assets/catalogs/{cat}/collections/{coll}
```

Returns all assets as virtual STAC collections.

#### Asset Collection

```bash
GET /stac/virtual/assets/{asset_code}/catalogs/{cat}/collections/{coll}
```

Returns asset metadata as a STAC collection.

#### Items by Asset

```bash
GET /stac/virtual/assets/{asset_code}/catalogs/{cat}/collections/{coll}/items
```

Returns only items derived from the specified asset.

---

### Hierarchy-Based Views

Navigate collections via dynamic hierarchies.

#### Hierarchy Collection

```bash
GET /stac/virtual/hierarchy/{hierarchy_id}/catalogs/{cat}/collections/{coll}
```

Returns virtual collection for a hierarchy level with child links.

**Example Response**:
```json
{
  "type": "Collection",
  "id": "boundaries_countries",
  "title": "Countries",
  "links": [
    {"rel": "child", "href": ".../items?parent_value=USA", "title": "USA (50 items)"},
    {"rel": "child", "href": ".../items?parent_value=ITA", "title": "ITA (20 items)"}
  ]
}
```

#### Hierarchy Items

```bash
GET /stac/virtual/hierarchy/{hierarchy_id}/catalogs/{cat}/collections/{coll}/items?parent_value={value}
```

Returns items filtered by hierarchy rule and optional parent value.

#### Hierarchy Search

```bash
POST /stac/virtual/hierarchy/{hierarchy_id}/catalogs/{cat}/collections/{coll}/search?parent_value={value}
```

Full search capabilities within a hierarchy level.

---

## Aggregations

Compute statistics without retrieving all items.

**Endpoint**: `POST /stac/catalogs/{cat}/collections/{coll}/aggregate`

### Request Format

```json
{
  "aggregations": [
    {
      "name": "unique_name",
      "type": "term|stats|geohash|datetime|bbox|temporal_extent",
      "property": "properties.field_name",
      "limit": 10,
      "precision": 5,
      "interval": "1 month"
    }
  ],
  "bbox": [minx, miny, maxx, maxy],
  "datetime": "2024-01-01T00:00:00Z/..",
  "filter": {...}
}
```

---

## Term Aggregation

Count unique values and frequencies.

**Use Cases**: Categories, tags, enums

```json
{
  "name": "land_use",
  "type": "term",
  "property": "properties.land_use",
  "limit": 20
}
```

**Response**:
```json
{
  "aggregations": {
    "land_use": {
      "buckets": [
        {"key": "residential", "doc_count": 1523},
        {"key": "commercial", "doc_count": 876}
      ]
    }
  }
}
```

---

## Stats Aggregation

Statistical metrics on numeric fields.

**Metrics**: min, max, avg, sum, count

```json
{
  "name": "elevation",
  "type": "stats",
  "property": "properties.elevation_m"
}
```

**Response**:
```json
{
  "aggregations": {
    "elevation": {
      "min": 0.0,
      "max": 4810.0,
      "avg": 1245.67,
      "sum": 15678900.0,
      "count": 12589
    }
  }
}
```

---

## Geohash Aggregation

Spatial clustering using geohash grid.

**Use Cases**: Heat maps, density analysis

```json
{
  "name": "spatial",
  "type": "geohash",
  "property": "geom",
  "precision": 5,
  "limit": 100
}
```

**Precision Guide**:

| Precision | Cell Size | Use Case |
|-----------|-----------|----------|
| 1-2 | ~1000+ km | Continental/Country |
| 3-4 | ~40-150 km | Regional/City |
| 5-6 | ~1-5 km | Neighborhood/District |
| 7-8 | ~40-150 m | Street/Building |
| 9-12 | < 5 m | Precise location |

**Response**:
```json
{
  "aggregations": {
    "spatial": {
      "buckets": [
        {"key": "u4pru", "doc_count": 234},
        {"key": "u4prv", "doc_count": 189}
      ]
    }
  }
}
```

---

## Datetime Aggregation

Temporal histogram.

**Use Cases**: Time series, activity timelines

```json
{
  "name": "monthly",
  "type": "datetime",
  "property": "properties.created_at",
  "interval": "1 month",
  "limit": 24
}
```

**Intervals**: `"1 day"`, `"1 week"`, `"1 month"`, `"1 year"`, `"P1D"`, `"P1M"`

**Response**:
```json
{
  "aggregations": {
    "monthly": {
      "buckets": [
        {"key": "2024-01-01T00:00:00Z", "doc_count": 145},
        {"key": "2024-02-01T00:00:00Z", "doc_count": 189}
      ]
    }
  }
}
```

---

## Bbox Aggregation

Calculate combined bounding box.

```json
{
  "name": "extent",
  "type": "bbox"
}
```

**Response**:
```json
{
  "aggregations": {
    "extent": {
      "bbox": [-124.48, 32.53, -114.13, 42.01]
    }
  }
}
```

---

## Temporal Extent Aggregation

Calculate min/max datetime range.

```json
{
  "name": "time_range",
  "type": "temporal_extent"
}
```

**Response**:
```json
{
  "aggregations": {
    "time_range": {
      "interval": [["2020-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]]
    }
  }
}
```

---

## Complete Configuration Schema

Full example combining all configuration options:

```json
{
  "enabled": true,
  "enabled_extensions": ["datacube", "projection"],
  "summaries": {
    "gsd": [10, 20, 30]
  },
  "cube_dimensions": {
    "x": {"type": "spatial", "axis": "x", "extent": [-180, 180]},
    "y": {"type": "spatial", "axis": "y", "extent": [-90, 90]},
    "time": {"type": "temporal", "extent": ["2020-01-01T00:00:00Z", null]}
  },
  "cube_variables": {
    "temperature": {
      "type": "data",
      "unit": "°C",
      "dimensions": ["x", "y", "time"]
    }
  },
  "navigation_links": [],
  "hierarchy": {
    "enabled": true,
    "rules": {
      "level_0": {
        "hierarchy_id": "level_0",
        "strategy": "FIXED",
        "item_code_field": "code",
        "condition": "level = '0'"
      }
    }
  },
  "asset_tracking": {
    "enabled": true,
    "access_mode": "PROXY"
  },
  "aggregations": {
    "enabled": true,
    "allow_custom": true,
    "max_aggregations_per_request": 5,
    "default_rules": []
  },
  "simplification": {
    "vertex_thresholds": {
      "50000": 0.1,
      "10000": 0.01,
      "1000": 0.001
    },
    "default_tolerance": 0.0001
  }
}
```

---

## Performance Optimization

### Database Indexes

Recommended indexes for optimal performance:

```sql
-- JSONB attribute indexes
CREATE INDEX idx_attributes_gin ON "catalog"."collection" USING GIN (attributes);

-- Specific property indexes
CREATE INDEX idx_country ON "catalog"."collection" ((attributes->>'country_code'));

-- Spatial index (auto-created)
CREATE INDEX idx_geom ON "catalog"."collection" USING GIST (geom);

-- Temporal indexes
CREATE INDEX idx_datetime ON "catalog"."collection" ((attributes->>'datetime'));

-- H3/S2 indexes (auto-created if configured)
CREATE INDEX idx_h3_5 ON "catalog"."collection" (h3_5);
CREATE INDEX idx_s2_10 ON "catalog"."collection" (s2_10);
```

### Query Optimization

1. **Use Filters**: Narrow datasets before aggregating
2. **Limit Buckets**: Don't request more than needed
3. **Choose Appropriate Precision**: Start low for geohash
4. **Combine Aggregations**: Reduce round trips
5. **Use Pre-configured Aggregations**: Faster than ad-hoc

### Geometry Simplification

Configure thresholds based on use case:
- **Web maps**: Aggressive simplification (0.01-0.1)
- **Analysis**: Moderate simplification (0.001-0.01)
- **Precision work**: Minimal simplification (0.0001)

---

## Troubleshooting

### 404 Errors

**Catalog not found**: Verify catalog exists via `GET /stac/catalogs/{id}`  
**Collection not found**: Check `GET /stac/catalogs/{cat}/collections/{coll}`  
**Item not found**: Confirm item ID matches `geoid` or `external_id`  
**Table not found**: Collection may be empty (lazy table creation)

### 400 Errors

**Invalid geometry**: Check GeoJSON validity  
**Bad filter**: Verify property names exist in `attribute_schema`  
**Invalid aggregation**: Ensure property format is `properties.<name>`  
**ID mismatch**: PUT request ID must match payload ID

### 409 Errors

**Config already set**: Storage config is immutable after first insert  
**Solution**: Delete collection and recreate, or create new collection

### Performance Issues

**Slow queries**: Add indexes on frequently filtered properties  
**Large result sets**: Use pagination (`limit`/`offset`)  
**Complex aggregations**: Reduce precision or limit buckets  
**Geometry overhead**: Enable simplification

---

## Additional Resources

- [STAC Specification](https://stacspec.org/)
- [OGC API Features](https://ogcapi.ogc.org/features/)
- [CQL2 Specification](https://docs.ogc.org/DRAFTS/21-065.html)
- [STAC Aggregation Extension](https://github.com/stac-api-extensions/aggregation)