# Scalable Dimension Member Dissemination and Algorithmic Generation for STAC Datacube Extension

**Author**: Carlo Cancellieri (FAO, OGC Member)
**Date**: 2026-03-27
**Target**: STAC Datacube Extension v2.3.0+ / OGC GeoDataCube SWG / OGC API Building Block
**Status**: Draft for Community Review

---

## Abstract

This proposal extends the STAC Datacube Extension with three backwards-compatible capabilities:

1. **`size` + `values_href`** -- paginated access to dimension member arrays via OGC API - Common pagination
2. **`generator`** -- algorithmic member generation with machine-discoverable OpenAPI definitions, applicable to any dimension type
3. **Invertible inversion** -- value-to-coordinate mapping enabling dimension integrity enforcement at item ingestion

No existing OGC, STAC, SDMX, or openEO standard provides pagination for dimension members, algorithmic generation rules, or formal inversion for dimension validation. OGC Testbed 19 (doc 23-047) and Testbed 20 (doc 24-035) both identified these as open gaps in the GeoDataCube API profile.

The proposal defines five conformance levels (Basic, Invertible, Searchable, Similarity, Intelligent) and positions STAC collections as approximate multidimensional indices navigable by concept proximity -- bridging traditional OGC metadata with AI/ML capabilities.

---

## 1. Paginated Dimension Members

### 1.1 Problem

The `values` array in `cube:dimensions` works for dimensions with tens or hundreds of members but fails for:

- **Temporal dimensions**: daily time series 2000-2025 = 9,131 values
- **Dekadal/pentadal time at scale**: 25 years of dekadal data = 900 members
- **Indicator dimensions**: FAO catalogs with 10,000+ indicator codes
- **Administrative boundaries**: 250+ countries x sub-national levels = 50,000+ members
- **Combinatorial dimensions**: cross-products reaching millions

OGC Testbed 19 (doc 23-047) and Testbed 20 (doc 24-035) both identified "no pagination for dimension members" as an open gap. The STAC Datacube Extension has an open request for a `size` property (issue #31).

### 1.2 Schema Changes

Add two properties to the dimension object (backwards-compatible):

```json
{
  "cube:dimensions": {
    "indicator": {
      "type": "other",
      "description": "Agricultural indicators",
      "size": 45000,
      "values_href": "./dimensions/indicator/values",
      "values": null
    },
    "bands": {
      "type": "bands",
      "values": ["B01", "B02", "B03", "B04"],
      "size": 4
    }
  }
}
```

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `size` | integer | RECOMMENDED | Total number of discrete members. Allows clients to know cardinality without downloading values. |
| `values_href` | string (URI) | OPTIONAL | Link to a paginated endpoint returning dimension values. When present, `values` MAY be omitted. Relative or absolute URI. |
| `values` | array | OPTIONAL | When `size` exceeds an implementation-defined threshold, MAY be omitted if `values_href` is provided. Recommended threshold: 1000. |

### 1.3 Paginated Endpoint

```
GET {values_href}?limit={limit}&offset={offset}&filter={filter}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 100 | Max items per page (1-10000) |
| `offset` | integer | 0 | Items to skip |
| `filter` | string | null | Substring match on values |

**Response** (follows OGC API - Features `numberMatched`/`numberReturned` convention):

```json
{
  "dimension": "indicator",
  "type": "other",
  "numberMatched": 45000,
  "numberReturned": 100,
  "values": ["NDVI", "EVI", "LAI"],
  "links": [
    {"rel": "self", "href": ".../values?limit=100&offset=0", "type": "application/json"},
    {"rel": "next", "href": ".../values?limit=100&offset=100", "type": "application/json"},
    {"rel": "collection", "href": "/collections/{id}", "type": "application/json"}
  ]
}
```

### 1.4 Conformance with OGC API - Common

- Pagination follows OGC API - Common Part 2 (`limit` + RFC 5988 link relations)
- `numberMatched` / `numberReturned` follows OGC API - Features convention
- Link relations: `self`, `next`, `prev`, `collection`
- Content type: `application/json`

### 1.5 Backwards Compatibility

- Clients reading only `values` continue to work for small dimensions
- `size` is informational -- clients can ignore it
- `values_href` is optional -- servers can always provide inline `values`
- Threshold for omitting `values` is implementation-defined

---

## 2. The Generator Object

### 2.1 Problem

Many dimensions follow deterministic rules: temporal calendars (dekadal, pentadal, ISO week), integer ranges, grid indices, administrative hierarchies with known codification. Enumerating and paginating millions of algorithmically-derivable members is wasteful. Clients should be able to generate members locally when the algorithm is known -- or discover and call a generation API when it is not.

This is not limited to temporal dimensions. Examples include:

- **Temporal**: dekadal (36/year), pentadal (72 or 73/year), ISO week, custom fiscal periods
- **Spatial**: grid cell indices (row/col), tile coordinates (z/x/y)
- **Integer range**: elevation bands, age groups, percentile bins
- **Coded**: ISO 3166 country codes, NUTS regions, HS commodity codes

### 2.2 Schema: The `generator` Property

Add a `generator` property to any dimension:

```json
{
  "cube:dimensions": {
    "time": {
      "type": "temporal",
      "extent": ["2000-01-01T00:00:00Z", "2024-12-31T23:59:59Z"],
      "generator": {
        "type": "dekadal",
        "api": "http://www.opengis.net/def/generator/ogc/0/dekadal/openapi.json",
        "parameters": {},
        "output": { "type": "string", "format": "date-time" },
        "invertible": true,
        "search": ["exact", "range"],
        "on_invalid": "reject"
      },
      "unit": "dekad",
      "step": null,
      "size": 900,
      "values_href": "./dimensions/time/values"
    }
  }
}
```

### 2.3 Generator Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string or URI | REQUIRED | Short identifier for well-known algorithms (`dekadal`, `pentadal-monthly`, `pentadal-annual`, `iso-week`, `integer-range`, `grid-index`) or full URI for custom generators. |
| `api` | string (URI) | CONDITIONAL | OpenAPI definition URI. REQUIRED for custom generators. Implicit (resolvable from `type`) for well-known types. Machine clients fetch this to discover the generation endpoint. |
| `parameters` | JSON Schema | OPTIONAL | Input parameters for the algorithm. Defined per [JSON Schema 2020-12](https://json-schema.org/draft/2020-12/json-schema-core). Maps to the OpenAPI request schema. |
| `output` | JSON Schema | REQUIRED | Type and structure of each generated member. Defined per JSON Schema 2020-12. Maps to the OpenAPI response schema. |
| `invertible` | boolean | OPTIONAL | `true` if the generator is invertible (value to parameters). Enables `/inverse` capability. Default: `false`. See Section 3. |
| `search` | array of strings | OPTIONAL | Supported search protocols: `["exact", "range", "like", "vector"]`. See Section 2.6. |
| `on_invalid` | string | OPTIONAL | Item ingestion behavior when inverse validation fails: `"reject"`, `"accept"`, or `"warn"`. See Section 3.4. |

### 2.4 Hybrid Type Resolution: Shorthand + OpenAPI URI

Well-known generators have short string identifiers that resolve to registered OGC OpenAPI specs. Custom generators provide their own OpenAPI URI directly.

**Client resolution logic**:
1. If `type` is a known shorthand (e.g., `"dekadal"`): client MAY use a built-in algorithm OR fetch the registered OpenAPI spec at the OGC Definition URI
2. If `type` is a full URI: client MUST fetch the OpenAPI spec to discover the generation endpoint
3. The `api` field, when present, overrides the default resolution -- useful for alternative implementations of the same algorithm

This keeps the schema clean for common cases while being fully extensible for custom generators.

### 2.5 Generator OpenAPI Capabilities

Each generator's OpenAPI spec exposes up to four capabilities:

| Capability | Path | Required | Description |
|------------|------|----------|-------------|
| **Generate** | `/generate` | REQUIRED | Paginated member generation. Supports `limit`/`offset`. `values_href` points here. |
| **Extent** | `/extent` | REQUIRED | Returns dimension boundaries in both native and standard representations. |
| **Search** | `/search` | OPTIONAL | Find dimension members matching a query. See Section 2.6. |
| **Inverse** | `/inverse` | OPTIONAL | Map a value back to parameters/coordinates. Requires `invertible: true`. See Section 3. |

**Content negotiation**: The generator API supports output format selection via `Accept` header or `?format=` parameter:
- `format=datetime` -- standard ISO types (default, backwards-compatible)
- `format=native` -- custom notation (e.g., `YYYY-Knn`)
- `format=structured` -- full objects with code, start, end, etc.

### 2.6 Search API (Optional Capability)

The generator MAY expose a search endpoint for finding dimension members. The OpenAPI spec declares which protocols the generator supports. Clients discover capabilities dynamically -- they never guess.

| Protocol | Query | Description | Example |
|----------|-------|-------------|---------|
| `exact` | `?exact={value}` | Exact match on member value | `?exact=2024-K15` |
| `range` | `?min={v}&max={v}` | Members within boundaries | `?min=2024-K01&max=2024-K12` |
| `like` | `?like={pattern}` | Pattern/substring matching | `?like=2024-K*` |
| `vector` | POST `{"vector": [...], "k": 10}` | k-nearest neighbors by similarity | Spectral signature search |

**Design principle**: The generator decides whether it can handle a query given its implementation. A deterministic generator (dekadal) handles `exact` and `range` trivially. A vector-indexed generator handles `vector` searches. Unsupported protocols return HTTP 501.

**When input parameters are absent**: For generated dimensions (dekadal, integer-range), the search parameters replace input parameters. The generator knows its own extent and algorithm -- it searches within its generated space. For data-driven dimensions, the search hits the underlying index (DB, vector store).

### 2.7 The `output` Field: JSON Schema Standard

The `generator.output` field is a standard JSON Schema document per [JSON Schema 2020-12](https://json-schema.org/draft/2020-12/json-schema-core). This avoids reinventing type declarations and leverages existing tooling, validators, and developer familiarity.

#### Simple types

**ISO datetime output** (temporal generators producing standard timestamps):
```json
{ "output": { "type": "string", "format": "date-time" } }
```

**Date-only output**:
```json
{ "output": { "type": "string", "format": "date" } }
```

**Integer output** (grid index, elevation band):
```json
{ "output": { "type": "integer", "minimum": 0, "maximum": 35 } }
```

#### Structured types

**Object output** (dekadal period with code, start, end):
```json
{
  "output": {
    "type": "object",
    "properties": {
      "code":  { "type": "string" },
      "start": { "type": "string", "format": "date" },
      "end":   { "type": "string", "format": "date" },
      "days":  { "type": "integer" }
    },
    "required": ["code", "start", "end"]
  }
}
```

**Array output** (bounding box tuple):
```json
{
  "output": {
    "type": "array",
    "items": { "type": "number" },
    "minItems": 4,
    "maxItems": 4
  }
}
```

Any valid JSON Schema is accepted. Implementations SHOULD support at minimum: `type`, `format`, `properties`, `required`, `items`, `minimum`, `maximum`, `enum`, `description`.

---

## 3. Generator Invertibility and Dimension Integrity

### 3.1 Forward and Inverse Functions

A generator function `f` is a mapping between two spaces:
- **Parameter space** (P): the inputs that define the dimension (extent, step, algorithm parameters)
- **Value space** (V): the dimension members (the actual values items are indexed by)

**Forward function** (always available): `f: P -> V` -- generate dimension members from parameters.
**Inverse function** (only for invertible generators): `f-1: V -> P` -- given a value, compute which dimension member it belongs to and its coordinates in parameter space.

```
Forward:  f(extent=[2000,2024], type=dekadal) -> [2000-K01, 2000-K02, ..., 2024-K36]
Inverse:  f-1(2024-01-15) -> {member: "2024-K02", year: 2024, month: 1, dekad: 2, valid: true}
Inverse:  f-1(2024-01-32) -> {valid: false, reason: "Day 32 does not exist"}
```

### 3.2 Classification of Generator Functions

| Property | Invertible (injective) | Invertible (surjective, non-injective) | Non-invertible |
|---|---|---|---|
| Forward | Parameters -> unique values | Parameters -> values (many-to-one) | Parameters -> values |
| Inverse | Value -> exact coordinate | Value -> member (deterministic, many-to-one) | Not available / approximate |
| Validation | Exact | Exact | Approximate or enumeration-based |
| Example | Integer range (step=100) | Dekadal: Jan 1-10 all map to K01 | Embedding space |

**Important nuance**: Most practical invertible generators are **surjective** -- multiple raw values map to the same dimension member. The dekadal generator is surjective: all dates from Jan 1-10 map to K01. What matters is that the mapping is **deterministic and total** -- every valid input maps to exactly one member.

### 3.3 Mathematical Foundations

**From statistics**: The forward function is analogous to a **generative model** (parameters theta produce data X). The inverse is the **sufficient statistic** T(X) -> theta capturing all information about the parameter.

**From information theory**: The inverse function performs **quantization** -- mapping continuous or high-cardinality inputs to discrete dimension members. The dekadal generator quantizes continuous time into 36 bins per year. The quantization is lossy (many dates map to one dekad) but deterministic.

**From data engineering**: This maps directly to **partitioning functions** in data warehousing. The forward function defines the partition scheme. The inverse computes which partition a record belongs to. This is how Hive, Iceberg, and Delta determine which partition to write to.

### 3.4 The `/inverse` API Capability

The inverse endpoint takes a raw value and returns the dimension member it belongs to:

```
GET /inverse?value=2024-01-15

{
  "valid": true,
  "member": "2024-K02",
  "coordinate": {"year": 2024, "month": 1, "dekad": 2},
  "range": {"start": "2024-01-11", "end": "2024-01-20"},
  "index": 2
}
```

```
GET /inverse?value=2024-01-32

{
  "valid": false,
  "reason": "Day 32 does not exist in January",
  "nearest": "2024-K03"
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `valid` | boolean | Is this value in the dimension's domain? |
| `member` | string | Dimension member code this value maps to |
| `coordinate` | object | Parameter-space coordinates |
| `range` | object | Value range covered by this member (start/end or min/max) |
| `index` | integer | Ordinal position of this member in the dimension |
| `nearest` | string | Closest valid member when `valid: false` (if computable) |
| `reason` | string | Human-readable explanation when `valid: false` |

#### Batch Inverse

For data pipelines ingesting millions of records, the API SHOULD support batch inverse:

```
POST /inverse
{
  "values": ["2024-01-15", "2024-02-28", "2024-03-01", "invalid-date"],
  "on_invalid": "nearest"
}

{
  "results": [
    {"valid": true, "member": "2024-K02", "index": 2},
    {"valid": true, "member": "2024-K06", "index": 6},
    {"valid": true, "member": "2024-K07", "index": 7},
    {"valid": false, "nearest": null, "reason": "Cannot parse 'invalid-date'"}
  ]
}
```

### 3.5 Item Ingestion: The Double Operation

When a new STAC item is inserted into a collection, the system performs a **dimension membership check** for each dimension with `invertible: true`:

**Step 1 -- Inverse mapping**: Extract the item's raw value for this dimension and call `/inverse`.

**Step 2 -- Membership decision**: Based on the `on_invalid` policy:

```
                                    +--- valid: true --> CONFIRM coordinate
Item value --> /inverse --> result --+
                                    +--- valid: false --> on_invalid policy
                                                         |-- "reject" --> 422 error
                                                         |-- "accept" --> expand dimension
                                                         +-- "warn"   --> accept + warning
```

The insert is a **double operation** -- it touches both:
1. The **item itself** (data record with dimension coordinates)
2. The **dimension** (its member list may grow if `on_invalid: "accept"`)

| `on_invalid` | Behavior | Use case |
|---|---|---|
| `"reject"` | Item rejected if value is not a valid dimension member | Fixed dimensions: dekadal time, grid indices, known codelists |
| `"accept"` | Item accepted, dimension may expand to include new member | Open dimensions: new indicators, new land cover types |
| `"warn"` | Item accepted with a warning in the response | Transitional: collecting data while monitoring dimension growth |

### 3.6 Dimension Lifecycle Patterns

| Configuration | Cardinality | Data engineering analogy |
|---|---|---|
| `invertible: true` + `on_invalid: "reject"` | **Fixed**: exactly `size` members, no growth | Strict partition scheme, referential integrity |
| `invertible: true` + `on_invalid: "accept"` | **Bounded growth**: validated but accepted | Partition with overflow |
| `invertible: true` + `on_invalid: "warn"` | **Fixed with exceptions**: monitors health | Data quality alerting |
| `invertible: false` + `on_invalid: "accept"` | **Unbounded growth**: expands with data | Schema-on-read, SCD Type 2 |
| `invertible: false` + `on_invalid: "reject"` | **Enumerated only**: must match existing | Codelist enforcement |

### 3.7 Impact on Data Pipelines

1. **Ingestion validation**: Precipitation data arrives with dates; the dekadal generator validates and assigns each observation to its dekad. Invalid dates are rejected before entering the cube.

2. **Cross-collection consistency**: Two collections sharing the same temporal dimension (e.g., NDVI and precipitation, both dekadal) are guaranteed to have aligned time coordinates because the same generator validates both.

3. **Dimension drift detection**: `on_invalid: "warn"` on an indicator dimension alerts operators when new, unexpected indicators appear in the data feed.

4. **Backfill validation**: The invertible generator ensures every historical observation maps to a valid member. Batch inverse makes this efficient for millions of records.

5. **Quality metrics**: The ratio of valid to invalid inverse results is itself a data quality metric.

---

## 4. Concrete Examples

### 4.1 Dekadal Calendar (36 periods/year)

Each month is split into 3 dekads: days 1-10, 11-20, 21-end. D3 varies from 8 days (Feb non-leap) to 11 days (months with 31 days). Used globally in agricultural drought monitoring (FAO ASIS, GIEWS), precipitation datasets (CHIRPS, TAMSAT, ARC2), and vegetation indices (NDVI composites).

**Collection metadata** (structured output):
```json
{
  "cube:dimensions": {
    "time": {
      "type": "temporal",
      "extent": ["2000-01-01T00:00:00Z", "2024-12-31T23:59:59Z"],
      "generator": {
        "type": "dekadal",
        "api": "http://www.opengis.net/def/generator/ogc/0/dekadal/openapi.json",
        "parameters": {},
        "output": {
          "type": "object",
          "properties": {
            "code":  {"type": "string", "description": "YYYY-Knn (nn=01..36)"},
            "start": {"type": "string", "format": "date"},
            "end":   {"type": "string", "format": "date"},
            "days":  {"type": "integer", "minimum": 8, "maximum": 11}
          },
          "required": ["code", "start", "end"]
        },
        "invertible": true,
        "search": ["exact", "range"],
        "on_invalid": "reject"
      },
      "step": "1K",
      "unit": "dekad",
      "size": 900,
      "values_href": "./dimensions/time/values"
    }
  }
}
```

**Alternative -- simple datetime output** (legacy-compatible):
```json
{
  "generator": {
    "type": "dekadal",
    "output": { "type": "string", "format": "date" },
    "invertible": true
  }
}
```

The generator produces the start date of each dekad as an ISO date string: `2000-01-01`, `2000-01-11`, `2000-01-21`, etc. Legacy clients see a standard irregular temporal dimension.

**Notation** (following ISO week `YYYY-Www` precedent):

| Notation | Range | Example | Date Range |
|----------|-------|---------|------------|
| `YYYY-Knn` | nn=01..36 | `2024-K01` | Jan 1-10 |
| `YYYY-MM-Kd` | d=1..3 | `2024-01-K3` | Jan 21-31 |

**Algorithm**:
```
for each year in [start_year..end_year]:
  for each month in [1..12]:
    for d in [1, 2, 3]:
      index = (month - 1) * 3 + d
      start_day = [1, 11, 21][d-1]
      end_day   = [10, 20, last_day_of_month][d-1]
      code = f"{year}-K{index:02d}"
      yield {code, date(year, month, start_day), date(year, month, end_day)}
```

### 4.2 Pentadal Calendar -- Month-Based (72 periods/year)

Each month split into 6 pentads of 5 days each; P6 absorbs remaining days (26-end, giving 3-6 days). Used by CHIRPS, CDT, FAO.

```json
{
  "generator": {
    "type": "pentadal-monthly",
    "output": {
      "type": "object",
      "properties": {
        "code":  {"type": "string", "description": "YYYY-Pnn (nn=01..72)"},
        "start": {"type": "string", "format": "date"},
        "end":   {"type": "string", "format": "date"}
      },
      "required": ["code", "start", "end"]
    },
    "invertible": true,
    "search": ["exact", "range"]
  }
}
```

### 4.3 Pentadal Calendar -- Year-Based (73 periods/year)

Consecutive 5-day periods starting Jan 1. Period 73 has 5 or 6 days (leap year). Used by GPCP, CPC/NOAA.

```json
{
  "generator": {
    "type": "pentadal-annual",
    "output": {
      "type": "object",
      "properties": {
        "code":  {"type": "string", "description": "YYYY-Ann (nn=01..73)"},
        "start": {"type": "string", "format": "date"},
        "end":   {"type": "string", "format": "date"}
      },
      "required": ["code", "start", "end"]
    },
    "invertible": true
  }
}
```

### 4.4 Integer Range (non-temporal)

Elevation bands every 100m:
```json
{
  "cube:dimensions": {
    "elevation": {
      "type": "other",
      "description": "Elevation bands (meters)",
      "extent": [0, 5000],
      "generator": {
        "type": "integer-range",
        "parameters": {"type": "object", "properties": {"step": {"type": "integer"}}, "required": ["step"]},
        "output": {"type": "integer", "minimum": 0, "maximum": 5000},
        "invertible": true,
        "search": ["exact", "range"],
        "on_invalid": "reject"
      },
      "unit": "m",
      "size": 51
    }
  }
}
```

### 4.5 Grid Index (non-temporal)

Tile coordinates:
```json
{
  "cube:dimensions": {
    "tile": {
      "type": "other",
      "description": "Tile grid coordinates",
      "generator": {
        "type": "grid-index",
        "parameters": {
          "type": "object",
          "properties": {"rows": {"type": "integer"}, "cols": {"type": "integer"}},
          "required": ["rows", "cols"]
        },
        "output": {
          "type": "object",
          "properties": {
            "row": {"type": "integer", "minimum": 0},
            "col": {"type": "integer", "minimum": 0}
          },
          "required": ["row", "col"]
        },
        "invertible": true,
        "search": ["exact", "range"]
      },
      "size": 64800
    }
  }
}
```

### 4.6 Legacy Client Bridge

A temporal dekadal dimension with `output: {"type": "string", "format": "date-time"}` and `values_href` pointing to the generator's paginated endpoint. Legacy clients hit `values_href` and see standard ISO datetimes -- a perfectly valid irregular temporal dimension. They never need to know about dekads:

```json
["2000-01-01T00:00:00Z", "2000-01-11T00:00:00Z", "2000-01-21T00:00:00Z", "2000-02-01T00:00:00Z", ...]
```

---

## 5. Similarity-Driven Hypercube Navigation

### 5.1 The Problem: Navigating Unknown Cubes

Traditional cube access requires exact coordinates: `subset=time("2024-K15")&indicator("NDVI")`. But what if:

- You don't know the dimension members (45,000 indicators -- which one is relevant?)
- You have a *concept* but not an exact coordinate ("vegetation health in drought conditions")
- You want to explore the cube by semantic proximity, not exact position

### 5.2 Dimensions as Navigable Vector Spaces

Each STAC item exists at a position in the hypercube defined by its dimension coordinates. If dimension members can be searched by **similarity**, the cube itself becomes a navigable semantic space:

1. **Client has a concept** -- a text query, a feature vector, a reference observation, an embedding
2. **Client searches dimension generators** across multiple axes for similar members
3. **The intersection of similar members** across dimensions defines a **region** in the hypercube
4. **STAC items within that region** are the "next items" -- data at the conceptually nearest position
5. **Pagination = navigation** -- "next page" means "next most similar region in the cube"

This reframes STAC item pagination: instead of `offset=100` meaning "skip 100 items in insertion order," it means "show me items at the next-nearest position in the dimension space."

### 5.3 Multi-Dimensional Similarity Query

```json
POST /catalogs/{id}/collections/{id}/search
{
  "dimension_search": {
    "time": {"range": {"min": "2023-K01", "max": "2024-K36"}},
    "indicator": {"vector": [0.12, 0.85, 0.03, ...], "k": 5},
    "area": {"like": "ETH*"}
  },
  "limit": 20
}
```

For each dimension with a searchable generator:
- The generator's `/search` endpoint returns the k most similar members
- Each result includes a similarity score (0-1)
- The cross-product of similar members across dimensions defines candidate positions
- STAC items at those positions are ranked by combined similarity and returned

### 5.4 STAC as a Multidimensional Vector Index

| Traditional STAC | Similarity-Extended STAC |
|---|---|
| Items found by exact filters (`datetime`, `bbox`, properties) | Items found by proximity in dimension space |
| Navigate by pagination (offset/cursor) | Navigate by concept approximation |
| Dimensions are metadata labels | Dimensions are searchable vector spaces |
| Fixed query coordinates | Approximate query positions |

Each dimension generator that supports vector search contributes one axis of the multidimensional index. A collection with N searchable dimensions becomes an N-dimensional approximate nearest neighbor index over its items.

### 5.5 Concrete Scenario: FAO Agricultural Monitoring

A researcher has a drought observation (spectral signature + location + season):

1. **Spectral dimension**: search with measured NDVI profile -- generator returns similar vegetation states via embedding similarity
2. **Temporal dimension**: search with "agricultural season onset" concept -- dekadal generator returns relevant time periods via range search
3. **Indicator dimension**: search with "drought impact on cereals" -- generator returns similar indicators from the 45,000 in the catalog via semantic embedding search
4. **Spatial dimension**: search with observation coordinates -- returns similar agro-ecological zones via spatial proximity

The intersection: STAC items at the nearest (vegetation state x time period x indicator x zone) positions. The researcher navigates the cube without knowing a single exact dimension member code.

### 5.6 The Generator as a Facade

The generator is an abstraction over any backend:

- **Simple**: in-memory algorithm (dekadal, integer-range)
- **Database**: SQL query with LIKE/range filters
- **Vector index**: pgvector, FAISS, Milvus, Pinecone
- **AI model**: embedding model + ANN index
- **Composite**: hybrid search combining keyword + vector

The OpenAPI spec is the contract. The backend is an implementation detail. This makes the generator pattern a universal extension point where traditional OGC metadata standards meet AI/ML capabilities.

### 5.7 Extension Points

- **Embedding generation**: a generator could expose an `/embed` capability that converts a member into its vector representation, enabling client-side similarity computations
- **Dimensionality reduction**: `/project` capability returning 2D/3D projections (UMAP, t-SNE, PCA) for visualization of the dimension space
- **Cross-dimension correlation**: combined similarity across dimensions, weighted by relevance
- **Learning from navigation**: track which dimension regions users visit to improve similarity models

---

## 6. Conformance Levels

| Level | Capabilities | Requirement |
|---|---|---|
| **Basic** | `/generate` + `/extent` | Paginated members + boundaries. All generators MUST support this. |
| **Invertible** | + `/inverse` | Value to coordinate mapping + validation. Enables item ingestion control. |
| **Searchable** | + `/search` (exact, range, like) | Deterministic search on generated/stored members. SHOULD support for non-trivial dimensions. |
| **Similarity** | + `/search` (vector) | Vector/embedding-based approximate search. MAY support. Requires vector index backend. |
| **Intelligent** | + `/embed`, `/project` | Full AI integration. MAY support. Future extension. |

Conformance levels are additive -- each builds on the previous. Basic and Invertible are this proposal's core scope. Searchable is recommended. Similarity and Intelligent are documented as the architectural runway.

---

## 7. Extended Step Notation

| Step Value | Meaning | Context |
|------------|---------|---------|
| `"P1D"` | 1 day (ISO 8601 duration) | Gregorian |
| `"P1M"` | 1 month | Gregorian |
| `"1K"` | 1 dekad | Dekadal generator |
| `"1P"` | 1 pentad | Pentadal generators |
| `"1W"` | 1 week | ISO week |

---

## 8. OGC Definition URIs

Register generator algorithm definitions at OGC Naming Authority:

```
http://www.opengis.net/def/generator/ogc/0/dekadal
http://www.opengis.net/def/generator/ogc/0/pentadal-monthly
http://www.opengis.net/def/generator/ogc/0/pentadal-annual
http://www.opengis.net/def/generator/ogc/0/iso-week
http://www.opengis.net/def/generator/ogc/0/integer-range
http://www.opengis.net/def/generator/ogc/0/grid-index
```

Each definition document (SKOS RDF) includes:
- Formal algorithm specification
- Edge case rules (Feb leap year, month-end boundaries)
- OpenAPI specification link
- JSON Schema for `output` and `parameters`
- Notation specification
- Reference implementation link
- Mapping examples

---

## 9. Generator Guidelines

1. **Backward compatibility**: Generators SHOULD produce standard types by default (`date-time` for temporal, `number` for numeric). Custom notation is opt-in via content negotiation.

2. **Pagination built-in**: The generator API MUST support `limit`/`offset` on `/generate`, enabling both bulk retrieval and single-value lookup through the same mechanism.

3. **Unit of measure**: The dimension's `unit` field SHOULD be consistent with the generator's output. For dekadal time, `unit` would be `"dekad"` (approximately 10 days). Reference UCUM or OGC Definition URIs for standard units.

4. **Description and title**: OPTIONAL, multilingual following OGC API - Common `Accept-Language` / `hreflang` patterns.

5. **Key insight -- pagination vs generation**: The two mechanisms are complementary, not competing:

| Scenario | Mechanism |
|----------|-----------|
| Members follow a deterministic algorithm | `generator` -- clients compute locally or call API |
| Members are arbitrary/data-driven | `values_href` -- paginated endpoint |
| Small number of members | `values` -- inline array (existing behavior) |
| Algorithm exists but client wants pre-computed | `values_href` pointing to generator's `/generate` endpoint |

A dimension MAY have both `generator` and `values_href`. The generator defines the algorithm; the endpoint provides a pre-computed, filterable, paginated view.

---

## 10. OGC Standardization Pathway

### Phase A: STAC Community Extension (months 0-3)

1. Open GitHub issue on `stac-extensions/datacube` describing the gaps
2. Submit PR with JSON Schema changes (`size`, `values_href`, `generator`)
3. Reference TB-19/20 findings (docs 23-047, 23-048, 24-035) and this document
4. Link to STAC datacube issue #31

### Phase B: OGC GeoDataCube SWG (months 3-6)

1. Contact GeoDataCube SWG chair (SWG formed January 2025)
2. Submit as Change Request Proposal (CRP) to the GDC specification
3. Present at SWG meeting with gap evidence from TB-19/20
4. Propose `generator` as a new conformance class

### Phase C: OGC Temporal WKT SWG + Naming Authority (months 3-9)

1. Contact Temporal WKT for Calendars SWG -- propose dekadal/pentadal definitions
2. Submit SKOS RDF definitions for each calendar algorithm
3. Register URIs at OGC Naming Authority

### Phase D: OGC Innovation Program (months 6-18, optional)

1. Propose "Scalable Dimension Members" as Testbed thread topic (TB-21 or TB-22)
2. Provide reference implementation as testbed component
3. Deliverable: OGC Engineering Report

### Phase E: OGC RFC + Formal Vote (months 12-24)

1. SWG submits candidate spec for 30-day public comment (RFC)
2. Revision based on comments
3. OGC Technical Committee / Planning Committee vote
4. Outcome: OGC Implementation Standard or Best Practice

---

## 11. Prior Art

- **FAO ASIS gismgr API**: Internal testbed with 4-level paginated dimension hierarchy (`/workspaces/{ws}/dimensions/{code}/members`). Production system at FAO. Validates the need but is not a standard.

- **OGC Testbed 19/20 GeoDataCubes** (docs 23-047, 23-048, 24-035): Identified dimension pagination, irregular temporal enumeration, and non-Gregorian calendars as open gaps.

- **STAC Datacube Extension issue #31**: Community request for `size` property.

- **cadati** (TU Wien, MIT license): Python reference implementation for dekadal date arithmetic.

- **SDMX 3.0 Codelists**: Complete structural decoupling of dimension members from data, but no pagination on structure endpoints. SDMX 3.0 adds GeoCodelist and GeoGridCodelist.

- **openEO API**: Recommends omitting `cube:dimensions` from collection list responses due to size -- acknowledging the problem without solving it.

---

## 12. References

1. STAC Datacube Extension v2.2.0: https://github.com/stac-extensions/datacube
2. STAC as OGC Community Standard (Oct 2025): https://docs.ogc.org/cs/25-004/25-004.html
3. OGC API - Common Part 2: https://docs.ogc.org/is/20-024/20-024.html
4. OGC Testbed 19 GeoDataCubes ER (23-047): https://docs.ogc.org/per/23-047.html
5. OGC Testbed 19 GDC API Draft (23-048): https://docs.ogc.org/per/23-048.html
6. OGC Testbed 20 GDC Profile (24-035): https://docs.ogc.org/per/24-035.html
7. OGC GeoDataCube SWG: https://www.ogc.org/press-release/ogc-forms-new-geodatacube-standards-working-group/
8. OGC Temporal WKT SWG: https://external.ogc.org/twiki_public/TemporalDWG/TemporalSWG
9. OGC Naming Authority Procedures: https://docs.ogc.org/pol/09-046r6.html
10. JSON Schema 2020-12 Core: https://json-schema.org/draft/2020-12/json-schema-core
11. JSON Schema 2020-12 Validation: https://json-schema.org/draft/2020-12/json-schema-validation
12. OpenAPI Specification 3.1: https://spec.openapis.org/oas/v3.1.0
13. SDMX 3.0 Information Model: https://sdmx.org/wp-content/uploads/SDMX_3-0-0_SECTION_2_FINAL-1_0.pdf
14. cadati (dekadal arithmetic): https://github.com/TUW-GEO/cadati
15. STAC Datacube issue #31: https://github.com/stac-extensions/datacube/issues/31
