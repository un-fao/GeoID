# DynaStore: The Hyperscale Geospatial Platform

**Version: 6.0 Enterprise Release**

The Cloud-Native Geospatial Infrastructure

A modular, high-performance platform engineered for OGC compliance and infinite horizontal scalability. Deploy anywhere—on-premise or cloud.

---

## Core Principles

Built on a foundation of strict standards and database-driven orchestration.

### Database as OS
We push orchestration into PostgreSQL. Triggers, Partitioning, and JSONB handle complexity, ensuring data integrity at the lowest level.

### Logical/Physical Split
User-facing `codes` are stable. Physical schemas (`s_a1b2...`) are immutable and collision-free. Rename anything, break nothing.

### OGC Native
Built for OGC API - Features, STAC, Coverages, and Processes from day one. Interoperability is not an afterthought, it's the foundation.

---

## Enterprise Capabilities

Dynastore transforms how organizations manage geospatial data. It replaces brittle monoliths with a flexible, modular ecosystem that adapts to your needs.

### True Multi-Tenancy
Create isolated environments for departments or clients instantly. Each `catalog_id` maps to a distinct, physically isolated database schema.
*   Data Isolation via Physical Schemas
*   Performance Isolation
*   Cloud & On-Premise Support

### Comprehensive OGC Support
Stop building custom APIs. Dynastore exposes your data through standardized, interoperable interfaces compatible with QGIS, ArcGIS, and web maps.
*   OGC API - Features (Parts 1-4: Core, CRS, Filtering, CRUD)
*   SpatioTemporal Asset Catalog (STAC API 1.0.0)
*   OGC API - Coverages, Tiles, Maps, Processes, Records
*   OGC Dimensions (paginated datacube dimensions)

### Deployment Flexibility
#### Cloud Native
Designed for Kubernetes and serverless environments. Leverages cloud object storage (S3, GCS) for raw assets while keeping structured data in high-performance PostgreSQL.

#### On-Premise Ready
Fully containerized architecture allows for secure, air-gapped deployments in sensitive environments without sacrificing scalability.

---

## The Three Pillars Architecture

Strict separation of concerns prevents the "Big Ball of Mud".

### Pillar I: Modules
Backend-agnostic libraries. The Single Source of Truth for data domains.
Example: `catalog_module.py`
```python
@dynastore_module
class CatalogModule:
    # Owns table 'catalogs'
    async def create_catalog(...):
```

### Pillar II: Extensions
Stateless API adapters. Translate HTTP to Module calls.
Example: `stac_service.py`
```python
@router.get("/collections/{code}")
async def get_coll(code):
    # Calls Module
    return module.get_collection(code)
```

### Pillar III: Tasks
Isolated background workers via OGC API Processes.
Example: `ingestion_task`
```python
# Separate Container
def run_job(payload):
    with sync_db_pool() as conn:
        process_file(payload)
```

---

## Supported Extensions

Modular capabilities that extend the core platform.

### OGC API - Features
Core vector data service. Supports Part 1 (Core) and Part 2 (CRS). Provides filtering and GeoJSON output.

### STAC API
SpatioTemporal Asset Catalog. Enables discovery of geospatial assets (images, point clouds) linked to features.

### OGC API - Tiles
High-performance vector (MVT) and raster tiles generated on-the-fly from the database.

### OGC API - Processes
Standardized interface for asynchronous tasks (e.g., ingestion, analysis). Includes job status tracking.

### WFS 2.0
Legacy support for Web Feature Service to ensure compatibility with older GIS clients (ArcMap, QGIS).

### OGC API - Maps
Server-side rendering of map images (PNG/JPEG) using SLD/Mapbox styles.

### OGC API - Coverages
Raster and datacube data access via the OGC API Coverages standard. Exposes coverage landing pages, domain sets, range types, and data retrieval endpoints per catalog/collection. Backed by the pluggable storage driver architecture (DuckDB, Iceberg, PostgreSQL).

---

## Storage Abstraction Layer

How `catalog` maps logical **Codes** to physical **Schemas**, enabling renaming without data movement.

*(Note: The original SVG infographic is not directly convertible to Markdown. This section describes the concept.)*

The system uses a layered approach:
*   **Logical Layer (API / Catalog Module):** User-facing concepts like "Catalog" and "Collection" with human-readable codes (e.g., "agriculture", "fields_v1").
*   **Configuration Layer (Config Manager):** Stores metadata about how logical entities map to physical storage, including partitioning strategies and SRIDs.
*   **Physical Layer (PostgreSQL / Schemas / Partitions):** The actual database structures. Logical codes are mapped to generated, immutable physical schema and table names (e.g., `s_a1b2c3`, `t_x9y8z7`).

---

## Database Topology

Understanding how Dynastore maps logical concepts to physical storage structures.

### 1. The Catalog Schema
Every `Catalog` has a **Physical Schema** resolved via its `code`. The logical code (e.g., `agriculture`) maps to a generated schema (e.g., `s_a1b2c3`). This ensures zero data leakage and allows `code` renaming.
```sql
CREATE SCHEMA IF NOT EXISTS "s_a1b2c3";
```

### 2. The Collection Table
A `Collection` becomes a **Partitioned Table** inside that schema. The logical code (e.g., `fields_v1`) maps to a generated table name (e.g., `t_x9y8z7`). Configuration for this table is stored in the same physical schema.
```sql
CREATE TABLE "s_a1b2c3"."t_x9y8z7" (...) PARTITION BY LIST (partition_key);
```

### 3. "Lazy" Partitions
Dynastore creates partitions **Just-In-Time** (JIT) using Database Triggers. When a row is inserted, the DB checks if a partition exists. If not, it creates it instantly.
```
TRIGGER BEFORE INSERT -> create_partition_if_not_exists()
```

#### Partitioning Logic Visualization
1.  **API Request:** POST Feature to `/collections/fields_v1`
2.  **Resolution:** Catalog Module resolves `fields_v1` -> `t_x9y8z7`
3.  **Trigger Fires:** `create_partition_if_not_exists()` creates `t_x9y8z7_p_h123`
4.  **Data Stored:** Row persists in new physical partition.

---

## Why Dynastore?

Dynastore is not just code; it's a strategic asset designed to lower TCO, mitigate risk, and capture the explosive growth of the geospatial market.

### Total Cost of Ownership (TCO)
Traditional GIS systems are monolithic license-heavy beasts. Dynastore's modularity means you deploy *only* what you need. By leveraging **PostgreSQL** capabilities (Partitioning, JSONB) instead of expensive external orchestrators, infrastructure costs are slashed by up to 60%.

### Risk Mitigation
Monoliths are brittle; one bug brings down the system. Our **"Three Pillars"** isolation ensures that a failure in data processing never impacts customer-facing APIs. This structural resilience protects SLAs and brand reputation.

### Market Agility
The geospatial market moves fast. New standards (like OGC API) emerge yearly. Dynastore's API-agnostic core allows us to adopt new standards via plug-and-play **Extensions** without refactoring the engine. We are future-proof by design.

### The Trillion-Row Scale
With the new **Physical vs. Logical** abstraction layer, Dynastore handles data at a scale competitors crumble under. "Lazy" partitioning and base62-encoded physical schemas allow for infinite horizontal scaling of tenant data.