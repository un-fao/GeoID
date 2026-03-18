# The Catalog Module

The `catalog` module serves as the authoritative source of truth for all metadata related to data organization within Agro-Informatics Platform (AIP) - Catalog Services. It is the system's "librarian", responsible for knowing what data exists, how it is grouped, and whether it is configured for physical storage.

## Responsibilities and Ownership
- **Ownership:** The `catalog` module has exclusive ownership of the `catalogs` and `collections` database tables. No other component is permitted to perform DDL or DML operations on these tables.
- **Lifecycle Management:** It manages the complete lifecycle of catalogs and collections, from creation to deletion (both soft and hard).
- **Information Hub:** It provides a centralized, cached, and performant API for all other components to query this organizational metadata.

## Core Functions and API
The public API of the `catalog` module is a set of clean, asynchronous Python functions that abstract away the underlying database queries:
- `create_catalog(conn, definition)`: Creates a new database schema and inserts a corresponding record into the `catalogs` table.
- `get_catalog` / `list_catalogs`: Cached, read-only functions for retrieving catalog metadata.
- `create_collection(conn, catalog_id, definition)`: The most critical function. It inserts a metadata record into the `collections` table. If the definition object contains a `layer_config`, this function orchestrates a call to `shared_queries.create_layer` to build the physical data table.
- `get_collection` / `list_collections`: Cached functions for retrieving collection metadata.
- `get_collection_layer_definition`: A specialized, cached function that retrieves only the `layer_config` JSONB object for a given collection. Highly performant method for services to check storage blueprints.

## The LayerConfig and "Physical" vs. "Logical" Collections
A fundamental concept in Agro-Informatics Platform (AIP) - Catalog Services's design is the distinction between a "logical" and a "physical" collection. The sole determinant of a collection's type is the presence or absence of the `layer_config` attribute.

### Logical Collections
- **Definition:** A collection created without a `layer_config`. Its entry in the `collections` table has `NULL` in the `layer_config` column.
- **Purpose:** To serve as a metadata grouping mechanism. Used to organize datasets, link to external resources, or build conceptual hierarchies, even if data is not stored.
- **Behavior:** Discoverable via APIs, but cannot be used as targets for data ingestion (`POST /items` will fail).

### Physical Collections
- **Definition:** A collection created with a valid `LayerConfig` object defining physical parameters (SRID, indexing, versioning behavior).
- **Purpose:** To store, manage, and serve geospatial feature data. Sits on top of real PostgreSQL tables.
- **Behavior:** Physical collections expose the full suite of CRUD functionality. Valid targets for bulk ingestion or feature manipulation.
