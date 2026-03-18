# The Features Extension

The `features` extension is Agro-Informatics Platform (AIP) - Catalog Services's primary, modern interface for data interaction. It is a comprehensive implementation of the **OGC API - Features - Part 1: Core** standard, with additional capabilities from Part 2 (Coordinate Reference Systems) and Part 4 (Create, Replace, Update, and Delete). 

This is a stateless orchestrator to translate and manage physical data models bridging OGC clients to Agro-Informatics Platform (AIP) - Catalog Services cores.

## Data Flow for Read and Write Operations

### Creating a Collection (`POST /catalogs/{catalog_id}/collections`)
1. Request body parsed to `ogc_models.CollectionDefinition`.
2. Passed to `catalog_module.create_collection`.
3. The catalog isolates the `.layer_config` attribute.
    - If `None`: Spins up a Logical collection solely for grouping.
    - If active: Creates a Physical collection by triggering the `shared_queries.create_layer` applying PostgreSQL DDL physically rendering a new DB object.
4. Payload responds seamlessly via generic mappings.

### Reading Items (`GET .../items`)
1. Calls the `shared_queries.get_items_paginated_query.execute` on the physical feature array.
2. Data iteration passes down to the `ogc_generator._db_row_to_ogc_feature`.
3. Generator translates standard DB dictionaries parsing `WKBElement` geometry formats strictly out into `GeoJSON` Feature structures automatically.

### Creating an Item (`POST .../items`)
1. Parses standard `GeoJSON Feature`. 
2. Executes absolute "fail-fast". If the catalog determines this is a "logical" collection grouping, the request terminates immediately preventing errors downstream.
3. Feature parsed by `_process_feature_for_db` checking geometry schemas and computing content metadata signatures.
4. Drops into `shared_queries.insert_or_update_feature`. This script triggers heavy resolution on `versioning_behavior`, automatically checking current rows rendering diffs and archiving prior versions internally rendering time-series queries seamless.
