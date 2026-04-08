# DynaStore PostgreSQL Database Audit -- v1.0 Stable Release

**Date**: 2026-04-08  
**Updated**: 2026-04-08 (post-cleanup)  
**Scope**: All schemas and tables created at startup and during usage across all 5 services (catalog, geoid, worker, tools, maps) + 6 Cloud Run Jobs

---

## Executive Summary

DynaStore originally created **~52 tables per single-catalog deployment** (29 global + 23 per-tenant). After the v1.0 cleanup, the footprint is **~37 tables** (20 global + 17 per-tenant).

| Change | Tables Removed |
|--------|---------------|
| Migration files folded into base DDL | 0 (code cleanup only) |
| Stats moved to OpenSearch | -4 (system.access_logs, system.stats_aggregates, tenant access_logs, tenant stats_aggregates) |
| Auth consolidation (IAG + local OAuth) | -6 (identity_authorization, identity_roles, identity_policies, users, oauth_codes, oauth_tokens) |
| Proxy analytics moved to Elasticsearch | -2 per-tenant (url_analytics, proxy_hourly_aggregates) |
| Collection metadata renamed | 0 (pg_collection_metadata -> metadata, rename only) |
| Notebooks activated | 0 (was dead code, now in scope_catalog) |
| **Total** | **-12 global, -4 per-tenant = ~-15** |

| Schema | Before | After |
|--------|--------|-------|
| **Global** | 29 | 20 |
| **Per-Tenant** | 23 | 17 |
| **Total (1 catalog)** | **52** | **37** |

---

## Changes Applied

### Phase 1: Migration Cleanup

All 14 migration SQL files deleted. Their DDL changes were already folded into the base Python DDL constants.

### Phase 2: Stats to OpenSearch

- Removed `SYSTEM_STATS_DDL` and `TENANT_STATS_DDL` from `stats_module.py`
- Created `modules/stats/elasticsearch_storage.py` — `ElasticsearchStatsDriver`
- Index pattern: `{prefix}_access_logs_{catalog_id}`
- **Tables removed**: `system.access_logs`, `system.stats_aggregates`, tenant `access_logs`, tenant `stats_aggregates`
- **Kept in PG**: tenant `logs` + `system.system_logs`

### Phase 3: Auth Consolidation

- Extended `principals` with `valid_from TIMESTAMPTZ DEFAULT NOW()`
- Removed IAG tables DDL: `identity_authorization`, `identity_roles`, `identity_policies`
- Removed local OAuth tables DDL: `users`, `oauth_codes`, `oauth_tokens`
- Deleted `simplified_iag_storage_methods.py`, `identity_providers/local_db_identity.py`
- Added compatibility methods to `postgres_apikey_storage.py` that read roles/policies from `principals`
- Updated `authentication.py` — Keycloak-only, principal-based sysadmin provisioning
- Sysadmin UUID: `uuid5(NAMESPACE_DNS, "sysadmin.dynastore.local")` — deterministic
- **Tables removed**: 6 global tables

### Phase 4: Collection Metadata Rename

- Renamed `pg_collection_metadata` -> `metadata` across 7 files
- DDL constant renamed: `PG_COLLECTION_METADATA_DDL` -> `METADATA_DDL`
- All SQL references updated (tenant_schema.py, catalog_service.py, collection_service.py, pg_asset_driver.py, postgresql.py, stored_procedures.py, storage_driver.py)

### Phase 5: Proxy Analytics to Elasticsearch

- Removed `TENANT_URL_ANALYTICS_DDL` from `proxy_module.py`
- Removed PG analytics DDL/queries from `queries.py` (CREATE_URL_ANALYTICS_TABLE, CREATE_PROXY_AGGREGATES_*, analytics queries, INSERT_LOG_REDIRECT_BATCH, UPSERT_PROXY_AGGREGATE)
- Created `modules/proxy/elasticsearch_storage.py` — `ElasticsearchProxyAnalytics`
- Index pattern: `{prefix}_proxy_analytics_{schema}`
- Updated `default_storage_driver.py` — URL shortening in PG, analytics in ES
- Removed `initialize_partitions` from `ProxyProtocol` and `ProxyModule`
- **Tables removed**: 2 per-tenant (url_analytics, proxy_hourly_aggregates)
- **Kept in PG**: `short_urls` + `short_urls_catalog` (partitioned by collection_id)

### Phase 6: Notebooks Activation

- Added `notebooks` to `scope_catalog` in `pyproject.toml`
- Registered notebooks as `lifecycle_registry.sync_catalog_initializer` in `notebooks_db.py`
- Notebooks table now created during catalog provisioning

---

## Final Table Inventory

### Global Tables (20)

| # | Schema | Table | Partitioning |
|---|--------|-------|-------------|
| 1 | platform | schema_migrations | None |
| 2 | platform | app_state | None |
| 3 | configs | platform_configs | None |
| 4 | catalog | catalogs | None |
| 5 | catalog | shared_properties | None |
| 6 | tasks | tasks | RANGE (timestamp) |
| 7 | events | events | RANGE (created_at) |
| 8 | events | event_subscriptions | None |
| 9 | styles | styles | LIST (catalog_id) |
| 10 | crs | crs_definitions | LIST (catalog_id) |
| 11 | apikey | principals | None |
| 12 | apikey | identity_links | None |
| 13 | apikey | api_keys | None |
| 14 | apikey | usage_counters | RANGE (period_start) |
| 15 | apikey | quota_counters | None |
| 16 | apikey | roles | None |
| 17 | apikey | role_hierarchy | None |
| 18 | apikey | policies | LIST (partition_key) |
| 19 | apikey | refresh_tokens | None |
| 20 | apikey | audit_log | None |

**Removed global tables**: system.access_logs, system.stats_aggregates (-> OpenSearch), system.system_logs (kept as logs fallback in log_manager.py), identity_authorization, identity_roles, identity_policies, users, oauth_codes, oauth_tokens

**Note**: `system.system_logs` is kept — it serves as fallback in `log_manager.py:404` when catalog schema cannot be resolved.

### Per-Tenant Tables (17)

| # | Table | Partitioning | Module |
|---|-------|-------------|--------|
| 1 | collections | None | catalog |
| 2 | pg_storage_locations | None | catalog |
| 3 | metadata | None | catalog |
| 4 | catalog_configs | None | catalog |
| 5 | collection_configs | None | catalog |
| 6 | assets | LIST (collection_id) | catalog/pg_asset_driver |
| 7 | asset_references | None | catalog/pg_asset_driver |
| 8 | schema_migrations | None | db_config |
| 9 | catalog_events | None | events |
| 10 | catalog_events_dead_letter | None | events |
| 11 | collection_events | LIST (collection_id) | events |
| 12 | collection_events_default | Partition | events |
| 13 | collection_events_dead_letter | None | events |
| 14 | logs | LIST (collection_id) | catalog/log_manager |
| 15 | logs_default | Partition | catalog/log_manager |
| 16 | short_urls | LIST (collection_id) | proxy |
| 17 | short_urls_catalog | Partition | proxy |

**Moved to Elasticsearch**: url_analytics, proxy_hourly_aggregates (proxy analytics), access_logs, stats_aggregates (stats)

**Activated**: notebooks (table 18 when notebooks module is in scope)

### Dynamic Tables (per collection)

| Table Pattern | Purpose |
|--------------|---------|
| `{schema}.t_{hash}` | Item/feature storage with dynamic columns |
| Collection partitions on `assets`, `collection_events`, `logs`, `short_urls` | Per-collection partitions |

---

## Data Moved to Elasticsearch/OpenSearch

| Data | PG Table (removed) | ES Index Pattern |
|------|-------------------|-----------------|
| Access logs | system.access_logs, {tenant}.access_logs | `{prefix}_access_logs_{catalog_id}` |
| Stats aggregates | system.stats_aggregates, {tenant}.stats_aggregates | Computed via ES date histogram aggregations |
| Proxy click analytics | {tenant}.url_analytics | `{prefix}_proxy_analytics_{schema}` |
| Proxy hourly aggregates | {tenant}.proxy_hourly_aggregates | Computed via ES aggregations |

---

## Service-to-Schema Matrix

| Service | Scope | Schemas Created | Tables Written |
|---------|-------|----------------|----------------|
| **catalog** | scope_catalog | public, platform, configs, catalog, tasks, events, styles, crs, {tenant}xN | All core tables |
| **geoid** | scope_geoid | All of catalog + apikey | All + auth tables |
| **worker** | scope_worker | public, platform, catalog, tasks | tasks.tasks (status updates) |
| **tools** | scope_tools | public, platform, configs | proxy URL tables |
| **maps** | scope_maps | public, platform, catalog, styles, crs | None (reads only) |

---

## Architectural Notes

### Auth Model (v1.0)

- **Principals-only**: All identity management flows through `principals` table
- **Keycloak as IdP**: External identity validation via Keycloak (`KEYCLOAK_ISSUER_URL`)
- **API keys**: Linked to principals via `api_keys.principal_id`
- **Local OAuth removed**: No username/password auth. Use Keycloak for on-premise deployments.
- **Sysadmin**: Auto-provisioned with deterministic UUID5(`sysadmin.dynastore.local`)

### Storage Split: PG + ES

- **PG**: Relational data, transactional writes, ACID guarantees (collections, configs, assets, URLs)
- **ES**: Time-series analytics, full-text search, aggregations (access logs, proxy analytics, feature search)
