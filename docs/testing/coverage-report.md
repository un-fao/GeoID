# DynaStore Test Coverage Report

> **Last run:** 2026-04-09  
> **Baseline (stable branch):** ~50% — 39,588 statements  
> **Current (apikey→iam refactor in progress):** ~34% — 38,883 statements  
> **Test suite:** 565 collected | 526 passed | 15–38 errors (refactor-related) | 1 skipped  
> **Runtime:** ~25 min sequential (`-p no:xdist`); ~8 min parallel (`-n auto`)

---

## How to Run

```bash
# Run with coverage (local, sequential - DB must be on port 54320)
cd docker && docker compose -f docker-compose.test.yml up -d db
cd ..
.venv/bin/pytest tests -p no:xdist -o "addopts=" -q --tb=no \
    --cov=src/dynastore --cov-report=term

# Run with coverage (parallel, faster)
.venv/bin/pytest tests -q --tb=no \
    --cov=src/dynastore --cov-report=html --cov-report=term

# Run via Docker (full integration stack)
cd docker && docker compose -f docker-compose.test.yml up --build
```

---

## Coverage Summary by Layer

### Extensions (API layer — FastAPI routers, services)

| Module | Stmts | Miss | Cover | Priority |
|--------|------:|-----:|------:|----------|
| features | 684 | 239 | **65%** | medium |
| configs | 324 | 117 | **63%** | medium |
| tools | 576 | 297 | **48%** | medium |
| iam | 634 | 341 | **46%** | HIGH (refactor) |
| dwh | 271 | 151 | **44%** | medium |
| gcp | 613 | 400 | **34%** | medium |
| logs | 261 | 170 | **34%** | medium |
| assets | 281 | 187 | **33%** | HIGH |
| tiles | 292 | 214 | **26%** | medium |
| stac | 2,540 | 2,042 | **19%** | CRITICAL (largest) |
| web | 716 | 612 | **14%** | HIGH |
| maps | 505 | 491 | **2%** | low |
| wfs | 672 | 663 | **1%** | low |
| admin | 522 | 522 | **0%** | HIGH |
| auth | 176 | 176 | **0%** | HIGH |
| crs | 75 | 75 | **0%** | low |
| dimensions | 218 | 218 | **0%** | medium |
| events | 54 | 54 | **0%** | low |
| gdal | 118 | 118 | **0%** | low |
| geoid | 73 | 73 | **0%** | low |
| httpx | 67 | 67 | **0%** | low |
| notebooks | 89 | 89 | **0%** | low |
| processes | 392 | 392 | **0%** | medium |
| proxy | 215 | 215 | **0%** | medium |
| records | 327 | 327 | **0%** | low |
| search | 366 | 366 | **0%** | HIGH |
| stats | 72 | 72 | **0%** | low |
| styles | 75 | 75 | **0%** | low |
| tasks (ext) | 42 | 42 | **0%** | low |
| template | 482 | 482 | **0%** | low |

### Modules (Business logic layer)

| Module | Stmts | Miss | Cover | Priority |
|--------|------:|-----:|------:|----------|
| stac (config) | 126 | 6 | **95%** | — |
| db | 84 | 32 | **61%** | medium |
| processes | 193 | 78 | **59%** | medium |
| tasks | 925 | 411 | **55%** | HIGH |
| crs | 155 | 81 | **47%** | medium |
| iam | 1,630 | 873 | **46%** | HIGH (refactor) |
| catalog | 7,338 | 3,970 | **45%** | CRITICAL (largest) |
| db_config | 2,194 | 1,210 | **44%** | HIGH |
| stats | 305 | 171 | **43%** | medium |
| notebooks | 286 | 168 | **41%** | low |
| tiles | 577 | 360 | **37%** | medium |
| tools | 193 | 126 | **34%** | medium |
| gcp | 2,243 | 1,560 | **30%** | HIGH |
| storage | 2,099 | 1,611 | **23%** | CRITICAL |
| events | 492 | 403 | **18%** | medium |
| local | 120 | 100 | **16%** | medium |
| cache | 42 | 42 | **0%** | low |
| datastore | 67 | 67 | **0%** | low |
| elasticsearch | 405 | 405 | **0%** | HIGH |
| gdal | 136 | 136 | **0%** | low |
| httpx | 16 | 16 | **0%** | low |
| proxy | 363 | 363 | **0%** | medium |
| styles | 141 | 141 | **0%** | low |
| web | 391 | 391 | **0%** | low |

### Tasks (Background jobs)

| Module | Stmts | Miss | Cover | Priority |
|--------|------:|-----:|------:|----------|
| ingestion | 448 | 287 | **35%** | HIGH |
| tiles_preseed | 149 | 107 | **28%** | medium |
| gcp | 151 | 86 | **43%** | medium |
| elasticsearch | 49 | 29 | **40%** | medium |
| dwh_join | 85 | 53 | **37%** | low |
| export_features | 79 | 40 | **49%** | low |
| elasticsearch_indexer | 164 | 164 | **0%** | HIGH |
| gcp_cloud_runner | 100 | 100 | **0%** | medium |
| gcp_provision | 81 | 81 | **0%** | low |
| gdal | 85 | 85 | **0%** | low |
| schema_migration | 273 | 273 | **0%** | HIGH |
| structural_migration | 30 | 30 | **0%** | low |

### Tools (Shared utilities)

| Module | Stmts | Miss | Cover |
|--------|------:|-----:|------:|
| geospatial_exceptions | 12 | 0 | **100%** |
| plugin | 13 | 0 | **100%** |
| process_factory | 18 | 1 | **94%** |
| identifiers | 35 | 4 | **88%** |
| env | 15 | 2 | **86%** |
| protocol_helpers | 22 | 4 | **81%** |
| discovery | 152 | 31 | **79%** |
| async_utils | 225 | 61 | **72%** |
| cache | 524 | 284 | **45%** |
| geometry_stats | 64 | 42 | **34%** |
| json | 101 | 67 | **33%** |
| geospatial | 209 | 144 | **31%** |
| features | 112 | 79 | **29%** |
| file_io | 335 | 295 | **11%** |
| cache_valkey | 117 | 117 | **0%** |
| dependencies | 80 | 80 | **0%** |
| expression | 67 | 67 | **0%** |
| place_stats | 213 | 213 | **0%** |
| pydantic | 25 | 25 | **0%** |
| timer | 10 | 10 | **0%** |

### Models

| Module | Stmts | Miss | Cover |
|--------|------:|-----:|------:|
| otf | 19 | 0 | **100%** |
| tasks | 78 | 0 | **100%** |
| protocols/ | 614 | 19 | **96%** |
| query_builder | 102 | 13 | **87%** |
| auth_models | 60 | 8 | **86%** |
| auth | 89 | 13 | **85%** |
| shared_models | 231 | 42 | **81%** |
| localization | 247 | 95 | **61%** |
| ogc | 26 | 11 | **57%** |

---

## Test Results (Current Run)

```
3 failed | 526 passed | 2 skipped | ~538 errors (DB/refactor)
```

### Failures

| Test | Reason |
|------|--------|
| `test_keycloak_auth` (3 tests) | Keycloak not in test compose stack |
| Iceberg driver tests (2 failed + 21 errors) | In-memory catalog session management |

### Known Errors (Refactor-related)

The `apikey → iam` rename introduced import errors in tests that still reference the old `apikey` module paths. These resolve once the refactor is complete:

- `tests/dynastore/extensions/iam/integration/test_authorization_api.py` — fixture setup fails
- `tests/dynastore/extensions/iam/integration/test_web_access_repro.py`
- Many tests importing from `dynastore.modules.apikey.*`

---

## Test Duplications Found

### 3-Way CRUD Overlap
Catalog/Collection lifecycle is tested at three layers — each valid but with diminishing returns:

| Test | File | Layer |
|------|------|-------|
| `test_catalog_ops` | `modules/catalog/integration/test_catalog_ops.py` | Protocol |
| `test_catalog_lifecycle` | `extensions/features/integration/test_catalogs_ops.py` | HTTP /features/ |
| `test_stac_catalog_lifecycle` | `extensions/stac/integration/test_stac_ops.py` | HTTP /stac/ |

**Recommendation:** Keep the module-level test as source of truth. Replace the extension tests with lighter parametrized API-contract tests (check status codes + response shape only, not full CRUD).

### Fixture Duplication (3x each)
`catalog_id`, `collection_id`, `item_id`, `catalog_data`, `collection_data`, `setup_catalog`, `setup_collection` are defined independently in:
- `tests/conftest.py`
- `tests/dynastore/extensions/features/integration/conftest.py`
- `tests/dynastore/modules/catalog/integration/conftest.py`

**Recommendation:** Remove child conftest definitions that are identical to the parent. Keep child overrides only where the fixture intentionally differs.

### Language Tests (2x)
`test_create_catalog_with_language`, `test_get_catalog_with_language_resolution`, `test_create_collection_with_language` appear in both `test_stac_multilanguage.py` and `test_language_extension.py`.

### Non-Test Files in `tests/`
These are scripts, not pytest tests, and pollute the collection:

| File | Status |
|------|--------|
| `tests/repro_issue.py` | Debug script — move to `scripts/` |
| `tests/verify_search_optimization.py` | Standalone script — move to `scripts/` |
| `tests/verify_identity_refactor.py` | Standalone script — move to `scripts/` |

---

## Prioritized Recommendations

### Phase 1 — Fix Broken Tests (unblock coverage measurement)
1. Complete `apikey → iam` rename in all test imports (`tests/dynastore/extensions/apikey/` → `tests/dynastore/extensions/iam/`)
2. Fix `wait_for_db` autouse fixture to be skippable for unit tests that don't need DB
3. Fix Iceberg driver test fixture (`in_memory_catalog` session scope issue)

### Phase 2 — High-Impact Coverage Gains
Focus on integration tests (platform preference) for these modules in order of LOC × zero-coverage:

| Target | LOC | Current | Approach |
|--------|----:|--------:|---------|
| `extensions/stac` (stac_generator, stac_service, search) | 7,234 | 19% | Extend existing `test_stac_ops.py` parametrically |
| `modules/catalog` (asset_manager, config_manager, drivers) | 20,294 | 45% | Add to `test_catalog_ops.py` + new `test_asset_api.py` |
| `modules/storage/drivers` (elasticsearch, iceberg, duckdb) | 4,979 | 23% | Fix Iceberg fixture; add ES driver integration test |
| `modules/gcp` (bucket_manager, eventing_ops) | 5,259 | 30% | Mock GCS client; test event routing |
| `tasks/schema_migration` | 813 | 0% | Add `test_schema_migration.py` with catalog schema round-trip |
| `extensions/search` | 1,159 | 0% | Add `test_search_api.py` via in-process client |
| `extensions/web` (web.py dashboard) | 1,742 | 14% | Fix `test_dashboard_api.py` fixture |
| `extensions/wfs` | 1,824 | 1% | Fix `test_wfs_*.py` errors |
| `tools/file_io` | 335 | 11% | Add unit tests for shapefile/parquet/geopackage writers |

### Phase 3 — Consolidate Duplicates
1. Merge fixture definitions into root `conftest.py`
2. Replace 3-way CRUD lifecycle tests with parametrized endpoint tests
3. Move `repro_issue.py`, `verify_*.py` to `scripts/debug/`

### Phase 4 — New Coverage for Zero-Coverage Modules
Lower priority (0% but smaller or GCP-only):

- `extensions/admin` — admin routes need sysadmin-role integration test
- `extensions/processes` — OGC Processes API test with mock job runner
- `tasks/elasticsearch_indexer` — mock ES client + task dispatch test
- `tools/cache_valkey` — unit test with `fakeredis`/mock valkey
- `tools/expression` — pure unit test for CQL expression builder

---

## `wait_for_db` Fix for Unit Tests

The current `wait_for_db` session-scoped autouse fixture blocks ALL tests (including pure unit tests) when no DB is available. Proposed fix:

```python
@pytest.fixture(scope="session", autouse=True)
def wait_for_db(request):
    """Skip DB wait for tests marked as unit tests or when no DB needed."""
    # Skip if all collected tests are unit tests (no DB dependency)
    # Check if any test in session uses app_lifespan or db_engine
    needs_db = any(
        "app_lifespan" in item.fixturenames or "db_engine" in item.fixturenames
        for item in request.session.items
    )
    if not needs_db:
        return  # No DB-dependent tests collected
    # ... existing DB wait logic
```

This allows `pytest tests/dynastore/modules/storage/unit` to run without a live DB.
