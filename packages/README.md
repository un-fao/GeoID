# `packages/` — per-extension wheel migration

This directory hosts the per-extension wheel split (Phase 1+ of the
wheel migration plan). Each extension migrates to its own pip-installable
distribution from this same monorepo.

## Layout (target)

```
packages/
├── core/                           # produces dynastore-core (FUTURE)
│   ├── pyproject.toml
│   └── src/dynastore/
│       ├── modules/
│       ├── tools/
│       ├── models/
│       └── extensions/             # PEP 420 namespace pkg root
│           ├── protocols.py
│           ├── ogc_base.py
│           ├── ogc_models_shared.py
│           ├── lifespan.py
│           ├── registry.py
│           ├── bootstrap.py
│           └── tools/
└── extensions/
    ├── tasks/                      # produces dynastore-ext-tasks (PILOT)
    │   ├── pyproject.toml
    │   └── src/dynastore/extensions/tasks/
    ├── stats/                      # produces dynastore-ext-stats (NEXT)
    ├── ... (28+ more)
    └── gdal/
```

## Status (2026-05-07)

**Migrated to wheels (Phase 1):**
- `tasks/` → `dynastore-ext-tasks`
- `stats/` → `dynastore-ext-stats`
- `edr/` → `dynastore-ext-edr`
- `moving_features/` → `dynastore-ext-moving-features`
- `connected_systems/` → `dynastore-ext-connected-systems`

5 of 7 ungatable extensions structurally isolated. Remaining ungatable:

- `assets` — needs asset-service extraction from `modules/catalog/`
- `logs` — needs externalised event-listener API in `dynastore-core`

**Pending (Phase 2):** `core/` — framework code still in `src/dynastore/`.

**Pending (Phase 3-4):** the 24 dep-gated extensions (crs, dggs, processes, features, records, coverages, wfs, dwh, styles, joins, gdal, maps, tiles, stac, geoid, gcp_bucket, search, dimensions, notebooks, proxy, template, web, iam, auth, configs, admin, httpx, documentation, events, volumes).

## Build

Each wheel builds independently:

```bash
uv build packages/extensions/tasks
# produces dist/dynastore_ext_tasks-*.whl
```

Workspace-wide build (when `[tool.uv.workspace]` is enabled at the repo
root):

```bash
uv build --all-packages
```

## Why

See the migration plan for context (`~/.claude/plans/...`):
- 7 extensions in dynastore have no natural unique non-SCOPE dep and
  cannot be gated by the dep-import mechanism (assets, logs, stats,
  moving_features, tasks, connected_systems, edr).
- Per-extension wheels solve this structurally: an extension that isn't
  installed isn't discoverable. No SCOPE env, no marker deps.
- Same mechanism handles the 24 currently dep-gated extensions and any
  future extension uniformly.

## Cross-repo

- **geoid** (this repo): SSOT — the source of every wheel lives here.
- **dynastore**: deployment wrapper; `pip install dynastore[$SCOPE] @ git+geoid` resolves to the new wheel set transparently once Phase 5 lands.
- **fao-aip-catalog**: plugin layer; declares `dynastore[api_open, events, module_cache] @ git+geoid` — continues to work unchanged.

The three repos share a `cc/wheels-*` worktree per phase. Land
synchronised commits per `feedback_geoid_dynastore_catalog_sync.md`.
