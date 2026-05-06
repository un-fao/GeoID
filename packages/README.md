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

- `tasks/` — **PILOT**, source migrated, building as `dynastore-ext-tasks`
- `core/` — pending; framework code still in `src/dynastore/`
- everything else — still in `src/dynastore/extensions/`

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
