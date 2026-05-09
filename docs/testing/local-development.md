# Running tests locally

DynaStore's test suite expects a PostgreSQL instance reachable at `postgresql://testuser:testpassword@localhost:54320/gis_dev`. The simplest path is the bundled docker-compose stack; the Makefile wraps it.

## Prerequisites

- Docker (Compose v2 plugin)
- Python 3.12 + project virtualenv at `.venv/`
- Test dependencies installed: `pip install -e '.[dev]'`

## One-shot run

```bash
make db-up      # start postgres on 127.0.0.1:54320 and wait for readiness
make test       # run unit + integration suite
make db-down    # stop and drop the volume when finished
```

`make test` depends on `db-up`, so a fresh checkout only needs `make test`. `make db-down` drops the volume — destructive, but the only way to reset between schema changes during development.

## Targeted runs

```bash
make test-unit          # unit tests only; no database required for the bulk of them
make test-integration   # integration tests only (requires db-up)
make test-coverage      # full suite + HTML coverage at htmlcov/index.html
```

For ad-hoc invocations bypass the Makefile:

```bash
.venv/bin/pytest tests/dynastore/extensions/stac/integration/test_stac_put_replace.py -vv
```

## Without docker

If you already run a local PostgreSQL on `:54320`:

```bash
createdb -h localhost -p 54320 -U testuser gis_dev
.venv/bin/pytest tests -vv
```

The schema is created on demand by `tests/conftest.py::pytest_sessionstart`. The user `testuser` must own `gis_dev` so xdist workers can clone it into per-worker template databases (`gis_dev_gw0`, `gis_dev_gw1`, …) under an advisory lock.

## xdist parallelism

Tests run with `-n auto --dist worksteal` by default. Per-worker databases are cloned from `gis_dev` at session start; an advisory lock (`pg_advisory_lock(8472001)`) serializes the clones. If you see `ObjectInUseError: source database "gis_dev" is being accessed by other users`, a previous run left connections open — `make db-down && make db-up` resets cleanly.

To disable parallelism (useful when stepping through with `--pdb`):

```bash
.venv/bin/pytest tests -p no:xdist -o "addopts=" --pdb
```

## Elasticsearch integration tests

Optional and advisory-only in CI. To run locally, bring up the ES stack alongside the database:

```bash
docker compose -f packages/core/src/dynastore/docker/docker-compose.yml up -d db elasticsearch
.venv/bin/pytest tests/dynastore/modules/elasticsearch/integration -vv
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `connection refused on :54320` | DB not started | `make db-up` |
| `source database "gis_dev" is being accessed` | leftover xdist connections | `make db-down && make db-up` |
| Tests pass locally but fail in CI | docker image lag | rebuild with `docker compose build db` |
| `pyright: ...` errors but no test failure | typecheck only | `.venv/bin/pyright packages/` |

## Linting

Ruff is not in `.venv/`; run via `uv tool run ruff check <files>`. Pyright is at `.venv/bin/pyright`.
