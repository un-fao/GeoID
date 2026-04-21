# Docker Setup — moved

The docker compose stack now lives under [`src/dynastore/docker/`](../src/dynastore/docker/)
so it ships as pip package data and can be consumed by downstream wrappers
(e.g. [fao-aip-catalog](https://github.com/un-fao/fao-aip-catalog)).

## Quick Start

```bash
cd src/dynastore/docker
cp .env.example .env              # first time only; edit secrets + SCOPE
docker compose up -d              # production-like
# or
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d  # local overlay
```

See [`src/dynastore/docker/.env.example`](../src/dynastore/docker/.env.example)
for the user-facing knobs (SCOPE selectors, host ports, IdP, DB creds).

## What stayed here

- `scripts/` — geoid-maintainer dev tooling (bump-version, build-jupyterlite,
  vendor-web-fonts, `db.sh` thin delegator, Windows entrypoints).

## Rebuild script

`src/dynastore/scripts/rebuild.sh` is the shared dev/test rebuild script and
picks up the composes from their new location automatically. Run it from the
repo root:

```bash
src/dynastore/scripts/rebuild.sh dev           # wipe + rebuild + up
src/dynastore/scripts/rebuild.sh test          # same for test stack
src/dynastore/scripts/rebuild.sh dev --no-wipe # rebuild images only
```
