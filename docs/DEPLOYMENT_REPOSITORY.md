# Deployment Repository Guide

This guide explains how to set up a private deployment repository that deploys
GeoID-based services to GCP Cloud Run.

## Architecture Overview

The platform uses a multi-repository architecture:

```
GeoID (public)                     Dynastore (private infra)         Extension repos (private)
├─ src/dynastore/  (framework)     ├─ .github/actions/               ├─ src/  (custom code)
├─ tests/          (all tests)     │  ├─ setup-workspace/             ├─ tests/
├─ geoid-refs.json (manifest)      │  ├─ merge-config/               ├─ .github/config/
├─ .github/workflows/             │  ├─ setup-env/                  │  ├─ apps.base.yml
│  ├─ test.yml                    │  ├─ test-badge/                 │  ├─ cloudbuild.yml
│  └─ publish-refs.yml            │  └─ update-geoid-refs/          │  └─ iac.yml (optional)
├─ .github/scripts/                ├─ .github/workflows/             ├─ docker/Dockerfile
│  └─ generate_refs_manifest.py   │  ├─ deploy.yml  (reusable)      └─ .github/workflows/
└─ pyproject.toml                  │  └─ update-geoid-refs.yml          ├─ test.yml
                                   ├─ .github/scripts/                  ├─ deploy.yml
                                   │  ├─ build_matrix.py                └─ update-geoid-refs.yml
                                   │  └─ setup_gcs_bucket.sh
                                   └─ .github/config/
                                      ├─ iac.yml  (default templates)
                                      ├─ iac-job.yml
                                      ├─ iac-worker.yml
                                      └─ policy.yml
```

### How `setup-workspace` Works

When an external project calls dynastore's `deploy.yml` via `workflow_call`,
`actions/checkout@v4` checks out the **caller's** repo. The `setup-workspace`
composite action then injects shared scripts and default IaC templates from
dynastore into the caller's workspace:

- **Scripts** (`build_matrix.py`, `setup_gcs_bucket.sh`) — always injected
- **IaC templates** (`iac.yml`, `iac-job.yml`, etc.) — injected only if the
  caller doesn't provide their own (caller overrides take precedence)

This works because `uses: ./.github/actions/setup-workspace` in a reusable
workflow resolves to the **workflow's own repository** (dynastore), not the
caller. The action copies files via `${{ github.action_path }}`.

## Minimum Required Files

A downstream deployment repository needs only:

```
my-project/
├── pyproject.toml                     # Declares dependencies on GeoID
├── src/my_project/                    # Custom modules/extensions
├── docker/
│   ├── Dockerfile                     # Multi-stage Docker build
│   └── docker-compose.test.yml        # Test infrastructure
├── tests/                             # Project-specific tests
├── pytest.ini                         # Test configuration
└── .github/
    ├── workflows/
    │   ├── test.yml                   # Project's own test workflow
    │   └── deploy.yml                 # Calls dynastore's deploy.yml
    └── config/
        ├── apps.base.yml              # Service definitions (REQUIRED)
        ├── apps.production.yml        # Production overrides
        ├── apps.review.yml            # Review overrides
        └── cloudbuild.yml             # Cloud Build config (REQUIRED)
```

Optional overrides (if not provided, dynastore's defaults are used):
- `.github/config/iac.yml` — Cloud Run service template
- `.github/config/iac-job.yml` — Cloud Run job template
- `.github/config/iac-worker.yml` — Cloud Run worker template
- `.github/config/policy.yml` — IAM policy
- `.github/config/bucket_disk_policy.json` — GCS lifecycle rules

## Setup Instructions

### 1. Create `pyproject.toml`

```toml
[project]
name = "my-project"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "dynastore[api_core,catalog] @ git+https://github.com/un-fao/GeoID.git@main",
]

[project.entry-points."dynastore.modules"]
my_module = "my_project.modules.my_module:MyModule"

[project.entry-points."dynastore.extensions"]
my_extension = "my_project.extensions.my_ext:MyExtension"

[project.optional-dependencies]
my_app = ["my-project[module_my_module,extension_my_extension]"]
dev = ["my-project[my_app]", "dynastore[test,dev] @ git+https://github.com/un-fao/GeoID.git@main"]
```

### 2. Create `apps.base.yml`

```yaml
my-service:
  type: "service"
  env:
    SCOPE: "my_app"
    APP: "my_project"
    APP_USER: "my_project"
    NAME: "my-service"
    LOG_LEVEL: "DEBUG"
    API_ROOT_PATH: "/api/my-service"
    GUNICORN_WORKERS: 2
    GUNICORN_THREADS: 1
    CONCURRENCY: 80
    TIMEOUT: 60
    RAM: "1Gi"
    CPU: "1000m"
    MIN_SCALE: 0
    MAX_SCALE: 5
    TCP_PORT: 8080
```

### 3. Create `deploy.yml`

```yaml
name: Deploy
on:
  workflow_dispatch:
    inputs:
      services:
        type: string
        default: 'my-service'
      ignore_tests:
        type: boolean
        default: false
      environment:
        type: choice
        options: ['', review, production]
        default: ''
  push:
    tags:
      - "my-project-prod-*"
      - "my-project-review-*"

jobs:
  prepare:
    runs-on: ubuntu-22.04
    outputs:
      environment: ${{ steps.set-env.outputs.environment }}
      services: ${{ steps.set-env.outputs.services }}
    steps:
      - id: set-env
        run: |
          if [[ "${{ github.ref_type }}" == "tag" ]]; then
            SERVICES="all"
            if [[ "${{ github.ref_name }}" == *-prod-* ]]; then ENV="production"
            else ENV="review"; fi
          else
            SERVICES="${{ inputs.services || 'my-service' }}"
            ENV="${{ inputs.environment || 'review' }}"
          fi
          echo "environment=$ENV" >> "$GITHUB_OUTPUT"
          echo "services=$SERVICES" >> "$GITHUB_OUTPUT"

  run-tests:
    needs: prepare
    if: ${{ !inputs.ignore_tests }}
    uses: ./.github/workflows/test.yml
    with:
      run_integration: true

  deploy:
    needs: [prepare, run-tests]
    if: |
      always() &&
      (needs.run-tests.result == 'success' || needs.run-tests.result == 'skipped')
    uses: un-fao/dynastore/.github/workflows/deploy.yml@main
    with:
      environment: ${{ needs.prepare.outputs.environment }}
      services: ${{ needs.prepare.outputs.services }}
      ignore_tests: ${{ inputs.ignore_tests || false }}
      core_repo: un-fao/GeoID
      core_ref: 'main'
    secrets: inherit
```

### 4. Configure GitHub Variables

In repository settings (Settings → Secrets and variables → Actions → Variables):

| Variable | Description | Example |
|---|---|---|
| `PROJECT_ID` | GCP project ID | `my-gcp-project` |
| `REGION` | Deployment region | `europe-west1` |
| `ARTIFACT_REGISTRY` | Artifact registry name | `my-registry` |
| `WORKLOAD_ID_PROVIDER` | Workload identity provider | `projects/123/...` |
| `SERVICE_ACCOUNT` | Service account email | `deploy@project.iam.gserviceaccount.com` |

### 5. Override IaC Template (Optional)

If the default Cloud Run template doesn't suit your needs (e.g. no GDAL, no
GCSFuse volumes), create your own `.github/config/iac.yml`. The
`setup-workspace` action will NOT overwrite it.

## Deployment Flow

```
my-project tag push / dispatch
  ├─ prepare: resolve environment + services
  ├─ run-tests: my-project's own test.yml (Spanner / custom tests)
  └─ deploy: dynastore deploy.yml@main
        ├─ run-tests: GeoID test.yml@main (core framework tests)
        ├─ build-matrix:
        │     ├─ checkout (my-project code)
        │     ├─ setup-workspace (injects shared scripts from dynastore)
        │     ├─ merge-config (merges my-project's apps.yml)
        │     └─ build_matrix.py → JSON matrix
        └─ deploy-apps (parallel per service):
              ├─ checkout + setup-workspace
              ├─ setup-env → .env (GitHub vars + app config)
              ├─ substitute → IaC templates (my-project's or default)
              ├─ Cloud Build → my-project's Dockerfile
              ├─ Secret Manager update
              └─ gcloud run services/jobs replace
```

## GeoID Version Management (Cross-Repo CI)

When GeoID code changes, downstream repos need to:
1. Update their deploy dropdown with the latest GeoID commits/tags
2. Know whether GeoID tests passed for the latest code

This is fully automated via a **manifest + pull** pipeline. GeoID publishes
a `geoid-refs.json` manifest to its own repo; downstream repos read it on
a schedule. No cross-repo write permissions are needed.

### Live Manifest

GeoID publishes `geoid-refs.json` at:
```
https://raw.githubusercontent.com/un-fao/GeoID/main/geoid-refs.json
```

This is a public URL — no authentication required. Downstream services can
also fetch it at runtime to display GeoID status in their web UI.

Format:
```json
{
  "generated_at": "2026-03-24T10:30:00Z",
  "head_sha": "abc123def456...",
  "tests_passed": true,
  "test_run_url": "https://github.com/un-fao/GeoID/actions/runs/123",
  "options": ["main", "abc123def4 - commit msg", "tag:v1.0.0"]
}
```

### Flow

```
GeoID push to main / new tag
  └─ test.yml runs (existing)
  └─ publish-refs.yml (triggered after test.yml completes):
       ├─ Fetches own recent commits + tags via gh api
       ├─ Captures test pass/fail from workflow_run event
       └─ Commits geoid-refs.json with [skip ci]

Downstream repo (every 5 min schedule OR manual dispatch)
  └─ update-geoid-refs.yml:
       ├─ Fetches geoid-refs.json from raw.githubusercontent.com
       ├─ Updates deploy.yml choice list between marker comments
       ├─ Commits and pushes if changed
       └─ (dynastore only) Runs GeoID tests if manifest says tests failed
```

### Components

| File | Repo | Purpose |
|------|------|---------|
| `.github/workflows/publish-refs.yml` | GeoID | Generates `geoid-refs.json` after tests |
| `.github/scripts/generate_refs_manifest.py` | GeoID | Python script to build the manifest |
| `.github/actions/update-geoid-refs/` | dynastore | Reusable composite action (reads manifest, updates deploy.yml) |
| `.github/workflows/update-geoid-refs.yml` | dynastore | Schedule-based, updates dropdown, conditionally runs tests |
| `.github/workflows/update-geoid-refs.yml` | fao-aip-catalog | Same, reuses dynastore's composite action |

### deploy.yml Marker Comments

The `geoid_ref` choice list in `deploy.yml` is auto-updated between these markers:

```yaml
geoid_ref:
  type: choice
  options:
    # ── geoid-refs-start (auto-updated by update-geoid-refs workflow) ──
    - 'main'
    - 'f6c0b4fabcd1 - fix: skip root_path stripping...'
    - 'tag:v1.2.0'
    # ── geoid-refs-end ──
```

The deploy workflow parses the selected value:
- `main` → uses main branch
- `abc123def456 - commit message` → extracts SHA `abc123def456`
- `tag:v1.0.0` → extracts tag name `v1.0.0`

### Setup for a New Downstream Repo

1. Add marker comments in the repo's `deploy.yml` (see above).

2. Create `.github/workflows/update-geoid-refs.yml`:
   ```yaml
   name: Update GeoID Refs
   on:
     schedule:
       - cron: '*/5 * * * *'
     workflow_dispatch: {}
   jobs:
     update:
       runs-on: ubuntu-22.04
       steps:
         - name: Decode GitHub App private key
           id: decode
           env:
             B64_KEY: ${{ secrets.B64_AUTOMATION_APP_KEY }}
           run: |
             DECODED=$(echo "$B64_KEY" | base64 -d)
             while IFS= read -r line; do
               [ -n "$line" ] && echo "::add-mask::$line"
             done <<< "$DECODED"
             {
               echo "private_key<<PRIVATE_KEY_EOF"
               echo "$DECODED"
               echo "PRIVATE_KEY_EOF"
             } >> "$GITHUB_OUTPUT"
         - name: Generate GitHub App token
           id: auth
           uses: actions/create-github-app-token@v3
           with:
             app-id: ${{ secrets.AUTOMATION_APP_ID }}
             private-key: ${{ steps.decode.outputs.private_key }}
             repositories: my-new-repo
         - uses: actions/checkout@v6
           with:
             token: ${{ steps.auth.outputs.token }}
         - name: Update deploy.yml refs
           uses: un-fao/dynastore/.github/actions/update-geoid-refs@main
           with:
             manifest-url: 'https://raw.githubusercontent.com/un-fao/GeoID/main/geoid-refs.json'
         - name: Commit if changed
           run: |
             git diff --quiet .github/workflows/deploy.yml && exit 0
             git config user.name "github-actions[bot]"
             git config user.email "github-actions[bot]@users.noreply.github.com"
             git add .github/workflows/deploy.yml
             git commit -m "ci: auto-update GeoID ref choices"
             git push
   ```

3. Pass `geoid_ref` through to dynastore's deploy workflow:
   ```yaml
   uses: un-fao/dynastore/.github/workflows/deploy.yml@main
   with:
     geoid_ref: ${{ inputs.geoid_ref || '' }}
   ```

### Required Org Secrets

These must be available as org-level secrets (already configured):

| Secret | Description |
|--------|-------------|
| `AUTOMATION_APP_ID` | GitHub App ID for pushing workflow file changes |
| `B64_AUTOMATION_APP_KEY` | Base64-encoded GitHub App private key |

## Troubleshooting

### Deployment Fails

1. Check GitHub Actions logs for the specific failing step
2. Verify all GitHub variables are set (PROJECT_ID, REGION, etc.)
3. Ensure `cloudbuild.yml` references the correct Dockerfile path
4. Check Cloud Build logs in GCP Console

### Module Not Found at Runtime

1. Verify the entry-point is declared in `pyproject.toml`
2. Ensure the correct `SCOPE` extras are listed in `apps.base.yml`
3. Check that the Dockerfile installs `.[SCOPE]`

### IaC Template Substitution Errors

If env var substitution fails, check that all variables referenced in your
`iac.yml` (e.g. `$NAME`, `${CPU}`, `${TCP_PORT}`) are defined either in
`apps.base.yml` env block or in GitHub repository variables.

## Security Considerations

- Keep deployment repositories **private**
- Limit access to deployment team only
- Use GitHub environment protection rules for production
- Enable required reviews for production deployments
- Never commit `.env` files or GCP credentials
