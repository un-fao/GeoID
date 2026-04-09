# Docker Setup

## Quick Start

```bash
# Local development (exposes DB port, adds healthchecks)
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d

# Production-like
docker compose up -d
```

## Services & Ports

| Service       | Container          | Host Port | Internal Port |
|---------------|--------------------|-----------|---------------|
| Catalog API   | geoid_catalog      | 80        | 80            |
| GeoID Web     | geoid_web          | 8080      | 80            |
| Worker        | geoid_worker       | 81        | 81            |
| Tools         | geoid_tools        | 8082      | 8082          |
| Maps          | geoid_maps         | 8083      | 8083          |
| Keycloak      | geoid_keycloak     | 8180      | 8080          |
| OpenSearch     | geoid_elasticsearch| 9200      | 9200          |
| OS Dashboards | geoid_kibana       | 5601      | 5601          |
| PostgreSQL    | geoid_db           | 54320*    | 5432          |

\* DB port only exposed with `docker-compose.local.yml` overlay.

## Authentication

### Keycloak Admin Console

URL: `http://localhost:8180/`

| Username | Password | Source |
|----------|----------|--------|
| `admin`  | `admin`  | `KEYCLOAK_ADMIN_PASSWORD` in `.env` |

### Test Users

Provisioned via `keycloak/realm-export.json`. All use password `testpassword`.

| Username     | Realm Roles      | Client Roles (`geoid-api`) |
|--------------|------------------|----------------------------|
| `testadmin`  | admin            | catalog_admin              |
| `testuser`   | user             | -                          |
| `testviewer` | viewer           | -                          |

### Environment Variables

| Variable              | Purpose                                              |
|-----------------------|------------------------------------------------------|
| `KEYCLOAK_ISSUER_URL` | Internal URL for JWT validation / JWKS (Docker-internal) |
| `KEYCLOAK_PUBLIC_URL` | Browser-facing URL for OAuth redirects               |
| `KEYCLOAK_CLIENT_ID`  | OAuth2 client ID                                     |
| `KEYCLOAK_CLIENT_SECRET` | OAuth2 client secret (confidential client)        |

## Configuration

All build/runtime configuration is in `.env`. See comments there for details.
SCOPE variables control which modules each service loads (see `pyproject.toml` optional-dependencies).
