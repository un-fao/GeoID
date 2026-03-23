# Authentication & Authorization

This document describes the security architecture of AIP Catalog Services. The system implements a zero-trust model where every request is evaluated against a policy engine.

## Architecture

```
Request → ApiKeyMiddleware → Identity Resolution → Policy Evaluation → Allow / Deny
```

### Identity Resolution

The middleware extracts credentials from:
1. `Authorization: Bearer <jwt_token>` header
2. `X-API-Key: <raw_key>` header
3. `?api_key=<raw_key>` query parameter

Credentials are passed to registered **Identity Providers** (discoverable via `IdentityProviderProtocol`):

| Provider | Validates | Issues JWT |
|----------|-----------|------------|
| `LocalDBIdentityProvider` | Raw API keys (SHA-256 hash lookup), JWT tokens (signature verification) | Yes |
| `KeycloakIdentityProvider` | Keycloak-issued JWTs via JWKS endpoint | No |

If no credentials are provided, the request is treated as **anonymous** with the `anonymous` role.

### Principal Model

Every authenticated request produces a `Principal`:

```
Principal
├── subject_id    — UUID of the authenticated entity
├── roles         — ["sysadmin", "user", ...] from principals table
├── custom_policies — inline Policy objects attached to the key
├── catalog_match — optional regex restricting catalog access
├── collection_match — optional regex restricting collection access
└── metadata      — arbitrary key-value attributes
```

## Policy Engine

### Evaluation Flow

```
1. Collect roles from principal (subject_id → roles via principals table)
2. For each role, look up policy IDs (roles → policies mapping)
3. Fetch full Policy objects from storage
4. Include custom_policies directly attached to the principal
5. For each policy: check action regex against HTTP method, resource regex against path
6. First matching DENY → deny immediately
7. First matching ALLOW → allow
8. No match → deny by default (implicit deny)
```

### Policy Model

```
Policy
├── id              — unique identifier (e.g. "sysadmin_full_access")
├── actions         — regex patterns matching HTTP methods ["GET", "POST", ".*"]
├── resources       — regex patterns matching URL paths ["/features/.*", "/admin/.*"]
├── effect          — "ALLOW" or "DENY"
├── conditions      — optional list of Condition objects (rate limits, IP, time)
├── partition_key   — "global" or catalog_id for tenant-scoped policies
└── description     — human-readable description
```

Wildcard `"*"` in actions/resources is auto-converted to `".*"` regex via `transform_wildcards` validator.

### Default Roles and Policies

Provisioned on first startup via `PolicyService.provision_default_policies()`:

| Role | Policies | Description |
|------|----------|-------------|
| `sysadmin` | `sysadmin_full_access` | Unrestricted `.*` on `.*` |
| `admin` | `sysadmin_full_access` | Same as sysadmin |
| `anonymous` | `public_access` | GET/POST/OPTIONS/HEAD on `/`, `/health`, `/docs.*`, `/apikey/auth/*` |
| `user` | `self_service_access` | GET on `/apikey/me/.*`, `/auth/me` |

Extensions register additional policies during their `lifespan()`. For example:
- `features_public_access` → `/features/.*` (GET/POST/OPTIONS)
- `stac_public_access` → `/stac/.*`
- `maps_public_access` → `/maps/.*`
- `admin_access` → `/admin/.*` (linked to sysadmin/admin roles)

### Conditions

Policies support conditions evaluated at request time:

| Condition Type | Config | Description |
|----------------|--------|-------------|
| `rate_limit` | `max_requests`, `period_seconds`, `scope` | Token-bucket rate limiting. Scope: `global`, `catalog`, `collection` |
| `ip_whitelist` | `allowed_ips` | CIDR-based IP restriction |
| `time_window` | `start_hour`, `end_hour` | Time-of-day access window |

### Concurrent Role Registration

During startup, multiple extensions register policies for the same roles (e.g., adding `maps_public_access` to `anonymous`). This is handled via fire-and-forget `asyncio.create_task()` calls, serialized by an `asyncio.Lock` in `ApiKeyModule._persist_role()` to prevent read-modify-write races.

## API Key Lifecycle

```
POST /apikey/keys       — create a new API key (returns raw key once)
GET  /apikey/me/keys    — list own keys
DELETE /apikey/keys/{id} — revoke a key
POST /apikey/auth/login  — exchange API key for JWT token
GET  /apikey/auth/validate — validate a JWT token
GET  /apikey/auth/jwks.json — public JWKS for token verification
```

### Key Storage

Raw API keys are never stored. On creation:
1. A random key is generated
2. The SHA-256 hash is stored in `apikey.api_keys` (partitioned by hash)
3. A 10-char prefix is stored for identification
4. The raw key is returned once to the caller

### JWT Tokens

- Issued by `LocalDBIdentityProvider` on successful login
- Signed with a per-instance secret stored in `apikey.jwt_config`
- Contains `sub` (principal UUID), `roles`, `exp`, `iat`
- Validated via JWKS endpoint or direct secret verification

## Database Tables

All auth tables live in the `apikey` schema:

| Table | Purpose |
|-------|---------|
| `principals` | Identity registry (UUID, roles, custom_policies) |
| `api_keys` | Key hashes, linked to principals (HASH-partitioned) |
| `identity_links` | Maps external identities (provider + subject_id) to principals |
| `users` | Username/password_hash for local auth |
| `policies` | Policy definitions (LIST-partitioned by partition_key) |
| `roles` | Role-to-policy mappings |
| `jwt_config` | JWT signing secrets |
| `refresh_tokens` | Refresh token storage |

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/modules/apikey/module.py` | ApiKeyModule — lifespan, register_policy, register_role |
| `src/dynastore/modules/apikey/policies.py` | PolicyService — CRUD, evaluation engine |
| `src/dynastore/modules/apikey/apikey_service.py` | ApiKeyService — key creation, authentication |
| `src/dynastore/extensions/apikey/middleware.py` | Request interception, principal resolution, policy evaluation |
| `src/dynastore/extensions/apikey/service.py` | REST API endpoints for key management |
| `src/dynastore/modules/apikey/identity_providers/` | LocalDB and Keycloak identity providers |
