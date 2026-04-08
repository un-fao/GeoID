# DynaStore Authentication -- v1.0

## Identity Provider: Keycloak

DynaStore v1.0 uses Keycloak as the sole external identity provider. Local username/password authentication was removed to reduce security surface area.

### Configuration

| Environment Variable | Required | Description |
|---------------------|----------|-------------|
| `KEYCLOAK_ISSUER_URL` | Yes | Keycloak realm issuer URL (e.g., `https://keycloak.example.com/realms/dynastore`) |
| `KEYCLOAK_CLIENT_ID` | Yes | OAuth2 client ID registered in Keycloak |
| `KEYCLOAK_AUDIENCE` | No | Expected JWT audience claim |
| `SESSION_SECRET_KEY` | Recommended | Secret for session cookie encryption. Auto-generated if not set. |

### Auth Flow

1. Client calls `GET /auth/authorize` with OAuth2 parameters
2. DynaStore redirects to Keycloak authorization endpoint
3. After Keycloak login, callback returns with authorization code
4. Client exchanges code for tokens at Keycloak's token endpoint
5. Client calls DynaStore APIs with `Authorization: Bearer <access_token>`
6. DynaStore validates JWT via Keycloak's JWKS endpoint

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/authorize` | GET | OAuth2 authorization redirect to Keycloak |
| `/auth/userinfo` | GET | Returns user profile from Keycloak token |
| `/auth/me` | GET | Alias for `/auth/userinfo` |
| `/auth/logout` | GET | Clears session, optional `redirect_uri` |
| `/auth/debug` | GET | Auth state inspection (requires valid token) |

## Principal Model

All authenticated identities resolve to a `Principal`:

- **Table**: `apikey.principals`
- **Key fields**: `id` (UUID), `identifier`, `display_name`, `roles` (JSONB), `is_active`, `valid_from`
- External identities linked via `apikey.identity_links` (provider + subject_id -> principal_id)

### Sysadmin Principal

Auto-provisioned during startup with deterministic UUID:
```python
uuid5(NAMESPACE_DNS, "sysadmin.dynastore.local")
```

### API Keys

API keys are linked to principals. Each key has:
- Hashed storage (`key_hash`)
- Optional domain/catalog/collection restrictions
- Expiration and usage quotas
- Scoped policies for fine-grained access

## Authorization

Authorization uses the `PermissionProtocol` with RBAC + ABAC:

- **Roles**: Defined in `apikey.roles` with hierarchical inheritance
- **Policies**: ALLOW/DENY rules with action/resource/condition matching
- **Registration**: Extensions register policies during lifespan via `PermissionProtocol.register_policy()`

## On-Premise Deployment

For deployments without internet access to a cloud Keycloak instance:

1. Deploy Keycloak alongside DynaStore (Docker Compose or K8s)
2. Create a realm and client for DynaStore
3. Set `KEYCLOAK_ISSUER_URL` and `KEYCLOAK_CLIENT_ID`
4. Configure user federation (LDAP, Active Directory) in Keycloak if needed
