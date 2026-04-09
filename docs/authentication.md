# DynaStore Authentication -- v2.0

## Identity Provider: External OIDC (IdP-Agnostic)

DynaStore v2.0 delegates authentication to external OIDC-compliant identity providers (Keycloak, Auth0, Azure AD, etc.). The platform is IdP-agnostic — any provider implementing `IdentityProviderProtocol` can be registered.

### Configuration

| Environment Variable | Required | Description |
|---------------------|----------|-------------|
| `KEYCLOAK_ISSUER_URL` | Yes* | IdP realm issuer URL (e.g., `https://keycloak.example.com/realms/dynastore`) |
| `KEYCLOAK_CLIENT_ID` | Yes* | OAuth2 client ID registered in the IdP |
| `KEYCLOAK_PUBLIC_URL` | No | Browser-reachable IdP URL (if different from internal `KEYCLOAK_ISSUER_URL`) |
| `SESSION_SECRET_KEY` | Recommended | Secret for session cookie encryption. Auto-generated if not set. |

*Required when using Keycloak. Other IdP implementations may use different env vars.

### Auth Flow

1. Client calls `GET /auth/authorize` with OAuth2 parameters
2. DynaStore redirects to IdP authorization endpoint
3. After IdP login, callback returns with authorization code
4. Client exchanges code for tokens at IdP's token endpoint
5. Client calls DynaStore APIs with `Authorization: Bearer <access_token>`
6. DynaStore validates JWT via IdP's JWKS endpoint

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/authorize` | GET | OAuth2 authorization redirect to IdP |
| `/auth/userinfo` | GET | Returns user profile from IdP token |
| `/auth/me` | GET | Alias for `/auth/userinfo` |
| `/auth/logout` | GET | Clears session, optional `redirect_uri` |
| `/auth/debug` | GET | Auth state inspection (requires valid token) |

## Principal Model

All authenticated identities resolve to a `Principal`:

- **Table**: `iam.principals`
- **Key fields**: `id` (UUID), `identifier`, `display_name`, `roles` (JSONB), `is_active`, `valid_from`
- External identities linked via `iam.identity_links` (provider + subject_id -> principal_id)

## Authorization

Authorization uses the `PermissionProtocol` with RBAC + ABAC:

- **Roles**: Defined in `iam.roles` with hierarchical inheritance
- **Policies**: ALLOW/DENY rules with action/resource/condition matching
- **Registration**: Extensions register policies during lifespan via `PermissionProtocol.register_policy()`

### Sysadmin Role

The `sysadmin` role provides full platform administration. It is assigned via the external IdP (e.g., Keycloak realm role) and resolved to a local Principal with elevated privileges.

## On-Premise Deployment

For deployments without internet access to a cloud IdP instance:

1. Deploy Keycloak (or another OIDC provider) alongside DynaStore (Docker Compose or K8s)
2. Create a realm and client for DynaStore
3. Set `KEYCLOAK_ISSUER_URL` and `KEYCLOAK_CLIENT_ID`
4. Configure user federation (LDAP, Active Directory) in the IdP if needed

## Programmatic Access (Service-to-Service)

For machine-to-machine authentication, use OAuth2 Client Credentials flow:

1. Create a confidential client in your IdP (e.g., Keycloak)
2. Request a token using client credentials:
   ```bash
   curl -X POST "${KEYCLOAK_ISSUER_URL}/protocol/openid-connect/token" \
     -d "grant_type=client_credentials" \
     -d "client_id=my-service" \
     -d "client_secret=my-service-secret"
   ```
3. Use the returned `access_token` as a Bearer token in DynaStore API calls:
   ```bash
   curl -H "Authorization: Bearer ${ACCESS_TOKEN}" \
     http://localhost/catalogs
   ```

The IdP issues short-lived JWTs; no long-lived API keys are stored in DynaStore.
