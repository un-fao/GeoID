# Identity Providers

This directory contains the concrete implementations of `IdentityProviderProtocol`.
The active provider is selected at startup from the **`IdpConfig` platform config**
(class_key `idp_config`), with a deprecated environment-variable fallback (see
[Configuration](#configuration-idpconfig-preferred) below).

---

## Protocol Contract

Every provider must implement `IdentityProviderProtocol`
(`src/dynastore/modules/iam/interfaces.py`):

| Method | Purpose |
|--------|---------|
| `get_provider_id() -> str` | Unique string tag stored in `identity_links.provider` (e.g. `"oidc"`) |
| `validate_token(token) -> dict \| None` | Validate an access token and return a normalised identity dict |
| `get_authorization_url(redirect_uri, state) -> str` | Build the browser login redirect URL |
| `exchange_code_for_token(code, redirect_uri) -> dict` | Exchange auth code for tokens |
| `get_user_info(access_token) -> dict` | Fetch user profile from the IdP |

The identity dict returned by `validate_token` must include at minimum:
```python
{
    "provider": "oidc",           # must match get_provider_id()
    "sub": "<immutable user id>", # used as the stable external identity key
    "email": "...",
    "name": "...",
    "realm_roles": [...],         # flat list of role names
    "client_roles": [...],
    "raw_claims": {...},          # full token payload for audit
}
```

---

## Current Implementation: `OidcIdentityProvider`

**File:** `oidc_identity.py`  
**Selected by:** `IDP_TYPE=oidc` (default)

Uses [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414) / OIDC Discovery
(`/.well-known/openid-configuration`) to resolve all endpoints at runtime.
This makes it compatible with any standards-compliant IdP without code changes.

Tested with: **Keycloak**, **Okta**, **Auth0**, **Azure AD**, **Google**.

> **Setting up Keycloak?** See [`KEYCLOAK_SETUP.md`](./KEYCLOAK_SETUP.md) — recipe for the **Keycloak admin** covering realm / role / client setup, the load-bearing `roles` scope + audience mapper, and a smoke-test command.

## Configuration: `IdpConfig` (preferred)

The IdP factory is **config-first**. Settings live in the `IdpConfig` platform
config (class_key `idp_config`, defined in
`src/dynastore/modules/iam/idp_config.py`) and are editable at runtime through
the Configs API — no restart, no redeploy.

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `type` | `"oidc" \| "saml2"` | `oidc` | `saml2` is reserved (no provider registered yet). |
| `issuer_url` | string | `null` | Backend-internal issuer URL. Stored **verbatim** (must match the token `iss` exactly per RFC 8414). Unset ⇒ IdP not configured. |
| `client_id` | string | `dynastore-api` | OAuth2 client ID. |
| `client_secret` | secret | `null` | Persisted **encrypted**, masked (`***`) in responses/logs. Omit for public clients. |
| `audience` | string | `null` | Expected `aud` claim. Defaults to `client_id`. |
| `public_url` | string | `null` | Browser-facing issuer URL when it differs from `issuer_url`. |
| `roles_claim_path` | string | `realm_access.roles` | Dotted path to the role list in the token claims. |

The IdP is registered only when `type == "oidc"` **and** `issuer_url` is set.

### Migration: ENV → IdpConfig

1. PATCH the `idp_config` platform config with your existing values (the same
   ones currently in `IDP_*` / `KEYCLOAK_*`). The `client_secret` is encrypted
   at rest and never returned in full.
2. Verify the startup log shows `Registered OIDC identity provider from
   IdpConfig: <issuer>` (no `DEPRECATED` warning).
3. Remove the `IDP_*` / `KEYCLOAK_*` environment variables from your deployment.

> **Deprecation:** when `idp_config` does not set an `issuer_url`, startup falls
> back to the environment variables below and logs a `DEPRECATED` warning. **The
> ENV fallback is removed in the next release** — migrate to `IdpConfig`.

### Environment Variables (deprecated)

| Variable | Alias (legacy) | Description |
|----------|---------------|-------------|
| `IDP_TYPE` | — | Set to `oidc` (default) |
| `IDP_ISSUER_URL` | `KEYCLOAK_ISSUER_URL` | Backend-internal issuer URL (used for discovery, token validation, userinfo). Example: `http://keycloak:8080/realms/myrealm` |
| `IDP_PUBLIC_URL` | `KEYCLOAK_PUBLIC_URL` | Browser-facing URL for auth redirects. Example: `http://localhost:8180/realms/myrealm`. Defaults to `IDP_ISSUER_URL`. |
| `IDP_CLIENT_ID` | `KEYCLOAK_CLIENT_ID` | OAuth2 client ID |
| `IDP_CLIENT_SECRET` | `KEYCLOAK_CLIENT_SECRET` | OAuth2 client secret (confidential clients) |
| `IDP_AUDIENCE` | `KEYCLOAK_AUDIENCE` | Expected `aud` claim. Defaults to `IDP_CLIENT_ID`. |

> **Note:** The `KEYCLOAK_*` aliases are read as fallbacks so existing
> deployments keep working without changes.

---

## Adding a New Provider

1. **Create** `<type>_identity.py` implementing `IdentityProviderProtocol`.

2. **Add** the backend to `IamModule._register_identity_provider()`
   (`src/dynastore/modules/iam/module.py`): branch on `IdpConfig.type` and
   register the provider from the relevant config fields, e.g.
   ```python
   if cfg.type == "saml2" and cfg.is_configured:
       from .identity_providers.saml2_identity import Saml2IdentityProvider
       register_plugin(Saml2IdentityProvider(metadata_url=..., ...))
       return
   ```
   Add the new fields to `IdpConfig` (`idp_config.py`) so they are
   runtime-editable through the Configs API.

3. **Export** the class in `__init__.py`.

4. **Document** the new `IdpConfig` fields in the table above.

---

## SAML2 (Planned)

SAML2 support is not yet implemented but the env-var structure is reserved:

| Variable | Description |
|----------|-------------|
| `IDP_TYPE=saml2` | Activates the SAML2 provider |
| `IDP_METADATA_URL` | URL of the IdP SAML metadata XML |
| `IDP_SP_ENTITY_ID` | Service Provider entity ID |
| `IDP_SP_CERT` | SP signing certificate (PEM) |
| `IDP_SP_KEY` | SP private key (PEM) |

The `IdentityProviderProtocol` interface is intentionally protocol-agnostic
so the same IAM core handles both OIDC and SAML2 sessions without changes.
