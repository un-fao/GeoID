# Local Keycloak realm for GeoID

This directory holds the realm-export imported by Keycloak at startup
(see `compose.keycloak.yml`). It declares two clients:

- `geoid-api` — confidential (has a secret), used for service-to-service
  flows and as the API audience. Tokens for this client carry
  `aud=geoid-api`.
- `geoid-web` — public/PKCE, used by browser-based clients (the SPA
  dashboard AND Swagger UI's Authorize button). Tokens issued by
  `geoid-web` ALSO carry `aud=geoid-api` thanks to a protocol-level
  audience mapper, so they pass server-side validation against the
  same audience.

## Test users (committed for dev convenience — DO NOT use in real envs)

Each user's password equals their username:

| Username | Realm role | Client roles |
|---|---|---|
| `sysadmin` | `sysadmin` | — |
| `admin` | `admin` | `geoid-api:catalog_admin` |
| `user` | `user` | — |
| `viewer` | `viewer` | — |
| `testadmin` | `admin` | `geoid-api:catalog_admin` (legacy alias) |
| `testuser` | `user` | — (legacy alias) |
| `testviewer` | `viewer` | — (legacy alias) |

## How Swagger UI's Authorize button works

After `docker compose up` the API is at `http://localhost:8080` (or the
port set by `HOST_PORT_API`). Open `/docs` in a browser:

1. Click the green Authorize button.
2. Swagger pops `http://localhost:8180/realms/geoid/protocol/openid-connect/auth?client_id=geoid-web&...&audience=geoid-api`.
3. Log in as `sysadmin` / `sysadmin`.
4. Browser redirects to `/docs/oauth2-redirect`, the popup closes,
   Swagger now has a JWT bound to `aud=geoid-api`.
5. Try `GET /iam/me` — returns 200 with the principal carrying the
   `sysadmin` role.

If the popup hangs or errors:

- Check `IDP_CLIENT_ID=geoid-web` (NOT `geoid-api`) in `.env`. The
  confidential `geoid-api` client cannot run the in-browser PKCE flow.
- Check `KC_HOSTNAME=http://localhost:8180` and that the host port
  matches your `HOST_PORT_KEYCLOAK` env var.
- Check the realm-export's `geoid-web.redirectUris` includes
  `http://localhost:<api-port>/*`. The default 8080 wildcard catches
  the standard dev port; non-standard ports need their own entry.
