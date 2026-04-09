# ApiKey Extension (Administrator & User Guide)

The `ApiKey` extension provides a RESTful interface for Identity and Access Governance (IAG). It enables administrators to manage identities, credentials, and access rules, while providing users with self-service tools for authentication and quota management.

## Key Use Cases

1.  **Machine-to-Machine (M2M)**: Issue dedicated API keys to external services with specific rate limits and restricted path access.
2.  **User Authentication**: Exchange API keys for short-lived JWT tokens to be used by web applications.
3.  **Governance & Compliance**: Enforce global security policies across all catalogs and monitor detailed access logs for auditing.
4.  **Quota Management**: Limit system usage (e.g., total requests or storage) per tenant or per user.

## Core Business Objects

### 1. Principals
A **Principal** represents a unique identity (User, Service, or Bot).
- `identifier`: A unique string (e.g., `user:john.doe`, `service:ingestion`).
- `roles`: A list of roles assigned to the identity.
- `policy`: An optional inline policy for fine-grained control.

### 2. API Keys
A **Credential** used to authenticate requests.
- `key_hash`: The secure storage identifier for the key.
- `principal_id`: The owner of the key.
- `max_usage`: The total number of requests allowed for the life of this key.
- `policy`: Specific constraints (e.g., allowed IP ranges, allowed domains).

### 3. Policies
Rule-based objects that define **who** can do **what** and **where**.
- `target_pattern`: Regex pattern matching the Principal identity or Role.
- `path_pattern`: Regex pattern matching the request path (e.g., `^/data/.*`).
- `method_pattern`: HTTP methods (e.g., `GET|POST`).
- `action`: Either `ALLOW` or `DENY`.
- `priority`: Ordering for evaluation (Higher values win).

## Configuration (Environment Variables)

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DYNASTORE_SYSTEM_ADMIN_KEY` | The secret key used for administrative bootstrapping. | `None` |
| `USAGE_BUFFER_THRESHOLD` | Number of usage increments to batch before writing to DB. | `100` |
| `USAGE_BUFFER_INTERVAL` | Seconds to wait between background flushes. | `5.0` |
| `STATS_FLUSH_INTERVAL` | Frequency of access log flushes to the database. | `5.0` |

## API Discovery

The extension organizes endpoints into logical domains:

### Authentication (`/apikey/auth`)
- `POST /login`: Exchange a raw API Key for a Bearer JWT.
- `POST /token/refresh`: Get a new access token using a refresh token.
- `GET /jwks.json`: Public keys for JWT signature verification.

### Governance (`/apikey/governance`)
- `/principals`: Manage identities.
- `/roles`: Define dynamic roles.
- `/hierarchies`: Manage role inheritance.
- `/policies`: System-wide security rules.

### Credentials (`/apikey/credentials`)
- `/keys`: Issue and revoke credentials.
- `/validate`: Check key status.
- `/usage`: Current usage and quota summary.

## OAuth2 & Enterprise Features

DynaStore supports standardized OAuth2 labels for seamless integration with external IDPs.

> [!IMPORTANT]
> **Enterprise Edition (GCP Module)**:
> While the core `ApiKey` module works perfectly on-premise, the **GCP Enterprise Module** adds:
> - **Secret Manager Integration**: Automatic rotation and storage of System Keys.
> - **Identity Platform Sync**: Connect DynaStore principals to Google Cloud Identity.
> - **High-Performance Analytics**: Offload access logs to BigQuery for massive-scale auditing.

## Examples

### Creating an API Key for a Service
```bash
# Issue a key for the 'IngestBot' principal
curl -X POST -H "X-System-Key: ..." \
     -d '{"principal_identifier": "service:ingest-bot", "max_usage": 10000}' \
     http://localhost/apikey/credentials/keys
```

### Allowing Web Access to Health Endpoints
By default, some paths are bypassed (unauthenticated):
- `/auth/*`
- `/web`
- `/docs`
- `/health`
