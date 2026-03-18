# The GCP Extension & Module

The `gcp` module and its corresponding extension provide a comprehensive, cloud-native data fabric for integrating Agro-Informatics Platform (AIP) - Catalog Services with Google Cloud Platform. 

It is designed following the "Three Pillars" rule strictly: A silent foundation `module`, a stateless scalable API `extension`, and asynchronous `tasks`.

## Module Core Features
The `GCPModule` acts as the centralized foundation securely holding thread-safe authenticated Cloud Storage and Pub/Sub credentials via Application Default configs.

### Just-in-Time (JIT) Setups
When an orchestration file is requested, the system automatically spins up a dedicated `GCS` bucket. It relies on internal logic to evaluate geographical locality rules reducing bandwidth operations.

### Automated Serverless Eventing
1. Establishes a dedicated `Pub/Sub` topic.
2. Identifies the `GCS` service account binding strict IAM publish rights to the topic preventing setup configuration drift gaps automatically. 
3. Commands GCS to attach native Bucket File Change triggers binding to the Pub/Sub.
4. Generates an encrypted `OIDC JWT` webhook pushing back strictly into Agro-Informatics Platform (AIP) - Catalog Services core APIs natively executing events entirely headless relying on GCP native internal infrastructure for stability guarantees.

## Endpoints
`POST /gcp/buckets/initiate-upload`

The local API does zero data transfer. 
1. Orchestrates the namespace verification.
2. Bootstraps JIT configurations.
3. Obtains a direct `Signed Resumable Upload URI` from GCP.
4. Reverses out yielding simply the payload mapping token returning scalability constraints back onto Google. 

## Clean Up Safety
Because GCP is billable, on active `CATALOG_HARD_DELETION` events the system triggers a cascade deletion process systematically wiping Notification mappings, Pub/Sub Topics, Subscription IDs, and then aggressively tearing down the GCS Bucket ensuring no orphaned artifacts remain.

Cleanup is performed **asynchronously via `GcpCatalogCleanupTask`** (not inline in the HTTP handler):

1. The catalog event listener (`register_listeners`) registers adapters for `BEFORE_CATALOG_HARD_DELETION` and `BEFORE_COLLECTION_HARD_DELETION` using `register_event_listener` from `catalog.event_service` — no direct import of `catalog_module`.
2. Each adapter enqueues a `gcp_catalog_cleanup` task (scope=CATALOG or COLLECTION) via `create_task_for_catalog`, then returns immediately.
3. The task executor picks it up with retry/heartbeat guarantees and calls `StorageProtocol`, `EventingProtocol`, and `ConfigsProtocol` via protocol discovery.

## GCS Pub/Sub → Asset Synchronisation

When GCS fires a Pub/Sub object notification (OBJECT_FINALIZE, OBJECT_DELETE, OBJECT_ARCHIVE):

1. `handle_gcs_notification` in `gcp_events.py` decodes the message and calls `handle_asset_events`.
2. `handle_asset_events` **enqueues a `gcs_storage_event` task** and returns immediately.
3. `GcsStorageEventTask` runs asynchronously and calls `AssetsProtocol` (via `get_protocol`) to create or delete the asset record.

This fully decouples the GCP push endpoint from the catalog asset service.  The push receiver is always fast; asset operations are retried independently if they fail.

## Protocol Coupling Rules

- The GCP extension/module must **never** import `catalog_module` directly.
- All catalog interactions go through `AssetsProtocol`, `CatalogsProtocol`, `ConfigsProtocol`, `StorageProtocol`, or `EventingProtocol` resolved via `get_protocol()`.
- Event listener registration uses `register_event_listener` from `dynastore.modules.catalog.event_service`.
