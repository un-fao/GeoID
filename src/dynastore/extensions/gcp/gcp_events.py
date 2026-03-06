#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

#    dynastore/modules/gcp/gcp_events.py
from dynastore.modules.concurrency import run_in_thread
import logging
from typing import Optional, cast, Dict, Any, List, Callable, Coroutine
from collections import defaultdict
import fnmatch
import asyncio
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules import get_protocol
from dynastore.modules.catalog import catalog_module
from dynastore.modules.catalog.asset_manager import Asset, AssetBase
from dynastore.modules.gcp.tools import bucket as bucket_tool
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DQLQuery,
    ResultHandler,
    DbResource,
)
from dynastore.modules.gcp.gcp_config import (
    GcpEventingConfig,
    GcpCatalogBucketConfig,
    GcpCollectionBucketConfig,
    GcsNotificationEventType,
    TriggeredAction,
    GCP_CATALOG_BUCKET_CONFIG_ID,
    GCP_EVENTING_CONFIG_ID,
    GCP_COLLECTION_BUCKET_CONFIG_ID,
)
from dynastore.modules.catalog.catalog_module import CatalogEventType
from dynastore.modules.events.models import (
    API_KEY_NAME,
    AuthConfigAPIKey,
    AuthMethod,
    EventSubscriptionCreate,
)
from fastapi.security import APIKeyHeader
from dynastore.tasks.ingestion.ingestion_models import TaskIngestionRequest
from dynastore.modules.processes.models import ExecuteRequest
import dynastore.modules.processes.processes_module as processes_module
from google.api_core import exceptions as google_exceptions

logger = logging.getLogger(__name__)

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


# --- Helpers ---

from dynastore.models.protocols import (
    StorageProtocol,
    EventingProtocol,
    AssetsProtocol,
    CatalogsProtocol,
    ConfigsProtocol,
)


def _get_providers():
    """
    Helper to safely retrieve the required protocol implementations.
    """
    from dynastore.modules import get_protocol

    storage = get_protocol(StorageProtocol)
    eventing = get_protocol(EventingProtocol)
    assets = get_protocol(AssetsProtocol)
    configs = get_protocol(ConfigsProtocol)

    if not all([storage, eventing, assets, configs]):
        logger.warning(
            "One or more required protocols are not available. Skipping GCP event processing."
        )
        return None, None, None, None

    return storage, eventing, assets, configs


def _interpolate(template: Any, context: Dict[str, Any]) -> Any:
    """Recursively interpolates placeholders in strings within a nested data structure."""
    if isinstance(template, dict):
        return {k: _interpolate(v, context) for k, v in template.items()}
    if isinstance(template, list):
        return [_interpolate(i, context) for i in template]
    if isinstance(template, str):
        return template.format(**context)
    return template


# --- GCP Pub/Sub Event Dispatcher ---

_gcp_event_listeners: Dict[
    str, List[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]]
] = defaultdict(list)


def register_gcp_event_listener(subscription_id_pattern: str, listener: Callable):
    """
    Registers a listener function to be called for Pub/Sub messages that have a
    matching 'subscription_id' attribute.

    Args:
        subscription_id_pattern: A string pattern (supporting '*' wildcards) to match against the subscription ID.
        listener: An async function that accepts a payload dictionary.
    """
    _gcp_event_listeners[subscription_id_pattern].append(listener)
    logger.info(
        f"Registered GCP event listener for pattern '{subscription_id_pattern}'."
    )


async def dispatch_gcp_event(payload: Dict[str, Any]):
    """
    Dispatches a received Pub/Sub event payload to all registered listeners
    whose subscription ID pattern matches.
    """
    subscription_id = payload.get("subscription_id")
    if not subscription_id:
        return

    for pattern, listeners in _gcp_event_listeners.items():
        if fnmatch.fnmatch(subscription_id, pattern):
            await asyncio.gather(*(listener(payload) for listener in listeners))


# --- Event Handlers ---
# These functions contain the core logic for processing events.


async def on_catalog_hard_deletion(engine: DbResource, payload: Dict[str, Any]):
    """Handler to cleanup GCP resources when a catalog is hard-deleted."""
    catalog_id = payload.get("catalog_id")
    if not catalog_id:
        raise ValueError("Missing 'catalog_id' in event payload")

    storage, eventing, _, configs = _get_providers()
    if not all([storage, eventing, configs]):
        return

    logger.info(
        f"Event 'catalog_hard_deletion' received for '{catalog_id}'. Starting GCP resource cleanup."
    )

    # 1. Cleanup Eventing (Topics/Subscriptions)
    # config=None triggers force cleanup of deterministic/default resources in the protocol implementation.
    try:
        eventing_config = await configs.get_config(
            GCP_EVENTING_CONFIG_ID, catalog_id, engine
        )
        await eventing.teardown_catalog_eventing(catalog_id, config=eventing_config)
    except Exception as e:
        logger.info(
            f"Failed to cleanup configured eventing (likely already gone or missing). Attempting force force cleanup. Details: {e}"
        )
        await eventing.teardown_catalog_eventing(catalog_id, config=None)

    # 2. Delete bucket
    bucket_name_to_delete = await storage.get_storage_identifier(catalog_id)
    if bucket_name_to_delete:
        logger.info(f"Proceeding with deletion of bucket '{bucket_name_to_delete}'.")
        try:
            await storage.delete_file(
                bucket_name_to_delete
            )  # Note: StorageProtocol delete_file typically deletes the object, but if it supports bucket deletion it should be used.
            # In GCS, delete_file is implemented as delete_blob. We actually need delete_bucket.
            # I'll check if StorageProtocol has delete_bucket.
            await bucket_tool.delete_bucket(
                bucket_name_to_delete, force=True, client=None
            )  # client=None will use default client if possible, but better to use protocol
        except Exception as e:
            logger.warning(f"Failed to delete bucket '{bucket_name_to_delete}': {e}")


async def on_collection_hard_deletion(engine: DbResource, payload: Dict[str, Any]):
    """Handler to cleanup GCS resources when a collection is hard-deleted."""
    catalog_id = payload.get("catalog_id")
    collection_id = payload.get("collection_id")
    if not catalog_id or not collection_id:
        raise ValueError("Missing 'catalog_id' or 'collection_id' in event payload")

    # Retrieve providers using the helper
    storage, _, _, configs = _get_providers()
    if not all([storage, configs]):
        return

    logger.info(
        f"Event 'collection_hard_deletion' received for '{catalog_id}:{collection_id}'. Checking for associated GCS bucket folder."
    )

    # Check configuration: listen_catalog_events
    bucket_config = await configs.get_config(GCP_CATALOG_BUCKET_CONFIG_ID, catalog_id)
    if (
        isinstance(bucket_config, GcpCatalogBucketConfig)
        and not bucket_config.listen_catalog_events
    ):
        logger.info(
            f"Skipping GCP resource deletion for collection '{catalog_id}:{collection_id}' because 'listen_catalog_events' is False."
        )
        return

    # 1. Delete objects
    bucket_name = await storage.get_storage_identifier(catalog_id)
    if bucket_name:
        # We'll use the storage.delete_file for each blob if we want protocol-adherence
        # but for bulk deletion, bucket_tool is fine if it uses storage_client.
        # However, StorageProtocol should ideally have a delete_prefix method.
        # For now, let's stick to bucket_tool as it's an extension-level utility that can know about GCS.
        storage_client_provider = get_protocol(CloudStorageClientProtocol)
        storage_client = (
            storage_client_provider.get_storage_client()
            if storage_client_provider
            else None
        )
        bucket = storage_client.bucket(bucket_name) if storage_client else None
        folder_prefix = bucket_tool.get_blob_path_for_collection_folder(collection_id)

        logger.info(
            f"Deleting all objects in bucket '{bucket_name}' with prefix '{folder_prefix}'."
        )

        # list_blobs is partial lazy but iterator makes requests. delete_blobs makes requests.
        # Ideally we should push this entire block to thread.
        def _delete_helper():
            blobs_to_delete = list(bucket.list_blobs(prefix=folder_prefix))
            if blobs_to_delete:
                bucket.delete_blobs(blobs_to_delete)
            return len(blobs_to_delete)

        deleted_count = await run_in_thread(_delete_helper)

        if deleted_count:
            logger.info(
                f"Successfully deleted {deleted_count} objects for collection '{collection_id}'."
            )
        else:
            logger.info(
                f"No objects found to delete for collection '{collection_id}' in bucket '{bucket_name}'."
            )

    # 2. Check managed eventing prefix
    eventing_config = await config_manager.get_config(
        GCP_EVENTING_CONFIG_ID, catalog_id
    )
    if isinstance(eventing_config, GcpEventingConfig):
        if (
            eventing_config.managed_eventing
            and eventing_config.managed_eventing.enabled
        ):
            folder_prefix = bucket_tool.get_blob_path_for_collection_folder(
                collection_id
            )
            if eventing_config.managed_eventing.blob_name_prefix == folder_prefix:
                logger.info(
                    f"Managed eventing was tracking the deleted collection prefix '{folder_prefix}'. Tearing down the channel."
                )
                await gcp_module.teardown_managed_eventing_channel(
                    catalog_id, eventing_config.managed_eventing
                )


async def handle_gcs_notification(payload: Dict[str, Any]):
    """
    Universal handler for GCS notifications from Pub/Sub. It determines the
    target catalog/collection and triggers any configured actions.
    """
    logger.debug(f"Received GCS notification payload: {payload}")

    gcs_payload = payload.get("gcs_event_payload", {})
    attributes = payload.get("attributes", {})

    subscription_type = attributes.get("subscription_type")
    catalog_id = attributes.get("catalog_id")
    # Fix: Try fetching 'eventType' from 'attributes' first (standard GCS notification location)
    # then fallback to gcs_payload if not present.
    gcs_event_type_str = attributes.get("eventType") or gcs_payload.get("eventType")

    if not all([subscription_type, catalog_id, gcs_event_type_str]):
        logger.warning(
            f"Received GCS event, but missing required attributes or GCS eventType. "
            f"subscription_type={subscription_type}, catalog_id={catalog_id}, eventType={gcs_event_type_str}. Skipping."
        )
        return

    try:
        gcs_event_type = GcsNotificationEventType(gcs_event_type_str)
    except ValueError:
        logger.warning(
            f"Received unknown GCS eventType '{gcs_event_type_str}'. Skipping."
        )
        return

    collection_id = None
    if subscription_type == "managed":
        # For managed subscriptions, we determine the context (catalog vs collection) from the path.
        object_name = gcs_payload.get("name")
        if not object_name:
            logger.warning("GCS event missing object name. Skipping.")
            return

        path_parts = object_name.split("/")
        # Expected path for collection assets: collections/<collection_id>/<filename>
        if len(path_parts) >= 2 and path_parts[0] == bucket_tool.COLLECTIONS_FOLDER:
            collection_id = path_parts[1]
        # Expected path for catalog assets: catalog/<filename>
        elif len(path_parts) >= 1 and path_parts[0] == bucket_tool.CATALOG_FOLDER:
            collection_id = None  # Explicitly a catalog-level asset
        else:
            # Not in a managed prefix (e.g., tiles/) - though the notification should have filtered this.
            logger.debug(
                f"Object '{object_name}' is not in a recognized managed folder. Skipping."
            )
            return

        # Trigger asset event handling for managed buckets
        await handle_asset_events(
            catalog_id, collection_id, gcs_payload, event_type_str=gcs_event_type_str
        )
    elif subscription_type == "custom":
        custom_sub_id = attributes.get("custom_subscription_id")
        if not custom_sub_id:
            logger.warning(
                "Received event from custom subscription without 'custom_subscription_id' attribute. Cannot link to actions."
            )
            return

        _, eventing_provider, _, _ = _get_providers()
        if not eventing_provider:
            return
        eventing_config = await eventing_provider.get_eventing_config(catalog_id)
        if eventing_config:
            found_sub = next(
                (
                    s
                    for s in eventing_config.custom_subscriptions
                    if s.id == custom_sub_id
                ),
                None,
            )
            if found_sub:
                collection_id = found_sub.target_collection_id
            else:
                logger.warning(
                    f"Could not find custom subscription config with id '{custom_sub_id}' in catalog '{catalog_id}'."
                )
                return

    await _trigger_configured_actions(
        catalog_id, collection_id, gcs_event_type, gcs_payload
    )


async def _trigger_configured_actions(
    catalog_id: str,
    collection_id: Optional[str],
    event_type: GcsNotificationEventType,
    gcs_payload: Dict[str, Any],
):
    """Fetches collection config and executes actions based on the event type."""
    _, eventing_provider, _, configs = _get_providers()
    if not all([eventing_provider, configs]):
        return
    # Fetch config for the specific collection. The config manager will fall back
    # to the catalog level if no collection-specific config is set.
    # This allows actions to be defined at either the catalog or collection level.
    config = await configs.get_config(
        GCP_COLLECTION_BUCKET_CONFIG_ID, catalog_id, collection_id
    )

    if not isinstance(config, GcpCollectionBucketConfig) or not config.event_actions:
        logger.debug(
            f"No event actions configured for {catalog_id}:{collection_id}. Skipping."
        )
        return

    actions_to_run = config.event_actions.get(event_type)
    if not actions_to_run:
        return

    # Look up templates if we encounter strings
    eventing_config = None
    if any(isinstance(a, str) for a in actions_to_run):
        eventing_config = await configs.get_config(GCP_EVENTING_CONFIG_ID, catalog_id)

    metadata = gcs_payload.get("metadata", None)
    if metadata:
        asset_id = metadata.get("asset_id") or metadata.get("asset_code")
        if not asset_id:
            logger.warning(f"GCS event missing asset_id in metadata. Skipping.")
            return
    else:
        logger.warning(f"GCS event missing metadata. Skipping.")
        return

    context = {
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "bucket": gcs_payload.get("bucket"),
        "bucket_id": gcs_payload.get("bucket"),  # Alias for template compatibility
        "name": gcs_payload.get("name"),
        "object_id": gcs_payload.get("name"),  # Alias for template compatibility
        "event_type": event_type.value,
        "asset_id": asset_id,
        "asset_code": asset_id,  # maintain for template compatibility
    }

    for action_item in actions_to_run:
        # Resolve action
        action = None
        if isinstance(action_item, str):
            # 1. Try collection-specific asset tasks
            action = config.asset_tasks.get(action_item)

            # 2. Fallback to global templates if not found in collection
            if (
                not action
                and eventing_config
                and isinstance(eventing_config, GcpEventingConfig)
            ):
                action = eventing_config.action_templates.get(action_item)

            if not action:
                logger.warning(
                    f"Action template/task '{action_item}' referenced in {catalog_id}:{collection_id} but not found in collection asset_tasks or catalog eventing config. Skipping."
                )
                continue
        else:
            action = action_item

        logger.info(
            f"Triggering process '{action.process_id}' for {catalog_id}:{collection_id} based on event '{event_type.value}'."
        )
        try:
            # --- Special Handling for 'ingestion' process ---
            # This creates a complete, valid TaskIngestionRequest on the fly.
            if action.process_id == "ingestion":
                if not asset_id:
                    logger.warning(
                        f"Cannot trigger ingestion for object '{context['name']}' because it's missing the 'asset_id' metadata. Skipping."
                    )
                    continue
                # The template now defines the *ingestion_request* part of the OGC process inputs.
                ingestion_request_template = action.execute_request_template
                ingestion_request_body = _interpolate(
                    ingestion_request_template, context
                )

                # Construct the full OGC ExecuteRequest payload
                ogc_inputs = {
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                    "ingestion_request": ingestion_request_body,
                }
                execute_payload = ExecuteRequest(inputs=ogc_inputs)
            else:
                # Generic process execution
                interpolated_inputs = _interpolate(
                    action.execute_request_template, context
                )
                execute_payload = ExecuteRequest(inputs=interpolated_inputs)

            await processes_module.execute_process(
                process_id=action.process_id,
                execution_request=execute_payload,
                engine=get_engine(),
                caller_id=f"gcp_event:{event_type.value}",
            )
            logger.info(
                f"Successfully deferred process '{action.process_id}' for object '{context['name']}'."
            )
        except Exception as e:
            logger.error(
                f"Failed to construct and trigger process '{action.process_id}' for object '{context['name']}': {e}",
                exc_info=True,
            )


# --- In-Process Listener Adapters ---
# These adapters bridge the gap between the Catalog Module's internal event arguments
# and the logic defined above (which expects a standard payload dict and engine).


async def _adapter_catalog_hard_deletion(catalog_id: str, **kwargs):
    """Adapter for internal Catalog Module events."""
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.tools.discovery import get_protocol

    db = get_protocol(DatabaseProtocol)
    if not db:
        return

    # CRITICAL: We DO NOT pass db_resource here.
    # This event runs in a background workflow (after/during deletion).
    # Sharing the connection causes InterfaceError if the main logic is also using it.
    # We let the handler obtain its own fresh connection from the engine.
    await on_catalog_hard_deletion(db.engine, {"catalog_id": catalog_id})


async def _adapter_collection_hard_deletion(
    catalog_id: str, collection_id: str, **kwargs
):
    """Adapter for internal Catalog Module events."""
    from dynastore.models.protocols import DatabaseProtocol
    from dynastore.tools.discovery import get_protocol

    db = get_protocol(DatabaseProtocol)
    if not db:
        return

    # CRITICAL: We DO NOT pass db_resource here. See above.
    await on_collection_hard_deletion(
        db.engine, {"catalog_id": catalog_id, "collection_id": collection_id}
    )

    # --- Reactive Hooks for storage events ASSETS ---


async def handle_asset_events(
    catalog_id: str,
    collection_id: Optional[str],
    event_payload: Dict[str, Any],
    event_type_str: str = None,
):
    """Reactive sync when files are uploaded/deleted in external storage."""
    logger.debug(
        f"Handling asset events for catalog '{catalog_id}', collection '{collection_id}' with payload: {event_payload}"
    )
    from dynastore.modules.catalog.asset_manager import AssetTypeEnum

    _, _, assets, _ = _get_providers()
    if not assets:
        return
    asset_manager = (
        assets  # assets provider implements AssetsProtocol which HAS create_asset etc.
    )

    # Use passed event string or try to get it from payload (less reliable for attributes)
    event_type = event_type_str or event_payload.get("eventType")
    if event_type not in ["OBJECT_FINALIZE", "OBJECT_DELETE", "OBJECT_ARCHIVE"]:
        return

    object_name = event_payload.get("name")
    url = f"gs://{event_payload.get('bucket')}/{object_name}"

    # The asset_id is now reliably sourced from the object's custom metadata.
    metadata = event_payload.get("metadata", {})
    asset_id = metadata.get("asset_id") or metadata.get("asset_code")
    asset_type_str = metadata.get("asset_type", "ASSET")
    asset_type = AssetTypeEnum(asset_type_str)

    if not asset_id:
        logger.warning(
            f"GCS event for '{object_name}' is missing 'asset_id' in metadata. Cannot create asset."
        )
        return

    if event_type == "OBJECT_FINALIZE":
        logger.debug(
            f"Creating asset '{asset_id}' in catalog '{catalog_id}' from GCS OBJECT_FINALIZE event."
        )
        # 1. Create the asset record in the database.
        await asset_manager.create_asset(
            catalog_id=catalog_id,
            collection_id=collection_id,
            asset=AssetBase(
                uri=url, asset_id=asset_id, asset_type=asset_type, metadata=metadata
            ),
        )
        logger.info(
            f"Successfully created asset '{asset_id}' from GCS event in {catalog_id}:{collection_id or ''}."
        )
    elif event_type in ["OBJECT_DELETE"]:
        logger.debug(
            f"Deleting asset '{asset_id}' in catalog '{catalog_id}' from GCS OBJECT_DELETE event."
        )
        asset = await asset_manager.get_asset(
            asset_id=asset_id, catalog_id=catalog_id, collection_id=collection_id
        )
        if asset:
            logger.debug("Found matching asset. Proceeding with hard deletion.")
            # This triggers the hard_delete event which might clean up physical files
            # if the logic allows, but here we are reacting TO the storage deletion.
            await asset_manager.hard_delete_asset(
                asset.catalog_id, asset.asset_id, propagate=True
            )
    elif event_type in ["OBJECT_ARCHIVE"]:
        logger.debug(
            f"Soft-deleting asset '{asset_id}' in catalog '{catalog_id}' from GCS OBJECT_ARCHIVE event."
        )
        asset = await asset_manager.get_asset(
            asset_id=asset_id, catalog_id=catalog_id, collection_id=collection_id
        )
        if asset:
            logger.debug("Found matching asset. Proceeding with soft deletion.")
            # This triggers the hard_delete event which might clean up physical files
            # if the logic allows, but here we are reacting TO the storage deletion.
            await asset_manager.soft_delete_asset(asset.catalog_id, asset.asset_id)
    logger.debug(
        f"Completed handling asset event '{event_type}' for asset '{asset_id}'."
    )


# --- Registration Functions ---


def register_listeners():
    """
    Subscribes the GCP module to internal catalog events.
    This enables 'in-process' synchronization without needing an external webhook.
    """
    logger.info("Registering GCP module as a listener for Catalog events...")

    catalog_module.register_event_listener(
        CatalogEventType.BEFORE_CATALOG_HARD_DELETION, _adapter_catalog_hard_deletion
    )

    catalog_module.register_event_listener(
        CatalogEventType.BEFORE_COLLECTION_HARD_DELETION,
        _adapter_collection_hard_deletion,
    )
    logger.info("GCP module successfully subscribed to Catalog events.")


def register_default_gcp_listeners():
    """Registers the built-in GCP event handlers."""
    logger.info("Registering default GCP event listeners...")
    # Register a single, universal listener for all our subscriptions
    register_gcp_event_listener("*", handle_gcs_notification)


# --- Webhook Dispatcher (Legacy/Alternative) ---

EVENT_HANDLERS = {
    CatalogEventType.CATALOG_HARD_DELETION.value: on_catalog_hard_deletion,
    CatalogEventType.COLLECTION_HARD_DELETION.value: on_collection_hard_deletion,
}


async def dispatch_event(engine: DbResource, event_payload: Dict[str, Any]):
    event_type = event_payload.get("event_type")
    payload = event_payload.get("payload")

    if not event_type or payload is None:
        raise ValueError("Invalid event structure")

    handler = EVENT_HANDLERS.get(event_type)
    if handler:
        await handler(engine, payload)


async def register_self_as_subscriber(self_public_url):
    """Registers webhooks with the central event bus (if used)."""
    if not self_public_url:
        return

    webhook_url = f"{self_public_url.rstrip('/')}/gcp/events/webhook"
    auth_config = AuthConfigAPIKey(
        auth_method=AuthMethod.API_KEY, header_name=API_KEY_NAME
    )

    subscriptions = [
        EventSubscriptionCreate(
            subscriber_name="gcp_module_catalog_cleanup",
            event_type=CatalogEventType.CATALOG_HARD_DELETION.value,
            webhook_url=webhook_url,
            auth_config=auth_config,
        ),
        EventSubscriptionCreate(
            subscriber_name="gcp_module_collection_cleanup",
            event_type=CatalogEventType.COLLECTION_HARD_DELETION.value,
            webhook_url=webhook_url,
            auth_config=auth_config,
        ),
    ]
    await _register_self_as_subscriber_direct(subscriptions)


async def _register_self_as_subscriber_direct(
    subscriptions: list[EventSubscriptionCreate],
):
    from dynastore.modules.events.events_module import subscribe

    for sub in subscriptions:
        try:
            await subscribe(subscription_data=sub)
        except Exception:
            pass
