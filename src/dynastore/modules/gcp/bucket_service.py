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

import logging
import asyncio
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud import storage
else:
    try:
        from google.cloud import storage
    except ImportError:
        storage = None

from dynastore.modules.db_config.query_executor import managed_transaction, DbResource
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import ConfigsProtocol, CatalogsProtocol
from dynastore.modules.catalog.lifecycle_manager import LifecycleContext
from dynastore.tools.discovery import get_protocol
from dynastore.modules.gcp import gcp_db
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpLocation,
    GCP_CATALOG_BUCKET_CONFIG_ID,
)
from dynastore.modules.gcp.tools import bucket as bucket_tool
from dynastore.modules.concurrency import run_in_thread

logger = logging.getLogger(__name__)


class BucketService:
    """
    Manages GCS Buckets for Catalogs, including naming, creation, and database registration.
    """

    def __init__(
        self,
        engine: Optional[DbResource],
        config_service: Optional[ConfigsProtocol],
        storage_client: "storage.Client",
        project_id: str,
        region: str,
    ):
        self._engine = engine
        self._config_service = config_service
        self.storage_client = storage_client
        self.project_id = project_id
        self.region = region

    @property
    def engine(self) -> DbResource:
        if self._engine:
            return self._engine
        from dynastore.tools.protocol_helpers import get_engine

        engine = get_engine()
        if engine is None:
            raise RuntimeError("No database engine available.")
        return engine

    @property
    def config_service(self) -> ConfigsProtocol:
        if self._config_service:
            return self._config_service
        from dynastore.tools.discovery import get_protocol

        mgr = get_protocol(ConfigsProtocol)
        if not mgr:
            raise RuntimeError("ConfigsProtocol not available.")
        return mgr

    def generate_bucket_name(self, catalog_id: str, physical_schema: Optional[str] = None) -> str:
        """Generates the deterministic GCS bucket name for a catalog.

        Format: {6-char project hash}-{physical schema}
        e.g.  ab12cd-s_2ka8fbc3

        The 6-char project hash gives sufficient global uniqueness across GCP projects
        (1 in ~16M collision chance). The physical schema is already short (10 chars,
        e.g. s_2ka8fbc3) so the full name is ≤ 18 chars — well within the 63-char limit.
        The bucket name is identical to the DB schema name for easy cross-reference.
        """
        import hashlib
        if not self.project_id:
            raise RuntimeError("GCP Project ID not available.")
        proj_hash = hashlib.sha1(self.project_id.encode()).hexdigest()[:6]
        identifier = (physical_schema or catalog_id).lower().replace("_", "-")
        bucket_name = f"{proj_hash}-{identifier}"
        # Safety check — should never exceed limit with our short schema names
        if len(bucket_name) > 63:
            id_hash = hashlib.sha1(identifier.encode()).hexdigest()[:8]
            bucket_name = f"{proj_hash}-{identifier[:54]}-{id_hash}"
        return bucket_name

    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        """Returns the GCS path (bucket name) for a catalog's root folder."""
        if not self.engine:
            raise RuntimeError("Database engine not available.")
        async with managed_transaction(self.engine) as conn:
            return await gcp_db.get_bucket_for_catalog_query.execute(
                conn, catalog_id=catalog_id
            )

    async def get_catalog_storage_path(self, catalog_id: str) -> Optional[str]:
        """Returns the GCS path for a catalog's root folder (e.g., gs://bucket-name/catalog/)."""
        bucket_name = await self.ensure_storage_for_catalog(catalog_id)
        return bucket_tool.get_gcs_catalog_path(bucket_name) if bucket_name else None

    async def get_collection_storage_path(
        self, catalog_id: str, collection_id: str
    ) -> Optional[str]:
        """Returns the GCS path for a collection's folder (e.g., gs://bucket-name/collections/my-collection/)."""
        bucket_name = await self.ensure_storage_for_catalog(catalog_id)
        return (
            bucket_tool.get_gcs_collection_path(bucket_name, collection_id)
            if bucket_name
            else None
        )

    async def prepare_upload_target(
        self, catalog_id: str, collection_id: Optional[str] = None
    ):
        """
        Ensures that the target catalog and, if provided, collection exist before an
        operation like an upload. This will create them just-in-time if they don't exist.
        """
        if not self.engine:
            raise RuntimeError("Database engine not available.")

        async with managed_transaction(self.engine) as conn:
            catalogs = get_protocol(CatalogsProtocol)
            if catalogs:
                # Ensure the parent catalog exists, creating it if necessary.
                # This is the minimum requirement for any upload target.
                await catalogs.ensure_catalog_exists(catalog_id, ctx=DriverContext(db_resource=conn))

                if collection_id:
                    # If a collection is targeted, ensure it exists as a logical entity first.
                    collection_record = await catalogs.get_collection(
                        catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
                    )
                    if not collection_record:
                        logger.info(
                            f"Logical collection '{catalog_id}:{collection_id}' does not exist for upload. Creating it now."
                        )
                        from dynastore.modules.catalog.models import Collection

                        default_collection_metadata = Collection.model_validate({
                            "id": collection_id,
                            "title": f"Auto-created collection for {collection_id}",
                            "extent": {
                                "spatial": {"bbox": [[0.0, 0.0, 0.0, 0.0]]},
                                "temporal": {"interval": [[None, None]]},
                            },
                        })
                        # This creates only the logical collection record, without a LayerConfig.
                        await catalogs.create_collection(
                            catalog_id, default_collection_metadata, ctx=DriverContext(db_resource=conn)
                        )
            else:
                logger.warning(
                    "CatalogsProtocol not available. Skipping catalog/collection existence check."
                )

    async def upload_file(
        self, source_path: str, target_path: str, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads a local file to storage."""
        if not target_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {target_path}")

        bucket_name, blob_name = target_path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        await run_in_thread(
            blob.upload_from_filename, source_path, content_type=content_type
        )
        return target_path

    async def upload_file_content(
        self, target_path: str, content: bytes, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads content (bytes) directly to storage."""
        if not target_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {target_path}")

        bucket_name, blob_name = target_path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        await run_in_thread(blob.upload_from_string, content, content_type=content_type)
        return target_path

    async def download_file(self, source_path: str, target_path: str) -> None:
        """StorageProtocol: Downloads a file from storage to local."""
        if not source_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {source_path}")

        bucket_name, blob_name = source_path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        await run_in_thread(blob.download_to_filename, target_path)

    async def file_exists(self, path: str) -> bool:
        """StorageProtocol: Checks if a file exists in storage."""
        if not path.startswith("gs://"):
            return False

        bucket_name, blob_name = path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        return await run_in_thread(blob.exists)

    async def wait_for_bucket_ready(
        self,
        bucket_name: str,
        max_retries: int = 8,
        initial_delay: float = 0.5,
    ) -> bool:
        """
        Polls for bucket existence with exponential backoff.
        Returns True if the bucket becomes visible, False if retries are exhausted.
        """
        bucket = self.storage_client.bucket(bucket_name)
        for attempt in range(1, max_retries + 1):
            if await run_in_thread(bucket.exists):
                return True
            if attempt < max_retries:
                delay = initial_delay * (2 ** (attempt - 1))
                logger.debug(
                    f"Attempt {attempt}/{max_retries}: Bucket '{bucket_name}' not ready. "
                    f"Retrying in {delay}s..."
                )
                await asyncio.sleep(delay)
        logger.warning(
            f"Bucket '{bucket_name}' not ready after {max_retries} attempts."
        )
        return False

    async def delete_file(self, path: str) -> None:
        """StorageProtocol: Deletes a file from storage."""
        if not path.startswith("gs://"):
            return

        bucket_name, blob_name = path[5:].split("/", 1)
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        await run_in_thread(blob.delete)

    async def update_bucket_config(
        self, catalog_id: str, config: GcpCatalogBucketConfig
    ):
        """
        Updates the actual GCS bucket to match the provided configuration.
        """
        bucket_name = await self.get_storage_identifier(catalog_id)
        if bucket_name:
            await self._apply_bucket_settings(bucket_name, config)
        else:
            logger.warning(
                f"Cannot update bucket settings for catalog '{catalog_id}': No bucket found."
            )

    async def ensure_storage_for_catalog(
        self,
        catalog_id: str,
        conn: Optional[DbResource] = None,
        context: Optional[LifecycleContext] = None,
        auto_create: bool = True,
        config_override: Optional[GcpCatalogBucketConfig] = None,
    ) -> Optional[str]:
        """
        Retrieves the bucket name for a catalog, creating it if it doesn't exist.
        The bucket name will be unique and tied to the catalog.

        Args:
            catalog_id: The catalog ID
            conn: Optional database connection
            context: Optional lifecycle context to avoid querying non-existent catalog
            auto_create: If True, create the bucket if it doesn't exist.
            config_override: Optional configuration to use for creation (overrides DB/context).
        """

        # This inner function contains the core logic and expects a connection.
        async def _execute(conn):
            bucket_name = await gcp_db.get_bucket_for_catalog_query.execute(
                conn, catalog_id=catalog_id
            )
            if bucket_name:
                return bucket_name

            # Bucket does not exist, check if we should create it.
            if not auto_create:
                logger.info(f"Bucket for catalog '{catalog_id}' does not exist and auto_create=False. Skipping creation.")
                return None

            # Determine the bucket configuration to use.
            # Priority: 1. Parameter 2. Context Snapshot 3. Database
            effective_config = config_override
            if effective_config is None:
                if context is not None:
                    # If a context is provided (e.g. from init hook), prioritize it.
                    # crucially: do NOT query the DB here, as the catalog might not be visible yet.
                    if GCP_CATALOG_BUCKET_CONFIG_ID in context.config:
                        from dynastore.modules.db_config.platform_config_service import (
                            require_config_class,
                        )

                        effective_config = require_config_class(
                            GCP_CATALOG_BUCKET_CONFIG_ID
                        ).model_validate(
                            context.config[GCP_CATALOG_BUCKET_CONFIG_ID]
                        )
                    else:
                        # Snaphot present but key missing -> explicit default
                        effective_config = GcpCatalogBucketConfig(location=GcpLocation(self.region))
                else:
                    effective_config = await self.config_service.get_config(
                        GCP_CATALOG_BUCKET_CONFIG_ID, catalog_id, ctx=DriverContext(db_resource=conn
                    ))

            if not isinstance(effective_config, GcpCatalogBucketConfig):
                # If no config is set, create a default one.
                logger.debug(
                    f"No GcpCatalogBucketConfig found for catalog '{catalog_id}'. Using default settings."
                )
                effective_config = GcpCatalogBucketConfig(location=GcpLocation(self.region))
                # Persist the default configuration (idempotent setup)
                await self.config_service.set_config(
                    GCP_CATALOG_BUCKET_CONFIG_ID,
                    effective_config,
                    catalog_id=catalog_id,
                    ctx=DriverContext(db_resource=conn),
                )
            
            # Use effective_config for the rest of the creation logic

            if not self.project_id:
                raise RuntimeError("GCP Project ID could not be determined.")

            # Generate a globally unique bucket name
            new_bucket_name = self.generate_bucket_name(
                catalog_id, physical_schema=context.physical_schema if context else None
            )

            try:
                # Use bucket_tool to create bucket (ASYNC, using our injected client)
                bucket = await bucket_tool.create_bucket(
                    new_bucket_name,
                    effective_config,
                    self.project_id,
                    client=self.storage_client,
                )

                # Log bucket creation event (catalog-level event)
                try:
                    from dynastore.modules.catalog.log_manager import log_info

                    await log_info(
                        catalog_id,
                        event_type="gcp_bucket_created",
                        message=f"GCS bucket '{new_bucket_name}' created successfully.",
                        details={
                            "bucket_name": new_bucket_name,
                            "location": effective_config.location,
                        },
                        immediate=False,  # Buffer for batch write
                    )
                except Exception as log_e:
                    logger.warning(f"Failed to log bucket creation event: {log_e}")

                # Apply the bucket settings (CORS, etc.).
                # Callers that require strict readiness (e.g. init-upload) can
                # explicitly invoke wait_for_bucket_ready after this method.
                await self._apply_bucket_settings(new_bucket_name, effective_config)

                # Create placeholder folders (we can assume bucket object is valid).
                # These are blocking HTTP calls, so we execute them via the shared
                # concurrency backend instead of directly using asyncio.to_thread.
                def _setup_placeholders():
                    bucket = self.storage_client.bucket(new_bucket_name)
                    bucket.blob(f"{bucket_tool.CATALOG_FOLDER}/").upload_from_string("")
                    bucket.blob(
                        f"{bucket_tool.COLLECTIONS_FOLDER}/"
                    ).upload_from_string("")

                await run_in_thread(_setup_placeholders)

                # Link bucket to catalog in the database
                # ON CONFLICT DO UPDATE will return the bucket_name
                result = await gcp_db.link_bucket_to_catalog_query.execute(
                    conn, catalog_id=catalog_id, bucket_name=new_bucket_name
                )

                if result:
                    logger.info(
                        f"Successfully linked bucket '{result}' to catalog '{catalog_id}'."
                    )
                    return result
                else:
                    logger.error(
                        f"Failed to link bucket '{new_bucket_name}' to catalog '{catalog_id}' and no result returned."
                    )
                    return None

            except Exception as e:
                logger.error(
                    f"Failed to create or link bucket for catalog '{catalog_id}': {e}",
                    exc_info=True,
                )
                # Attempt to clean up if bucket was created but DB link failed
                try:
                    await bucket_tool.delete_bucket(
                        new_bucket_name, force=True, client=self.storage_client
                    )
                except Exception as cleanup_e:
                    logger.error(
                        f"Failed to cleanup partially created bucket '{new_bucket_name}': {cleanup_e}"
                    )
                return None

        # If a connection is provided, run the logic on it directly.
        if conn:
            return await _execute(conn)

        # If no connection is provided, create and manage a new transaction.
        async with managed_transaction(self.engine) as new_conn:
            return await _execute(new_conn)

    async def _apply_bucket_settings(
        self, bucket_name: str, config: GcpCatalogBucketConfig
    ):
        """
        Maps the declarative Pydantic configuration to the GCS bucket properties.
        This handles partial updates for mutable fields.
        """
        if not self.storage_client:
            logger.warning(
                f"Storage client not available. Skipping update for bucket '{bucket_name}'."
            )
            return

        def _sync_update():
            bucket = self.storage_client.bucket(bucket_name)
            needs_patch = False

            # 1. CORS Configuration
            if config.cors is not None:
                cors_list = []
                for rule in config.cors:
                    rule_dict: Dict[str, Any] = {
                        "origin": rule.origin,
                        "method": rule.method,
                    }
                    if rule.response_header:
                        rule_dict["responseHeader"] = rule.response_header
                    if rule.max_age_seconds:
                        rule_dict["maxAgeSeconds"] = rule.max_age_seconds
                    cors_list.append(rule_dict)

                logger.info(
                    f"Applying CORS rules to bucket '{bucket_name}': {cors_list}"
                )
                bucket.cors = cors_list
                needs_patch = True

            # 2. Lifecycle Rules (Placeholder)
            # if config.lifecycle_rules:
            #     pass

            if needs_patch:
                bucket.patch()
                logger.info(
                    f"Successfully applied settings to GCS bucket '{bucket_name}'."
                )

        try:
            await run_in_thread(_sync_update)
        except Exception as e:
            logger.error(
                f"Failed to patch GCS bucket '{bucket_name}': {e}", exc_info=True
            )

    async def teardown_gcs_notification(
        self, bucket_name: str, gcs_notification_id: str
    ):
        """Deletes a GCS notification configuration from a bucket."""
        if not gcs_notification_id or not bucket_name:
            return

        def _delete_notification():
            try:
                bucket = self.storage_client.bucket(bucket_name)
                notification = bucket.notification(notification_id=gcs_notification_id)
                if notification.exists():
                    notification.delete()
                    logger.info(
                        f"Deleted GCS notification {gcs_notification_id} from bucket {bucket_name}."
                    )
                else:
                    logger.debug(
                        f"GCS notification {gcs_notification_id} not found in bucket {bucket_name}. Nothing to delete."
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to delete GCS notification {gcs_notification_id}: {e}"
                )

        await run_in_thread(_delete_notification)

    async def delete_storage_for_catalog(
        self, catalog_id: str, conn: Optional[DbResource] = None
    ):
        """
        Deletes the bucket associated with the catalog.
        """
        bucket_name = await self.get_storage_identifier(catalog_id)

        # Fallback: if DB record is gone (e.g. CASCADE delete), try deterministic name
        if not bucket_name:
            try:
                bucket_name = self.generate_bucket_name(catalog_id)
                logger.info(
                    f"Bucket DB record not found for '{catalog_id}'. Attempting deletion using deterministic name: '{bucket_name}'"
                )
            except Exception as e:
                logger.warning(
                    f"Could not determine bucket name for catalog '{catalog_id}': {e}"
                )
                return

        if not bucket_name:
            return

        try:
            # First, delete from DB link (idempotent)
            async with managed_transaction(self.engine) as conn:
                await gcp_db.DDLQuery(
                    f"DELETE FROM gcp.catalog_buckets WHERE catalog_id = :catalog_id"
                ).execute(conn, catalog_id=catalog_id)

            # Then force delete the bucket (including contents)
            # This handles "NotFound" gracefully usually (or we should check)
            try:
                await bucket_tool.delete_bucket(
                    bucket_name, force=True, client=self.storage_client
                )
                logger.info(
                    f"Successfully deleted bucket '{bucket_name}' for catalog '{catalog_id}'."
                )
            except Exception as e:
                # If it doesn't exist, that's fine
                from google.api_core.exceptions import NotFound

                if "404" in str(e) or isinstance(e, NotFound):
                    logger.info(f"Bucket '{bucket_name}' already deleted or not found.")
                else:
                    raise

        except Exception as e:
            logger.error(
                f"Failed to delete bucket '{bucket_name}' for catalog '{catalog_id}': {e}",
                exc_info=True,
            )
            raise
