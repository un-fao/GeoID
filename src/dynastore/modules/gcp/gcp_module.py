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
import os
from contextlib import asynccontextmanager
from typing import Optional, AsyncIterator, Dict, Any, List, Tuple, Union

try:
    from google.api_core import exceptions as google_exceptions
    from google.api_core.exceptions import Aborted
except ImportError:
    google_exceptions = None
    Aborted = None # Ensure Aborted is defined even if google.api_core is not available
from async_lru import alru_cache
import dynastore.modules as dm
from dynastore.modules import ModuleProtocol
from dynastore.modules.concurrency import run_in_thread
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import (
    DbResource,
    managed_transaction,
    DDLQuery,
    DQLQuery,
    ResultHandler,
)
from dynastore.models.protocols import (
    ConfigsProtocol,
    StorageProtocol,
    JobExecutionProtocol,
    CloudStorageClientProtocol,
    CloudIdentityProtocol,
    EventingProtocol,
    DatabaseProtocol,
    CatalogsProtocol,
)
from dynastore.modules.gcp.tools.service_account import get_credentials
from dynastore.modules.gcp import gcp_db
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry, LifecycleContext
from dynastore.modules.catalog.log_manager import log_info, log_error, log_warning
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.gcp.gcp_config import (
    GcpCatalogBucketConfig,
    GcpEventingConfig,
    ManagedBucketEventing,
    GcpCollectionBucketConfig,
    GcsNotificationEventType,
    GCP_CATALOG_BUCKET_CONFIG_ID,
    GCP_EVENTING_CONFIG_ID,
    GCP_MODULE_CONFIG_ID,
    GcpModuleConfig,
    TriggeredAction,
)
from dynastore.modules.gcp.models import (
    GcpEventType,
    PushSubscriptionConfig,
    PUBSUB_JWT_AUDIENCE,
)

try:
    from google.cloud import storage
    from google.cloud import pubsub_v1
    from google.cloud import run_v2
except ImportError:
    storage = None
    pubsub_v1 = None
    run_v2 = None
from dynastore.modules.gcp.bucket_service import BucketService
# from google.cloud import compute_v1

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Provisioning retry tunables
# ---------------------------------------------------------------------------
# Provisioning retry tunables - will be initialized from GcpModuleConfig if available
_CATALOG_VISIBILITY_MAX_RETRIES: int = 20
_CATALOG_VISIBILITY_RETRY_INTERVAL: float = 0.2
_CATALOG_EXISTS_QUERY = DQLQuery(
    "SELECT 1 FROM catalog.catalogs WHERE id = :catalog_id AND deleted_at IS NULL",
    result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
)
class GCPModule(
    ModuleProtocol,
    StorageProtocol,
    JobExecutionProtocol,
    CloudStorageClientProtocol,
    CloudIdentityProtocol,
    EventingProtocol,
):
    _credentials: Optional[Any] = None
    _identity: Optional[Dict[str, Any]] = None
    _engine: Optional[DbResource] = None
    _config_service: Optional[ConfigsProtocol] = None
    _module_config: Optional[GcpModuleConfig] = None

    @property
    def engine(self) -> DbResource:
        if self._engine:
            return self._engine
        from dynastore.tools.protocol_helpers import get_engine
        return get_engine()

    @engine.setter
    def engine(self, value: DbResource):
        self._engine = value

    ################################################
    # Synchronous clients
    _storage_client: Optional["storage.Client"] = None
    _publisher_client: Optional["pubsub_v1.PublisherClient"] = None
    _subscriber_client: Optional["pubsub_v1.SubscriberClient"] = None

    ################################################
    # Asynchronous clients (initialized in lifespan)
    _jobs_client: Optional["run_v2.JobsAsyncClient"] = None
    _run_client: Optional["run_v2.ServicesAsyncClient"] = None
    _bucket_service: Optional[BucketService] = None

    def __init__(self, app_state: object) -> None:
        super().__init__()
        logger.info("Attempting to identify active GCP credentials...")

        try:
            from google.auth.exceptions import DefaultCredentialsError

            self._credentials, self._identity = get_credentials()
            logger.info(f"GCP identity found: {self.get_account_email()}")
            self.reinitialize_clients()
        except Exception as e:
            logger.warning(
                f"GCP Module: Failed to initialize clients due to missing or invalid credentials: {e}. Module will be partially functional."
            )
            self._credentials = None
            self._identity = None

    def reinitialize_clients(self) -> None:
        """
        (Re)instantiates shared Google Cloud clients using current credentials.
        """
        project_id = self.get_project_id()
        if self._credentials:
            logger.info(
                f"Instantiating shared Google Cloud clients for project: {project_id or 'default'}"
            )
            # Pass credentials explicitly to ensure consistent identity
            self._storage_client = storage.Client(
                project=project_id, credentials=self._credentials
            )
            self._publisher_client = pubsub_v1.PublisherClient(
                credentials=self._credentials
            )
            self._subscriber_client = pubsub_v1.SubscriberClient(
                credentials=self._credentials
            )

            # Re-initialize async clients if they were already initialized
            if self._jobs_client:
                self._jobs_client = run_v2.JobsAsyncClient(
                    credentials=self._credentials
                )
            if self._run_client:
                self._run_client = run_v2.ServicesAsyncClient(
                    credentials=self._credentials
                )

            # Update BucketService if it exists
            if self._bucket_service:
                self._bucket_service.storage_client = self._storage_client
                if project_id:
                    self._bucket_service.project_id = project_id
                region = self.get_region()
                if region:
                    self._bucket_service.region = region
        else:
            logger.warning("No credentials available to reinitialize clients.")

    """
    A foundational module to manage GCP credentials and state for the application.
    """

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncIterator[None]:
        logger.info("GCP Module: Entering lifespan - initializing clients.")

        # 1. Retrieve ConfigManager and Global Module Config
        self._config_service = get_protocol(ConfigsProtocol)
        if self._config_service:
            self._module_config = await self._config_service.get_config(GCP_MODULE_CONFIG_ID)
            
            # Synchronize local tunables with global config
            if self._module_config:
                global _CATALOG_VISIBILITY_MAX_RETRIES, _CATALOG_VISIBILITY_RETRY_INTERVAL
                _CATALOG_VISIBILITY_MAX_RETRIES = self._module_config.catalog_visibility_max_retries
                _CATALOG_VISIBILITY_RETRY_INTERVAL = self._module_config.catalog_visibility_retry_interval
        else:
            logger.warning(
                "GCP Module: ConfigsProtocol not available. Global settings will use defaults/env."
            )
            self._module_config = GcpModuleConfig() # Use defaults (including env fallbacks)

        # 2. Ensure synchronous clients are open (re-open if closed from previous lifespan)
        self.reinitialize_clients()

        # Retrieve ConfigsProtocol via dm.get_protocol
        self._config_service = get_protocol(ConfigsProtocol)
        if not self._config_service:
            logger.warning(
                "GCP Module: ConfigsProtocol not available. Configuration management disabled."
            )

        try:
            # Reuse credentials discovered in __init__
            if self._credentials:
                logger.info(
                    f"GCP Module: Reusing discovered identity: {self.get_account_email()}"
                )
                # Pass the credentials object to the async clients.
                self._jobs_client = run_v2.JobsAsyncClient(
                    credentials=self._credentials
                )
                self._run_client = run_v2.ServicesAsyncClient(
                    credentials=self._credentials
                )
            else:
                logger.warning(
                    "GCP Module: No credentials available for async clients."
                )

            # Initialize BucketService (Safe even if clients are None)
            self._bucket_service = BucketService(
                engine=self._engine, # Explicitly pass current engine check
                config_service=self._config_service,
                storage_client=self._storage_client,
                project_id=self.get_project_id(),
                region=self.get_region(),
            )

            # Initialize database schema for the module
            if self.engine:
                try:
                    async with managed_transaction(self.engine) as conn:
                        await maintenance_tools.ensure_schema_exists(conn, "gcp")
                        await gcp_db.DDLQuery(gcp_db.CATALOG_BUCKETS_SCHEMA).execute(
                            conn
                        )
                    logger.info("GCP Module: Database schema initialized.")
                except Exception as e:
                    logger.error(f"GCP Module: Failed to initialize schema: {e}")
            else:
                logger.warning(
                    "GCP Module: No DB engine available. Schema initialization skipped."
                )

            # --- Register Lifecycle Hooks ---
            from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry

            lifecycle_registry.sync_catalog_initializer(self._on_sync_init_catalog)
            # We keep these as async because they don't block the core creation flow 
            # and don't cause race conditions in tests as easily as the creation one.
            lifecycle_registry.async_catalog_destroyer(self._on_async_destroy_catalog)
            lifecycle_registry.async_collection_destroyer(
                self._on_async_destroy_collection
            )

            yield
        finally:
            logger.info("GCP Module: Exiting lifespan - closing all clients.")
            await self.close()
            logger.info("GCP Module: Lifespan shutdown complete.")

    async def close(self) -> None:
        """
        Explicitly closes all GCP client instances and clears their references.
        This is critical for preventing 'Closed Channel' errors during test isolation.
        """
        clients_to_close = [
            ("_storage_client", self._storage_client),
            ("_publisher_client", self._publisher_client),
            ("_subscriber_client", self._subscriber_client),
            ("_jobs_client", self._jobs_client),
            ("_run_client", self._run_client),
        ]

        for attr, client in clients_to_close:
            if client:
                try:
                    # Generic close for both sync and async clients
                    if hasattr(client, "close"):
                        if asyncio.iscoroutinefunction(client.close):
                            await client.close()
                        else:
                            client.close()
                    setattr(self, attr, None)
                except Exception as e:
                    logger.debug(f"GCP Module: Error closing {attr}: {e}")

        self._bucket_service = None
        self._config_service = None
        # self._module_config is kept to preserve settings across lifespans if needed, 
        # but re-fetched on start anyway.

    # --- Lifecycle Hooks ---

    async def _on_sync_init_catalog(self, conn: DbResource, physical_schema: str, catalog_id: str):
        """
        Sync hook to initialize GCP resources (Bucket, Eventing) when a catalog is created.
        Executed within the catalog creation transaction.
        """
        try:
            logger.info(
                f"GCP Module: Sync initialization for catalog '{catalog_id}' started."
            )

            # 1. Resolve configuration (with defaults if missing)
            # Since this is a sync hook, we don't have LifecycleContext.config pre-calculated.
            # We fetch it from the DB using the hierarchical get_config.
            from dynastore.models.protocols.configs import ConfigsProtocol
            bucket_config = GcpCatalogBucketConfig() # Default
            
            config_mgr = get_protocol(ConfigsProtocol)
            if config_mgr:
                # get_config follows the waterfall: Collection -> Catalog -> Platform -> Defaults
                bucket_config = await config_mgr.get_config(GCP_CATALOG_BUCKET_CONFIG_ID, catalog_id=catalog_id, db_resource=conn)

            # 2. Check if provisioning is enabled for this catalog
            if not bucket_config.enabled:
                logger.info(
                    f"GCP Module: Provisioning disabled for catalog '{catalog_id}' via configuration. Linking bucket name and marking ready."
                )
                
                # Mark catalog as ready immediately
                from dynastore.models.protocols import CatalogsProtocol
                catalogs_svc = get_protocol(CatalogsProtocol)
                if catalogs_svc:
                    await catalogs_svc.update_provisioning_status(catalog_id, "ready", db_resource=conn)
                
                # Link the deterministic bucket name in the DB to allow uploads
                from dynastore.modules.gcp import gcp_db
                bucket_name = self.get_bucket_service().generate_bucket_name(catalog_id, physical_schema=physical_schema)
                # Catalog existence check is not strictly needed here as the lifecycle hook 
                # only runs if the catalog was successfully created.
                await gcp_db.link_bucket_to_catalog_query.execute(
                    conn, catalog_id=catalog_id, bucket_name=bucket_name
                )
                return

            # Log to Tenant Log
            try:
                from dynastore.modules.catalog.log_manager import log_info
                await log_info(
                    catalog_id, "gcp.init.start", "Starting GCP resource initialization.", db_resource=conn
                )
            except Exception as e:
                logger.debug(f"Could not log init start for catalog '{catalog_id}' (it might be gone): {e}")

            try:
                from dynastore.modules.tasks.models import TaskCreate
                from dynastore.modules.tasks.tasks_module import create_task
                from dynastore.modules.catalog.event_service import CatalogEventType

                task_request = TaskCreate(
                    task_type="gcp_provision_catalog",
                    inputs={"catalog_id": catalog_id},
                    caller_id="system",
                    type="task",
                    # originating_event lets CatalogModule route rollback without knowing about GCP
                    extra_context={"originating_event": CatalogEventType.CATALOG_CREATION},
                )

                # Enqueue the task within the transaction
                await create_task(conn, task_request, physical_schema)
                logger.info(
                    f"GCP Module: Provisioning task enqueued for catalog '{catalog_id}'."
                )

            except Exception as e:
                logger.error(
                    f"GCP Module: Failed to enqueue provisioning task for '{catalog_id}': {e}",
                    exc_info=True,
                )
                raise

        except Exception as e:
            logger.error(
                f"GCP Module: Sync initialization for catalog '{catalog_id}' failed: {e}",
                exc_info=True,
            )
            # We don't necessarily want to fail the whole catalog creation if GCP setup fails to QUEUE,
            # but usually this implies a DB error which would fail the TX anyway.
            from dynastore.modules.catalog.log_manager import log_error
            try:
                await log_error(
                    catalog_id, "gcp.init.failure", f"GCP initialization failed: {str(e)}", db_resource=conn
                )
            except Exception as log_e:
                logger.debug(f"Could not log init failure for catalog '{catalog_id}': {log_e}")
            raise

    async def _on_async_destroy_catalog(
        self, catalog_id: str, context: LifecycleContext
    ):
        """
        Async hook to tear down GCP resources when a catalog is hard-deleted.
        """
        logger.info(
            f"GCP Module: Async destruction for catalog '{catalog_id}' started."
        )
        # Note: We cannot log to Tenant Logs here if the schema is already dropped.
        # But we can log to System Logs using the same function (it handles fallback).
        await log_info(
            catalog_id, "gcp.destroy.start", "Starting GCP resource teardown."
        )

        try:
            eventing_data = context.config.get(GCP_EVENTING_CONFIG_ID)

            if eventing_data:
                try:
                    from dynastore.modules.db_config.platform_config_service import (
                        ConfigRegistry,
                    )

                    eventing_config = ConfigRegistry.validate_config(
                        GCP_EVENTING_CONFIG_ID, eventing_data
                    )

                    if (
                        isinstance(eventing_config, GcpEventingConfig)
                        and eventing_config.managed_eventing
                    ):
                        logger.info(
                            f"Tearing down managed eventing for catalog '{catalog_id}'."
                        )
                        await self.teardown_managed_eventing_channel(
                            catalog_id, eventing_config.managed_eventing
                        )
                        await log_info(
                            catalog_id,
                            "gcp.eventing.teardown",
                            "Managed eventing torn down.",
                        )

                except Exception as e:
                    logger.error(
                        f"Failed to teardown eventing for catalog '{catalog_id}': {e}"
                    )
                    await log_warning(
                        catalog_id,
                        "gcp.eventing.failure",
                        f"Eventing teardown failed: {e}",
                    )

            # Bucket deletion logic (optional/configurable) would go here
            # For now, we force delete the bucket if it exists to satisfy the lifecycle contract.
            # In a production system, we might want a 'retain_bucket' flag in the config.
            bucket_manager = self.get_bucket_service()
            bucket_name = await bucket_manager.get_storage_identifier(catalog_id)
            if bucket_name:
                logger.info(
                    f"Deleting bucket '{bucket_name}' for catalog '{catalog_id}'..."
                )
                await bucket_manager.delete_storage_for_catalog(catalog_id)
                await log_info(
                    catalog_id, "gcp.bucket.deleted", f"Bucket {bucket_name} deleted."
                )
            else:
                logger.warning(
                    f"No bucket found for catalog '{catalog_id}' during teardown."
                )

            await log_info(
                catalog_id, "gcp.destroy.success", "GCP resource teardown completed."
            )
            logger.info(
                f"GCP Module: Async destruction for catalog '{catalog_id}' completed."
            )

        except Exception as e:
            logger.error(
                f"GCP Module: Async destruction for catalog '{catalog_id}' failed: {e}",
                exc_info=True,
            )
            await log_error(
                catalog_id, "gcp.destroy.failure", f"GCP teardown failed: {e}"
            )

    async def _on_async_init_collection(
        self, catalog_id: str, collection_id: str, context: LifecycleContext
    ):
        # GCP doesn't have explicit collection resources (just folders).
        # Eventing is catalog-level.
        # Nothing critical to do here yet.
        pass

    async def _on_async_destroy_collection(
        self,
        catalog_id: str,
        collection_id: str,
        context: LifecycleContext,
    ):
        # GCP doesn't have explicit collection resources (just folders).
        # Eventing is catalog-level.
        # Nothing critical to do here yet.
        pass

    def get_config_service(self) -> ConfigsProtocol:
        if not self._config_service:
            from dynastore.tools.discovery import get_protocol
            self._config_service = get_protocol(ConfigsProtocol)
        if not self._config_service:
            raise RuntimeError("GCPModule: ConfigsProtocol not available.")
        return self._config_service

    def get_storage_client(self) -> "storage.Client":
        """
        Returns the shared, thread-safe Google Cloud Storage client instance.
        """
        # Google Cloud client libraries automatically handle token refresh using the
        # credentials object. We do not need to manually check/refresh here.
        if not self._storage_client:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to create a storage client."
            )
        return self._storage_client

    def get_publisher_client(self) -> "pubsub_v1.PublisherClient":
        """Returns the shared Pub/Sub Publisher client instance."""
        # Google Cloud client libraries automatically handle token refresh.
        if not self._publisher_client:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to create a publisher client."
            )
        return self._publisher_client

    def get_bucket_service(self) -> BucketService:
        """Returns the initialized BucketService."""
        if not self._bucket_service:
            raise RuntimeError("GCPModule has not been initialized.")
        return self._bucket_service

    def get_subscriber_client(self) -> "pubsub_v1.SubscriberClient":
        """Returns the shared Pub/Sub Subscriber client instance."""
        # Google Cloud client libraries automatically handle token refresh.
        if not self._subscriber_client:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to create a subscriber client."
            )
        return self._subscriber_client

    def get_jobs_client(self) -> "run_v2.JobsAsyncClient":
        """
        Returns the shared, thread-safe Google Cloud Run Jobs client instance.
        """
        # Google Cloud client libraries automatically handle token refresh.
        if not self._jobs_client:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to create a jobs client."
            )
        return self._jobs_client

    # --- JobExecutionProtocol Implementation ---

    async def run_job(
        self,
        job_name: str,
        args: Optional[List[str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
    ) -> Any:
        """
        JobExecutionProtocol: Triggers a serverless job (Cloud Run job) asynchronously.
        """
        if not self._jobs_client:
            raise RuntimeError("GCP Project or Jobs client not available.")

        project_id = self.get_project_id()
        region = self.get_region()
        client = self.get_jobs_client()
        name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

        request = run_v2.RunJobRequest(name=name)

        if args or env_vars:
            container_overrides = run_v2.RunJobRequest.Overrides.ContainerOverride()
            if args:
                container_overrides.args.extend(args)
            if env_vars:
                for key, value in env_vars.items():
                    container_overrides.env.append(run_v2.EnvVar(name=key, value=value))
            request.overrides.container_overrides.append(container_overrides)

        try:
            operation = await client.run_job(request=request)
            logger.info(
                f"GCP Job '{job_name}' triggered asynchronously, operation name: {operation.operation.name if hasattr(operation, 'operation') else 'unknown'}"
            )
            return operation
        except Exception as e:
            logger.error(f"Error triggering GCP job '{job_name}': {e}", exc_info=True)
            raise

    async def get_job_config(self) -> Dict[str, str]:
        """
        JobExecutionProtocol: Discovers deployed jobs and returns mapping of task_type -> job_name.
        """
        job_map = {}
        project_id = self.get_project_id()
        region = self.get_region()

        if not project_id:
            logger.warning("GCP Project ID not available. Cannot discover jobs.")
            return job_map

        # Use common region fallback if metadata server fails or local
        region = region or os.getenv("REGION", "europe-west1")

        try:
            client = self.get_jobs_client()
            parent = f"projects/{project_id}/locations/{region}"
            request = run_v2.ListJobsRequest(parent=parent)

            logger.info(f"Discovering GCP jobs in {parent}...")
            async for job in await client.list_jobs(request=request):
                job_name = job.name.split("/")[-1]
                if (
                    job.template
                    and job.template.template
                    and job.template.template.containers
                ):
                    for container in job.template.template.containers:
                        for env_var in container.env:
                            if env_var.name == "DYNASTORE_TASK_MODULES":
                                task_type = env_var.value
                                if task_type:
                                    job_map[task_type.strip()] = job_name
                                    logger.info(
                                        f"Discovered GCP job mapping: task '{task_type}' -> job '{job_name}'"
                                    )
                                break
        except Exception as e:
            logger.error(
                f"Error discovering GCP jobs (Project: {project_id}, Region: {region}): {e}",
                exc_info=True,
            )

        return job_map

    def get_run_client(self) -> "run_v2.ServicesAsyncClient":
        """
        Returns the shared, thread-safe Google Cloud Run client instance.
        """
        # Google Cloud client libraries automatically handle token refresh.
        if not self._run_client:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to create a run client."
            )
        return self._run_client

    def _refresh_credentials(self) -> None:
        """
        Synchronously checks and refreshes GCP credentials if they are expired or invalid.

        Note: This is primarily used by get_fresh_token() where we need the raw token string.
        Standard clients (storage, pubsub) handle refresh automatically.
        """
        if not self._credentials:
            logger.warning("Cannot refresh: no credentials object available.")
            return False

        import google.auth.transport.requests

        # Check 'expired' explicitly. 'valid' might be True even if the token is stale
        # but not yet wiped. 'expired' checks the actual expiry timestamp.
        if not self._credentials.valid or self._credentials.expired:
            request = google.auth.transport.requests.Request()
            self._credentials.refresh(request)
            logger.info("GCP credentials successfully refreshed.")
        return True

    async def get_fresh_token(self) -> str:
        """
        Asynchronously ensures credentials are valid and returns a fresh access token.
        This method is safe to call from the event loop as it offloads the refresh
        to a background thread.
        """
        await run_in_thread(self._refresh_credentials)
        return self._credentials.token

    def get_credentials_object(self) -> Any:
        """Returns the shared google.auth.credentials object."""
        if not self._credentials:
            raise RuntimeError(
                "GCPModule has not been initialized or failed to find credentials."
            )
        return self._credentials

    def get_identity_info(self) -> Optional[Dict[str, Any]]:
        """
        CloudIdentityProtocol: Returns the full GCP identity dictionary discovered at startup.
        """
        return self._identity

    def get_identity(self) -> Optional[Dict[str, Any]]:
        """Legacy identity getter."""
        return self.get_identity_info()

    def get_project_id(self) -> Optional[str]:
        """
        Returns the discovered GCP Project ID from the active credentials.
        Falls back to environment variables if no identity is available.
        """
        if self._identity and self._identity.get("project_id"):
            return self._identity.get("project_id")
        
        if self._module_config:
            return self._module_config.project_id

        # Legacy/Bootstrap fallback
        return os.getenv("PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")

    def get_project_number(self) -> Optional[str]:
        """
        Returns the discovered GCP Project number from the active credentials.
        """
        if not self._identity:
            return None
        return self._identity.get("project_number")

    def get_account_email(self) -> Optional[str]:
        """
        Returns the email of the active GCP service account or user.
        """
        if not self._identity:
            return None
        return self._identity.get("account_email")

    def get_region(self) -> Optional[str]:
        """
        Returns the auto-detected GCP region, if available.
        """
        if self._identity and self._identity.get("region"):
            return self._identity.get("region")

        if self._module_config:
            return self._module_config.region

        return os.getenv("REGION")

    def get_service_name(self) -> Optional[str]:
        """
        Returns the name of the current Cloud Run service, if available.
        Relies on the K_SERVICE environment variable automatically set by Cloud Run.
        """
        return os.getenv("K_SERVICE")

    @alru_cache(maxsize=1)
    async def get_self_url(self) -> str:
        """
        Dynamically discovers and returns the public URL of the running Cloud Run service.
        The result is cached for subsequent calls.
        This requires the service account to have the 'run.services.get' permission.
        """
        # Allow manual override via environment variable (useful for testing or non-Cloud Run envs)
        service_url_override = os.getenv("SERVICE_URL")
        if service_url_override:
            return service_url_override

        service_name = self.get_service_name()
        if not service_name:
            # Fallback for local development or testing via SERVICE_URL environment variable
            service_url = os.getenv("SERVICE_URL")
            if service_url:
                return service_url
            raise RuntimeError(
                "Cannot determine self URL: K_SERVICE environment variable is not set and SERVICE_URL is missing. "
                "This is not a Cloud Run environment."
            )
        try:
            project_id = self.get_project_id()
            region = self.get_region()
            logger.info(
                f"Discovering public URL for Cloud Run service '{service_name}' in region '{region}'."
            )
            client = self.get_run_client()
            service_path = client.service_path(project_id, region, service_name)
            service_details = await client.get_service(name=service_path)
            logger.info(f"Discovered and cached self URL: {service_details.uri}")
            return service_details.uri
        except Exception as e:
            # Fallback for local development or testing where Cloud Run Admin API might not be accessible
            # or permissions are missing, but we still need a URL for push subscriptions (even if it's localhost)
            service_url = os.getenv("SERVICE_URL", "http://localhost")
            logger.warning(
                f"Failed to discover Cloud Run service URL: {e}. Falling back to default: {service_url}"
            )
            return service_url

    def generate_bucket_name(self, catalog_id: str) -> str:
        """Generates the deterministic bucket name for a catalog."""
        return self.get_bucket_service().generate_bucket_name(catalog_id)

    def generate_default_topic_id(self, catalog_id: str) -> str:
        """Generates the deterministic default Pub/Sub topic ID for a catalog."""
        return f"ds-{catalog_id}-events"

    def generate_default_subscription_id(self, catalog_id: str) -> str:
        """Generates the deterministic default Pub/Sub subscription ID for a catalog."""
        return f"ds-{catalog_id}-default-sub"

    # --- StorageProtocol Implementation ---

    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        """StorageProtocol: Returns the bucket name associated with a catalog."""
        return await self.get_bucket_service().get_storage_identifier(catalog_id)

    async def get_catalog_storage_path(self, catalog_id: str) -> Optional[str]:
        """StorageProtocol: Returns the storage path (e.g., gs://...) for a catalog."""
        return await self.get_bucket_service().get_catalog_storage_path(catalog_id)

    async def upload_file(
        self, source_path: str, target_path: str, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads a local file to storage."""
        return await self.get_bucket_service().upload_file(
            source_path, target_path, content_type
        )

    async def upload_file_content(
        self, target_path: str, content: bytes, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads content (bytes) directly to storage."""
        return await self.get_bucket_service().upload_file_content(
            target_path, content, content_type
        )

    async def download_file(self, source_path: str, target_path: str) -> None:
        """StorageProtocol: Downloads a file from storage to local."""
        return await self.get_bucket_service().download_file(source_path, target_path)

    async def file_exists(self, path: str) -> bool:
        """StorageProtocol: Checks if a file exists in storage."""
        return await self.get_bucket_service().file_exists(path)

    async def delete_file(self, path: str) -> None:
        """StorageProtocol: Deletes a file from storage."""
        return await self.get_bucket_service().delete_file(path)

    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        """StorageProtocol: Returns the storage identifier (bucket name) associated with a catalog."""
        return await self.get_bucket_service().get_storage_identifier(catalog_id)

    async def ensure_storage_for_catalog(
        self, catalog_id: str, conn: Optional[Any] = None
    ) -> Optional[str]:
        """StorageProtocol: Ensures that storage exists for a catalog, creating it if it doesn't."""
        return await self.get_bucket_service().ensure_storage_for_catalog(
            catalog_id, conn=conn
        )

    async def delete_storage_for_catalog(
        self, catalog_id: str, conn: Optional[Any] = None
    ) -> bool:
        """StorageProtocol: Deletes all storage resources associated with a catalog."""
        return await self.get_bucket_service().delete_storage_for_catalog(
            catalog_id, conn=conn
        )

    async def prepare_upload_target(
        self, catalog_id: str, collection_id: Optional[str] = None
    ):
        """
        Ensures that the target catalog and, if provided, collection exist before an
        operation like an upload. This will create them just-in-time if they don't exist.
        """
        return await self.get_bucket_service().prepare_upload_target(
            catalog_id, collection_id
        )

    async def upload_file(
        self, source_path: str, target_path: str, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads a local file to storage."""
        return await self.get_bucket_service().upload_file(
            source_path, target_path, content_type=content_type
        )

    async def upload_file_content(
        self, target_path: str, content: bytes, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads content (bytes) directly to storage."""
        return await self.get_bucket_service().upload_file_content(
            target_path, content, content_type=content_type
        )

    async def download_file(self, source_path: str, target_path: str) -> None:
        """StorageProtocol: Downloads a file from storage to local."""
        return await self.get_bucket_service().download_file(source_path, target_path)

    async def file_exists(self, path: str) -> bool:
        """StorageProtocol: Checks if a file exists in storage."""
        return await self.get_bucket_service().file_exists(path)

    async def delete_file(self, path: str) -> None:
        """StorageProtocol: Deletes a file from storage."""
        return await self.get_bucket_service().delete_file(path)

    async def setup_catalog_gcp_resources(
        self, catalog_id: str, context: Optional[LifecycleContext] = None
    ) -> Tuple[str, GcpEventingConfig]:
        """
        High-level orchestrator to ensure all necessary GCP resources for a catalog
        (bucket, eventing) are created just-in-time. This method is idempotent.

        IMPORTANT: This method deliberately does NOT hold a single DB transaction open
        across GCP API calls (bucket creation, Pub/Sub, IAM, GCS notifications).
        asyncpg will close idle connections, causing ConnectionDoesNotExistError if
        a long-running gRPC call is made while a connection is held open.
        """
        if not self.engine:
            raise RuntimeError("Database engine not available in GCPModule.")

        # ── Phase 1: DB reads (short transaction, released before any GCP API call) ──
        async with managed_transaction(self.engine) as conn:
            existing_bucket_name = await gcp_db.get_bucket_for_catalog_query.execute(
                conn, catalog_id=catalog_id
            )
            existing_eventing_config = await self.get_eventing_config(
                catalog_id, conn=conn, context=context
            )

        # ── Phase 2: GCP API calls (no DB connection held) ──
        # We track provisioned resources to ensure cleanup on ANY failure until DB commit.
        provisioned_bucket = None
        provisioned_topic = None

        try:
            # 2a. Ensure the bucket exists (creates it if needed, returns name)
            # This method already queries the DB (short) then makes GCP calls (no DB)
            bucket_name = await self.get_bucket_service().ensure_storage_for_catalog(
                catalog_id,
                conn=None,  # No connection — manages its own short transaction
                context=context,
            )
            
            # CRITICAL CHECK: ensure storage was actually provisioned
            if bucket_name is None:
                 msg = f"Failed to provision storage for catalog '{catalog_id}': Bucket name returned as None."
                 logger.error(msg)
                 raise RuntimeError(msg)
            
            provisioned_bucket = bucket_name

            # 2b. Determine the eventing config to apply
            eventing_config = existing_eventing_config
            if eventing_config is None:
                logger.info(
                    f"No GcpEventingConfig found for catalog '{catalog_id}'. Creating default managed eventing system with ingestion template."
                )
                default_ingestion_template = TriggeredAction(
                    process_id="ingestion",
                    execute_request_template={
                        "catalog_id": "{catalog_id}",
                        "collection_id": "{collection_id}",
                        "ingestion_request": {
                            "database_batch_size": 1000,
                            "asset": {
                                "asset_id": "{asset_code}",
                                "uri": "gs://{bucket_id}/{object_id}",
                            },
                            "reporting": {
                                "gcs_detailed": {
                                    "enabled": True,
                                    "report_file_path": "gs://{bucket_id}/ingestion_reports/report_{asset_code}.json",
                                }
                            },
                            "column_mapping": {
                                "external_id": "CODE",
                                "attributes_source_type": "all",
                            },
                        },
                    },
                )
                eventing_config = GcpEventingConfig(
                    managed_eventing=ManagedBucketEventing(
                        enabled=True,
                        triggered_actions=[default_ingestion_template],
                    )
                )

            # 2c. Setup Topic and Notifications if managed eventing is enabled
            if (
                eventing_config.managed_eventing
                and eventing_config.managed_eventing.enabled
            ):
                eventing_config.managed_eventing.subscription = PushSubscriptionConfig(
                    subscription_id=self.generate_default_subscription_id(catalog_id)
                )
                updated_managed_eventing = await self.setup_managed_eventing_channel(
                    catalog_id,
                    eventing_config.managed_eventing,
                    bucket_name=bucket_name,
                    context=context,
                )
                logger.debug(
                    f"updated_managed_eventing.topic_path being saved: {updated_managed_eventing.topic_path}"
                )
                eventing_config.managed_eventing = updated_managed_eventing
                provisioned_topic = updated_managed_eventing.topic_path

            # ── Phase 3: DB writes (short transaction, after all GCP API calls complete) ──
            catalog_exists = None
            for attempt in range(_CATALOG_VISIBILITY_MAX_RETRIES):
                # Use a fresh connection/transaction for each check to avoid snapshot isolation issues
                async with managed_transaction(self.engine) as conn:
                    catalog_exists = await _CATALOG_EXISTS_QUERY.execute(conn, catalog_id=catalog_id)

                if catalog_exists:
                    break

                logger.warning(
                    f"Catalog '{catalog_id}' not visible yet "
                    f"(attempt {attempt + 1}/{_CATALOG_VISIBILITY_MAX_RETRIES}). Retrying in {_CATALOG_VISIBILITY_RETRY_INTERVAL}s..."
                )
                await asyncio.sleep(_CATALOG_VISIBILITY_RETRY_INTERVAL)

            if not catalog_exists:
                logger.warning(
                    f"Catalog '{catalog_id}' not found or deleted during GCP resource provisioning. Aborting DB registration and triggering teardown."
                )
                # This will trigger the catch-all cleanup in 'finally' below
                raise asyncio.CancelledError(f"Catalog {catalog_id} not found during provisioning.")

            async with managed_transaction(self.engine) as conn:
                # Persist eventing config if managed eventing was set up
                if (
                    eventing_config.managed_eventing
                    and eventing_config.managed_eventing.enabled
                ):
                    saved_config = await self.set_eventing_config(
                        catalog_id, eventing_config, conn=conn
                    )
                    logger.debug(
                        f"saved_config.managed_eventing.topic_path from DB: {saved_config.managed_eventing.topic_path}"
                    )

            # SUCCESS - Resources committed to DB. Clear provisioning tracking.
            provisioned_bucket = None
            provisioned_topic = None
            return bucket_name, eventing_config

        except BaseException as e:
            # Catch-all cleanup for Phase 2/3 failures (including CancelledError and Exception)
            if not isinstance(e, (Exception, asyncio.CancelledError)):
                # Re-raise immediately for things like SystemExit, KeyboardInterrupt
                raise

            logger.error(f"GCP Provisioning failed for catalog '{catalog_id}': {e}")
            
            # Orphaned resource cleanup
            if provisioned_bucket:
                logger.info(f"Cleanup: Deleting orphaned bucket {provisioned_bucket}...")
                try:
                    await self.delete_storage_for_catalog(catalog_id)
                except Exception as cleanup_e:
                    logger.warning(f"Failed to cleanup orphaned bucket: {cleanup_e}")

            if provisioned_topic:
                logger.info(f"Cleanup: Tearing down orphaned eventing topic/channel...")
                try:
                    # We need the config object to teardown topics/subscriptions
                    # Ensure eventing_config is defined and has managed_eventing
                    if eventing_config and eventing_config.managed_eventing:
                        await self.teardown_managed_eventing_channel(catalog_id, eventing_config.managed_eventing)
                except Exception as cleanup_e:
                    logger.warning(f"Failed to cleanup orphaned eventing resources: {cleanup_e}")
            
            # Re-raise to let the caller handle the failure
            raise

    async def get_catalog_bucket_config(
        self, catalog_id: str
    ) -> Optional[GcpCatalogBucketConfig]:
        """Internal helper to fetch and parse a catalog's bucket config."""
        config_service = self.get_config_service()
        config = await config_service.get_config(
            GCP_CATALOG_BUCKET_CONFIG_ID, catalog_id
        )
        return config if isinstance(config, GcpCatalogBucketConfig) else None

    async def set_catalog_bucket_config(
        self, catalog_id: str, config: GcpCatalogBucketConfig
    ) -> GcpCatalogBucketConfig:
        """Persists the bucket configuration for a catalog."""
        config_service = self.get_config_service()
        await config_service.set_config(
            GCP_CATALOG_BUCKET_CONFIG_ID, config, catalog_id=catalog_id
        )
        return await config_service.get_config(GCP_CATALOG_BUCKET_CONFIG_ID, catalog_id)

    async def apply_storage_config(
        self, catalog_id: str, config: GcpCatalogBucketConfig
    ):
        """StorageProtocol: Applies bucket configuration changes (CORS, Lifecycle)."""
        bucket_manager = self.get_bucket_service()
        await bucket_manager.update_bucket_config(catalog_id, config)

    async def apply_eventing_config(
        self, catalog_id: str, config: GcpEventingConfig, conn=None
    ):
        """Applies eventing configuration changes to the live GCP resources."""
        logger.info(
            f"Applying eventing configuration changes for catalog '{catalog_id}'."
        )

        # Handle the managed eventing system
        if config.managed_eventing:
            if config.managed_eventing.enabled:
                logger.info(
                    f"Setting up or updating managed eventing for catalog '{catalog_id}'."
                )
                # This call is already idempotent and manages Topic, Notification, and Subscription
                updated_managed_config = await self.setup_managed_eventing_channel(
                    catalog_id, config.managed_eventing, conn=conn
                )
                # We don't save back here to avoid infinite loops, the caller (ConfigManager) just saved the config.
                # However, setup_managed_eventing_channel might update output fields (topic_path, etc.)
                # If they changed, we might need a way to update the DB without triggering the hook again.
                # For now, we assume these output fields are stable once established.
            else:
                logger.info(
                    f"Tearing down managed eventing for catalog '{catalog_id}'."
                )
                await self.teardown_managed_eventing_channel(
                    catalog_id, config.managed_eventing
                )

        # Handle custom subscriptions to external topics
        for sub in config.custom_subscriptions:
            if sub.enabled:
                logger.info(
                    f"Setting up external subscription '{sub.id}' for catalog '{catalog_id}'."
                )
                push_attributes = {
                    "subscription_id": sub.id,
                    "catalog_id": catalog_id,
                    "subscription_type": "custom",
                    "custom_subscription_id": sub.id,
                }
                await self.setup_push_subscription(
                    sub.topic_path, sub.subscription, custom_attributes=push_attributes
                )
            else:
                logger.info(
                    f"Tearing down external subscription '{sub.id}' for catalog '{catalog_id}'."
                )
                await self.teardown_external_subscription(sub.subscription)

    async def get_collection_storage_path(
        self, catalog_id: str, collection_id: str
    ) -> Optional[str]:
        """Returns the GCS path for a collection's folder (e.g., gs://bucket-name/collections/my-collection/)."""
        return await self.get_bucket_service().get_collection_storage_path(
            catalog_id, collection_id
        )

    async def setup_managed_eventing_channel(
        self,
        catalog_id: str,
        managed_config: ManagedBucketEventing,
        bucket_name: Optional[str] = None,
        conn=None,
        context: Optional[LifecycleContext] = None,
    ) -> ManagedBucketEventing:
        """Creates/updates the full managed eventing pipeline: Topic -> GCS Notification -> Subscription.

        conn is optional. If provided it is used only for the bucket name lookup (a short read).
        All GCP API calls (gRPC) happen after the connection is released to avoid
        ConnectionDoesNotExistError from asyncpg closing idle connections.
        """
        project_id = self.get_project_id()
        if not project_id:
            raise RuntimeError(
                "Cannot setup managed eventing: GCP Project ID is not available."
            )
        # If managed_config.subscription is None, initialize it with a default ID.
        if managed_config.subscription is None:
            managed_config.subscription = PushSubscriptionConfig(
                subscription_id=self.generate_default_subscription_id(catalog_id)
            )

        # 1. Create the managed topic idempotently.
        publisher_client = self.get_publisher_client()
        topic_id = managed_config.topic_id or self.generate_default_topic_id(catalog_id)
        topic_path = publisher_client.topic_path(project_id, topic_id)

        try:
            logger.info(f"Attempting to create topic with path: '{topic_path}'")
            await run_in_thread(publisher_client.create_topic, name=topic_path)
            logger.info(f"Created managed Pub/Sub topic: {topic_path}")
        except google_exceptions.AlreadyExists:
            logger.debug(f"Managed Pub/Sub topic '{topic_path}' already exists.")
        managed_config.topic_path = topic_path

        # Grant the GCS service account permission to publish to the newly created topic.
        storage_client = self.get_storage_client()
        gcs_service_account_email = await run_in_thread(
            storage_client.get_service_account_email, project=project_id
        )
        logger.info(
            f"Granting Pub/Sub Publisher role to GCS service account '{gcs_service_account_email}' on topic '{topic_path}'."
        )

        # Retry logic for IAM policy operations to handle transient "Socket closed" errors
        for attempt in range(1, 6):
            try:
                policy = await run_in_thread(
                    publisher_client.get_iam_policy, request={"resource": topic_path}
                )
                policy.bindings.add(
                    role="roles/pubsub.publisher",
                    members=[f"serviceAccount:{gcs_service_account_email}"],
                )
                await run_in_thread(
                    publisher_client.set_iam_policy,
                    request={"resource": topic_path, "policy": policy},
                )
                break
            except (
                google_exceptions.ServiceUnavailable,
                google_exceptions.InternalServerError,
                google_exceptions.Unknown,
                Aborted,
            ) as e:
                # "Socket closed" can appear as ServiceUnavailable or Unknown depending on the gRPC layer
                if attempt == 5:
                    logger.error(
                        f"Failed to update IAM policy for topic '{topic_path}' after {attempt} attempts: {e}"
                    )
                    raise

                delay = 1.0 * (2 ** (attempt - 1))
                logger.warning(
                    f"Retrying IAM policy update for topic '{topic_path}' due to error: {e}. Attempt {attempt}/5. Retrying in {delay}s..."
                )
                await asyncio.sleep(delay)

        # 2. Create GCS notifications for each configured prefix.
        if not bucket_name:
            if conn:
                bucket_name = await gcp_db.get_bucket_for_catalog_query.execute(
                    conn, catalog_id=catalog_id
                )
            else:
                async with managed_transaction(self.engine) as legacy_conn:
                    bucket_name = await gcp_db.get_bucket_for_catalog_query.execute(
                        legacy_conn, catalog_id=catalog_id
                    )
        if not bucket_name:
            raise RuntimeError(
                f"Cannot setup GCS notification: Bucket for catalog '{catalog_id}' does not exist."
            )

        bucket = storage_client.bucket(bucket_name)

        # Listing notifications can briefly return 404 if the bucket has just been
        # created or was externally deleted. We treat NotFound as recoverable:
        # ensure the bucket exists (via BucketService), wait for readiness, then retry with backoff.
        async def _list_notifications_with_retry(
            bucket_obj,
            bucket_name_str,
            max_retries: int = 8,
            initial_delay: float = 0.5,
        ):
            """
            Retries listing notifications with exponential backoff to handle GCS eventual consistency.
            Even after a bucket exists, it may not be immediately ready for all operations.
            """

            def _list_notifications_sync():
                return list(bucket_obj.list_notifications())

            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return await run_in_thread(_list_notifications_sync)
                except google_exceptions.NotFound as e:
                    last_exception = e
                    if attempt < max_retries:
                        # Exponential backoff: 0.5s, 1s, 2s, 4s, 8s, 16s, 32s, 64s (max ~127s total)
                        delay = initial_delay * (2 ** (attempt - 1))
                        logger.debug(
                            f"Attempt {attempt}/{max_retries}: Bucket '{bucket_name_str}' not ready for listing notifications. "
                            f"Retrying in {delay}s..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.warning(
                            f"Failed to list notifications after {max_retries} attempts. "
                            f"Bucket '{bucket_name_str}' may not be fully ready yet."
                        )
                        raise
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception

        try:
            existing_notifications = await _list_notifications_with_retry(
                bucket, bucket_name
            )
        except google_exceptions.NotFound as e:
            # Attempt a repair: ensure the bucket exists and is ready, then retry with backoff.
            logger.warning(
                f"GCS bucket '{bucket_name}' for catalog '{catalog_id}' not found when listing notifications. "
                f"Attempting to repair by ensuring bucket existence and retrying. Error: {e}"
            )

            bucket_manager = self.get_bucket_service()
            repaired_bucket_name = (
                await bucket_manager.ensure_storage_for_catalog(
                    catalog_id, conn=conn
                )
            )
            if not repaired_bucket_name:
                logger.error(
                    f"Failed to repair missing bucket for catalog '{catalog_id}'. "
                    f"Original bucket name was '{bucket_name}'."
                )
                raise RuntimeError(
                    f"Cannot setup GCS notification: Bucket for catalog '{catalog_id}' "
                    f"does not exist and could not be recreated."
                ) from e

            # If the bucket name changed, rebuild the bucket handle.
            if repaired_bucket_name != bucket_name:
                logger.info(
                    f"Bucket name for catalog '{catalog_id}' changed from '{bucket_name}' "
                    f"to '{repaired_bucket_name}' during repair."
                )
                bucket_name = repaired_bucket_name
                bucket = storage_client.bucket(bucket_name)

            # Wait for the bucket to be fully ready/visible.
            # Note: Even if wait_for_bucket_ready returns True, the bucket might not be ready
            # for all operations (like listing notifications) due to eventual consistency.
            # We'll rely on the retry logic below to handle this.
            ready = await bucket_manager.wait_for_bucket_ready(bucket_name)
            if not ready:
                logger.warning(
                    f"Repaired bucket '{bucket_name}' for catalog '{catalog_id}' did not report as ready "
                    f"within the expected time window, but will attempt to list notifications with retries."
                )
            else:
                # Even if the bucket reports as ready, give it a moment for eventual consistency
                # before attempting to list notifications
                await asyncio.sleep(0.5)

            # Retry listing notifications with backoff after repair.
            # The retry logic will handle eventual consistency even if wait_for_bucket_ready
            # returned False or if the bucket exists but isn't ready for listing notifications yet.
            try:
                existing_notifications = await _list_notifications_with_retry(
                    bucket, bucket_name
                )
            except google_exceptions.NotFound as e2:
                logger.error(
                    f"Bucket '{bucket_name}' for catalog '{catalog_id}' is still reported as missing "
                    f"after repair and retry attempts. Project: '{project_id}'. Error: {e2}"
                )

                raise RuntimeError(
                    f"Cannot setup GCS notification: Bucket '{bucket_name}' for catalog '{catalog_id}' "
                    f"is missing even after repair and retry attempts."
                ) from e2

        event_types = managed_config.event_types or [
            GcsNotificationEventType.OBJECT_FINALIZE
        ]
        managed_config.gcs_notification_ids = []

        # We need to ensure each prefix in managed_config.blob_name_prefixes has a notification.
        prefixes_to_setup = managed_config.blob_name_prefixes or [
            None
        ]  # None means entire bucket

        for prefix in prefixes_to_setup:
            match_found = False
            for notif in existing_notifications:
                # topic_name in GCS notification is the short ID, not full path
                if notif.topic_name == topic_id and notif.topic_project == project_id:
                    if (
                        notif.blob_name_prefix == prefix
                        and set(notif.event_types or []) == set(event_types)
                        and notif.payload_format == managed_config.payload_format
                    ):
                        managed_config.gcs_notification_ids.append(
                            notif.notification_id
                        )
                        logger.info(
                            f"Existing matching GCS notification '{notif.notification_id}' found for prefix '{prefix}' on bucket '{bucket_name}'."
                        )
                        match_found = True
                        break

            if not match_found:
                notification = bucket.notification(
                    topic_name=topic_id,
                    topic_project=project_id,
                    payload_format=managed_config.payload_format,
                    event_types=event_types,
                    blob_name_prefix=prefix,
                )

                await run_in_thread(notification.create)
                managed_config.gcs_notification_ids.append(notification.notification_id)
                logger.info(
                    f"Successfully created GCS notification '{notification.notification_id}' for prefix '{prefix}' on bucket '{bucket_name}'."
                )

        managed_config.bucket_id = bucket_name

        # 3. Create the push subscription to the managed topic.
        push_attributes = {
            "subscription_id": managed_config.subscription.subscription_id,
            "catalog_id": catalog_id,
            "subscription_type": "managed",
        }
        updated_subscription = await self.setup_push_subscription(
            topic_path, managed_config.subscription, custom_attributes=push_attributes
        )
        managed_config.subscription = updated_subscription

        return managed_config

    async def setup_push_subscription(
        self,
        topic_path: str,
        sub_config: PushSubscriptionConfig,
        custom_attributes: Optional[Dict[str, str]] = None,
    ) -> PushSubscriptionConfig:
        """Creates a push subscription to a given topic. Idempotent."""
        project_id = self.get_project_id()
        region = self.get_region()
        if not project_id or not region:
            raise RuntimeError(
                "Cannot determine self URL for push subscription: Project ID or Region could not be determined."
            )

        base_url = await self.get_self_url()
        push_endpoint = f"{base_url}/gcp/events/pubsub-push"

        # Ensure custom_attributes is not None for logging and usage
        attributes = custom_attributes or {}

        # Pub/Sub requires HTTPS for push endpoints. In local/test environments
        # where HTTPS is not available, we skip the push configuration to avoid 400 errors.
        if not push_endpoint.startswith("https://"):
            logger.warning(
                f"Push endpoint '{push_endpoint}' does not use HTTPS. Skipping push configuration as it is required by GCP Pub/Sub."
            )
            push_config = None
        else:
            # The custom attributes must be attached to the parent PushConfig object,
            # not the OidcToken object, for them to be included in the push message.
            oidc_token_config = pubsub_v1.types.PushConfig.OidcToken(
                service_account_email=self.get_account_email(),
                audience=PUBSUB_JWT_AUDIENCE,
            )

            # For compatibility with some library versions, attributes must be placed
            # on the PushConfig. This will send them as HTTP headers.
            push_config = pubsub_v1.types.PushConfig(
                push_endpoint=push_endpoint,
                oidc_token=oidc_token_config,
                attributes=attributes,
            )

        subscriber_client = self.get_subscriber_client()
        subscription_path = subscriber_client.subscription_path(
            project_id, sub_config.subscription_id
        )

        subscription_args = {
            "name": subscription_path,
            "topic": topic_path,
            "push_config": push_config,
            "ack_deadline_seconds": sub_config.ack_deadline_seconds,
            # "retain_acked_messages": sub_config.retain_acked_messages,
            # "enable_message_ordering": sub_config.enable_message_ordering,
            # "filter": sub_config.filter,
            # "enable_exactly_once_delivery": sub_config.enable_exactly_once_delivery
        }

        # Message retention duration
        # retention_duration = pubsub_v1.types.Duration()
        # retention_duration.seconds = sub_config.message_retention_duration_days * 24 * 60 * 60
        # subscription_args["message_retention_duration"] = retention_duration

        # Dead letter policy
        # if sub_config.dead_letter_policy:
        #     subscription_args["dead_letter_policy"] = pubsub_v1.types.DeadLetterPolicy(
        #         dead_letter_topic=sub_config.dead_letter_policy.dead_letter_topic,
        #         max_delivery_attempts=sub_config.dead_letter_policy.max_delivery_attempts,
        #     )

        # # Retry policy (exponential backoff)
        # if sub_config.retry_policy:
        #     min_backoff = pubsub_v1.types.Duration(seconds=sub_config.retry_policy.minimum_backoff_seconds)
        #     max_backoff = pubsub_v1.types.Duration(seconds=sub_config.retry_policy.maximum_backoff_seconds)
        #     subscription_args["retry_policy"] = pubsub_v1.types.RetryPolicy(
        #         minimum_backoff=min_backoff,
        #         maximum_backoff=max_backoff,
        #     )

        # # Expiration policy (TTL)
        # if sub_config.expiration_policy:
        #     ttl_duration = pubsub_v1.types.Duration(seconds=sub_config.expiration_policy.ttl_days * 24 * 60 * 60)
        #     subscription_args["expiration_policy"] = pubsub_v1.types.ExpirationPolicy(ttl=ttl_duration)

        try:
            # Run blocking create_subscription via the shared concurrency backend
            await run_in_thread(
                subscriber_client.create_subscription, **subscription_args
            )
            logger.info(
                f"Created Pub/Sub push subscription '{subscription_path}' to endpoint '{push_endpoint}' with attributes {list(attributes.keys())}."
            )
        except google_exceptions.AlreadyExists:
            logger.debug(
                f"Pub/Sub subscription '{subscription_path}' already exists. Updating PushConfig to ensure attributes are current."
            )
            # If the subscription exists, we MUST update the PushConfig to ensure that
            # any new custom attributes (like subscription_id) are applied.
            # create_subscription does not update existing resources.
            try:
                # Run blocking modify_push_config via the shared concurrency backend
                await run_in_thread(
                    subscriber_client.modify_push_config,
                    request={
                        "subscription": subscription_path,
                        "push_config": push_config,
                    },
                )
                logger.info(
                    f"Successfully updated PushConfig (attributes: {list(attributes.keys())}) for existing subscription '{subscription_path}'."
                )
            except Exception as e:
                logger.error(
                    f"Failed to update PushConfig for existing subscription '{subscription_path}': {e}"
                )

        sub_config.subscription_path = subscription_path
        return sub_config

    async def teardown_managed_eventing_channel(
        self, catalog_id: str, managed_config: ManagedBucketEventing
    ):
        """Tears down the full managed eventing pipeline."""
        # 1. Delete all GCS notification resources from the bucket.
        if managed_config.gcs_notification_ids:
            bucket_name = await self.get_bucket_service().get_storage_identifier(
                catalog_id
            )
            if bucket_name:
                for notif_id in managed_config.gcs_notification_ids:
                    await self.get_bucket_service().teardown_gcs_notification(
                        bucket_name, notif_id
                    )
            else:
                logger.warning(
                    f"Could not determine bucket name for catalog '{catalog_id}'. Skipping GCS notification teardown."
                )

        # 2. Delete the managed topic. This will also delete its subscriptions.
        if managed_config.topic_path:
            publisher_client = self.get_publisher_client()
            try:
                # Run blocking delete_topic via the shared concurrency backend
                await run_in_thread(
                    publisher_client.delete_topic,
                    request={"topic": managed_config.topic_path},
                )
                logger.info(
                    f"Deleted managed Pub/Sub topic: {managed_config.topic_path}"
                )
            except google_exceptions.NotFound:
                logger.debug(
                    f"Managed Pub/Sub topic '{managed_config.topic_path}' not found. Nothing to delete."
                )

            # # Dispatch event: Managed Eventing Teardown
            # await events_module.create_event(
            #     event_type=GcpEventType.MANAGED_EVENTING_TEARDOWN.value,
            #     payload={"catalog_id": catalog_id, "topic_path": managed_config.topic_path})

    async def teardown_external_subscription(self, sub_config: PushSubscriptionConfig):
        """
        Deletes a single external Pub/Sub subscription.
        """
        if sub_config.subscription_path:
            subscriber_client = self.get_subscriber_client()
            try:
                # Run blocking delete_subscription via the shared concurrency backend
                await run_in_thread(
                    subscriber_client.delete_subscription,
                    request={"subscription": sub_config.subscription_path},
                )
                logger.info(
                    f"Deleted Pub/Sub subscription: {sub_config.subscription_path}"
                )
            except google_exceptions.NotFound:
                logger.debug(
                    f"Pub/Sub subscription '{sub_config.subscription_path}' not found. Nothing to delete."
                )
            except Exception as e:
                logger.error(
                    f"Failed to delete Pub/Sub subscription '{sub_config.subscription_path}': {e}",
                    exc_info=True,
                )

    async def set_eventing_config(
        self, catalog_id: str, config: GcpEventingConfig, conn=None
    ) -> GcpEventingConfig:
        """Persists the eventing configuration for a catalog."""
        config_service = self.get_config_service()
        # Pass the connection to reuse the transaction
        await config_service.set_config(
            GCP_EVENTING_CONFIG_ID, config, catalog_id=catalog_id, db_resource=conn
        )
        # Re-fetch to confirm and return the validated model
        return await config_service.get_config(
            GCP_EVENTING_CONFIG_ID, catalog_id, db_resource=conn
        )

    async def setup_catalog_eventing(self, catalog_id: str) -> Tuple[str, Any]:
        """EventingProtocol: Sets up GCP eventing for a catalog."""
        # For GCP, this ensures both bucket and eventing are ready
        return await self.setup_catalog_gcp_resources(catalog_id)

    async def teardown_catalog_eventing(
        self, catalog_id: str, config: Optional[Any] = None
    ) -> None:
        """EventingProtocol: Tears down GCP eventing for a catalog."""
        if config and hasattr(config, "managed_eventing") and config.managed_eventing:
            await self.teardown_managed_eventing_channel(
                catalog_id, config.managed_eventing
            )

        if config is None:
            # Force cleanup of deterministic default resources (topics/subscriptions)
            # This is useful during hard deletion where config might be already missing.
            project_id = self.get_project_id()
            if not project_id:
                return

            # Default Topic (deleting topic deletes its subscriptions)
            topic_id = self.generate_default_topic_id(catalog_id)
            topic_path = self.get_publisher_client().topic_path(project_id, topic_id)
            try:
                await run_in_thread(
                    self.get_publisher_client().delete_topic,
                    request={"topic": topic_path},
                )
                logger.info(f"Forcefully deleted default topic: {topic_path}")
            except google_exceptions.NotFound:
                pass
            except Exception as e:
                logger.error(
                    f"Failed to force delete default topic '{topic_path}': {e}"
                )

            # Default Subscription (if it exists separately, though deleting topic should handle it)
            sub_id = self.generate_default_subscription_id(catalog_id)
            sub_path = self.get_subscriber_client().subscription_path(
                project_id, sub_id
            )
            try:
                await run_in_thread(
                    self.get_subscriber_client().delete_subscription,
                    request={"subscription": sub_path},
                )
                logger.info(f"Forcefully deleted default subscription: {sub_path}")
            except google_exceptions.NotFound:
                pass
            except Exception as e:
                logger.error(
                    f"Failed to force delete default subscription '{sub_path}': {e}"
                )

    async def get_eventing_config(
        self,
        catalog_id: str,
        conn=None,
        context: Optional[LifecycleContext] = None,
    ) -> Optional[GcpEventingConfig]:
        """Internal helper to fetch and parse a catalog's eventing config."""
        config_service = self.get_config_service()
        # Pass the connection to reuse the transaction if provided
        config = await config_service.get_config(
            GCP_EVENTING_CONFIG_ID,
            catalog_id,
            db_resource=conn,
            config_snapshot=context.config if context else None,
        )

        if isinstance(config, GcpEventingConfig):
            return config
        if isinstance(config, dict):
            return GcpEventingConfig.model_validate(config)
        return None
