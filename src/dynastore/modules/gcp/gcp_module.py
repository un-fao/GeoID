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
from typing import TYPE_CHECKING, Optional, AsyncIterator, Dict, Any, List, Tuple, Union

if TYPE_CHECKING:
    from google.api_core import exceptions as google_exceptions
    from google.api_core.exceptions import Aborted
    from google.cloud import storage, pubsub_v1, run_v2
else:
    try:
        from google.api_core import exceptions as google_exceptions
        from google.api_core.exceptions import Aborted
    except ImportError:
        google_exceptions = None
        Aborted = None
from dynastore.tools.cache import cached
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
    AssetUploadProtocol,
    UploadTicket,
    UploadStatus,
    UploadStatusResponse,
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
    GcpModuleConfig,
    TriggeredAction,
)
from dynastore.modules.gcp.models import (
    GcpEventType,
    PushSubscriptionConfig,
    PUBSUB_JWT_AUDIENCE,
)
from dynastore.modules.processes.protocols import ProcessRegistryProtocol

if not TYPE_CHECKING:
    try:
        from google.cloud import storage
    except ImportError:
        storage = None
    try:
        from google.cloud import pubsub_v1
    except ImportError:
        pubsub_v1 = None
    try:
        from google.cloud import run_v2
    except ImportError:
        run_v2 = None
from dynastore.modules.gcp.bucket_service import BucketService
# from google.cloud import compute_v1
from .gcp_catalog_ops import GcpCatalogOpsMixin
from .gcp_eventing_ops import GcpEventingOpsMixin
from .gcp_storage_ops import GcpStorageOpsMixin

logger = logging.getLogger(__name__)


def _task_type_from_scope_token(token: str) -> Optional[str]:
    """Extract a task entry-point name from a Cloud Run Job's SCOPE env token.

    Handles both the current canonical form and the legacy hyphenated form so
    jobs deployed before the ``worker_task_*`` rename can still be discovered.

    Canonical  : ``worker_task_gdal``           → ``gdal``
    Legacy     : ``task-gdal-job``              → ``gdal``
                 ``task_export_features_job``   → ``export_features``
    Infra noise: ``task_base``, ``core``, …     → ``None`` (skip)
    """
    # Canonical form: worker_task_<name>
    if token.startswith("worker_task_"):
        name = token[len("worker_task_"):]
        return name or None

    # Legacy form: task[-_]<name>[-_]job (normalize separators first)
    normalized = token.replace("-", "_").lower()
    if normalized.startswith("task_") and normalized.endswith("_job"):
        inner = normalized[len("task_"):-len("_job")]
        return inner or None

    return None


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
    GcpCatalogOpsMixin,
    GcpEventingOpsMixin,
    GcpStorageOpsMixin,
    ModuleProtocol,
    StorageProtocol,
    JobExecutionProtocol,
    CloudStorageClientProtocol,
    CloudIdentityProtocol,
    EventingProtocol,
    AssetUploadProtocol,
    ProcessRegistryProtocol,
):
    _credentials: Optional[Any] = None
    _identity: Optional[Dict[str, Any]] = None
    _engine: Optional[DbResource] = None
    _config_service: Optional[ConfigsProtocol] = None
    _module_config: Optional[GcpModuleConfig] = None
    # In-memory upload ticket store: ticket_id → {asset_id, catalog_id, collection_id, expires_at}
    _upload_tickets: Dict[str, Dict[str, Any]] = {}

    priority: int = 30  # Priority for protocol implementation (lower means higher priority)

    @property
    def engine(self) -> DbResource:
        if self._engine:
            return self._engine
        from dynastore.tools.protocol_helpers import get_engine
        eng = get_engine()
        assert eng is not None, "No DB engine available"
        return eng

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

            # Async clients: always (re)create so that the module is usable as
            # soon as __init__ runs — TasksModule starts its runner.setup() at
            # priority 15, before GCPModule's lifespan runs (priority 30), so
            # _jobs_client must be populated at __init__ time.
            self._jobs_client = run_v2.JobsAsyncClient(
                credentials=self._credentials
            )
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
            cfg = await self._config_service.get_config(GcpModuleConfig)
            self._module_config = cfg if isinstance(cfg, GcpModuleConfig) else None
            
            # Synchronize local tunables with global config
            if self._module_config:
                global _CATALOG_VISIBILITY_MAX_RETRIES, _CATALOG_VISIBILITY_RETRY_INTERVAL
                _CATALOG_VISIBILITY_MAX_RETRIES = self._module_config.catalog_visibility_max_retries
                _CATALOG_VISIBILITY_RETRY_INTERVAL = self._module_config.catalog_visibility_retry_interval
        else:
            logger.warning(
                "GCP Module: ConfigsProtocol not available. Global settings will use defaults/env."
            )
            self._module_config = GcpModuleConfig()

        # 2. Ensure synchronous clients are open (re-open if closed from previous lifespan)
        self.reinitialize_clients()

        # Retrieve ConfigsProtocol via dm.get_protocol
        self._config_service = get_protocol(ConfigsProtocol)
        if not self._config_service:
            logger.warning(
                "GCP Module: ConfigsProtocol not available. Configuration management disabled."
            )

        try:
            # Async clients are already initialized in __init__ via reinitialize_clients().
            # Recreate here so lifespan has fresh instances (e.g. after a hot-reload).
            self.reinitialize_clients()

            # Initialize BucketService (Safe even if clients are None)
            self._bucket_service = BucketService(
                engine=self._engine, # Explicitly pass current engine check
                config_service=self._config_service,
                storage_client=self._storage_client,  # type: ignore[arg-type]
                project_id=self.get_project_id() or "",
                region=self.get_region() or "",
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

            lifecycle_registry.sync_catalog_initializer()(self._on_sync_init_catalog)
            # We keep these as async because they don't block the core creation flow
            # and don't cause race conditions in tests as easily as the creation one.
            lifecycle_registry.async_catalog_destroyer()(self._on_async_destroy_catalog)
            lifecycle_registry.async_collection_destroyer()(
                self._on_async_destroy_collection
            )

            # Register BigQueryService and BigQueryCollectionEnricher
            from dynastore.modules.gcp.bigquery_service import BigQueryService
            from dynastore.modules.gcp.bq_collection_enricher import BigQueryCollectionEnricher
            from dynastore.tools.discovery import register_plugin

            self._bq_service = BigQueryService()
            self._bq_collection_enricher = BigQueryCollectionEnricher()
            register_plugin(self._bq_service)
            register_plugin(self._bq_collection_enricher)

            # Register GCS-backed asset processes (``download``).
            from dynastore.modules.gcp.asset_processes import GcsDownloadAssetProcess

            self._asset_download_process = GcsDownloadAssetProcess(
                client_provider=self,
                identity_provider=self if hasattr(self, "get_fresh_token") else None,
            )
            register_plugin(self._asset_download_process)

            yield
        finally:
            logger.info("GCP Module: Exiting lifespan - closing all clients.")
            # Unregister BigQuery plugins
            from dynastore.tools.discovery import unregister_plugin

            for attr in ("_bq_service", "_bq_collection_enricher", "_asset_download_process"):
                obj = getattr(self, attr, None)
                if obj:
                    unregister_plugin(obj)
                    setattr(self, attr, None)
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
                task_types_found = set()

                # Strategy 1: Check SCOPE env var for explicit task type mapping
                if (
                    job.template
                    and job.template.template
                    and job.template.template.containers
                ):
                    for container in job.template.template.containers:
                        for env_var in container.env:
                            if env_var.name == "SCOPE":
                                scope_value = env_var.value
                                if scope_value:
                                    for token in (
                                        s.strip() for s in scope_value.split(",") if s.strip()
                                    ):
                                        task_type = _task_type_from_scope_token(token)
                                        if task_type is not None:
                                            task_types_found.add(task_type)
                                break

                # Strategy 2: Infer task key from job name (fallback if no SCOPE)
                # Only process jobs named with the "dynastore-" prefix — this is the
                # implicit marker that a Cloud Run job belongs to this deployment.
                # Strip prefix + optional "-job" suffix; hyphens preserved so
                # try_load_process_definition can handle both hyphenated and underscored forms.
                if not task_types_found and job_name.startswith("dynastore-"):
                    inferred = job_name[len("dynastore-"):]
                    if inferred.endswith("-job"):
                        inferred = inferred[: -len("-job")]
                    if inferred:
                        task_types_found.add(inferred)

                # Add to job_map; will be validated when Process definitions are loaded
                for task_type in task_types_found:
                    job_map[task_type] = job_name
                    if len(task_types_found) > 1 or not job_name.replace("-", "_") == task_type:
                        logger.info(
                            f"Discovered GCP job mapping: task '{task_type}' -> job '{job_name}' (via SCOPE)"
                        )
                    else:
                        logger.info(
                            f"Discovered GCP job mapping: task '{task_type}' -> job '{job_name}' (inferred from job name)"
                        )
        except Exception as e:
            logger.error(
                f"Error discovering GCP jobs (Project: {project_id}, Region: {region}): {e}",
                exc_info=True,
            )

        return job_map

    # --- ProcessRegistryProtocol Implementation ---

    async def list_processes(self, tenant: Optional[str] = None) -> List[Any]:
        """ProcessRegistryProtocol: list all Cloud Run Job process definitions.

        Includes both explicitly-defined processes and synthetic entries for
        Cloud Run jobs without Process definitions, making all jobs discoverable
        and executable as external services.
        """
        from dynastore.modules.gcp.tools.jobs import load_job_config, try_load_process_definition
        from dynastore.modules.processes.models import Process, ProcessScope, JobControlOptions, TransmissionMode

        job_map = await load_job_config()
        result = []
        seen_ids = set()

        for task_type, job_name in job_map.items():
            # Try to load explicit Process definition
            defn = try_load_process_definition(task_type)
            if defn is not None:
                result.append(defn)
                seen_ids.add(defn.id)
            else:
                # No definition found; use task_type (already in hyphenated form) as process id
                process_id = task_type
                if process_id not in seen_ids:
                    synthetic = Process(
                        id=process_id,
                        title=f"Cloud Run Job: {job_name}",
                        description=f"External Cloud Run job deployed as {job_name}",
                        version="1.0.0",
                        scopes=[ProcessScope.PLATFORM],
                        jobControlOptions=[JobControlOptions.ASYNC_EXECUTE],
                        outputTransmission=[TransmissionMode.VALUE],
                        inputs={},
                        outputs={},
                        links=[],
                    )
                    result.append(synthetic)
                    seen_ids.add(process_id)
                    logger.info(
                        f"Created synthetic Process for Cloud Run job '{job_name}' (task_type={task_type})"
                    )

        return result

    async def get_process(self, process_id: str, tenant: Optional[str] = None) -> Optional[Any]:
        """ProcessRegistryProtocol: look up a single process definition by id."""
        return next((p for p in await self.list_processes(tenant) if p.id == process_id), None)

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

    def _refresh_credentials(self) -> bool:
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
        assert self._credentials is not None
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

    @cached(maxsize=1, distributed=False)
    async def get_self_url(self) -> str:  # type: ignore[override]
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
            service_path = client.service_path(project_id or "", region or "", service_name)
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


try:
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner
    from dynastore.tools.discovery import register_plugin as _reg_runner
    _reg_runner(GcpJobRunner())
except ImportError:
    pass
