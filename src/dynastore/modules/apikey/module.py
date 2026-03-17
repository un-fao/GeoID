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

# File: dynastore/modules/apikey/module.py

import logging
from contextlib import asynccontextmanager
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.auth import (
    AuthenticationProtocol,
    AuthorizationProtocol,
    Principal,
)
from .apikey_service import ApiKeyService
from .apikey_storage import AbstractApiKeyStorage
from .policies import PolicyService
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.query_executor import DbResource
from typing import Optional, Any, AsyncGenerator
from dynastore.tools.discovery import register_plugin, unregister_plugin

logger = logging.getLogger(__name__)


from dynastore.models.protocols.policies import PermissionProtocol
class ApiKeyModule(ModuleProtocol, AuthenticationProtocol, AuthorizationProtocol, PermissionProtocol):
    priority: int = 100
    _apikey_manager: Optional[ApiKeyService] = None
    _policy_service: Optional[PolicyService] = None
    storage: Optional[AbstractApiKeyStorage] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Register as early as possible to be discoverable during extension configuration
        register_plugin(self)

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        from .postgres_apikey_storage import PostgresApiKeyStorage
        from .postgres_policy_storage import PostgresPolicyStorage
        from contextlib import AsyncExitStack
        from dynastore.modules.db_config.query_executor import managed_transaction
        from dynastore.models.protocols import DatabaseProtocol

        self.storage = PostgresApiKeyStorage(app_state)
        policy_storage = PostgresPolicyStorage(app_state)
        self._policy_service = PolicyService(
            app_state, storage=policy_storage, apikey_storage=self.storage
        )
        self._apikey_manager = ApiKeyService(
            self.storage, self._policy_service, app_state=app_state
        )
        # Register early to avoid race conditions in middleware discovery
        register_plugin(self._apikey_manager)

        async with AsyncExitStack() as stack:
            # Manage manager lifespan
            await stack.enter_async_context(self._apikey_manager.lifespan(app_state))

            # Register Identity Providers (Configured via Env)
            from .identity_providers.local_db_identity import LocalDBIdentityProvider
            from .identity_providers.keycloak_identity import KeycloakIdentityProvider
            import os

            # Always register LocalDB provider for internal JWT tokens
            local_provider = LocalDBIdentityProvider(
                storage=self.storage,
                jwt_secret=None,  # Temporary, will be updated below
            )

            # Global initialization
            try:
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None
                async with managed_transaction(engine) as conn:
                    # Use the new private initialization methods
                    await self.storage._initialize_schema(conn, schema="apikey")
                    await policy_storage._initialize_schema(conn, schema="apikey")

                    # Provision default global policies
                    await self._policy_service.provision_default_policies(conn=conn)

                    # Initialize users schema for identity management
                    await self.storage._initialize_schema(conn, schema="users")

                    # Sync JWT secret
                    jwt_secret = await self._apikey_manager.get_jwt_secret()
                    local_provider.jwt_secret = jwt_secret

            except Exception as e:
                logger.error(f"Failed to initialize ApiKeyModule: {e}", exc_info=True)

            # Register other plugins
            register_plugin(self._apikey_manager)
            register_plugin(local_provider)

            keycloak_url = os.environ.get("KEYCLOAK_URL")
            if keycloak_url:
                register_plugin(
                    KeycloakIdentityProvider(
                        issuer_url=keycloak_url,
                        client_id=os.environ.get("KEYCLOAK_CLIENT_ID", "dynastore-api"),
                        audience=os.environ.get("KEYCLOAK_AUDIENCE"),
                    )
                )

            yield
            
            # Unregister plugins on exit
            unregister_plugin(local_provider)
            unregister_plugin(self._apikey_manager)
            # ApiKeyService stops via AsyncExitStack
        
        # Finally unregister self
        unregister_plugin(self)

    async def resolve_principal(self, credentials: Any) -> Optional[Principal]:
        """
        Implementation of AuthenticationProtocol.
        Validates credentials directly, avoiding HTTP layer dependencies.
        """
        if not self._apikey_manager:
            return None

        # Support only string credentials (Bearer tokens / API Keys)
        if isinstance(credentials, str):
            try:
                principal, _, _ = await self._apikey_manager.authenticate_apikey(
                    credentials
                )
                return principal
            except Exception:
                return None
        return None

    async def check_permission(
        self, principal: Principal, action: str, resource: str
    ) -> bool:
        """
        Implementation of AuthorizationProtocol.
        Delegates to the PolicyService.
        """
        if not self._policy_service:
            return False

        return await self._policy_service.check_permission(principal, action, resource)

    async def _provision_late_item(self, item: Any, is_policy: bool = True):
        """Helper to provision a late-registered item."""
        if not self._policy_service:
            return

        try:
            # We use the background task system or a direct call if safe
            from dynastore.modules.db_config.query_executor import managed_transaction
            from dynastore.models.protocols import DatabaseProtocol
            db = get_protocol(DatabaseProtocol)
            engine = db.engine if db else None
            
            async with managed_transaction(engine) as conn:
                if is_policy:
                    await self._policy_service.storage.update_policy(item, schema="apikey", conn=conn)
                    logger.debug(f"Late-provisioned policy: {item.id}")
                else:
                    await self.storage.update_role(item, schema="apikey", conn=conn)
                    logger.debug(f"Late-provisioned role: {item.name}")
                    
                # Invalidate cache to ensure changes take effect
                self._policy_service._invalidate_cache()
        except Exception as e:
            logger.debug(f"Proactive provisioning of {item} failed: {e}")

    def register_policy(self, policy: Any) -> Any:
        """
        Implementation of AuthorizationProtocol and PermissionProtocol.
        Registers a policy for late provisioning.
        """
        from .policies import register_policy as _reg
        res = _reg(policy)
        
        # Proactively attempt provisioning if modules are up
        if self._policy_service:
            import asyncio
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._provision_late_item(res, is_policy=True))
            except RuntimeError:
                pass # No loop running
        return res

    def register_role(self, role: Any) -> Any:
        """
        Implementation of PermissionProtocol.
        Registers a role for late provisioning.
        """
        from .policies import register_role as _reg
        res = _reg(role)

        # Proactively attempt provisioning if modules are up
        if self._policy_service:
            import asyncio
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._provision_late_item(res, is_policy=False))
            except RuntimeError:
                pass # No loop running
        return res



@lifecycle_registry.sync_catalog_initializer
async def initialize_apikey_tenant(conn: DbResource, schema: str, catalog_id: str):
    """Initializes API Key and Policy tables within a tenant schema."""
    logger.info(
        f"Initializing ApiKey tables for tenant: {catalog_id} in schema {schema}"
    )

    from .postgres_apikey_storage import PostgresApiKeyStorage
    from .postgres_policy_storage import PostgresPolicyStorage

    storage = PostgresApiKeyStorage()
    policy_storage = PostgresPolicyStorage()

    await policy_storage._initialize_schema(conn, schema=schema)
    await storage._initialize_schema(conn, schema=schema)

    # Provision default policies for the new tenant
    from .policies import PolicyService

    policy_service = PolicyService(
        None, storage=policy_storage, apikey_storage=storage
    )  # app_state not strictly needed for provisioning with explicit conn
    await policy_service.provision_default_policies(
        catalog_id, conn=conn, schema=schema
    )
