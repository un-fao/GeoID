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

# File: dynastore/modules/iam/module.py

import asyncio
import logging
from contextlib import asynccontextmanager
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.auth import (
    AuthenticationProtocol,
    AuthorizationProtocol,
    Principal,
)
from .iam_service import IamService
from .iam_storage import AbstractIamStorage
from .policies import PolicyService
from .authorization.iam_authorizer import IamAuthorizer
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.query_executor import DbResource
from typing import Optional, Any, AsyncGenerator
from dynastore.tools.discovery import register_plugin, unregister_plugin

logger = logging.getLogger(__name__)


from dynastore.models.protocols.policies import PermissionProtocol
class IamModule(ModuleProtocol, AuthenticationProtocol, AuthorizationProtocol, PermissionProtocol):
    priority: int = 100
    _iam_manager: Optional[IamService] = None
    _policy_service: Optional[PolicyService] = None
    _authorizer: Optional[IamAuthorizer] = None
    storage: Optional[AbstractIamStorage] = None
    _pending_tasks: list = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._pending_tasks = []
        self._role_lock = asyncio.Lock()
        # Register as early as possible to be discoverable during extension configuration
        register_plugin(self)

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        from .postgres_iam_storage import PostgresIamStorage
        from .postgres_policy_storage import PostgresPolicyStorage
        from contextlib import AsyncExitStack
        from dynastore.modules.db_config.query_executor import managed_transaction
        from dynastore.models.protocols import DatabaseProtocol

        self.storage = PostgresIamStorage(app_state)
        policy_storage = PostgresPolicyStorage(app_state)
        self._policy_service = PolicyService(
            app_state, storage=policy_storage, iam_storage=self.storage
        )
        self._iam_manager = IamService(
            self.storage, self._policy_service, app_state=app_state
        )
        # Register early to avoid race conditions in middleware discovery
        register_plugin(self._iam_manager)

        async with AsyncExitStack() as stack:
            # Manage manager lifespan
            await stack.enter_async_context(self._iam_manager.lifespan(app_state))

            import os

            # Global initialization
            try:
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None
                async with managed_transaction(engine) as conn:
                    await self.storage._initialize_schema(conn, schema="iam")
                    await policy_storage._initialize_schema(conn, schema="iam")

                    # Provision default global policies
                    await self._policy_service.provision_default_policies(conn=conn)

            except Exception as e:
                logger.error(f"Failed to initialize IamModule: {e}", exc_info=True)

            # Register plugins
            register_plugin(self._iam_manager)

            # Register the AuthorizerProtocol implementor. When IAM is loaded,
            # `require_permission` routes through IamAuthorizer; otherwise
            # DefaultAuthorizer enforces fail-closed role-only checks.
            self._authorizer = IamAuthorizer()
            register_plugin(self._authorizer)

            # IdP factory — IDP_TYPE selects the backend (default: oidc)
            # KEYCLOAK_* vars are read as fallbacks for backward compatibility.
            # See modules/iam/identity_providers/README.md for adding new types.
            idp_type = os.environ.get("IDP_TYPE", "oidc").lower()
            idp_issuer = (
                os.environ.get("IDP_ISSUER_URL")
                or os.environ.get("KEYCLOAK_ISSUER_URL")
            )
            if idp_type == "oidc" and idp_issuer:
                from .identity_providers.oidc_identity import OidcIdentityProvider
                register_plugin(
                    OidcIdentityProvider(
                        issuer_url=idp_issuer,
                        client_id=(
                            os.environ.get("IDP_CLIENT_ID")
                            or os.environ.get("KEYCLOAK_CLIENT_ID", "dynastore-api")
                        ),
                        client_secret=(
                            os.environ.get("IDP_CLIENT_SECRET")
                            or os.environ.get("KEYCLOAK_CLIENT_SECRET")
                        ),
                        audience=(
                            os.environ.get("IDP_AUDIENCE")
                            or os.environ.get("KEYCLOAK_AUDIENCE")
                        ),
                        public_url=(
                            os.environ.get("IDP_PUBLIC_URL")
                            or os.environ.get("KEYCLOAK_PUBLIC_URL")
                        ),
                    )
                )
                logger.info("Registered OIDC identity provider: %s", idp_issuer)
            elif idp_type == "saml2":
                logger.warning(
                    "IDP_TYPE=saml2 is not yet implemented; no IdP registered. "
                    "See modules/iam/identity_providers/README.md."
                )
            elif idp_issuer:
                logger.warning(
                    "Unknown IDP_TYPE=%r with IDP_ISSUER_URL set; no IdP registered.",
                    idp_type,
                )

            yield

            # Unregister plugins on exit
            unregister_plugin(self._iam_manager)
            if self._authorizer is not None:
                unregister_plugin(self._authorizer)
                self._authorizer = None
            # IamService stops via AsyncExitStack
        
        # Finally unregister self
        unregister_plugin(self)

    async def resolve_principal(self, credentials: Any) -> Optional[Principal]:
        """
        Implementation of AuthenticationProtocol.
        Validates credentials directly, avoiding HTTP layer dependencies.
        """
        if not self._iam_manager:
            return None

        # Support only string credentials (Bearer tokens via JWT/Keycloak)
        if isinstance(credentials, str):
            try:
                roles, principal = await self._iam_manager.authenticate_and_get_role(
                    type("Request", (), {"headers": {"Authorization": f"Bearer {credentials}"}, "state": type("State", (), {"catalog_id": None})()})()
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

    async def _persist_policy(self, policy: Any):
        """Persist a policy to the database via storage upsert."""
        try:
            from dynastore.modules.db_config.query_executor import managed_transaction
            from dynastore.models.protocols import DatabaseProtocol
            db = get_protocol(DatabaseProtocol)
            engine = db.engine if db else None

            async with managed_transaction(engine) as conn:
                await self._policy_service.storage.update_policy(policy, schema="iam", conn=conn)
                logger.debug(f"Persisted policy: {policy.id}")
                self._policy_service.invalidate_cache()
        except Exception as e:
            logger.warning(f"Failed to persist policy {policy.id}: {e}")

    async def _persist_role(self, role: Any):
        """Persist a role to the database via storage upsert.

        Uses an asyncio lock to serialize concurrent role merges and
        prevent read-modify-write races when multiple extensions register
        policies for the same role (e.g. 'anonymous') during startup.
        """
        async with self._role_lock:
            try:
                from dynastore.modules.db_config.query_executor import managed_transaction
                from dynastore.models.protocols import DatabaseProtocol
                db = get_protocol(DatabaseProtocol)
                engine = db.engine if db else None

                async with managed_transaction(engine) as conn:
                    existing = await self.storage.get_role(role.name, schema="iam", conn=conn)
                    if existing:
                        # Merge policies
                        merged_policies = list(set(existing.policies + role.policies))
                        role = role.model_copy(update={"policies": merged_policies})
                        await self.storage.update_role(role, schema="iam", conn=conn)
                    else:
                        await self.storage.create_role(role, schema="iam", conn=conn)
                    logger.debug(f"Persisted role: {role.name}")
                    self._policy_service.invalidate_cache()
            except Exception as e:
                logger.warning(f"Failed to persist role {role.name}: {e}")

    def register_policy(self, policy: Any) -> Any:
        """Persist policy to database. Called by extensions during lifespan."""
        if not self._policy_service:
            logger.warning(f"PolicyService not ready; cannot register policy {policy.id}")
            return policy

        import asyncio
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._persist_policy(policy))
            self._pending_tasks.append(task)
        except RuntimeError:
            pass
        return policy

    def register_role(self, role: Any) -> Any:
        """Persist role to database. Called by extensions during lifespan."""
        if not self._policy_service:
            logger.warning(f"PolicyService not ready; cannot register role {getattr(role, 'name', role)}")
            return role

        import asyncio
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._persist_role(role))
            self._pending_tasks.append(task)
        except RuntimeError:
            pass
        return role

    async def flush_pending_registrations(self):
        """Await all pending policy/role registration tasks. Call after extensions register."""
        if self._pending_tasks:
            import asyncio
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks.clear()



@lifecycle_registry.sync_catalog_initializer
async def initialize_iam_tenant(conn: DbResource, schema: str, catalog_id: str):
    """Initializes IAM and Policy tables within a tenant schema."""
    logger.info(
        f"Initializing IAM tables for tenant: {catalog_id} in schema {schema}"
    )

    from .postgres_iam_storage import PostgresIamStorage
    from .postgres_policy_storage import PostgresPolicyStorage

    storage = PostgresIamStorage()
    policy_storage = PostgresPolicyStorage()

    await policy_storage._initialize_schema(conn, schema=schema)
    await storage._initialize_schema(conn, schema=schema)

    # Provision default policies for the new tenant
    from .policies import PolicyService

    policy_service = PolicyService(
        None, storage=policy_storage, iam_storage=storage
    )  # app_state not strictly needed for provisioning with explicit conn
    await policy_service.provision_default_policies(
        catalog_id, conn=conn, schema=schema
    )
