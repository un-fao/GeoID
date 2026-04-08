#    Copyright 2026 FAO
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

# File: dynastore/modules/apikey/apikey_service.py

import logging
import secrets
import hashlib
import os
import re
import jwt
from contextlib import asynccontextmanager
from typing import List, Optional, Tuple, Any, Dict, Union, AsyncGenerator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from .models import (
    Principal,
    ApiKey,
    ApiKeyCreate,
    ApiKeyStatus,
    ApiKeyValidationRequest,
    ApiKeyStatusFilter,
    TokenResponse,
    Role,
    RefreshToken,
    Policy,
)
from .apikey_storage import AbstractApiKeyStorage
from .postgres_apikey_storage import PostgresApiKeyStorage
from .interfaces import IdentityProviderProtocol, AuthorizationStorageProtocol
from .policies import PolicyService
from .exceptions import (
    PrincipalNotFoundError,
    ApiKeyNotFoundError,
    ApiKeyInvalidError,
    ApiKeyExpiredError,
    RateLimitExceededError,
    QuotaExceededError,
    ApiKeyError,
)

from dynastore.modules.db_config.tools import managed_transaction
from dynastore.modules import get_protocol
from dynastore.models.protocols import (
    ApiKeyProtocol,
    PropertiesProtocol,
    DatabaseProtocol,
    CatalogsProtocol,
)
from dynastore.models.protocols.policies import PermissionProtocol


logger = logging.getLogger(__name__)


class ApiKeyService(ApiKeyProtocol):
    """
    Business Logic Layer for IAG v2.0.
    Handles the Identity -> Context -> Principal resolution and API Key management.
    """

    priority: int = 100

    def __init__(
        self,
        storage: Optional[AbstractApiKeyStorage] = None,
        policy_service: Optional[object] = None,
        app_state: Optional[object] = None,
    ):
        from cachetools import LRUCache
        from dynastore.tools.async_utils import KeyValueAggregator

        # In V2, we prefer PostgresApiKeyStorage which implements both interfaces
        self.storage: Union[AbstractApiKeyStorage, AuthorizationStorageProtocol] = (
            storage or PostgresApiKeyStorage(app_state=app_state)
        )
        self.policy_service = policy_service
        self.app_state = app_state
        self.usage_cache = LRUCache(maxsize=1024)

        self._usage_aggregator = KeyValueAggregator(
            flush_callback=self._flush_usage_increments,
            threshold=int(os.environ.get("USAGE_BUFFER_THRESHOLD", 100)),
            interval=float(os.environ.get("USAGE_BUFFER_INTERVAL", 5.0)),
            name="api_usage",
        )
        self._jwt_secret: Optional[str] = None

    async def get_jwt_secret(self) -> str:
        """Retrieves or generates the active JWT secret."""
        props = get_protocol(PropertiesProtocol)
        if props:
            secret = await props.get_property("apikey_jwt_secret")
            if secret:
                self._jwt_secret = secret
                return secret

        secret = os.environ.get("JWT_SECRET", secrets.token_urlsafe(32))
        if props:
            await props.set_property("apikey_jwt_secret", secret, "apikey_extension")
        else:
            logger.warning("PropertiesProtocol not available. Cannot persist new JWT secret.")

        self._jwt_secret = secret
        return secret

    async def get_jwt_secrets_for_verification(self) -> list:
        """Returns [current, previous] secrets for token verification (rotation support)."""
        result = []
        props = get_protocol(PropertiesProtocol)
        current = await self.get_jwt_secret()
        result.append(current)
        if props:
            prev = await props.get_property("apikey_jwt_secret_prev")
            if prev:
                result.append(prev)
        return result

    async def rotate_jwt_secret(self) -> str:
        """Rotate JWT secret: current becomes previous, new secret is generated."""
        props = get_protocol(PropertiesProtocol)
        if not props:
            raise RuntimeError("PropertiesProtocol required for secret rotation.")
        current = await self.get_jwt_secret()
        new_secret = secrets.token_urlsafe(32)
        await props.set_property("apikey_jwt_secret_prev", current, "apikey_extension")
        await props.set_property("apikey_jwt_secret", new_secret, "apikey_extension")
        self._jwt_secret = new_secret
        logger.info("JWT secret rotated successfully.")
        return new_secret

    async def get_jwks(self) -> Dict[str, Any]:
        """Returns a simplified JWKS (Standard path for token verification)."""
        # For HS256, we don't have public keys.
        # But we could return a placeholder or information about the issuer.
        return {
            "keys": [],
            "info": "DynaStore Uses HS256 for internal tokens. Use /apikey/auth/validate for verification.",
        }

    async def resolve_schema(
        self, catalog_id: Optional[str] = None, conn: Optional[Any] = None
    ) -> str:
        """
        Determines the database schema.
        If catalog_id is provided, resolves the physical tenant schema.
        Otherwise, defaults to the global 'apikey' schema.
        """
        if not catalog_id or catalog_id == "_system_":
            return "apikey"

        catalogs = get_protocol(CatalogsProtocol)
        if not catalogs:
            logger.warning(
                f"CatalogsProtocol not available for schema resolution of '{catalog_id}'. Falling back to 'apikey'."
            )
            return "apikey"

        # We use the CatalogsProtocol to resolve the schema.
        # It handles caching and physical schema lookup.
        try:
            res = await catalogs.resolve_physical_schema(
                catalog_id, db_resource=conn or catalogs.engine
            )
            if res:
                return res
        except Exception as e:
            logger.warning(
                f"Error resolving schema for '{catalog_id}': {e}. Falling back to 'apikey'."
            )

        return "apikey"

    # Internal alias used by extensions and multi_catalog_helpers
    _resolve_schema = resolve_schema

    def get_identity_providers(self) -> List[IdentityProviderProtocol]:
        """Returns all registered identity providers."""
        from dynastore.tools.discovery import get_protocols

        return get_protocols(IdentityProviderProtocol)

    def get_identity_provider(
        self, provider_id: str
    ) -> Optional[IdentityProviderProtocol]:
        """Returns a specific identity provider by ID."""
        for provider in self.get_identity_providers():
            if provider.get_provider_id() == provider_id:
                return provider
        return None

    def get_policy_service(self) -> PermissionProtocol:
        """Returns the policy service for checking permissions."""
        return self.policy_service

    async def get_effective_permissions(
        self, identity: Dict[str, Any], catalog_id: str, conn: Optional[Any] = None
    ) -> Optional[Principal]:
        """
        Get effective permissions for an identity in a catalog.

        Uses simplified IAG v2.1:
        1. Get global roles from catalog.identity_roles
        2. Get catalog roles from {tenant}.identity_roles
        3. Merge and create runtime Principal

        Args:
            identity: Identity dict with provider and sub
            catalog_id: Catalog code to check permissions for
            conn: Optional database connection

        Returns:
            Principal object with merged permissions or None
        """
        provider = identity.get("provider")
        subject_id = identity.get("sub")

        if not provider or not subject_id:
            logger.warning("Invalid identity: missing provider or sub")
            return None

        # 1. Get global roles from catalog schema
        global_roles = await self.storage.get_identity_roles(
            provider=provider, subject_id=subject_id, schema="apikey", conn=conn
        )

        # 2. Get catalog-specific roles
        catalog_schema = await self._resolve_schema(catalog_id, conn)
        catalog_roles = await self.storage.get_identity_roles(
            provider=provider, subject_id=subject_id, schema=catalog_schema, conn=conn
        )

        # 3. Get authorization metadata (optional)
        # Prefer catalog-specific metadata over global
        auth_metadata = await self.storage.get_identity_authorization(
            provider=provider, subject_id=subject_id, schema=catalog_schema, conn=conn
        )

        if not auth_metadata:
            auth_metadata = await self.storage.get_identity_authorization(
                provider=provider, subject_id=subject_id, schema="apikey", conn=conn
            )

        # 4. If no roles and no metadata, user has no access
        if not global_roles and not catalog_roles and not auth_metadata:
            return None

        # 5. Merge roles
        effective_roles = list(set((global_roles or []) + (catalog_roles or [])))

        # 6. Get custom policies (optional)
        global_policies = await self.storage.get_identity_policies(
            provider=provider, subject_id=subject_id, schema="apikey", conn=conn
        )
        catalog_policies = await self.storage.get_identity_policies(
            provider=provider, subject_id=subject_id, schema=catalog_schema, conn=conn
        )

        # 7. Build runtime Principal
        return Principal(
            provider=provider,
            subject_id=subject_id,
            display_name=auth_metadata.get("display_name")
            if auth_metadata
            else subject_id,
            is_active=auth_metadata.get("is_active", True) if auth_metadata else True,
            valid_until=auth_metadata.get("valid_until") if auth_metadata else None,
            roles=effective_roles,
            custom_policies=(global_policies or []) + (catalog_policies or []),
            attributes=auth_metadata.get("attributes", {}) if auth_metadata else {},
        )

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> AsyncGenerator[None, None]:
        """Manages the lifecycle of background tasks and buffers."""
        await self._usage_aggregator.start()
        try:
            yield
        finally:
            await self._usage_aggregator.stop()

    async def flush(self):
        """Manually triggers and awaits a flush of the usage buffer."""
        await self._usage_aggregator._trigger_flush(wait=True)

    # --- Usage & Aggregation ---

    async def increment_usage(
        self,
        key_hash: str,
        period_start: datetime,
        catalog_id: Optional[str] = None,
        amount: int = 1,
        schema: Optional[str] = None,
    ) -> None:
        """
        Increments usage/quota in a buffered/aggregated way.
        """
        target_schema = schema or await self._resolve_schema(catalog_id)
        # We group by (schema, key_hash, period_start)
        aggregation_key = (target_schema, key_hash, period_start)
        await self._usage_aggregator.add_increment(aggregation_key, amount)

    async def _flush_usage_increments(
        self, batches: List[Tuple[Tuple[str, str, datetime], int]]
    ):
        """Callback for the usage aggregator to perform batch writes."""
        for (schema, key_hash, period_start), amount in batches:
            try:
                await self.storage.increment_usage(
                    key_hash=key_hash,
                    period_start=period_start,
                    amount=amount,
                    schema=schema,
                )
            except Exception as e:
                logger.error(
                    f"Failed to flush usage increment for {key_hash[:8]} in {schema}: {e}"
                )

    # --- IAG V2: Core Identity Logic ---

    async def authenticate_and_get_principal(
        self,
        identity: Dict[str, Any],
        target_schema: str,
        auto_register: bool = False,
    ) -> Optional[Principal]:
        """
        Resolves an Identity (from OAuth2) to a Principal with effective permissions.

        Two-tier principal system:
        1. Global principals (catalog schema) - platform-wide permissions
        2. Catalog principals (tenant schema) - catalog-specific permissions
        Permissions are merged hierarchically (global + catalog).

        Args:
            identity: Identity dict with 'provider' and 'sub' keys
            target_schema: Target catalog code
            auto_register: If True, create principal if not found

        Returns:
            Principal with merged permissions, or None if no access
        """
        # Use new permission resolution algorithm
        logger.warning(
            f"DEBUG: authenticate_and_get_principal for {identity.get('sub')} in {target_schema}"
        )
        principal = await self.get_effective_permissions(
            identity=identity, catalog_id=target_schema
        )

        if principal:
            logger.warning(
                f"DEBUG: principal found: {principal.id} with roles: {principal.roles}"
            )
            # Check if principal is active
            if not principal.is_active:
                logger.warning(f"Principal {principal.id} is inactive")
                return None

            # Check expiration
            if principal.valid_until and principal.valid_until < datetime.now(
                timezone.utc
            ):
                logger.warning(f"Principal {principal.id} has expired")
                return None

            return principal

        # Auto-registration (if enabled)
        if auto_register:
            return await self._auto_register_principal(identity, target_schema)

        return None

    async def _auto_register_principal(
        self, identity: Dict[str, Any], catalog_id: str
    ) -> Optional[Principal]:
        """
        Auto-register a new principal with default VIEWER role.

        Args:
            identity: Identity dict with 'provider' and 'sub' keys
            catalog_id: Target catalog code

        Returns:
            Newly created principal
        """
        provider = identity.get("provider")
        subject_id = identity.get("sub")
        email = identity.get("email", f"{subject_id}@{provider}")

        logger.info(
            f"Auto-registering new principal: {provider}:{subject_id} in {catalog_id}"
        )

        # Create principal with default user role
        new_principal = Principal(
            id=uuid4(),
            provider=provider,
            subject_id=subject_id,
            display_name=email,
            roles=["user"],  # Default role for authenticated users
            is_active=True,
            custom_policies=[],
            attributes={},
        )

        # Resolve schema
        schema = await self._resolve_schema(catalog_id)

        # Create principal
        await self.storage.create_principal(new_principal, schema=schema)

        # Create identity link
        await self.storage.create_identity_link(
            principal_id=new_principal.id,
            provider=provider,
            subject_id=subject_id,
            schema=schema,
        )

        logger.info(
            f"Auto-registered principal {new_principal.id} for {provider}:{subject_id}"
        )

        return new_principal

    async def _register_new_principal(
        self, identity: Dict[str, Any], schema: str
    ) -> Principal:
        """Helper to onboard a valid identity into a new tenant context."""
        new_p = Principal(
            id=uuid4(),
            display_name=identity.get("email", "Unknown"),
            roles=["viewer"],  # Default role
            attributes={"source": "auto_registration"},
        )
        return await self.storage.create_principal_link(new_p, identity, schema)

    # --- Principal Management (CRUD) ---

    async def create_principal(
        self, principal: Principal, catalog_id: Optional[str] = None
    ) -> Principal:
        schema = await self._resolve_schema(catalog_id)
        # 1. Create legacy principal (now with better identifier mapping in storage)
        created = await self.storage.create_principal(principal, schema=schema)

        # 2. Sync to identity systems for uniform resolution
        if principal.provider and principal.subject_id:
            # A. Sync to v2 Identity Links (needed by get_principal_by_identity)
            await self.storage.create_identity_link(
                provider=principal.provider,
                subject_id=principal.subject_id,
                principal_id=created.id,
                schema=schema,
            )

            # Grant roles
            if principal.roles:
                await self.storage.grant_roles(
                    provider=principal.provider,
                    subject_id=principal.subject_id,
                    roles=principal.roles,
                    schema=schema,
                )
        return created

    async def get_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> Optional[Principal]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.get_principal(principal_id, schema=schema)

    async def update_principal(
        self, principal: Principal, catalog_id: Optional[str] = None
    ) -> Optional[Principal]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.update_principal(principal, schema=schema)

    async def delete_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.delete_principal(principal_id, schema=schema)

    async def list_principals(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None
    ) -> List[Principal]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_principals(
            limit=limit, offset=offset, schema=schema
        )

    async def search_principals(
        self,
        identifier: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Principal]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.search_principals(
            identifier=identifier, role=role, limit=limit, offset=offset, schema=schema
        )

    # --- API Key Management ---

    async def create_key(
        self, key_data: Any, catalog_id: Optional[str] = None
    ) -> Any:
        """Alias for create_api_key for protocol compliance."""
        return await self.create_api_key(key_data, catalog_id=catalog_id)

    async def create_api_key(
        self, key_data: ApiKeyCreate, catalog_id: Optional[str] = None
    ) -> Tuple[ApiKey, str]:
        schema = await self._resolve_schema(catalog_id)

        if key_data.max_usage is not None:
            if key_data.conditions is None:
                key_data.conditions = []

            # Check if a max_count condition already exists to avoid duplicates
            if not any(c.type == "max_count" for c in key_data.conditions):
                from .models import Condition

                max_count_condition = Condition(
                    type="max_count",
                    config={"max_count": key_data.max_usage, "scope": "apikey"},
                )
                key_data.conditions.append(max_count_condition)

            # We've converted it, so don't store it in the legacy field.
            key_data.max_usage = None

        return await self.storage.create_api_key(key_data, schema=schema)


    async def get_system_admin_key(self) -> str:
        # Check if sysadmin key is stored in DB
        props = get_protocol(PropertiesProtocol)
        if props:
            try:
                stored_key = await props.get_property("sysadmin_key")
                if stored_key:
                    return stored_key
            except Exception:
                pass

        # Fallback to env var
        return os.getenv("DYNASTORE_SYSTEM_ADMIN_KEY", "test_sysadmin_key")

    async def delete_api_key(
        self, key_hash: str, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.delete_api_key(key_hash, schema=schema)

    async def get_key_metadata(
        self, key_hash: str, catalog_id: Optional[str] = None
    ) -> Optional[ApiKey]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.get_key_metadata(key_hash, schema=schema)

    async def list_keys_for_principal(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ) -> List[ApiKey]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_keys_for_principal(principal_id, schema=schema)

    async def search_keys(
        self,
        principal_id: Optional[UUID] = None,
        status_filter: ApiKeyStatusFilter = ApiKeyStatusFilter.ALL,
        limit: int = 100,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[ApiKey]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.search_keys(
            principal_id=principal_id,
            status_filter=status_filter,
            limit=limit,
            offset=offset,
            schema=schema,
        )

    async def invalidate_api_key(
        self, key_hash: str, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.invalidate_api_key(key_hash, schema=schema)

    async def invalidate_key(
        self, key_hash: str, catalog_id: Optional[str] = None
    ) -> bool:
        """Alias for invalidate_api_key for protocol compliance."""
        return await self.invalidate_api_key(key_hash, catalog_id=catalog_id)

    async def regenerate_api_key(
        self, key_hash: str, catalog_id: Optional[str] = None
    ) -> Tuple[Optional[ApiKey], Optional[str]]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.regenerate_api_key(key_hash, schema=schema)

    # --- Authentication & Validation (Legacy + Hybrid) ---

    async def authenticate_apikey(
        self,
        api_key: str,
        catalog_id: Optional[str] = None,
        origin: Optional[str] = None,
    ) -> Tuple[Principal, ApiKey, Optional[str]]:
        """
        Authenticates an API Key and returns (Principal, Metadata, ErrorReason).
        """
        schema = await self._resolve_schema(catalog_id)
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        principal_schema = schema

        # 1. Fetch Key Metadata
        # Try catalog-specific schema first, then fall back to global 'apikey' schema
        metadata = await self.storage.get_key_metadata(key_hash, schema=schema)
        if not metadata:
            if schema != "apikey":
                metadata = await self.storage.get_key_metadata(
                    key_hash, schema="apikey"
                )
                if metadata:
                    principal_schema = (
                        "apikey"  # The principal is in the same schema as the key
                    )
            if not metadata:
                return None, None, "Key not found."

        # 2. Check Expiry/Status
        if not metadata.is_active:
            return None, metadata, "Key is revoked."

        if metadata.expires_at and metadata.expires_at < datetime.now(timezone.utc):
            return None, metadata, "Key expired."

        # 3. Check Scope (Catalog)
        if metadata.catalog_match and catalog_id:
            if not re.match(metadata.catalog_match, catalog_id):
                return (
                    None,
                    metadata,
                    f"Catalog scope violation: requested {catalog_id}, allowed {metadata.catalog_match}",
                )

        # 4. Check Domains
        if metadata.allowed_domains and origin:
            if origin not in metadata.allowed_domains:
                return (
                    None,
                    metadata,
                    f"Domain violation: origin {origin} not in allowed list",
                )

        # 6. Check Conditions
        if metadata.conditions:
            from .conditions import EvaluationContext, evaluate_conditions

            ctx = EvaluationContext(
                request=None,
                storage=self.storage,
                usage_cache=self.usage_cache,
                manager=self,
                api_key_hash=key_hash,
                principal_id=str(metadata.principal_id),
                path=origin or "",  # fallback path
                method="API",  # fallback method
                schema=schema,
                catalog_id=catalog_id,
            )
            try:
                if not await evaluate_conditions(metadata.conditions, ctx):
                    return None, metadata, "Key conditions violation"
            except ApiKeyError as e:
                return None, metadata, str(e)

        # 7. Check Policy
        if metadata.policy:
            # We need a context for policy conditions as well
            from .conditions import EvaluationContext

            ctx = EvaluationContext(
                request=None,
                storage=self.storage,
                usage_cache=self.usage_cache,
                manager=self,
                api_key_hash=key_hash,
                principal_id=str(metadata.principal_id),
                path=origin or "",
                method="API",
                schema=schema,
            )
            is_allowed = await self.policy_service.evaluate_policy_statements(
                metadata.policy, method="API", path=origin or "", request_context=ctx
            )
            if not is_allowed:
                return None, metadata, "Access denied by Key Policy"

        # 8. Fetch Principal
        principal = await self.storage.get_principal(
            metadata.principal_id, schema=principal_schema
        )
        return principal, metadata, None

    async def validate_key(
        self,
        validation_req: ApiKeyValidationRequest,
        catalog_id: Optional[str] = None,
        origin: Optional[str] = None,
    ) -> ApiKeyStatus:
        """
        Validates an API Key string without fetching the principal.
        """
        _, metadata, reason = await self.authenticate_apikey(
            validation_req.api_key, catalog_id=catalog_id, origin=origin
        )
        if not metadata:
            return ApiKeyStatus(is_valid=False, status=reason or "Key not found.")

        if reason:
            return ApiKeyStatus(is_valid=False, status=reason)

        return ApiKeyStatus(
            is_valid=True,
            status="Active",
            expires_at=metadata.expires_at,
            principal_id=metadata.principal_id,
        )

    async def exchange_token(
        self,
        api_key_hash: str,
        ttl_seconds: int = 3600,
        scoped_policy: Optional[Policy] = None,
        catalog_id: Optional[str] = None,
    ) -> TokenResponse:
        """
        Exchanges a valid API Key for a short-lived Bearer Token.
        """
        schema = await self._resolve_schema(catalog_id)

        # 1. Verify Key
        key_metadata = await self.storage.get_key_metadata(api_key_hash, schema=schema)
        if not key_metadata or not key_metadata.is_active:
            raise ApiKeyInvalidError("Invalid or revoked API Key.")

        if key_metadata.expires_at and key_metadata.expires_at < datetime.now(
            timezone.utc
        ):
            raise ApiKeyExpiredError("API Key has expired.")

        # 2. Extract Effective Policy
        # Use provided scoped policy or fallback to key's default policy
        # If scoped_policy is provided, it should be a subset of key's permissions.
        # This check is currently omitted but should be implemented in production.

        # 3. Generate JWT
        principal = await self.get_principal(
            key_metadata.principal_id, catalog_id=catalog_id
        )
        if not principal:
            raise ApiKeyInvalidError("Principal associated with key no longer exists.")

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl_seconds)
        token_id = secrets.token_hex(16)

        payload = {
            "sub": principal.subject_id,
            "prv": principal.provider,
            "pid": str(principal.id),
            "kid": api_key_hash,
            "jti": token_id,
            "scope": "api_access",
            "iat": now,
            "exp": expires_at,
            "token_type": "access",
        }

        if scoped_policy:
            payload["dpol"] = scoped_policy.model_dump()

        secret = await self.get_jwt_secret()
        token = jwt.encode(payload, secret, algorithm="HS256")

        # 4. Generate Refresh Token (with family_id for rotation tracking)
        refresh_token_val = secrets.token_urlsafe(64)
        family_id = secrets.token_urlsafe(32)
        rt_model = RefreshToken(
            id=refresh_token_val,
            key_hash=api_key_hash,
            principal_id=key_metadata.principal_id,
            api_key_hash=api_key_hash,
            family_id=family_id,
            expires_at=now + timedelta(days=7),
        )
        await self.storage.create_refresh_token(rt_model, schema=schema)

        return TokenResponse(
            access_token=token,
            token_type="Bearer",
            expires_in=ttl_seconds,
            expires_at=expires_at,
            principal_id=key_metadata.principal_id,
            refresh_token=refresh_token_val,
        )

    async def refresh_token_exchange(
        self,
        refresh_token: str,
        ttl_seconds: int = 3600,
        catalog_id: Optional[str] = None,
    ) -> TokenResponse:
        """Refreshes an access token using a refresh token with rotation."""
        schema = await self._resolve_schema(catalog_id)

        rt = await self.storage.get_refresh_token(refresh_token, schema=schema)
        if not rt:
            raise ApiKeyInvalidError("Invalid refresh token.")

        if not rt.is_active:
            # Reuse detected — invalidate the entire token family
            if rt.family_id:
                revoked = await self.storage.invalidate_token_family(
                    rt.family_id, schema=schema
                )
                logger.warning(
                    "Refresh token reuse detected (family %s), revoked %d tokens",
                    rt.family_id, revoked,
                )
            raise ApiKeyInvalidError(
                "Refresh token has been revoked (possible token reuse)."
            )

        if rt.expires_at < datetime.now(timezone.utc):
            raise ApiKeyExpiredError("Refresh token has expired.")

        # Invalidate current refresh token
        await self.storage.invalidate_refresh_token(refresh_token, schema=schema)

        # Issue new access token (JWT)
        now = datetime.now(timezone.utc)
        principal = await self.get_principal(rt.principal_id, catalog_id=catalog_id)
        secret = await self.get_jwt_secret()
        access_token = jwt.encode(
            {
                "sub": f"{principal.provider}:{principal.subject_id}" if principal else str(rt.principal_id),
                "pid": str(rt.principal_id),
                "kid": rt.key_hash,
                "iat": int(now.timestamp()),
                "exp": int((now + timedelta(seconds=ttl_seconds)).timestamp()),
                "token_type": "access",
            },
            secret,
            algorithm="HS256",
        )

        # Issue rotated refresh token in the same family
        new_refresh_val = secrets.token_urlsafe(64)
        new_rt = RefreshToken(
            id=new_refresh_val,
            key_hash=rt.key_hash,
            principal_id=rt.principal_id,
            api_key_hash=rt.api_key_hash,
            family_id=rt.family_id or secrets.token_urlsafe(32),
            expires_at=rt.expires_at,  # keep original family expiry
        )
        await self.storage.create_refresh_token(new_rt, schema=schema)

        return TokenResponse(
            access_token=access_token,
            token_type="Bearer",
            expires_in=ttl_seconds,
            expires_at=now + timedelta(seconds=ttl_seconds),
            principal_id=rt.principal_id,
            refresh_token=new_refresh_val,
        )

    async def increment_key_usage(
        self, api_key: str, amount: int = 1, catalog_id: Optional[str] = None
    ):
        """
        Increments the total usage count for an API Key.
        This is a helper for testing and direct usage, it increments the global quota counter.
        Matches the key generation logic of MaxCountHandler (quota:apikey:<hash>).
        """
        schema = await self._resolve_schema(catalog_id)
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        # Align with MaxCountHandler default scope='apikey'
        tracking_key = f"quota:apikey:{key_hash}"
        period_start = datetime(1970, 1, 1, tzinfo=timezone.utc)
        await self.increment_usage(
            tracking_key,
            period_start,
            catalog_id=catalog_id,
            amount=amount,
            schema=schema,
        )

    async def check_rate_limit(
        self, key_hash: str, policy: dict, catalog_id: Optional[str] = None
    ):
        """
        Enforces rate limits defined in policy.
        """
        schema = await self._resolve_schema(catalog_id)
        rate_limit = policy.get("rate_limit")
        if not rate_limit:
            return

        requests = rate_limit.get("requests")
        window = rate_limit.get("window")

        if not requests or not window:
            return

        # Calculate window start
        now = datetime.now(timezone.utc)
        if window == "minute":
            period_start = now.replace(second=0, microsecond=0)
        elif window == "hour":
            period_start = now.replace(minute=0, second=0, microsecond=0)
        elif window == "day":
            period_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return

        current_usage = await self.storage.get_usage(
            key_hash, period_start, schema=schema
        )
        if current_usage >= requests:
            raise RateLimitExceededError(
                f"Rate limit exceeded: {requests} requests per {window}."
            )

        # Async increment
        await self.storage.increment_usage(key_hash, period_start, schema=schema)

    async def get_usage_status(
        self,
        principal: Principal,
        key_hash: Optional[str] = None,
        catalog_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Returns usage and quota information."""
        schema = await self._resolve_schema(catalog_id)
        # Placeholder for complex stats
        return {
            "principal": principal.display_name,
            "total_keys": 1,
            "active_sessions": 0,
        }

    # --- Role & Hierarchy Management ---

    async def create_role(self, role: Role, catalog_id: Optional[str] = None) -> Role:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.create_role(role, schema=schema)

    async def list_roles(self, catalog_id: Optional[str] = None) -> List[Role]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_roles(schema=schema)

    async def update_role(
        self, role: Role, catalog_id: Optional[str] = None
    ) -> Optional[Role]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.update_role(role, schema=schema)

    async def delete_role(
        self, name: str, cascade: bool = False, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.delete_role(name, cascade=cascade, schema=schema)

    async def add_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ):
        schema = await self._resolve_schema(catalog_id)
        await self.storage.add_role_hierarchy(parent_role, child_role, schema=schema)

    async def remove_role_hierarchy(
        self, parent_role: str, child_role: str, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.remove_role_hierarchy(
            parent_role, child_role, schema=schema
        )

    async def get_role_hierarchy(
        self, role_name: str, catalog_id: Optional[str] = None
    ) -> List[str]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.get_role_hierarchy([role_name], schema=schema)

    # --- Policy Management ---

    async def create_policy(self, policy, catalog_id: Optional[str] = None):
        """Creates a new policy."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.create_policy(policy, schema=schema)

    async def get_policy(self, policy_id: str, catalog_id: Optional[str] = None):
        """Retrieves a policy by ID."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.get_policy(policy_id, schema=schema)

    async def list_policies(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None
    ):
        """Lists all policies with pagination."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_policies(
            limit=limit, offset=offset, schema=schema
        )

    async def update_policy(self, policy, catalog_id: Optional[str] = None):
        """Updates an existing policy."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.update_policy(policy, schema=schema)

    async def delete_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> bool:
        """Deletes a policy."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.delete_policy(policy_id, schema=schema)

    # --- Identity Link Management ---

    async def create_identity_link(
        self,
        principal_id: UUID,
        provider: str,
        subject_id: str,
        email: Optional[str] = None,
        catalog_id: Optional[str] = None,
    ) -> bool:
        """Creates an identity link for a principal."""
        schema = await self._resolve_schema(catalog_id)

        # 1. Create link in storage
        success = await self.storage.create_identity_link(
            principal_id, provider, subject_id, email, schema=schema
        )

        if success:
            # Sync roles to identity tables if principal has roles
            principal = await self.storage.get_principal(principal_id, schema=schema)
            if principal and principal.roles:
                await self.storage.grant_roles(
                    provider=provider,
                    subject_id=subject_id,
                    roles=principal.roles,
                    schema=schema,
                )
        return success

    async def list_identity_links(
        self, principal_id: UUID, catalog_id: Optional[str] = None
    ):
        """Lists all identity links for a principal."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.list_identity_links(principal_id, schema=schema)

    async def delete_identity_link(
        self, provider: str, subject_id: str, catalog_id: Optional[str] = None
    ) -> bool:
        """Deletes an identity link."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.delete_identity_link(
            provider, subject_id, schema=schema
        )

    async def run_maintenance(self, catalog_id: Optional[str] = None) -> dict:
        """Runs maintenance (pruning) for the specified catalog/schema."""
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.run_maintenance(schema=schema)

    # --- Middleware Helpers ---

    def extract_token_from_request(self, request: Any) -> Optional[str]:
        """
        Extracts Bearer Token from request headers (OAuth2 compliant).
        """
        auth_header = getattr(request, "headers", {}).get("Authorization")
        if auth_header and auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
            return token

        return None

    async def authenticate_and_get_role(
        self, request: Any
    ) -> Tuple[List[str], Optional[Principal]]:
        """
        Centrally authenticates a request and resolves the effective roles.
        Adapts between Legacy API Keys and V2 Identity Links.
        """
        token = self.extract_token_from_request(request)
        if not token:
            return ["anonymous"], None

        catalog_id = getattr(getattr(request, "state", {}), "catalog_id", None)
        schema = await self._resolve_schema(catalog_id)

        # 1. Check for system key (Machine/Internal bypass)
        system_key = await self.get_system_admin_key()
        if token == system_key:
            # System Admin is a virtual principal with ID 0
            return ["sysadmin"], Principal(
                id=UUID(int=0),
                provider="system",
                subject_id="admin",
                display_name="system:admin",
                roles=["sysadmin"],
            )

        # 2. Try V2 Identity Resolution (OAuth2 / OIDC)
        identity_providers = self.get_identity_providers()
        logger.debug("Checking %d identity providers", len(identity_providers))
        for provider in identity_providers:
            provider_id = provider.get_provider_id()
            try:
                logger.debug("Trying provider %s", provider_id)
                identity = await provider.validate_token(token)
                if identity:
                    logger.debug("Identity found via provider %s", provider_id)
                    # Resolve identity to Principal with contextual roles
                    principal = await self.authenticate_and_get_principal(
                        identity=identity, target_schema=catalog_id or "_system_"
                    )
                    if principal:
                        # Translate V2 roles to flattened hierarchy for legacy compatibility
                        roles = list(principal.roles or [])
                        if not roles:
                            roles = ["user"]  # Default role if none assigned

                        effective_roles = await self.storage.get_role_hierarchy(
                            roles, schema=schema
                        )
                        return list(set(effective_roles)), principal
                    else:
                        logger.debug(
                            "Principal not found for identity via provider %s", provider_id
                        )
            except Exception as e:
                logger.error(
                    f"Provider {provider_id} failed to validate token: {e}",
                    exc_info=True,
                )

        # 3. Authenticate as regular Principal (API Key)
        principal, api_key_meta, reason = await self.authenticate_apikey(
            token, catalog_id=catalog_id
        )

        # Cache metadata on request.state so middleware doesn't re-authenticate
        if hasattr(request, "state"):
            request.state._auth_api_key_metadata = api_key_meta

        if principal:
            # 3. Resolve Effective Roles (Hierarchy)
            roles = list(principal.roles or [])
            if not roles:
                roles = ["user"]  # Default role if none assigned

            effective_roles = await self.storage.get_role_hierarchy(
                roles, schema=schema
            )
            return [r for r in effective_roles if r], principal

        return ["anonymous"], None
