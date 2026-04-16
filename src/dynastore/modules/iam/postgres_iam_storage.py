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

# File: dynastore/modules/iam/postgres_iam_storage.py

from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from uuid import UUID
import json
import logging
import time

from dynastore.modules.db_config.exceptions import TableNotFoundError
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    DbResource,
    managed_transaction,
    managed_nested_transaction,
)
from dynastore.modules import get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules.db_config import maintenance_tools
from dynastore.tools.protocol_helpers import get_engine
from .models import (
    Principal,
    Role,
    RefreshToken,
    IdentityLink,
    Policy,
)
from .exceptions import PrincipalNotFoundError
from .iam_storage import AbstractIamStorage
from .interfaces import AuthorizationStorageProtocol

logger = logging.getLogger(__name__)

from .iam_queries import *


class PostgresIamStorage(AbstractIamStorage, AuthorizationStorageProtocol):
    engine: Optional[DbResource] = None

    _ROLE_HIERARCHY_TTL = 60  # seconds

    def __init__(self, app_state: Optional[object] = None) -> None:
        super().__init__()
        self.engine = get_engine()
        self._known_partitions = set()
        self._role_hierarchy_cache: Dict[tuple, tuple] = {}  # (roles_key, schema) -> (result, timestamp)

    async def initialize(self, conn: DbResource, schema: str = "iam"):
        """Compatibility alias for _initialize_schema."""
        return await self._initialize_schema(conn, schema=schema)

    async def _initialize_schema(self, conn: DbResource, schema: str = "iam"):
        """
        Initializes the IAM storage backend for a specific schema.
        Creates principals, identity links, roles, policies, and audit tables.
        Uses DDLBatch sentinel check — on warm start, 1 query confirms all tables exist.
        """
        # Strip quotes just in case, to prevent double quoting
        schema = schema.strip('"')

        logger.info(
            f"Initializing PostgresIamStorage schemas and tables for '{schema}'..."
        )

        # 0. Ensure Schema
        await maintenance_tools.ensure_schema_exists(conn, schema)

        # 1. Base Tables — DDLBatch checks the sentinel (audit_log, the last
        #    table) once; if it exists the entire batch is skipped in 1 query.
        await DDLBatch(
            sentinel=CREATE_AUDIT_LOG_TABLE,
            steps=[
                CREATE_PRINCIPALS_TABLE,
                CREATE_IDENTITY_LINKS_TABLE,
                CREATE_ROLES_TABLE,
                CREATE_ROLE_HIERARCHY_TABLE,
                CREATE_REFRESH_TOKENS_TABLE,
                CREATE_POLICIES_TABLE,
                CREATE_AUDIT_LOG_TABLE,
            ],
        ).execute(conn, schema=schema)

        # Partition tables (IF NOT EXISTS in SQL handles idempotency)
        if schema in ["iam"]:
            await CREATE_PARTITION_GLOBAL.execute(conn, schema=schema)

        # 2. Maintenance (System Schema Only)
        if schema == "iam":
            # Register nightly pruning of expired rows in flat tables
            _prune_job_name = f"prune_expired_{schema}"
            _prune_func_name = f"prune_expired_rows_{schema}"

            async def _check_prune_job_exists():
                from dynastore.modules.db_config.locking_tools import check_cron_job_exists
                return await check_cron_job_exists(conn, _prune_job_name)

            _prune_ddl = f"""
            CREATE OR REPLACE FUNCTION "{schema}"."{_prune_func_name}"() RETURNS void AS $$
            BEGIN
                -- Expired refresh tokens
                DELETE FROM "{schema}".refresh_tokens WHERE expires_at < NOW();

                -- Expired OAuth2 authorisation codes (short-lived, ~10 min)
                DELETE FROM "{schema}".oauth_codes WHERE expires_at < NOW();

                -- Expired OAuth2 access/bearer tokens
                DELETE FROM "{schema}".oauth_tokens WHERE expires_at < NOW();
            END;
            $$ LANGUAGE plpgsql;

            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{_prune_job_name}') THEN
                    PERFORM cron.unschedule('{_prune_job_name}');
                END IF;
            END;
            $$;

            SELECT cron.schedule('{_prune_job_name}', '0 4 * * *',
                $CMD$SELECT "{schema}"."{_prune_func_name}"()$CMD$);
            """

            await DDLQuery(
                _prune_ddl,
                check_query=_check_prune_job_exists,
                lock_key=f"prune_expired_rows_{schema}",
            ).execute(conn)

        logger.info(f"PostgresIamStorage initialization complete for '{schema}'.")

    # --- Standard CRUD Implementation ---

    async def create_principal(
        self,
        principal: Principal,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> Principal:
        async with managed_transaction(conn or self.engine) as db:
            # Map Pydantic model to DB columns. V2 Principal has new fields.
            # We treat 'identifier' as display_name if legacy, or use display_name if present.
            # V2 roles is already a List[str], which maps to JSONB.

            # Logic: If principal has provider AND subject_id, we use "provider:subject_id" as the identifier
            # for backward compatibility with systems that expect a single string identifier.
            # Otherwise we fall back to the explicit identifier attribute or the display_name.
            if principal.provider and principal.subject_id:
                identifier = f"{principal.provider}:{principal.subject_id}"
            else:
                identifier = getattr(principal, "identifier", principal.display_name)

            return await INSERT_PRINCIPAL.execute(
                db,
                schema=schema,
                id=principal.id,
                identifier=identifier,
                display_name=principal.display_name,
                is_active=principal.is_active,
                valid_until=principal.valid_until,
                metadata=json.dumps(getattr(principal, "metadata", {})),  # Compat
                roles=json.dumps(principal.roles),
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
                policy=None,  # Legacy policy field
            )

    async def get_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_ID.execute(db, schema=schema, id=principal_id)

    async def update_principal(
        self,
        principal: Principal,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await UPDATE_PRINCIPAL.execute(
                db,
                schema=schema,
                id=principal.id,
                identifier=getattr(principal, "identifier", principal.display_name),
                display_name=principal.display_name,
                metadata=json.dumps(getattr(principal, "metadata", {})),
                roles=json.dumps(principal.roles),
                policy=None,
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
            )

    async def get_principal_by_identifier(
        self, identifier: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTIFIER.execute(
                db, schema=schema, identifier=identifier
            )

    async def delete_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_PRINCIPAL.execute(db, schema=schema, id=principal_id)
            return count > 0

    async def get_principal_id_by_identifier(
        self, identifier: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[UUID]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_ID_BY_IDENTIFIER.execute(
                db, schema=schema, identifier=identifier
            )

    async def list_principals(
        self,
        offset: int,
        limit: int,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> List[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_PRINCIPALS.execute(
                db, schema=schema, offset=offset, limit=limit
            )

    # --- Enhanced Methods ---

    async def search_principals(
        self,
        identifier: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> List[Principal]:
        query, params = build_search_principals_query(
            identifier, role, limit, offset, schema=schema
        )
        async with managed_transaction(conn or self.engine) as db:
            return await query.execute(db, **params)

    async def add_role_hierarchy(
        self,
        parent_role: str,
        child_role: str,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ):
        async with managed_transaction(conn or self.engine) as db:
            await INSERT_ROLE_HIERARCHY.execute(
                db, schema=schema, parent_role=parent_role, child_role=child_role
            )
            self.invalidate_role_hierarchy_cache(schema)

    async def remove_role_hierarchy(
        self,
        parent_role: str,
        child_role: str,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_ROLE_HIERARCHY.execute(
                db, schema=schema, parent_role=parent_role, child_role=child_role
            )
            self.invalidate_role_hierarchy_cache(schema)
            return count > 0

    def invalidate_role_hierarchy_cache(self, schema: str = "iam") -> None:
        """Clear role hierarchy cache entries for a schema (call on role CRUD)."""
        self._role_hierarchy_cache = {
            k: v for k, v in self._role_hierarchy_cache.items() if k[1] != schema
        }

    async def get_role_hierarchy(
        self,
        role_names: List[str],
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> List[str]:
        if not role_names:
            return []

        cache_key = (tuple(sorted(role_names)), schema)
        cached = self._role_hierarchy_cache.get(cache_key)
        if cached:
            result, ts = cached
            if time.monotonic() - ts < self._ROLE_HIERARCHY_TTL:
                return list(result)

        async with managed_transaction(conn or self.engine) as db:
            children = await GET_FULL_ROLE_HIERARCHY.execute(
                db, schema=schema, role_names=role_names
            )
            merged = list(set(role_names + children))
            self._role_hierarchy_cache[cache_key] = (merged, time.monotonic())
            return merged

    async def create_refresh_token(
        self,
        token: RefreshToken,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> RefreshToken:
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_REFRESH_TOKEN.execute(
                db,
                schema=schema,
                id=token.id,
                key_hash=token.key_hash,
                principal_id=token.principal_id,
                family_id=token.family_id,
                expires_at=token.expires_at,
            )

    async def get_refresh_token(
        self, token_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[RefreshToken]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_REFRESH_TOKEN.execute(db, schema=schema, id=token_id)

    async def invalidate_refresh_token(
        self, token_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            result = await INVALIDATE_REFRESH_TOKEN.execute(
                db, schema=schema, id=token_id
            )
            return result is not None

    async def invalidate_token_family(
        self, family_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> int:
        """Invalidate all active refresh tokens in a family (reuse detection)."""
        async with managed_transaction(conn or self.engine) as db:
            rows = await INVALIDATE_REFRESH_TOKEN_FAMILY.execute(
                db, schema=schema, family_id=family_id
            )
            return len(rows) if rows else 0

    async def log_audit_event(
        self,
        event_type: str,
        principal_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        detail: Optional[Dict[str, Any]] = None,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> None:
        """Write a structured audit log entry."""
        try:
            async with managed_transaction(conn or self.engine) as db:
                await INSERT_AUDIT_EVENT.execute(
                    db,
                    schema=schema,
                    event_type=event_type,
                    principal_id=principal_id,
                    ip_address=ip_address,
                    detail=json.dumps(detail or {}),
                )
        except Exception:
            logger.debug("audit log write failed (table may not exist yet)", exc_info=True)

    async def run_maintenance(
        self, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> dict:
        """Runs storage-specific maintenance (e.g. pruning expired tokens)."""
        logger.info(
            f"Running maintenance for PostgresIamStorage (schema: {schema})..."
        )
        async with managed_transaction(conn or self.engine) as db:
            pruned_tokens = await PRUNE_EXPIRED_REFRESH_TOKENS.execute(
                db, schema=schema
            )

            return {
                "pruned_refresh_tokens": pruned_tokens,
            }

    # --- SPI V2 Implementation ---

    async def get_principal_by_identity(
        self,
        provider: str,
        subject_id: str,
        schema: str,
        conn: Optional[DbResource] = None,
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTITY.execute(
                db, schema=schema, provider=provider, subject_id=subject_id
            )

    async def create_principal_link(
        self,
        principal: Principal,
        identity: Dict[str, Any],
        schema: str,
        conn: Optional[DbResource] = None,
    ) -> Principal:
        async with managed_transaction(conn or self.engine) as db:
            # 1. Insert Principal
            p = await INSERT_PRINCIPAL.execute(
                db,
                schema=schema,
                id=principal.id,
                display_name=principal.display_name,
                is_active=principal.is_active,
                valid_until=principal.valid_until,
                roles=json.dumps(principal.roles),
                custom_policies=json.dumps(
                    [p.model_dump() for p in principal.custom_policies]
                ),
                attributes=json.dumps(principal.attributes),
                identifier=getattr(
                    principal, "identifier", principal.display_name
                ),  # Compat
                metadata=json.dumps(getattr(principal, "metadata", {})),  # Compat
                policy=None,
            )

            # 2. Insert Link
            await INSERT_IDENTITY_LINK.execute(
                db,
                schema=schema,
                provider=identity.get("provider"),
                subject_id=identity.get("sub"),
                principal_id=principal.id,
                email=identity.get("email"),
            )
            return p

    async def get_effective_roles(
        self, principal_id: str, schema: str, conn: Optional[DbResource] = None
    ) -> List[str]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_EFFECTIVE_ROLES.execute(
                db, schema=schema, principal_id=principal_id
            )

    # --- Policy Management ---

    async def create_policy(
        self, policy: Policy, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Policy:
        """Creates a new policy."""
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_POLICY.execute(
                db,
                schema=schema,
                id=policy.id,
                version=policy.version,
                description=policy.description,
                effect=policy.effect,
                actions=json.dumps(policy.actions),
                resources=json.dumps(policy.resources),
                conditions=json.dumps([c.model_dump() for c in policy.conditions]),
                partition_key=policy.partition_key or "global",
            )

    async def get_policy(
        self, policy_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[Policy]:
        """Retrieves a policy by ID."""
        async with managed_transaction(conn or self.engine) as db:
            return await GET_POLICY.execute(db, schema=schema, id=policy_id)

    async def list_policies(
        self,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> List[Policy]:
        """Lists all policies with pagination."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_POLICIES.execute(
                db, schema=schema, limit=limit, offset=offset
            )

    async def update_policy(
        self, policy: Policy, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[Policy]:
        """Updates an existing policy."""
        async with managed_transaction(conn or self.engine) as db:
            return await UPDATE_POLICY.execute(
                db,
                schema=schema,
                id=policy.id,
                version=policy.version,
                description=policy.description,
                effect=policy.effect,
                actions=json.dumps(policy.actions),
                resources=json.dumps(policy.resources),
                conditions=json.dumps([c.model_dump() for c in policy.conditions]),
            )

    async def delete_policy(
        self, policy_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> bool:
        """Deletes a policy."""
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_POLICY.execute(db, schema=schema, id=policy_id)
            return count > 0

    # --- Identity Link Management ---

    async def create_identity_link(
        self,
        principal_id: UUID,
        provider: str,
        subject_id: str,
        email: Optional[str] = None,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> bool:
        """Creates an identity link for a principal."""
        async with managed_transaction(conn or self.engine) as db:
            count = await INSERT_IDENTITY_LINK.execute(
                db,
                schema=schema,
                provider=provider,
                subject_id=subject_id,
                principal_id=principal_id,
                email=email,
            )
            return count > 0

    async def list_identity_links(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> List[IdentityLink]:
        """Lists all identity links for a principal."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_IDENTITY_LINKS.execute(
                db, schema=schema, principal_id=principal_id
            )

    async def delete_identity_link(
        self,
        provider: str,
        subject_id: str,
        conn: Optional[DbResource] = None,
        schema: str = "iam",
    ) -> bool:
        """Deletes an identity link."""
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_IDENTITY_LINK.execute(
                db, schema=schema, provider=provider, subject_id=subject_id
            )
            return count > 0

    # --- Role Management (Enhanced) ---

    async def create_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Role:
        """Creates a new role."""
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_ROLE.execute(
                db,
                schema=schema,
                id=role.id,
                name=role.name,
                description=role.description,
                level=0,  # Legacy support
                metadata=json.dumps(role.metadata),
                parent_roles=json.dumps(role.parent_roles),
                policies=json.dumps(role.policies),
            )

    async def get_role(
        self, role_id: str, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[Role]:
        """Retrieves a role by ID."""
        try:
            async with managed_transaction(conn or self.engine) as db:
                return await GET_ROLE.execute(db, schema=schema, name=role_id)
        except TableNotFoundError:
            return None

    async def list_roles(
        self, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> List[Role]:
        """Lists all roles."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_ROLES.execute(db, schema=schema)

    async def update_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> Optional[Role]:
        """Updates an existing role."""
        async with managed_transaction(conn or self.engine) as db:
            return await UPDATE_ROLE.execute(
                db,
                schema=schema,
                name=role.name,
                description=role.description,
                level=0,
                metadata=json.dumps(role.metadata),
                parent_roles=json.dumps(role.parent_roles),
                policies=json.dumps(role.policies),
            )

    async def delete_role(
        self, role_id: str, cascade: bool = False, conn: Optional[DbResource] = None, schema: str = "iam"
    ) -> bool:
        """Deletes a role."""
        async with managed_transaction(conn or self.engine) as db:
            await DELETE_ROLE.execute(db, schema=schema, name=role_id)
            return True

    # --- Local Authentication Methods ---

    # ------------------------------------------------------------------
    # Principal-backed compatibility methods (replaced IAG tables in v1.0)
    # ------------------------------------------------------------------

    async def _resolve_principal_by_identity(
        self, provider: str, subject_id: str, conn: Optional[DbResource] = None,
    ) -> Optional["Principal"]:
        """Resolve (provider, subject_id) → Principal via identity_links."""
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTITY.execute(
                conn=db, schema="iam", provider=provider, subject_id=subject_id,
            )

    async def get_identity_roles(
        self,
        provider: str,
        subject_id: str,
        schema: str = "iam",
        conn: Optional[DbResource] = None,
    ) -> List[str]:
        """Get roles for an identity. Backed by principals.roles JSONB."""
        principal = await self._resolve_principal_by_identity(provider, subject_id, conn)
        if principal and principal.roles:
            return list(principal.roles)
        return []

    async def grant_roles(
        self,
        provider: str,
        subject_id: str,
        roles: List[str],
        schema: str = "iam",
        granted_by: Optional[str] = None,
        conn: Optional[DbResource] = None,
    ) -> None:
        """Grant roles to an identity by updating principal.roles."""
        principal = await self._resolve_principal_by_identity(provider, subject_id, conn)
        if not principal:
            logger.warning(f"No principal found for {provider}:{subject_id}")
            return
        existing = set(principal.roles or [])
        existing.update(roles)
        async with managed_transaction(conn or self.engine) as db:
            await DQLQuery(
                "UPDATE {schema}.principals SET roles = :roles, updated_at = NOW() WHERE id = :id;",
                result_handler=ResultHandler.ROWCOUNT,
            ).execute(db, schema="iam", roles=json.dumps(list(existing)), id=principal.id)

    async def revoke_role(
        self,
        provider: str,
        subject_id: str,
        role_name: str,
        schema: str = "iam",
        conn: Optional[DbResource] = None,
    ) -> None:
        """Revoke a role from an identity by updating principal.roles."""
        principal = await self._resolve_principal_by_identity(provider, subject_id, conn)
        if not principal:
            return
        updated = [r for r in (principal.roles or []) if r != role_name]
        async with managed_transaction(conn or self.engine) as db:
            await DQLQuery(
                "UPDATE {schema}.principals SET roles = :roles, updated_at = NOW() WHERE id = :id;",
                result_handler=ResultHandler.ROWCOUNT,
            ).execute(db, schema="iam", roles=json.dumps(updated), id=principal.id)

    async def get_identity_authorization(
        self,
        provider: str,
        subject_id: str,
        schema: str = "iam",
        conn: Optional[DbResource] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get authorization metadata for an identity. Backed by principals table."""
        principal = await self._resolve_principal_by_identity(provider, subject_id, conn)
        if not principal:
            return None
        return {
            "provider": provider,
            "subject_id": subject_id,
            "display_name": principal.display_name,
            "is_active": principal.is_active,
            "valid_from": getattr(principal, "valid_from", None),
            "valid_until": principal.valid_until,
            "attributes": principal.attributes or {},
        }

    async def get_identity_policies(
        self,
        provider: str,
        subject_id: str,
        schema: str = "iam",
        conn: Optional[DbResource] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Get custom policies for an identity. Backed by principals.policies JSONB."""
        principal = await self._resolve_principal_by_identity(provider, subject_id, conn)
        if not principal:
            return None
        policies = getattr(principal, "policies", None)
        if not policies:
            return []
        if isinstance(policies, str):
            import json
            return json.loads(policies)
        return policies

    async def get_catalogs_for_identity(
        self, provider: str, subject_id: str
    ) -> List[str]:
        """Get catalog IDs where identity has access (global roles → all catalogs)."""
        principal = await self._resolve_principal_by_identity(provider, subject_id)
        if not principal or not principal.roles:
            return []
        async with managed_transaction(self.engine) as db:
            catalogs = await DQLQuery(
                "SELECT id FROM catalog.catalogs WHERE deleted_at IS NULL ORDER BY id;",
                result_handler=ResultHandler.ALL_SCALARS,
            ).execute(conn=db)
            return catalogs or []

    async def get_catalog_users(self, schema: str = "iam") -> List[Dict[str, Any]]:
        """Get all users (principals with identity links)."""
        async with managed_transaction(self.engine) as db:
            return await DQLQuery(
                """SELECT DISTINCT l.provider, l.subject_id, p.display_name, p.is_active
                   FROM {schema}.identity_links l
                   JOIN {schema}.principals p ON l.principal_id = p.id
                   ORDER BY p.display_name;""",
                result_handler=ResultHandler.ALL_DICTS,
            ).execute(conn=db, schema="iam")
