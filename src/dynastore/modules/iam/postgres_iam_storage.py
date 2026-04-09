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

# File: dynastore/modules/apikey/postgres_apikey_storage.py

from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from uuid import UUID
import json
import secrets
import hashlib
import random
import logging
import time

from dynastore.modules.db_config.exceptions import ForeignKeyViolationError, TableNotFoundError
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    DbResource,
    managed_transaction,
    managed_nested_transaction,
)
from dynastore.modules.db_config.partition_tools import ensure_partition_exists
from dynastore.modules import get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules.db_config import maintenance_tools
from dynastore.tools.protocol_helpers import get_engine
from .models import (
    Principal,
    ApiKey,
    ApiKeyCreate,
    ApiKeyStatusFilter,
    Role,
    RefreshToken,
    IdentityLink,
    Policy,
)
from .exceptions import PrincipalNotFoundError
from .apikey_storage import AbstractApiKeyStorage
from .interfaces import AuthorizationStorageProtocol

logger = logging.getLogger(__name__)

from .apikey_queries import *


class PostgresApiKeyStorage(AbstractApiKeyStorage, AuthorizationStorageProtocol):
    engine: Optional[DbResource] = None

    _ROLE_HIERARCHY_TTL = 60  # seconds

    def __init__(self, app_state: Optional[object] = None) -> None:
        super().__init__()
        self.engine = get_engine()
        self._known_partitions = set()
        self._role_hierarchy_cache: Dict[tuple, tuple] = {}  # (roles_key, schema) -> (result, timestamp)

    async def initialize(self, conn: DbResource, schema: str = "apikey"):
        """Compatibility alias for _initialize_schema."""
        return await self._initialize_schema(conn, schema=schema)

    async def _initialize_schema(self, conn: DbResource, schema: str = "apikey"):
        """
        Initializes the API Key storage backend for a specific schema.
        Includes V1 (Keys) and V2 (Identity/RBAC) tables.
        """
        # Strip quotes just in case, to prevent double quoting
        schema = schema.strip('"')

        logger.info(
            f"Initializing PostgresApiKeyStorage schemas and tables for '{schema}'..."
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
                CREATE_API_KEYS_TABLE,
                CREATE_USAGE_COUNTERS_TABLE,
                CREATE_QUOTA_COUNTERS_TABLE,
                CREATE_REFRESH_TOKENS_TABLE,
                CREATE_POLICIES_TABLE,
                CREATE_AUDIT_LOG_TABLE,
            ],
        ).execute(conn, schema=schema)

        # Partition tables (IF NOT EXISTS in SQL handles idempotency)
        if schema in ["apikey"]:
            await CREATE_PARTITION_GLOBAL.execute(conn, schema=schema)

        # 2. Maintenance (System Schema Only)
        if schema == "apikey":
            # 3. Proactive Maintenance (Usage Counters)
            await maintenance_tools.ensure_future_partitions(
                conn,
                schema=schema,
                table="usage_counters",
                interval="monthly",
                periods_ahead=2,
            )

            # 4. Register Partition Creation Policy (ongoing, via pg_cron)
            await maintenance_tools.register_partition_creation_policy(
                conn,
                schema=schema,
                table="usage_counters",
                interval="monthly",
                periods_ahead=2,
                schedule_cron="0 2 1 * *",
            )

            # 5. Register Retention Policy (prune old partitions)
            await maintenance_tools.register_retention_policy(
                conn,
                schema=schema,
                table="usage_counters",
                retention_period="24 months",
                schedule_cron="15 3 * * 0",
            )

            # 6. Register nightly pruning of expired rows in flat tables
            _prune_job_name = f"prune_expired_{schema}"
            _prune_func_name = f"prune_expired_rows_{schema}"

            async def _check_prune_job_exists():
                from dynastore.modules.db_config.locking_tools import check_cron_job_exists
                return await check_cron_job_exists(conn, _prune_job_name)

            _prune_ddl = f"""
            CREATE OR REPLACE FUNCTION "{schema}"."{_prune_func_name}"() RETURNS void AS $$
            BEGIN
                -- Expired + deactivated API keys
                DELETE FROM "{schema}".api_keys
                WHERE expires_at IS NOT NULL AND expires_at < NOW() AND is_active = FALSE;

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

        logger.info(f"PostgresApiKeyStorage initialization complete for '{schema}'.")

    async def initialize_partitions(
        self, conn: DbResource, for_date: datetime, schema: str = "apikey"
    ):
        await self._ensure_partition_for_timestamp(conn, for_date, schema=schema)

    async def _ensure_partition_for_timestamp(
        self, conn: DbResource, timestamp: datetime, schema: str = "apikey"
    ):
        """Proactive JIT Partition Creation for Usage Counters (Monthly)."""
        year, month = timestamp.year, timestamp.month
        partition_key = f"{schema}_{year}_{month:02d}"

        if partition_key in self._known_partitions:
            return

        start_of_month = timestamp.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

        await ensure_partition_exists(
            conn,
            table_name="usage_counters",
            schema=schema,
            strategy="RANGE",
            partition_value=start_of_month,
            interval="monthly",
        )
        self._known_partitions.add(partition_key)

    async def list_keys(
        self,
        principal_id: Optional[UUID] = None,
        is_active: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[ApiKey]:
        """Lists API keys with filtering."""
        async with managed_transaction(conn or self.engine) as db:
            rows = await DQLQuery(
                LIST_KEYS_FILTERED, result_handler=ResultHandler.ALL_DICTS
            ).execute(
                db,
                schema=schema,
                principal_id=principal_id,
                is_active=is_active,
                limit=limit,
                offset=offset,
            )
            return [ApiKey(**row) for row in rows]

    async def prune_stale_keys(
        self,
        retention_period: str = "30 days",
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> int:
        async with managed_transaction(conn or self.engine) as db:
            return await DELETE_STALE_KEYS.execute(
                db, schema=schema, retention=retention_period
            )

    # --- Standard CRUD Implementation ---

    async def create_principal(
        self,
        principal: Principal,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
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
        schema: str = "apikey",
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_ID.execute(db, schema=schema, id=principal_id)

    async def update_principal(
        self,
        principal: Principal,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
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
        self, identifier: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_PRINCIPAL_BY_IDENTIFIER.execute(
                db, schema=schema, identifier=identifier
            )

    async def delete_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_PRINCIPAL.execute(db, schema=schema, id=principal_id)
            return count > 0

    async def create_api_key(
        self,
        key_data: ApiKeyCreate,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> tuple[ApiKey, str]:
        raw_key = f"sk_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        key_prefix = raw_key[:6]

        async with managed_transaction(conn or self.engine) as db:
            principal_uuid = key_data.principal_id
            if key_data.principal_identifier:
                principal_uuid = await GET_PRINCIPAL_ID_BY_IDENTIFIER.execute(
                    db, schema=schema, identifier=key_data.principal_identifier
                )
                if not principal_uuid:
                    raise PrincipalNotFoundError(
                        f"Principal '{key_data.principal_identifier}' not found."
                    )

            policy_json = key_data.policy.model_dump_json() if key_data.policy else None
            conditions_json = (
                json.dumps([c.model_dump() for c in key_data.conditions])
                if key_data.conditions
                else None
            )

            try:
                api_key = await INSERT_API_KEY.execute(
                    db,
                    schema=schema,
                    key_hash=key_hash,
                    key_prefix=key_prefix,
                    principal_id=principal_uuid,
                    name=key_data.name,
                    note=key_data.note,
                    expires_at=key_data.expires_at,
                    policy=policy_json,
                    conditions=conditions_json,
                    max_usage=key_data.max_usage,
                    allowed_domains=json.dumps(key_data.allowed_domains)
                    if key_data.allowed_domains
                    else "[]",
                    referer_match=key_data.referer_match,
                    catalog_match=key_data.catalog_match,
                    collection_match=key_data.collection_match,
                )
                return api_key, raw_key
            except ForeignKeyViolationError as e:
                raise PrincipalNotFoundError(
                    f"Principal '{key_data.principal_id}' not found."
                ) from e

    async def delete_api_key(
        self, key_hash: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_KEY.execute(db, schema=schema, key_hash=key_hash)
            return count > 0

    async def get_key_metadata(
        self, key_hash: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[ApiKey]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_KEY_METADATA.execute(db, schema=schema, key_hash=key_hash)

    async def list_keys_for_principal(
        self,
        principal_id: UUID,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[ApiKey]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_KEYS_BY_PRINCIPAL.execute(
                db, schema=schema, principal_id=principal_id
            )

    async def get_principal_id_by_identifier(
        self, identifier: str, conn: Optional[DbResource] = None, schema: str = "apikey"
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
        schema: str = "apikey",
    ) -> List[Principal]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_PRINCIPALS.execute(
                db, schema=schema, offset=offset, limit=limit
            )

    async def increment_usage(
        self,
        key_hash: str,
        period_start: datetime,
        amount: int = 1,
        last_access: Optional[datetime] = None,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ):
        shard_id = random.randint(0, USAGE_SHARD_COUNT - 1)
        if not last_access:
            last_access = datetime.now(timezone.utc)

        async with managed_transaction(conn or self.engine) as db:
            if period_start.year == 1970:
                await INCREMENT_QUOTA.execute(
                    db,
                    schema=schema,
                    key_hash=key_hash,
                    shard_id=shard_id,
                    amount=amount,
                    last_access=last_access,
                )
            else:
                await self._ensure_partition_for_timestamp(
                    db, period_start, schema=schema
                )
                await INCREMENT_USAGE.execute(
                    db,
                    schema=schema,
                    key_hash=key_hash,
                    period_start=period_start,
                    shard_id=shard_id,
                    amount=amount,
                    last_access=last_access,
                )

    async def get_usage(
        self,
        key_hash: str,
        period_start: datetime,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> int:
        async with managed_transaction(conn or self.engine) as db:
            if period_start.year == 1970:
                count = await CHECK_QUOTA.execute(db, schema=schema, key_hash=key_hash)
            else:
                count = await CHECK_USAGE.execute(
                    db, schema=schema, key_hash=key_hash, period_start=period_start
                )
            return int(count) if count else 0

    async def regenerate_api_key(
        self, key_hash: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> tuple[Optional[ApiKey], Optional[str]]:
        async with managed_transaction(conn or self.engine) as db:
            old_key = await GET_KEY_METADATA.execute(
                db, schema=schema, key_hash=key_hash
            )
            if not old_key:
                return None, None

            raw_key = f"sk_{secrets.token_urlsafe(32)}"
            new_hash = hashlib.sha256(raw_key.encode()).hexdigest()
            new_prefix = raw_key[:6]

            await MIGRATE_USAGE.execute(
                db, schema=schema, old_hash=key_hash, new_hash=new_hash
            )
            await MIGRATE_QUOTA.execute(
                db, schema=schema, old_hash=key_hash, new_hash=new_hash
            )

            await DELETE_KEY.execute(db, schema=schema, key_hash=key_hash)

            policy_json = old_key.policy.model_dump_json() if old_key.policy else None

            new_key_obj = await INSERT_API_KEY.execute(
                db,
                schema=schema,
                key_hash=new_hash,
                key_prefix=new_prefix,
                principal_id=old_key.principal_id,
                name=old_key.name,
                note=old_key.note,
                expires_at=old_key.expires_at,
                policy=policy_json,
                max_usage=old_key.max_usage,
                allowed_domains=json.dumps(old_key.allowed_domains),
                referer_match=old_key.referer_match,
                catalog_match=old_key.catalog_match,
                collection_match=old_key.collection_match,
            )

            return new_key_obj, raw_key

    # --- Enhanced Methods ---

    async def search_principals(
        self,
        identifier: Optional[str] = None,
        role: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[Principal]:
        query, params = build_search_principals_query(
            identifier, role, limit, offset, schema=schema
        )
        async with managed_transaction(conn or self.engine) as db:
            return await query.execute(db, **params)

    async def search_keys(
        self,
        principal_id: Optional[UUID] = None,
        status_filter: ApiKeyStatusFilter = ApiKeyStatusFilter.ALL,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[ApiKey]:
        is_active_param = None
        if status_filter == ApiKeyStatusFilter.ACTIVE:
            is_active_param = True

        query, params = build_search_keys_query(
            principal_id, is_active_param, limit, offset, schema=schema
        )

        async with managed_transaction(conn or self.engine) as db:
            keys = await query.execute(db, **params)

            if status_filter == ApiKeyStatusFilter.EXPIRED:
                now = datetime.now(timezone.utc)
                return [k for k in keys if k.expires_at and k.expires_at < now]
            elif status_filter == ApiKeyStatusFilter.ACTIVE:
                now = datetime.now(timezone.utc)
                return [
                    k
                    for k in keys
                    if k.is_active and (k.expires_at is None or k.expires_at > now)
                ]

            return keys

    async def invalidate_api_key(
        self, key_hash: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            result = await INVALIDATE_KEY.execute(db, schema=schema, key_hash=key_hash)
            return result is not None

    # --- RBAC & Refresh Token SPI Implementation ---

    async def create_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Role:
        async with managed_transaction(conn or self.engine) as db:
            result = await INSERT_ROLE.execute(
                db,
                schema=schema,
                id=role.id,
                name=role.name,
                description=role.description,
                level=0,  # Legacy
                metadata=json.dumps(
                    role.dict(exclude={"parent_roles", "policies"})
                ),  # Store remaining as meta
                parent_roles=json.dumps(role.parent_roles),
                policies=json.dumps(role.policies),
            )
            self.invalidate_role_hierarchy_cache(schema)
            return result

    async def get_role(
        self, name: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Role]:
        try:
            async with managed_transaction(conn or self.engine) as db:
                return await GET_ROLE.execute(db, schema=schema, name=name)
        except TableNotFoundError:
            return None

    async def list_roles(
        self, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> List[Role]:
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_ROLES.execute(db, schema=schema)

    async def update_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Role]:
        async with managed_transaction(conn or self.engine) as db:
            result = await UPDATE_ROLE.execute(
                db,
                schema=schema,
                name=role.name,
                description=role.description,
                level=0,
                metadata=json.dumps(role.dict(exclude={"parent_roles", "policies"})),
                parent_roles=json.dumps(role.parent_roles),
                policies=json.dumps(role.policies),
            )
            self.invalidate_role_hierarchy_cache(schema)
            return result

    async def delete_role(
        self,
        name: str,
        cascade: bool = False,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            if cascade:
                # Remove from principals first
                await REMOVE_ROLE_FROM_PRINCIPALS.execute(
                    db, schema=schema, role_name=name
                )

            # The role_hierarchy has ON DELETE CASCADE in DDL, so children entries will be removed automatically.
            count = await DELETE_ROLE.execute(db, schema=schema, name=name)
            self.invalidate_role_hierarchy_cache(schema)
            return count > 0

    async def add_role_hierarchy(
        self,
        parent_role: str,
        child_role: str,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
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
        schema: str = "apikey",
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_ROLE_HIERARCHY.execute(
                db, schema=schema, parent_role=parent_role, child_role=child_role
            )
            self.invalidate_role_hierarchy_cache(schema)
            return count > 0

    def invalidate_role_hierarchy_cache(self, schema: str = "apikey") -> None:
        """Clear role hierarchy cache entries for a schema (call on role CRUD)."""
        self._role_hierarchy_cache = {
            k: v for k, v in self._role_hierarchy_cache.items() if k[1] != schema
        }

    async def get_role_hierarchy(
        self,
        role_names: List[str],
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
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
        schema: str = "apikey",
    ) -> RefreshToken:
        async with managed_transaction(conn or self.engine) as db:
            return await INSERT_REFRESH_TOKEN.execute(
                db,
                schema=schema,
                id=token.id,
                key_hash=token.key_hash,
                principal_id=token.principal_id,
                api_key_hash=token.api_key_hash,
                family_id=token.family_id,
                expires_at=token.expires_at,
            )

    async def get_refresh_token(
        self, token_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[RefreshToken]:
        async with managed_transaction(conn or self.engine) as db:
            return await GET_REFRESH_TOKEN.execute(db, schema=schema, id=token_id)

    async def invalidate_refresh_token(
        self, token_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> bool:
        async with managed_transaction(conn or self.engine) as db:
            result = await INVALIDATE_REFRESH_TOKEN.execute(
                db, schema=schema, id=token_id
            )
            return result is not None

    async def invalidate_token_family(
        self, family_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
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
        schema: str = "apikey",
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
        self, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> dict:
        """Runs storage-specific maintenance (e.g. pruning expired tokens)."""
        logger.info(
            f"Running maintenance for PostgresApiKeyStorage (schema: {schema})..."
        )
        async with managed_transaction(conn or self.engine) as db:
            pruned_tokens = await PRUNE_EXPIRED_REFRESH_TOKENS.execute(
                db, schema=schema
            )
            pruned_keys = await PRUNE_EXPIRED_KEYS.execute(db, schema=schema)

            return {
                "pruned_refresh_tokens": pruned_tokens,
                "pruned_expired_inactive_keys": pruned_keys,
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
        self, policy: Policy, conn: Optional[DbResource] = None, schema: str = "apikey"
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
        self, policy_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Policy]:
        """Retrieves a policy by ID."""
        async with managed_transaction(conn or self.engine) as db:
            return await GET_POLICY.execute(db, schema=schema, id=policy_id)

    async def list_policies(
        self,
        limit: int = 100,
        offset: int = 0,
        conn: Optional[DbResource] = None,
        schema: str = "apikey",
    ) -> List[Policy]:
        """Lists all policies with pagination."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_POLICIES.execute(
                db, schema=schema, limit=limit, offset=offset
            )

    async def update_policy(
        self, policy: Policy, conn: Optional[DbResource] = None, schema: str = "apikey"
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
        self, policy_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
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
        schema: str = "apikey",
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
        schema: str = "apikey",
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
        schema: str = "apikey",
    ) -> bool:
        """Deletes an identity link."""
        async with managed_transaction(conn or self.engine) as db:
            count = await DELETE_IDENTITY_LINK.execute(
                db, schema=schema, provider=provider, subject_id=subject_id
            )
            return count > 0

    # --- Role Management (Enhanced) ---

    async def create_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "apikey"
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
        self, role_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> Optional[Role]:
        """Retrieves a role by ID."""
        try:
            async with managed_transaction(conn or self.engine) as db:
                return await GET_ROLE.execute(db, schema=schema, name=role_id)
        except TableNotFoundError:
            return None

    async def list_roles(
        self, conn: Optional[DbResource] = None, schema: str = "apikey"
    ) -> List[Role]:
        """Lists all roles."""
        async with managed_transaction(conn or self.engine) as db:
            return await LIST_ROLES.execute(db, schema=schema)

    async def update_role(
        self, role: Role, conn: Optional[DbResource] = None, schema: str = "apikey"
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
        self, role_id: str, conn: Optional[DbResource] = None, schema: str = "apikey"
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
                conn=db, schema="apikey", provider=provider, subject_id=subject_id,
            )

    async def get_identity_roles(
        self,
        provider: str,
        subject_id: str,
        schema: str = "apikey",
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
        schema: str = "apikey",
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
            ).execute(db, schema="apikey", roles=json.dumps(list(existing)), id=principal.id)

    async def revoke_role(
        self,
        provider: str,
        subject_id: str,
        role_name: str,
        schema: str = "apikey",
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
            ).execute(db, schema="apikey", roles=json.dumps(updated), id=principal.id)

    async def get_identity_authorization(
        self,
        provider: str,
        subject_id: str,
        schema: str = "apikey",
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
        schema: str = "apikey",
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

    async def get_catalog_users(self, schema: str = "apikey") -> List[Dict[str, Any]]:
        """Get all users (principals with identity links)."""
        async with managed_transaction(self.engine) as db:
            return await DQLQuery(
                """SELECT DISTINCT l.provider, l.subject_id, p.display_name, p.is_active
                   FROM {schema}.identity_links l
                   JOIN {schema}.principals p ON l.principal_id = p.id
                   ORDER BY p.display_name;""",
                result_handler=ResultHandler.ALL_DICTS,
            ).execute(conn=db, schema="apikey")
