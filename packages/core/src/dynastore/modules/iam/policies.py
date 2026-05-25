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

import re
import enum
import logging
import uuid
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple, Any, Dict, cast
from uuid import UUID
from dynastore.tools.cache import cached
from dynastore.modules.db_config.exceptions import TableNotFoundError
from dynastore.models.protocols.access_filter import (
    AccessClause,
    AccessFilter,
    FieldPredicate,
)
import json

_SAFE_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def _validate_schema_name(schema: str) -> str:
    """Validate a schema name to prevent SQL injection via identifier interpolation."""
    if not _SAFE_SCHEMA_RE.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return schema

from dynastore.models.protocols.authorization import IamRolesConfig

from .models import PolicyBundle, Policy, Condition, Role, Principal  # noqa: F401
from .policy_storage import AbstractPolicyStorage
from .iam_storage import AbstractIamStorage
from .postgres_policy_storage import PostgresPolicyStorage
from dynastore.modules.db_config.query_executor import managed_transaction, DbResource
from dynastore.modules import get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)



class PolicyService:
    _instance = None
    storage: AbstractPolicyStorage
    iam_storage: Optional[AbstractIamStorage]
    _state: object
    _engine: Any

    def __init__(
        self,
        app_state: object,
        storage: Optional[AbstractPolicyStorage] = None,
        iam_storage: Optional[AbstractIamStorage] = None,
        role_config: Optional[IamRolesConfig] = None,
    ):
        self._state = app_state
        db = get_protocol(DatabaseProtocol)
        self._engine = db.engine if db else None
        self.storage = storage or PostgresPolicyStorage(app_state=app_state)
        self.iam_storage = iam_storage
        # Role names + tier split + named slots are sourced from the
        # ``IamRolesConfig`` PluginConfig (``("platform","modules","iam","roles")``).
        # Defaults seed platform-tier [sysadmin] and catalog-tier
        # [admin, editor, user, unauthenticated]. Operators rename, add,
        # or drop roles via PATCH at runtime — no subclassing.
        self._role_config = role_config or IamRolesConfig()

    async def _resolve_schema(
        self, catalog_id: Optional[str], conn: Optional[Any] = None
    ) -> str:
        """Resolve physical schema from catalog_id, with fallback to 'iam' for global."""
        from dynastore.models.protocols import CatalogsProtocol

        catalogs = get_protocol(CatalogsProtocol)

        if not catalog_id or catalog_id == "_system_":
            return "iam"

        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not available.")

        db_resource = conn or cast(Any, self.storage).engine
        # Use allow_missing=True during tenant initialization when catalog may not exist yet
        res = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=db_resource), allow_missing=True
        )
        schema = res if res else "iam"
        return _validate_schema_name(schema)

    async def initialize(
        self, catalog_id: Optional[str] = None, conn: Optional[Any] = None
    ):
        """Initializes storage for the specified catalog/schema."""
        schema = await self._resolve_schema(catalog_id, conn=conn)
        async with managed_transaction(conn or self._engine) as db:
            await cast(Any, self.storage).initialize(conn=db, schema=schema)

    async def check_permission(
        self, principal: Principal, action: str, resource: str
    ) -> bool:
        """
        Evaluates if the given principal can perform 'action' on 'resource'.
        Delegates to the evaluation engine (evaluate_access).

        Note: this method MUST NOT pattern-match on role names (D4 — no
        hardcoded role identifiers). The historical sysadmin
        short-circuit lives in ``IamMiddleware``'s privilege-elevation
        guard (see ``extensions/iam/guards.py``); permission decisions
        here flow exclusively through role → policy → permission.
        """
        # 1. Collect all roles and identity IDs to check
        identities = []
        if principal.subject_id:
            identities.append(principal.subject_id)
        if principal.roles:
            identities.extend(principal.roles)

        # 2. Delegate to the main evaluation logic
        # Note: we might need to extract catalog_id from resource if it's a catalog-scoped check
        catalog_id_match = re.search(r"catalog[:/]([^:/]+)", resource)
        catalog_id = catalog_id_match.group(1) if catalog_id_match else None

        allowed, reason = await self.evaluate_access(
            principals=identities,
            path=resource,
            method=action,
            catalog_id=catalog_id,
            custom_policies=principal.custom_policies or None,
        )
        return allowed

    def get_storage(self) -> AbstractPolicyStorage:
        if not self.storage:
            raise RuntimeError("PolicyService not initialized.")
        return self.storage

    def _derive_partition_key(self, path: str) -> str:
        """
        Derives a partition key from a path.
        """
        # 1. Custom mapping for documentation
        if any(path.startswith(p) for p in ["/docs", "/openapi.json", "/redoc"]):
            return "docs"

        # 2. Heuristic: first segment of the path
        pattern = path.strip(" /").split("/")[0]
        if not pattern or "*" in pattern or "|" in pattern:
            return "default"
        return pattern[:32]

    def invalidate_cache(self):
        """Invalidates the evaluation cache."""
        try:
            # Clear the @cached cache for get_effective_policies
            cast(Any, self.get_effective_policies).cache_clear()
        except AttributeError:
            # In case the decorator is missing or hasn't finished wrapping
            pass
        except Exception as e:
            logger.error(f"Failed to clear policy cache: {e}")

    # --- CRUD ---

    async def create_policy(
        self, policy: Policy, catalog_id: Optional[str] = None
    ) -> Policy:
        schema = await self._resolve_schema(catalog_id)
        if not policy.partition_key:
            policy.partition_key = "global"

        async with managed_transaction(self._engine) as conn:
            # 1. Ensure partition exists before inserting
            from .postgres_policy_storage import PostgresPolicyStorage

            if isinstance(self.storage, PostgresPolicyStorage):
                await self.storage.ensure_policy_partition(
                    conn, policy.partition_key, schema=schema
                )

            # 2. Check for duplicate IDs in the target partition. Filtering
            # by partition is required: the same id can exist across
            # partitions (e.g., default policies seeded into both
            # ``global`` and a catalog partition); a global lookup would
            # spuriously reject a legitimate per-catalog create.
            existing = await self.storage.get_policy(
                policy.id, schema=schema, conn=conn, partition_key=policy.partition_key,
            )
            if existing:
                raise ValueError(f"Policy with ID '{policy.id}' already exists.")

            res = await self.storage.create_policy(policy, schema=schema, conn=conn)
            self.invalidate_cache()
            return res

    async def get_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> Optional[Policy]:
        schema = await self._resolve_schema(catalog_id)
        try:
            return await self.storage.get_policy(
                policy_id, schema=schema, partition_key=catalog_id or "global",
            )
        except TableNotFoundError:
            # A catalog that doesn't use IAM has no per-tenant ``policies``
            # table. Treat the lookup as a miss so the evaluator's platform
            # fallback (``catalog_id=None`` -> ``iam``) still runs, instead of
            # 500-ing every read on that catalog. The platform ``iam`` schema
            # is always provisioned, so a missing table there is a real fault
            # and must surface. Mirrors the grants/roles resilience in
            # ``PostgresIamStorage.resolve_effective_grants``.
            if schema == "iam":
                raise
            logger.warning(
                "policies table missing in tenant schema %r (catalog %r is not "
                "IAM-provisioned); treating policy %r as absent",
                schema, catalog_id, policy_id,
            )
            return None

    async def update_policy(
        self, policy: Policy, catalog_id: Optional[str] = None
    ) -> Optional[Policy]:
        schema = await self._resolve_schema(catalog_id)
        res = await self.storage.update_policy(policy, schema=schema)
        self.invalidate_cache()
        return res

    async def list_policies(
        self, limit: int = 100, offset: int = 0, catalog_id: Optional[str] = None
    ) -> List[Policy]:
        schema = await self._resolve_schema(catalog_id)
        try:
            return await self.storage.list_policies(
                limit=limit, offset=offset, schema=schema
            )
        except TableNotFoundError:
            # See get_policy: a non-IAM catalog has no tenant policies table;
            # it simply has no catalog-scoped policies. The platform ``iam``
            # schema is always provisioned, so a missing table there is real.
            if schema == "iam":
                raise
            logger.warning(
                "policies table missing in tenant schema %r (catalog %r is not "
                "IAM-provisioned); returning no catalog-scoped policies",
                schema, catalog_id,
            )
            return []

    async def delete_policy(
        self, policy_id: str, catalog_id: Optional[str] = None
    ) -> bool:
        schema = await self._resolve_schema(catalog_id)
        res = await self.storage.delete_policy(
            policy_id, schema=schema, partition_key=catalog_id or "global",
        )
        self.invalidate_cache()
        return res

    async def search_policies(
        self,
        resource_pattern: str,
        action_pattern: str,
        limit: int = 10,
        offset: int = 0,
        catalog_id: Optional[str] = None,
    ) -> List[Policy]:
        schema = await self._resolve_schema(catalog_id)
        return await self.storage.search_policies(
            resource_pattern, action_pattern, limit, offset, schema=schema
        )

    # --- Provisioning & Defaults ---

    def _get_default_policies(self, partition_key: str = "global") -> List[Policy]:
        """Returns the core default policies for the platform."""
        return [
            Policy(
                id="sysadmin_full_access",
                description="Unrestricted access for system administrators.",
                actions=["*"],
                resources=[".*"],
                effect="ALLOW",
                partition_key=partition_key,
            ),
            Policy(
                id="public_access",
                description="Allows anonymous access to public endpoints. "
                            "Resources are enumerated explicitly (no /web/.* "
                            "catch-all) so dashboard data endpoints stay gated "
                            "by web_dashboard_*_access policies declared in "
                            "extensions/web/web.py via the PolicyContributor "
                            "Protocol (Web.get_policies / get_role_bindings).",
                actions=["GET", "POST", "OPTIONS", "HEAD"],
                resources=[
                    "/$",
                    "/health",
                    "/docs.*",
                    "/openapi.json",
                    "/redoc",
                    # OGC API & STAC API metadata — anonymous-visible by
                    # spec convention. Without /conformance the home-page
                    # live OGC matrix can never probe real platform state
                    # and always falls back to the static snapshot. Only
                    # the metadata surface (landing root + /conformance)
                    # is opened here; data endpoints (/collections,
                    # /items, item search) keep their existing gating.
                    "/conformance/?$",
                    "/stac/?$",
                    "/stac/conformance/?$",
                    "/features/?$",
                    "/features/conformance/?$",
                    "/processes/?$",
                    "/processes/conformance/?$",
                    "/records/?$",
                    "/records/conformance/?$",
                    "/coverages/?$",
                    "/coverages/conformance/?$",
                    "/dggs/?$",
                    "/dggs/conformance/?$",
                    "/consys/?$",
                    "/consys/conformance/?$",
                    "/movingfeatures/?$",
                    "/movingfeatures/conformance/?$",
                    # /web/* — explicit safe sub-paths only; mirrors the
                    # web_public_access policy in extensions/web/web.py
                    "/web/?$",
                    "/web/pages/.*",
                    "/web/extension-static/.*",
                    "/web/geoid/.*",  # GeoID-branded static assets — CSS, JS, snapshot, logo
                    "/web/static/.*",
                    "/web/website/.*",
                    "/web/docs-content/.*",
                    "/web/docs-manifest",
                    "/web/config/.*",
                    "/web/health",
                    "/web/dashboard/?$",
                    "/web/lite/.*",  # JupyterLite WASM kernel — anonymous-OK
                    "/iam/auth/login",
                    "/iam/auth/validate",
                    "/iam/auth/jwks.json",
                    "/auth/.*",
                ],
                effect="ALLOW",
                partition_key=partition_key,
            ),
            Policy(
                id="self_service_access",
                description="Allows authenticated users to access their own /me endpoints.",
                actions=["GET"],
                # `/iam/me` (no trailing path) must be listed explicitly: the
                # matcher uses re.match (start-anchored), so `/iam/me/.*`
                # alone does not cover the bare path. `/auth/userinfo` is
                # the OIDC-spec endpoint and remains separately allowable.
                resources=["/iam/me", "/iam/me/.*", "/auth/userinfo"],
                effect="ALLOW",
                partition_key=partition_key,
            ),
        ]

    def _get_default_roles(self, catalog_id: Optional[str] = None) -> List[Role]:
        """Return the role seed list for the requested tier.

        ``catalog_id is None`` returns platform-tier seeds (seeded once
        in the global ``iam`` schema); any catalog id returns
        catalog-tier seeds (seeded per-catalog in each tenant schema).

        Operators add, rename, or drop a role by editing
        ``IamRolesConfig.platform_roles`` / ``catalog_roles`` via
        ``PATCH /api/catalog/v2/configs`` — no code change.
        """
        seeds = (
            self._role_config.platform_roles
            if catalog_id is None
            else self._role_config.catalog_roles
        )
        return [
            Role(
                name=seed.name,
                description=seed.description,
                policies=list(seed.policies),
            )
            for seed in seeds
        ]

    def _get_default_role_hierarchy(
        self, catalog_id: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        """Return ``(parent, child)`` edges derived from ``RoleSeed.parent``
        for the requested tier.

        Parent inherits child's policies (see ``get_role_hierarchy`` in
        ``postgres_iam_storage.py``).
        """
        seeds = (
            self._role_config.platform_roles
            if catalog_id is None
            else self._role_config.catalog_roles
        )
        return [(s.parent, s.name) for s in seeds if s.parent]

    async def provision_default_policies(
        self,
        catalog_id: Optional[str] = None,
        conn: Optional[Any] = None,
        schema: Optional[str] = None,
        force: bool = False,
    ):
        """Provisions core default policies and roles.

        Args:
            catalog_id: Optional catalog for tenant-scoped provisioning.
            conn: Optional existing DB connection.
            schema: Optional explicit schema name.
            force: If True, upsert all defaults (reset). If False, only create missing ones.
        """
        if not schema:
            schema = await self._resolve_schema(catalog_id, conn=conn)
        pk = catalog_id or "global"

        async with managed_transaction(conn or self._engine) as db:
            # Ensure partition exists if catalog-scoped
            if catalog_id:
                await self.storage.ensure_policy_partition(
                    db, partition_key=catalog_id, schema=schema
                )

            # Provision default policies. ``public_access`` is force-updated
            # on every provision so deployments inheriting an older overly-broad
            # version (pre-2026-04-29 ``/web/.*`` catch-all that accidentally
            # allowed anonymous access to gated dashboard endpoints) self-heal
            # to the narrowed enumeration without operator intervention. Other
            # defaults stay idempotent so operator edits via the policy API
            # are preserved.
            _ALWAYS_REFRESH = {"public_access"}
            for policy_def in self._get_default_policies(partition_key=pk):
                if force or policy_def.id in _ALWAYS_REFRESH:
                    await self.storage.update_policy(policy_def, schema=schema, conn=db)
                else:
                    existing = await self.storage.get_policy(
                        policy_def.id, schema=schema, conn=db, partition_key=pk,
                    )
                    if not existing:
                        await self.storage.update_policy(policy_def, schema=schema, conn=db)

            # Provision default roles for the current tier.
            # ``catalog_id is None`` → platform tier (global ``iam`` schema);
            # any catalog_id → catalog tier (per-tenant schema).
            if self.iam_storage:
                for role_def in self._get_default_roles(catalog_id):
                    existing = await self.iam_storage.get_role(
                        role_def.name, schema=schema, conn=db
                    )
                    if force or not existing:
                        if existing:
                            await self.iam_storage.update_role(
                                role_def, schema=schema, conn=db
                            )
                        else:
                            await self.iam_storage.create_role(
                                role_def, schema=schema, conn=db
                            )

                # Seed role hierarchy edges derived from RoleSeed.parent
                # for the current tier. Catalog-tier default chain:
                # admin > editor > user > unauthenticated. Direction:
                # parent inherits child's policies (see `get_role_hierarchy`
                # in postgres_iam_storage.py — it returns `role_names + children`).
                # With this seed an authenticated ``user`` automatically
                # inherits every ``*_public_access`` policy bound to
                # ``unauthenticated``, so registering with Keycloak does
                # NOT silently narrow access vs. browsing unauthenticated.
                #
                # Idempotent: ON CONFLICT DO NOTHING in `add_role_hierarchy`,
                # so reprovision on every cold start is safe and self-heals
                # databases that pre-date this seed.
                for parent, child in self._get_default_role_hierarchy(catalog_id):
                    try:
                        await self.iam_storage.add_role_hierarchy(
                            parent_role=parent, child_role=child,
                            schema=schema, conn=db,
                        )
                    except Exception as e:
                        # add_role_hierarchy is upsert-by-PK in the default
                        # storage impl (`INSERT ... ON CONFLICT DO NOTHING`);
                        # the swallow exists for deployments that override
                        # the storage with a stricter, non-idempotent
                        # implementation. Log at WARNING so the swallow is
                        # observable — silent failures here put the platform
                        # into a "no inheritance" state where authenticated
                        # users see narrower access than anonymous browsers
                        # (the catalog-tier default seed wires admin → editor
                        # → user → unauthenticated, so users *inherit* every
                        # public policy bound to unauthenticated; without
                        # the chain, registering for an account silently
                        # *removes* access).
                        logger.warning(
                            "provision_default_policies: failed to seed role "
                            "hierarchy edge %r → %r in schema %r: %s",
                            parent, child, schema, e,
                        )

        # Platform-tier only: bootstrap OidcRoleSyncConfig so the
        # Keycloak realm role (e.g. ``geoid.sysadmin``) reconciles to
        # the internal grant on first login. Default Pydantic value of
        # ``reconcile_enabled`` is False (cautious), but a fresh deploy
        # has no row in ``iam.configs`` at all — without this seed the
        # sysadmin JWT user is evaluated with anonymous-tier
        # permissions and every protected endpoint returns 403.
        # Idempotent: skip when a row already exists so operator PATCHes
        # are preserved across cold boots.
        if catalog_id is None:
            await self._seed_oidc_role_sync_default()

    async def _seed_oidc_role_sync_default(self) -> None:
        """One-shot seed of ``OidcRoleSyncConfig(reconcile_enabled=True)``
        into platform configs when no row exists.

        Closes the #907 / #908 bootstrap gap: ``provision_default_policies``
        runs at every cold boot; this hook ensures the OIDC role
        reconciler is on by default but never overwrites a persisted
        operator PATCH.
        """
        from dynastore.models.protocols.platform_configs import (
            PlatformConfigsProtocol,
        )
        from .oidc_role_sync_config import OidcRoleSyncConfig

        configs = get_protocol(PlatformConfigsProtocol)
        if configs is None:
            logger.debug(
                "OidcRoleSyncConfig seed skipped: PlatformConfigsProtocol "
                "not registered (slim deployment)."
            )
            return

        try:
            persisted = await configs.list_configs()
        except Exception:
            # Cold-boot race (#1209): this seed runs from the IamModule
            # lifespan, which can start before DatastoreModule's
            # ``ensure_init_db`` creates ``configs.platform_configs`` — so the
            # first ``list_configs`` raises "relation does not exist". The seed
            # used to skip one-shot and never retry, leaving
            # ``reconcile_enabled`` defaulted ``False``: the sysadmin JWT then
            # resolves to user-tier and every protected route 403s until an
            # unrelated redeploy. Module/pod startup ordering is not guaranteed,
            # so rather than depend on it, self-heal — ensure the platform
            # configs storage (idempotent ``CREATE ... IF NOT EXISTS`` DDL) and
            # retry the read once.
            try:
                from dynastore.modules.db_config.platform_config_service import (
                    PlatformConfigService,
                )

                await PlatformConfigService.initialize_storage(self._engine)
                persisted = await configs.list_configs()
            except Exception:
                logger.warning(
                    "OidcRoleSyncConfig seed skipped: platform configs storage "
                    "unavailable after ensure-and-retry; OIDC role "
                    "reconciliation stays disabled until the next cold boot or "
                    "operator PATCH.",
                    exc_info=True,
                )
                return

        existing = cast(Optional[OidcRoleSyncConfig], persisted.get(OidcRoleSyncConfig))
        if existing is None:
            seed = OidcRoleSyncConfig(reconcile_enabled=True)
            try:
                await configs.set_config(OidcRoleSyncConfig, seed)
                logger.info(
                    "Seeded OidcRoleSyncConfig(reconcile_enabled=True) — "
                    "OIDC realm roles will now reconcile to internal grants "
                    "(operator may PATCH /configs/platform/.../oidc_role_sync "
                    "to disable)."
                )
                effective = seed
            except Exception:
                logger.warning(
                    "OidcRoleSyncConfig seed failed; OIDC role reconciliation "
                    "remains disabled until operator PATCH.",
                    exc_info=True,
                )
                return
        else:
            effective = existing

        if effective.reconcile_enabled and not effective.issuer_whitelist:
            logger.warning(
                "OidcRoleSyncConfig.reconcile_enabled=True with empty "
                "issuer_whitelist: any OIDC issuer accepted by the resource "
                "server can grant mapped platform roles. Set "
                "issuer_whitelist=[\"<your-keycloak-issuer-url>\"] to narrow."
            )

    # --- Evaluation ---

    @cached(maxsize=512, namespace="policies")
    async def get_effective_policies(
        self, partition_key: str, schema: str
    ) -> List[Policy]:
        """Caches policy sets per partition/schema."""
        return await self.storage.list_policies(
            partition_key=partition_key, limit=1000, schema=schema
        )

    async def evaluate_policy_statements(
        self, policy: PolicyBundle, method: str, path: str, request_context: Any = None
    ) -> bool:
        """
        Evaluates the statements in an PolicyBundle (embedded in Key or Principal).
        Iterates through statements: if any DENY matches, return False.
        If any ALLOW matches, return True.
        If none match, return False (Implicit Deny).
        """
        if not policy.statements:
            return True

        has_allow_match = False
        for s in policy.statements:
            # Check method and path
            method_match = not s.actions or ".*" in s.actions or method in s.actions or s.matches_action(method)
            path_match = s.matches_resource(path)

            if method_match and path_match:
                # Check conditions
                conditions_met = True
                if s.conditions:
                    for cond in s.conditions:
                        if not await self._evaluate_condition(cond, request_context):
                            conditions_met = False
                            break

                if conditions_met:
                    if s.effect == "DENY":
                        return False  # Explicit Deny wins
                    if s.effect == "ALLOW":
                        has_allow_match = True

        return has_allow_match

    async def _resolve_effective_policies(
        self,
        principals: List[str],
        schema: str,
        catalog_id: Optional[str] = None,
        custom_policies: Optional[List[Policy]] = None,
        principal_id: Optional[UUID] = None,
        collection_id: Optional[str] = None,
        request_context: Any = None,
    ) -> List[Policy]:
        """Resolve the full policy set for ``principals`` in one read scope.

        Single source of truth for which policies apply to a principal, so
        ``evaluate_access`` (single-resource decision) and
        ``compile_read_filter`` (document-level read scope) can never drift
        on policy resolution.

        Resolution order, identical to the historical inline block:
          1. Roles named by ``principals`` are looked up in the global
             ``iam`` schema and (when distinct) the catalog ``schema``;
             their policy ids are unioned.
          2. Each id is fetched from the catalog schema, falling back to
             the global schema.
          3. ``custom_policies`` attached to the principal are appended.

        ``schema`` is the value already resolved by
        :meth:`_resolve_schema` for ``catalog_id`` — passed in so callers
        that need the schema for other purposes resolve it once.
        """
        # 1. Fetch all roles matching any of the principals to resolve policy IDs
        # Always check the global "iam" schema first for roles
        all_policy_ids = set()
        # Role names already resolved via the flat-name path. Used by the
        # grant-based step below so a role granted *and* present as a flat
        # principal name is not double-counted.
        resolved_roles: set[str] = set()
        schemas_to_check = ["iam"]  # Always check global schema
        if schema != "iam":
            schemas_to_check.append(
                schema
            )  # Also check catalog-specific schema if different

        for check_schema in schemas_to_check:
            for principal in principals:
                if not principal:
                    continue
                role_obj = None
                # Lookup role permissions in storage
                if self.iam_storage:
                    role_obj = await self.iam_storage.get_role(
                        principal, schema=check_schema
                    )
                    if role_obj:
                        logger.debug(
                            f"EVAL: Found role '{principal}' schema={check_schema} policies={role_obj.policies}"
                        )
                        all_policy_ids.update(role_obj.policies)
                        resolved_roles.add(principal)
                    else:
                        logger.debug(
                            f"EVAL: Role '{principal}' not found in schema '{check_schema}'."
                        )

        # 2. Fetch all unique policies from both global and catalog-specific schemas
        effective_policies: List[Policy] = []
        for pid in all_policy_ids:
            # Try to get policy from catalog schema first, then fall back to global
            pol = await self.get_policy(pid, catalog_id=catalog_id)
            if not pol and catalog_id:
                # Fall back to global schema
                pol = await self.get_policy(pid, catalog_id=None)
            if pol:
                effective_policies.append(pol)

        # 3. Include custom policies directly attached to the principal
        if custom_policies:
            effective_policies.extend(custom_policies)

        # 4. Resource-scoped (and whole-catalog) grants from the unified
        # grants table. The flat-name path above only resolves roles a
        # caller passed in by name; a principal whose authority comes from a
        # *grant row* (e.g. a collection-scoped role binding) is invisible
        # to that path. Resolve those grant rows → role names → policies and
        # APPEND. This only ADDS policies, never removes — deny-precedence in
        # ``evaluate_access`` / ``compile_read_filter`` still wins. Fully
        # fail-closed: on any error we log a WARNING and evaluate on the
        # pre-step set (no widening).
        # ``resolve_effective_grants`` is a concrete PostgresIamStorage method,
        # not on the AbstractIamStorage interface — fetch via getattr so the
        # resolution is optional for storages that don't implement it.
        resolve_grants = getattr(self.iam_storage, "resolve_effective_grants", None)
        if (
            principal_id is not None
            and self.iam_storage is not None
            and resolve_grants is not None
        ):
            try:
                grant_rows = await resolve_grants(
                    principal_id=principal_id,
                    catalog_schema=schema,
                    collection_id=collection_id,
                )
                granted_role_names = {
                    row["object_ref"]
                    for row in (grant_rows or [])
                    if row.get("object_kind") == "role"
                    and row.get("object_ref")
                    and row["object_ref"] not in resolved_roles
                }
                for role_name in granted_role_names:
                    resolved_roles.add(role_name)
                    grant_policy_ids: set = set()
                    for check_schema in schemas_to_check:
                        role_obj = await self.iam_storage.get_role(
                            role_name, schema=check_schema
                        )
                        if role_obj:
                            grant_policy_ids.update(role_obj.policies)
                    for pid in grant_policy_ids:
                        pol = await self.get_policy(pid, catalog_id=catalog_id)
                        if not pol and catalog_id:
                            pol = await self.get_policy(pid, catalog_id=None)
                        if pol:
                            effective_policies.append(pol)
                # Direct policy grants (object_kind='policy'): a policy bound
                # straight to the principal, optionally collection-scoped.
                # Attach it; its own ALLOW/DENY effect governs via the
                # ranking in the caller. Dedup against policies already
                # collected so a policy reachable both via a role and
                # directly is not double-listed.
                seen_ids = {p.id for p in effective_policies}
                granted_policy_ids = {
                    row["object_ref"]
                    for row in (grant_rows or [])
                    if row.get("object_kind") == "policy" and row.get("object_ref")
                }
                for pid in granted_policy_ids:
                    if pid in seen_ids:
                        continue
                    pol = await self.get_policy(pid, catalog_id=catalog_id)
                    if not pol and catalog_id:
                        pol = await self.get_policy(pid, catalog_id=None)
                    if pol:
                        effective_policies.append(pol)
                        seen_ids.add(pol.id)
                # Per-binding quota / rate-limit (#1344). Every in-scope
                # ALLOW grant may carry a ``quota`` JSONB; turn it (or the
                # configured ``IamScaleConfig`` default) into rate_limit /
                # max_count conditions and stash them on the request context
                # so the middleware enforces them in its condition step
                # (with the right 429 / Retry-After / X-RateLimit headers).
                # The counter namespace is the grant id, so two grants that
                # differ only by ``resource_ref`` never share a bucket.
                # Skipped when there is no request_context (compile_read_filter
                # — search read-filtering does not enforce quota).
                extras = getattr(request_context, "extras", None)
                if isinstance(extras, dict) and grant_rows:
                    await self._collect_grant_quota_conditions(grant_rows, extras)
            except Exception:
                logger.warning(
                    "EVAL: resource-scoped grant resolution failed for "
                    "principal_id=%s collection_id=%s; evaluating on the "
                    "pre-grant policy set (fail-closed).",
                    principal_id,
                    collection_id,
                    exc_info=True,
                )

        return effective_policies

    async def _collect_grant_quota_conditions(
        self, grant_rows: List[Dict[str, Any]], extras: Dict[str, Any]
    ) -> None:
        """Synthesize per-binding quota conditions from resolved grant rows.

        Appends ``rate_limit`` / ``max_count`` :class:`Condition` objects to
        ``extras['_grant_quota_conditions']`` (consumed by the middleware
        condition step) and registers their counter namespace in
        ``extras['_policy_id_by_config_id']``. DENY grants impose no quota.
        Fully defensive: a malformed ``quota`` on one grant is skipped, not
        fatal — the surrounding step-4 ``try`` keeps the whole resolution
        fail-closed.
        """
        from .scale_config import (
            get_iam_scale_config,
            quota_namespace,
            quota_to_conditions,
        )

        has_quota = any(r.get("quota") for r in grant_rows)
        scale = await get_iam_scale_config()
        default_rl = scale.default_rate_limit
        default_q = scale.default_quota
        if not has_quota and default_rl is None and default_q is None:
            return

        sink: List[Condition] = extras.setdefault("_grant_quota_conditions", [])
        ns_map: Dict[int, str] = extras.setdefault("_policy_id_by_config_id", {})
        for row in grant_rows:
            if str(row.get("effect") or "allow").lower() == "deny":
                continue
            raw = row.get("quota")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (ValueError, TypeError):
                    raw = None
            quota = raw if isinstance(raw, dict) else None
            conds, mapping = quota_to_conditions(
                quota,
                quota_namespace(row.get("id")),
                default_rate_limit=default_rl,
                default_quota=default_q,
            )
            if conds:
                sink.extend(conds)
                ns_map.update(mapping)

    async def evaluate_access(
        self,
        principals: List[str],
        path: str,
        method: str,
        request_context: Any = None,
        catalog_id: Optional[str] = None,
        custom_policies: Optional[List[Policy]] = None,
        principal_id: Optional[UUID] = None,
        collection_id: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """
        The central Zero-Trust evaluation engine.
        Returns (is_allowed, reason).

        ``principal_id``/``collection_id`` (optional) enable resolution of
        resource-scoped (and whole-catalog) grants from the unified grants
        table for this principal. When ``principal_id`` is ``None`` the
        behaviour is unchanged from before grant-scope enforcement.
        """
        schema = await self._resolve_schema(catalog_id)
        logger.debug(
            f"EVAL: Evaluating access for {principals} on {method} {path} (schema: {schema})"
        )

        effective_policies = await self._resolve_effective_policies(
            principals=principals,
            schema=schema,
            catalog_id=catalog_id,
            custom_policies=custom_policies,
            principal_id=principal_id,
            collection_id=collection_id,
            request_context=request_context,
        )

        logger.debug(
            f"EVAL: Total effective policies to check: {len(effective_policies)}"
        )

        # 4. Evaluate — priority ranking with DENY-on-tie (#915).
        #
        # Highest ``priority`` wins regardless of effect. When the
        # strongest DENY and strongest ALLOW have equal priority, DENY
        # wins — preserves the deny-precedence invariant from #731/#866
        # for unprioritised policies (default ``priority=0`` means
        # legacy seeds behave exactly as before).
        #
        # Within the same effect, ties are broken by ``id`` ASC so
        # audit attribution is deterministic across pod restarts, DB
        # reseeds, and storage backends. ``created_at`` is not used —
        # it depends on row-creation order which is not a stable input.
        #
        # ``evaluate_policy_statements`` (inline statements) still
        # hardcodes DENY-wins because ``Statement`` has no ``priority``
        # field per the #915 scope decision.
        def _rank_key(pol: Policy) -> tuple:
            # Higher priority sorts first; lower id breaks ties.
            # Negative priority gives ``min`` semantics over the
            # strongest policy.
            return (-pol.priority, pol.id)

        best_deny: Optional[Policy] = None
        best_allow: Optional[Policy] = None
        for p in effective_policies:
            method_match = (
                not p.actions
                or ".*" in p.actions
                or method in p.actions
                or p.matches_action(method)
            )
            path_match = p.matches_resource(path)

            logger.debug(
                f"EVAL: Checking policy '{p.id}' (priority={p.priority}): "
                f"method_match={method_match}, path_match={path_match}"
            )

            if not (method_match and path_match):
                continue

            conditions_met = True
            if p.conditions:
                for cond in p.conditions:
                    if not await self._evaluate_condition(cond, request_context):
                        conditions_met = False
                        break

            if not conditions_met:
                continue

            if p.effect == "DENY":
                if best_deny is None or _rank_key(p) < _rank_key(best_deny):
                    best_deny = p
            elif p.effect == "ALLOW":
                if best_allow is None or _rank_key(p) < _rank_key(best_allow):
                    best_allow = p

        # Apply ranking: highest priority wins; equal priority → DENY.
        winner: Optional[Policy] = None
        loser: Optional[Policy] = None
        if best_deny is not None and best_allow is not None:
            if best_deny.priority >= best_allow.priority:
                winner, loser = best_deny, best_allow
            else:
                winner, loser = best_allow, best_deny
        elif best_deny is not None:
            winner = best_deny
        elif best_allow is not None:
            winner = best_allow

        if winner is None:
            logger.warning(
                f"EVAL: DENIED (No matching ALLOW policy found) for {principals} on {method} {path}"
            )
            return False, "Deny by Default (No matching ALLOW policy found)"

        if winner.effect == "DENY":
            if loser is not None:
                # "deny-precedence" wording is preserved for the
                # equal-priority case (the #866 invariant); when DENY
                # wins by a higher score the log line still shows both
                # priorities so the override is debuggable.
                tag = "deny-precedence " if winner.priority == loser.priority else ""
                logger.info(
                    f"EVAL: DENIED by {winner.id} (priority={winner.priority}, "
                    f"{tag}overrode ALLOW from {loser.id} priority={loser.priority})"
                )
            else:
                logger.info(f"EVAL: DENIED by {winner.id} (priority={winner.priority})")
            return False, f"Explicit DENY by policy {winner.id}"

        if loser is not None:
            logger.info(
                f"EVAL: ALLOWED by {winner.id} (priority={winner.priority}, "
                f"overrode DENY from {loser.id} priority={loser.priority})"
            )
        else:
            logger.info(f"EVAL: ALLOWED by {winner.id} (priority={winner.priority})")
        return True, f"Allowed by policy {winner.id}"

    async def compile_read_filter(
        self,
        principals: List[str],
        catalog_id: Optional[str] = None,
        collection_id: Optional[str] = None,
        *,
        principal: Optional[Principal] = None,
        principal_id: Optional[UUID] = None,
    ) -> AccessFilter:
        """Project this principal's read scope into a neutral AccessFilter.

        Document-level-security companion to :meth:`evaluate_access`. Same
        policy set (via :meth:`_resolve_effective_policies`), projected into
        an :class:`AccessFilter` a storage driver translates to a native
        predicate without importing IAM.

        Safety contract (proved by the drift-guard property test): the
        result is an **equal-or-stricter** projection of ``evaluate_access``.
        Deny-precedence is preserved *structurally*: ALLOW policies become
        OR clauses (``allow``), DENY policies become negated clauses
        (``deny``) that a driver applies as ``must_not`` and therefore win
        over any ALLOW — no re-ranking needed.

        Fail-closed rules:
          * An ALLOW grant whose condition the compiler cannot express as an
            index predicate is **dropped** (it does not contribute) and
            ``uncompilable`` is set. Under-returning is the safe direction —
            the document is hidden from search but still reachable by a
            direct GET that runs the full engine.
          * A relevant DENY grant with an uncompilable condition CANNOT be
            dropped (dropping a DENY would widen access). It forces a full
            :meth:`AccessFilter.deny_everything` — over-denying is safe.
          * When no ALLOW compiles and the principal is not an unconditional
            super-admin, the result is :meth:`AccessFilter.deny_everything`.
        """
        schema = await self._resolve_schema(catalog_id)
        effective_policies = await self._resolve_effective_policies(
            principals=principals,
            schema=schema,
            catalog_id=catalog_id,
            custom_policies=(principal.custom_policies or None) if principal else None,
            principal_id=principal_id,
            collection_id=collection_id,
        )

        # Representative read path for the requested scope. Reusing the
        # policy's own ``matches_resource`` regex matcher (the exact matcher
        # ``evaluate_access`` uses) keeps relevance resolution drift-free —
        # we never re-implement resource matching here.
        probe_paths = _read_scope_probe_paths(catalog_id, collection_id)

        attributes = principal.attributes if principal else {}

        allow_clauses: List[AccessClause] = []
        deny_clauses: List[AccessClause] = []
        uncompilable = False
        allow_all = False

        for pol in effective_policies:
            # Relevance: a READ-family action AND a resource pattern that can
            # match a read in this scope. Anything outside the read scope is
            # irrelevant to the filter and is skipped.
            if not _policy_has_read_action(pol, is_deny=pol.effect == "DENY"):
                continue
            if not _policy_matches_read_scope(pol, probe_paths):
                continue

            result = _compile_conditions(pol.conditions, attributes)

            if pol.effect == "DENY":
                if result.outcome is _Outcome.UNCOMPILABLE:
                    # A relevant DENY we cannot express → fully fail closed.
                    # Dropping it would let an ALLOW leak documents the engine
                    # would deny.
                    return AccessFilter.deny_everything(uncompilable=True)
                if result.outcome is _Outcome.UNSATISFIABLE:
                    # The DENY's own gate is false for this principal, exactly
                    # as ``evaluate_access`` would skip it → no exclusion.
                    continue
                # SATISFIED or PREDICATES → the DENY applies. A SATISFIED gate
                # yields a scope-only clause (deny everything in scope); an
                # empty clause at platform scope denies everything.
                deny_clauses.append(
                    _scope_clause(list(result.predicates), catalog_id, collection_id)
                )
                continue

            # ALLOW
            if result.outcome is _Outcome.UNCOMPILABLE:
                # Drop the grant; record that the filter is now stricter than
                # the engine so callers/telemetry know search may under-return.
                uncompilable = True
                continue
            if result.outcome is _Outcome.UNSATISFIABLE:
                # The ALLOW's gate is false for this principal → the grant never
                # fires, exactly as the engine. NOT "uncompilable": we evaluated
                # the gate, it simply does not hold, so this is not an
                # under-return relative to the engine.
                continue

            scope_preds = _scope_predicates(catalog_id, collection_id)
            if not result.predicates and not scope_preds:
                # Unconditional ALLOW spanning the whole read scope (e.g. a
                # platform super-admin: action ``.*`` + resource ``.*`` + no
                # compilable-and-satisfied conditions, called with no
                # catalog/collection pin). Record it but KEEP scanning: a DENY
                # still wins by deny-precedence, so we must not short-circuit
                # and drop the deny clauses.
                allow_all = True
                continue
            allow_clauses.append(
                _scope_clause(list(result.predicates), catalog_id, collection_id)
            )

        if allow_all:
            # Unconditional allow, minus any applicable DENY (deny-precedence
            # preserved structurally as ``must_not``).
            return AccessFilter(
                allow_all=True,
                deny=tuple(deny_clauses),
                uncompilable=uncompilable,
            )
        if not allow_clauses:
            # Nothing could be allowed (deny-by-default, every ALLOW gate was
            # false, or every ALLOW was dropped as uncompilable).
            return AccessFilter.deny_everything(uncompilable=uncompilable)

        return AccessFilter.from_clauses(
            allow_clauses, deny_clauses, uncompilable=uncompilable
        )

    async def _evaluate_condition(self, condition: Condition, context: Any) -> bool:
        """
        Resolves the appropriate handler and evaluates the condition.
        """
        from .conditions import evaluate_condition

        return await evaluate_condition(condition, context)


# --- compile_read_filter support ----------------------------------------- #
#
# Pure, side-effect-free helpers that translate a Policy's actions /
# resources / conditions into AccessFilter pieces. Kept module-level (not
# methods) so they are trivially unit-testable and carry no service state.

# Action patterns that count as a READ-family verb for document scope. A
# policy that does not admit at least one of these on its action list is
# irrelevant to a read filter. ``.*`` (the wildcard normalised from ``*``)
# admits everything, so a super-admin's ``["*"]`` qualifies.
_READ_ACTIONS: Tuple[str, ...] = ("GET", "SEARCH", "READ")


def _policy_has_read_action(pol: Policy, *, is_deny: bool = False) -> bool:
    """True when the policy's action patterns admit a READ-family verb.

    Reuses the policy's own compiled action matcher so the verb test is the
    same one ``evaluate_access`` applies — no separate matching logic to
    drift from the engine.

    ``is_deny`` widens the probe to include ``POST`` for DENY policies only.
    ``POST`` is the verb a STAC ``/search`` read uses, but it is *also* the
    create verb — so a ``POST``-only ALLOW must NOT be mistaken for a read
    grant (that would widen read access = leak), while a ``POST``-only DENY
    covering an item-read path MUST still be honoured (dropping it would widen
    access). Considering ``POST`` for DENY only keeps the filter
    equal-or-stricter in both directions.
    """
    # ``not pol.actions`` mirrors evaluate_access's "no actions ⟹ matches".
    if not pol.actions:
        return True
    verbs = (_READ_ACTIONS + ("POST",)) if is_deny else _READ_ACTIONS
    return any(pol.matches_action(verb) for verb in verbs)


def _read_scope_probe_paths(
    catalog_id: Optional[str], collection_id: Optional[str]
) -> Tuple[str, ...]:
    """Representative read URLs for the requested scope.

    A policy is relevant to the read scope when its resource pattern matches
    at least one of these. We probe the canonical per-catalog read surfaces
    (STAC + OGC Features item reads) plus the platform-tier item search, so
    a policy scoped to any of them is recognised. When no catalog is given
    we probe the platform-tier search root and the bare root, which a
    ``.*`` super-admin resource still matches.
    """
    if catalog_id is None:
        return ("/", "/search")
    cat = catalog_id
    if collection_id:
        col = collection_id
        return (
            f"/stac/catalogs/{cat}/collections/{col}/items",
            f"/stac/catalogs/{cat}/collections/{col}/items/_probe_",
            f"/features/catalogs/{cat}/collections/{col}/items",
            f"/search/catalogs/{cat}/items-search",
            f"/search/catalogs/{cat}",
        )
    # Catalog-wide read scope: also probe a representative collection-item
    # read path so a policy scoped to ``.../collections/{col}/items`` (the
    # common item-read grant) is recognised as relevant to a catalog-wide
    # read. ``_probe_`` is a placeholder collection/item id that the
    # per-segment ``[^/]+`` patterns match.
    return (
        f"/stac/catalogs/{cat}",
        f"/stac/catalogs/{cat}/collections",
        f"/stac/catalogs/{cat}/collections/_probe_/items",
        f"/stac/catalogs/{cat}/collections/_probe_/items/_probe_",
        f"/features/catalogs/{cat}",
        f"/features/catalogs/{cat}/collections/_probe_/items",
        f"/search/catalogs/{cat}/items-search",
        f"/search/catalogs/{cat}",
    )


def _policy_matches_read_scope(pol: Policy, probe_paths: Tuple[str, ...]) -> bool:
    """True when the policy's resource regex matches any probe path.

    Uses ``Policy.matches_resource`` — the same start-anchored regex matcher
    ``evaluate_access`` uses — so relevance can never diverge from the
    engine's notion of "this policy applies to this path".
    """
    return any(pol.matches_resource(p) for p in probe_paths)


# Condition handler types the compiler can express as a static document
# predicate (or compose). Everything not in this map is treated as
# uncompilable (depends on request state, time, counters, or a DB round-trip
# at eval time) and forces the fail-closed path. See the condition handlers
# in ``conditions.py`` / ``audience_handlers.py``.
_LOGICAL_TYPES: Tuple[str, ...] = ("and", "or", "not")


class _Outcome(enum.Enum):
    """How a single condition (or an AND/OR of them) projects onto documents.

    The distinction that closes the leak: a ``match`` on a principal attribute
    is *document-independent* — it is true or false for THIS principal
    regardless of any document. Such a gate must be evaluated at compile time
    (SATISFIED / UNSATISFIABLE), never folded into a per-document field
    predicate. Only conditions that genuinely constrain a document field (e.g.
    ``catalog_lookup_public_allowed`` → ``visibility``) yield PREDICATES.
    """

    #: Provably TRUE for this principal regardless of document — no predicate.
    SATISFIED = "satisfied"
    #: Provably FALSE for this principal regardless of document — drop the
    #: grant (the engine would also deny it; not an under-return).
    UNSATISFIABLE = "unsatisfiable"
    #: Maps to one or more per-document index predicates.
    PREDICATES = "predicates"
    #: Cannot be evaluated at compile time (request-time / stateful input) —
    #: drop the ALLOW + flag, or fully deny for a DENY (fail-closed).
    UNCOMPILABLE = "uncompilable"


@dataclass(frozen=True)
class _CondResult:
    outcome: "_Outcome"
    predicates: Tuple[FieldPredicate, ...] = ()


_SATISFIED = _CondResult(_Outcome.SATISFIED)
_UNSATISFIABLE = _CondResult(_Outcome.UNSATISFIABLE)
_UNCOMPILABLE = _CondResult(_Outcome.UNCOMPILABLE)


def _predicate_result(preds: List[FieldPredicate]) -> _CondResult:
    """A PREDICATES result, collapsing to SATISFIED when no predicate remains."""
    t = tuple(preds)
    return _CondResult(_Outcome.PREDICATES, t) if t else _SATISFIED


def _compile_conditions(
    conditions: List[Condition], attributes: Dict[str, Any]
) -> _CondResult:
    """Compile a policy's condition list (an implicit AND) for this principal.

    ``evaluate_access`` requires EVERY condition to pass, so this is an AND:
      * any child UNSATISFIABLE ⟹ the whole grant is dead → UNSATISFIABLE
        (drop, NOT flagged uncompilable — the engine denies it too);
      * else any child UNCOMPILABLE ⟹ UNCOMPILABLE (drop ALLOW + flag / fully
        deny for DENY);
      * SATISFIED children contribute nothing; PREDICATES children accumulate
        into one conjunctive clause.
    """
    predicates: List[FieldPredicate] = []
    saw_uncompilable = False
    for cond in conditions:
        r = _compile_condition(cond, attributes)
        if r.outcome is _Outcome.UNSATISFIABLE:
            return _UNSATISFIABLE
        if r.outcome is _Outcome.UNCOMPILABLE:
            saw_uncompilable = True
            continue
        if r.outcome is _Outcome.PREDICATES:
            predicates.extend(r.predicates)
        # SATISFIED contributes nothing.
    if saw_uncompilable:
        return _UNCOMPILABLE
    return _predicate_result(predicates)


def _compile_condition(
    cond: Condition, attributes: Dict[str, Any]
) -> _CondResult:
    """Compile a single condition for this principal (tri-/quad-state).

    Compilable cases:
      * ``catalog_lookup_public_allowed`` → PREDICATES ``visibility IN
        ("public",)``. The handler admits only when the catalog is public; on
        the document plane that is exactly "the document is public" — a
        faithful, equal-or-stricter projection.
      * ``match`` (AttributeMatchHandler) — a *principal gate*, evaluated at
        compile time to SATISFIED / UNSATISFIABLE (see
        :func:`_compile_attribute_match`). It NEVER becomes a per-document
        predicate, because the handler compares a principal/request value to a
        static value and never reads the document.
      * ``and`` / ``or`` composed of children. ``and`` is the implicit-AND of
        :func:`_compile_conditions`; ``or`` is SATISFIED if any branch is
        SATISFIED, UNSATISFIABLE if all branches are, and otherwise only
        expressible as a single-field value union (else UNCOMPILABLE).

    Everything else (``not``, rate_limit, max_count, time_window, expiration,
    query_match, lookup_only_search, filter, catalog_membership_required,
    catalog_admin_required, max_token_ttl, collection_write_anonymous_allowed,
    unknown types) is UNCOMPILABLE.
    """
    ctype = cond.type
    config = cond.config or {}

    if ctype == "catalog_lookup_public_allowed":
        return _CondResult(
            _Outcome.PREDICATES, (FieldPredicate("visibility", ("public",)),)
        )

    if ctype == "match":
        return _compile_attribute_match(config, attributes)

    if ctype == "and":
        children = [Condition(**c) for c in (config.get("conditions") or [])]
        return _compile_conditions(children, attributes)

    if ctype == "or":
        return _compile_or(config, attributes)

    # ``not`` and every stateful / request-time condition: uncompilable.
    return _UNCOMPILABLE


def _compile_or(config: Dict[str, Any], attributes: Dict[str, Any]) -> _CondResult:
    """Compile an ``or`` of conditions, mirroring LogicalOrHandler.

    LogicalOrHandler returns True if ANY branch is true (and True for an empty
    list). So: any SATISFIED branch ⟹ SATISFIED; UNSATISFIABLE branches never
    contribute and are ignored; if every branch is UNSATISFIABLE ⟹
    UNSATISFIABLE. A disjunction of per-document predicates is only expressible
    as one conjunctive clause when every predicate branch constrains the SAME
    field (values unioned); any branch we cannot compile (or a heterogeneous
    field) means an unknown branch could independently satisfy the OR, so we
    fail closed (UNCOMPILABLE).
    """
    children_cfg = config.get("conditions") or []
    if not children_cfg:
        return _SATISFIED
    field_name: Optional[str] = None
    values: List[str] = []
    saw_uncompilable = False
    saw_predicate = False
    for child_cfg in children_cfg:
        r = _compile_condition(Condition(**child_cfg), attributes)
        if r.outcome is _Outcome.SATISFIED:
            return _SATISFIED
        if r.outcome is _Outcome.UNSATISFIABLE:
            continue
        if r.outcome is _Outcome.UNCOMPILABLE or len(r.predicates) != 1:
            saw_uncompilable = True
            continue
        p = r.predicates[0]
        if field_name is None:
            field_name = p.field
        elif field_name != p.field:
            saw_uncompilable = True
            continue
        saw_predicate = True
        values.extend(p.values)
    if saw_uncompilable:
        return _UNCOMPILABLE
    if not saw_predicate:
        # Every branch was UNSATISFIABLE ⟹ the OR can never be true.
        return _UNSATISFIABLE
    assert field_name is not None
    return _CondResult(
        _Outcome.PREDICATES,
        (FieldPredicate(field_name, tuple(dict.fromkeys(values))),),
    )


def _compile_attribute_match(
    config: Dict[str, Any], attributes: Dict[str, Any]
) -> _CondResult:
    """Compile a ``match`` condition — a principal gate, never a doc predicate.

    ``AttributeMatchHandler`` compares a SOURCE value to a static ``value``;
    the source is one of ``principal.attributes.<k>``, ``principal.id``, or a
    request-time input (``query.*`` / ``header.*`` / ``path`` / ``method`` /
    ``extras.*``). It NEVER reads the document, so the result is
    document-independent.

    Only ``principal.attributes.<k>`` is resolvable from the principal alone at
    compile time. We resolve it and apply the SAME comparison the handler uses
    (see ``_match_compare``) to decide SATISFIED vs UNSATISFIABLE. Folding the
    principal's value into a ``<k> IN (...)`` document predicate — as an earlier
    version did — is a leak: it admits documents whose field happens to equal
    the principal's value even when the principal fails the gate. Any other
    source is request-time / not known here → UNCOMPILABLE (fail-closed).
    """
    attr_path = config.get("attribute")
    if not attr_path:
        # Mirrors AttributeMatchHandler: a missing attribute path passes.
        return _SATISFIED
    if not isinstance(attr_path, str):
        return _UNCOMPILABLE
    prefix = "principal.attributes."
    if not attr_path.startswith(prefix):
        # principal.id / query.* / header.* / path / method / extras.* — not
        # resolvable from the principal's attributes alone at compile time.
        return _UNCOMPILABLE

    key = attr_path[len(prefix):]
    actual = attributes.get(key)
    if actual is None:
        # Handler returns False when the source value is absent.
        return _UNSATISFIABLE
    operator = config.get("operator", "eq")
    expected = config.get("value")
    return _SATISFIED if _match_compare(actual, operator, expected) else _UNSATISFIABLE


def _match_compare(actual: Any, operator: str, expected: Any) -> bool:
    """Exact mirror of ``AttributeMatchHandler._compare`` (conditions.py).

    Kept byte-for-byte equivalent so a compile-time evaluation of a principal
    gate can never disagree with the runtime engine.
    """
    if operator == "eq":
        return str(actual) == str(expected)
    if operator == "neq":
        return str(actual) != str(expected)
    if operator == "contains":
        return str(expected) in str(actual)
    if operator == "regex":
        return bool(re.match(str(expected), str(actual)))
    if operator == "gt":
        try:
            return float(actual) > float(expected)
        except Exception:
            return False
    if operator == "lt":
        try:
            return float(actual) < float(expected)
        except Exception:
            return False
    if operator == "in":
        if isinstance(expected, list):
            return actual in expected
        return actual in str(expected).split(",")
    return False


def _scope_predicates(
    catalog_id: Optional[str], collection_id: Optional[str]
) -> List[FieldPredicate]:
    """Predicates pinning a clause to the requested catalog / collection.

    When the caller asks for a specific catalog (and optionally collection),
    every compiled clause is constrained to it so the filter never admits a
    document from another tenant. With no catalog pin the scope is the
    platform plane and no structural pin is added.
    """
    preds: List[FieldPredicate] = []
    if catalog_id is not None:
        preds.append(FieldPredicate("catalog_id", (catalog_id,)))
    if collection_id is not None:
        preds.append(FieldPredicate("collection_id", (collection_id,)))
    return preds


def _scope_clause(
    predicates: List[FieldPredicate],
    catalog_id: Optional[str],
    collection_id: Optional[str],
) -> AccessClause:
    """Build a clause = condition predicates AND the scope pin predicates."""
    return AccessClause(tuple(_scope_predicates(catalog_id, collection_id) + predicates))
