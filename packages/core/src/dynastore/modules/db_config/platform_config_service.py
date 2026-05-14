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

# File: src/dynastore/modules/db_config/platform_config_service.py

"""Platform Config Service.

Class-as-identity model:

- Every :class:`PluginConfig` subclass IS its own identity. ``class_key()``
  (a stable PascalCase string derived from ``__qualname__``, pinnable via
  ``_class_key: ClassVar[str]``) is used as the primary key in
  ``configs.platform_configs`` / ``<tenant>.catalog_configs`` /
  ``<tenant>.collection_configs``, and as the public identifier exposed by
  the ``/configs/*`` endpoints.
- Discovery goes through :class:`TypedModelRegistry` — the process-wide
  class registry populated automatically by ``PersistentModel.__init_subclass__``.
- Apply handlers attach to the class, not to a string key.
"""

import logging
import json
import typing
import inspect
from contextlib import asynccontextmanager
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Type,
    Union,
)

from dataclasses import dataclass, field as dc_field

from dynastore.tools.cache import cached

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from .maintenance_tools import ensure_schema_exists
from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
from dynastore.models.driver_context import DriverContext

# imported to avoid circular imports
from dynastore.modules.db_config.exceptions import ImmutableConfigError
from dynastore.tools.json import CustomJSONEncoder

# --- Mutability framework + PluginConfig base (re-exported) ---
#
# The marker types and the ``PluginConfig`` base were extracted into
# dependency-light leaf modules — ``dynastore.models.mutability`` and
# ``dynastore.modules.db_config.plugin_config`` — so that a
# Protocol-contracts file can subclass ``PluginConfig`` (e.g.
# ``IamRolesConfig`` in ``models/protocols/authorization.py``) without
# dragging this DB-facing service, and the ``models.protocols`` eager
# hub it imports, into a load-order import cycle (#686).  They are
# re-exported here so the existing call sites that import them from
# ``platform_config_service`` keep working unchanged.
from dynastore.models.mutability import (  # noqa: F401
    Computed,
    ComputedMarker,
    Immutable,
    ImmutableMarker,
    Mutable,
    MutableMarker,
    WriteOnce,
    WriteOnceMarker,
    is_immutable_field,
    is_write_once_field,
)
from dynastore.modules.db_config.plugin_config import (  # noqa: F401
    _APPLY_HANDLERS,
    PluginConfig,
    _collect_required_fields,
    list_registered_configs,
    require_config_class,
    resolve_config_class,
)

logger = logging.getLogger(__name__)

def enforce_config_immutability(
    current_config: Optional["PluginConfig"], new_config: "PluginConfig"
) -> None:
    """Reject Immutable / WriteOnce field mutations."""
    if current_config is None:
        return
    model_class = type(current_config)
    if not isinstance(new_config, model_class):
        return
    for field_name, field_info in model_class.model_fields.items():
        current_val = getattr(current_config, field_name)
        new_val = getattr(new_config, field_name)
        if is_immutable_field(field_info):
            if current_val != new_val:
                raise ImmutableConfigError(
                    f"Configuration field '{field_name}' in '{model_class.__name__}' is Immutable. "
                    f"Modification forbidden: {current_val} -> {new_val}"
                )
        elif is_write_once_field(field_info):
            if current_val is not None and current_val != new_val:
                raise ImmutableConfigError(
                    f"Configuration field '{field_name}' in '{model_class.__name__}' is WriteOnce. "
                    f"Cannot change a non-None value: {current_val!r} -> {new_val!r}"
                )


# --- Field-level change detection ---


@dataclass
class ConfigChange:
    """Structured diff emitted to on_apply handlers when config changes."""

    class_key: str
    old_config: Optional["PluginConfig"]
    new_config: "PluginConfig"
    changed_fields: Dict[str, Tuple[Any, Any]] = dc_field(default_factory=dict)
    catalog_id: Optional[str] = None
    collection_id: Optional[str] = None
    db_resource: Optional[Any] = None

    @property
    def is_creation(self) -> bool:
        return self.old_config is None

    def field_changed(self, name: str) -> bool:
        return name in self.changed_fields


def compute_config_diff(
    old_config: Optional["PluginConfig"], new_config: "PluginConfig"
) -> Dict[str, Tuple[Any, Any]]:
    model_cls = type(new_config)
    if old_config is None:
        return {
            name: (None, getattr(new_config, name))
            for name in model_cls.model_fields
        }
    changed: Dict[str, Tuple[Any, Any]] = {}
    for name in model_cls.model_fields:
        old_val = getattr(old_config, name, None)
        new_val = getattr(new_config, name, None)
        if old_val != new_val:
            changed[name] = (old_val, new_val)
    return changed


# --- Startup resilience ---


async def _platform_table_exists(conn: DbResource) -> bool:
    from dynastore.modules.db_config.locking_tools import check_table_exists
    return await check_table_exists(conn, "platform_configs", "configs")


# --- Schema (Platform Level Only) ---

from dynastore.modules.db_config.typed_store.ddl import PLATFORM_SCHEMAS_DDL
from dynastore.modules.db_config.typed_store import config_queries as _cq

# Aliases kept for call-site readability within this module.
get_platform_config_query = _cq.get_platform_config
upsert_platform_config_query = _cq.upsert_platform_config
list_platform_configs_query = _cq.list_platform_configs
delete_platform_config_query = _cq.delete_platform_config
get_platform_config_by_ref_query = _cq.get_platform_config_by_ref
list_platform_refs_query = _cq.list_platform_refs


async def _register_schema(conn: DbResource, config: "PluginConfig") -> None:
    """Upsert the config's current JSON schema into ``configs.schemas``."""
    cls = type(config)
    await _cq.register_schema.execute(
        conn,
        schema_id=cls.schema_id(),
        class_key=cls.class_key(),
        schema_json=json.dumps(cls.model_json_schema(), sort_keys=True),
    )


# --- Manager ---


def _post_commit_router_bust(cls: Type["PluginConfig"]) -> None:
    """Bust the distributed storage-router cache after a platform-tier commit.

    Only routing configs affect that cache; for anything else this is a
    no-op. Lazy import keeps ``modules/db_config`` free of a hard dep on
    ``modules/storage``.
    """
    try:
        from dynastore.modules.storage.routing_config import (
            ItemsRoutingConfig,
            AssetRoutingConfig,
        )
        if not issubclass(cls, (ItemsRoutingConfig, AssetRoutingConfig)):
            return
        from dynastore.modules.storage.router import invalidate_router_cache
        invalidate_router_cache(None, None)
    except Exception:
        pass


from dynastore.tools.plugin import ProtocolPlugin


class PlatformConfigService(ProtocolPlugin[object], PlatformConfigsProtocol):
    """Manages global configuration settings (Level 3)."""

    def __init__(self, engine: Optional[DbResource] = None):
        self._engine = engine
        self._setup_cache()

    @property
    def is_platform_manager(self) -> bool:
        return True

    @property
    def engine(self) -> Optional[DbResource]:
        if self._engine:
            return self._engine
        from dynastore.tools.protocol_helpers import get_engine
        return get_engine()

    def _setup_cache(self):
        self.get_platform_config_internal_cached = cached(
            maxsize=64, ttl=300, namespace="platform_config"
        )(self._get_platform_config_internal_db)

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> typing.AsyncGenerator[None, None]:
        if not hasattr(self, "get_platform_config_internal_cached"):
            self._setup_cache()
        if self.engine is not None:
            await self.initialize_storage(self.engine)
        logger.info("PlatformConfigService: Started.")
        yield
        logger.info("PlatformConfigService: Stopped.")

    @classmethod
    async def initialize_storage(cls, conn: DbResource):
        try:
            logger.info("Initializing Platform Config Storage (configs schema)...")
            await ensure_schema_exists(conn, "configs")
            await DDLQuery(PLATFORM_SCHEMAS_DDL).execute(conn)
            logger.info("Platform Config Storage initialized successfully.")
        except Exception as e:
            logger.error(
                f"FATAL: PlatformConfigService initialization failed: {e}",
                exc_info=True,
            )
            raise

    async def get_config(
        self,
        config_cls: Union[str, Type[PluginConfig]],
        ctx: Optional[DriverContext] = None,
    ) -> PluginConfig:
        cls = require_config_class(config_cls)
        db_resource = ctx.db_resource if ctx else None
        config = await self._get_platform_config_internal(cls, db_resource=db_resource)
        if config:
            return config
        try:
            return cls()
        except Exception as exc:
            from dynastore.modules.db_config.exceptions import ConfigResolutionError
            required = _collect_required_fields(cls)
            raise ConfigResolutionError(
                f"Config '{cls.class_key()}' has no usable default at any scope "
                f"(platform/code) and cannot be constructed with zero args: {exc}",
                missing_key=cls.class_key(),
                required_fields=required,
                scope_tried=["platform", "code_default"],
            ) from exc

    async def _get_platform_config_internal_db(
        self, class_key: str
    ) -> Optional[dict]:
        async with managed_transaction(self.engine) as conn:
            if not await _platform_table_exists(conn):
                return None
            return await get_platform_config_query.execute(conn, ref_key=class_key)

    async def _get_platform_config_internal(
        self,
        cls: Type[PluginConfig],
        db_resource: Optional[DbResource] = None,
    ) -> Optional[PluginConfig]:
        class_key = cls.class_key()
        if db_resource:
            if not await _platform_table_exists(db_resource):
                return None
            data = await get_platform_config_query.execute(
                db_resource, ref_key=class_key
            )
        else:
            data = await self.get_platform_config_internal_cached(class_key)
        if not data:
            return None
        if isinstance(data, cls):
            return data
        return cls.model_validate(data)

    async def set_config(
        self,
        config_cls: Union[str, Type[PluginConfig]],
        config: PluginConfig,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        cls = require_config_class(config_cls)
        class_key = cls.class_key()
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            old_config: Optional[PluginConfig] = None
            current_data = await get_platform_config_query.execute(
                conn, ref_key=class_key
            )
            if current_data:
                old_config = cls.model_validate(current_data) if not isinstance(
                    current_data, cls
                ) else current_data
                if check_immutability:
                    enforce_config_immutability(old_config, config)

            await _register_schema(conn, config)

            # exclude_unset=True → platform row stores only fields the caller
            # explicitly sent. Class defaults are resolved at read time, so
            # bumping a class default propagates without rewriting this row.
            await upsert_platform_config_query.execute(
                conn,
                ref_key=class_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(
                    config.model_dump(
                        mode="json",
                        context={"secret_mode": "db"},
                        exclude_unset=True,
                    ),
                    cls=CustomJSONEncoder,
                ),
            )

            for apply_handler in cls.get_apply_handlers():
                try:
                    res = apply_handler(config, None, None, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(
                        f"Failed to apply platform configuration for '{class_key}': {e}",
                        exc_info=True,
                    )

        self.get_platform_config_internal_cached.cache_invalidate(class_key)
        # Post-commit router bust closes the race where an apply_handler
        # invalidates inside the open transaction and a concurrent reader
        # re-caches the pre-commit row. Mirrors catalog/collection tiers.
        _post_commit_router_bust(cls)

    async def list_configs(self) -> Dict[Type[PluginConfig], PluginConfig]:
        """Return ``{class: config}`` for every persisted platform config."""
        async with managed_transaction(self.engine) as conn:
            rows = await list_platform_configs_query.execute(conn)

        configs: Dict[Type[PluginConfig], PluginConfig] = {}
        for row in rows:
            class_key = row["class_key"]
            cls = resolve_config_class(class_key)
            if cls is None:
                logger.warning(
                    "Skipping platform_configs row for unknown class_key %r",
                    class_key,
                )
                continue
            configs[cls] = cls.model_validate(row["config_data"])
        return configs

    async def list_refs(self) -> Dict[str, str]:
        """F.4c.2 — return ``{ref_key: class_key}`` for every platform-stored row.

        Tier-local: does NOT walk the waterfall.  Returns ``{}`` when the
        platform_configs table has no rows.
        """
        async with managed_transaction(self.engine) as conn:
            if not await _platform_table_exists(conn):
                return {}
            rows = await list_platform_refs_query.execute(conn)
        return {row["ref_key"]: row["class_key"] for row in rows}

    async def get_config_by_ref(
        self,
        ref_key: str,
        ctx: Optional[DriverContext] = None,
    ) -> Optional[PluginConfig]:
        """F.4c.2 — return the stored ``PluginConfig`` for ``ref_key`` at platform scope.

        Resolves the dispatch class from the row's ``class_key`` discriminator.
        Returns ``None`` when the row is absent or its ``class_key`` is no
        longer registered (warning logged).  Tier-local: does NOT walk the
        waterfall.
        """
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            if not await _platform_table_exists(conn):
                return None
            row = await get_platform_config_by_ref_query.execute(
                conn, ref_key=ref_key
            )
        if not row:
            return None
        cls = resolve_config_class(row["class_key"])
        if cls is None:
            logger.warning(
                "get_config_by_ref: ref %r stored class_key %r not in registry",
                ref_key,
                row["class_key"],
            )
            return None
        data = row["config_data"]
        return cls.model_validate(data)

    async def delete_config(
        self,
        config_cls: Union[str, Type[PluginConfig]],
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        cls = require_config_class(config_cls)
        class_key = cls.class_key()
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            rows_affected = await delete_platform_config_query.execute(
                conn, ref_key=class_key
            )
            if rows_affected > 0:
                self.get_platform_config_internal_cached.cache_invalidate(class_key)
                _post_commit_router_bust(cls)
                return True
        return False

    # F.4c.4 — ref-keyed write API (platform scope)

    async def set_config_by_ref(
        self,
        ref_key: str,
        config: PluginConfig,
        check_immutability: bool = True,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        """F.4c.4 — store ``config`` at ``platform.{ref_key}``.

        ``ref_key == class_key`` collapses to the single-instance path that
        :meth:`set_config` already covers.  Different ref_keys allow multiple
        rows per class (multi-instance).

        Immutability check, if enabled, runs against the row stored at the
        same ref_key (so an operator can't repurpose ``pg_main`` to a
        different class) — uses :func:`enforce_config_immutability` against
        the existing row when present.  Apply-handlers fire post-write.
        """
        cls = type(config)
        class_key = cls.class_key()
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            existing_row = await get_platform_config_by_ref_query.execute(
                conn, ref_key=ref_key
            )
            if existing_row:
                stored_class_key = existing_row["class_key"]
                if stored_class_key != class_key:
                    raise ValueError(
                        f"set_config_by_ref({ref_key!r}): row stored as "
                        f"class_key={stored_class_key!r}, refusing to "
                        f"overwrite with class_key={class_key!r}.  Delete "
                        f"the ref first or pick a different name."
                    )
                if check_immutability:
                    old_config = cls.model_validate(existing_row["config_data"])
                    enforce_config_immutability(old_config, config)

            await _register_schema(conn, config)

            await upsert_platform_config_query.execute(
                conn,
                ref_key=ref_key,
                class_key=class_key,
                schema_id=type(config).schema_id(),
                config_data=json.dumps(
                    config.model_dump(
                        mode="json",
                        context={"secret_mode": "db"},
                        exclude_unset=True,
                    ),
                    cls=CustomJSONEncoder,
                ),
            )

            for apply_handler in cls.get_apply_handlers():
                try:
                    res = apply_handler(config, None, None, conn)
                    if inspect.isawaitable(res):
                        await res
                except Exception as e:
                    logger.error(
                        f"Failed to apply platform configuration for "
                        f"ref={ref_key!r} class={class_key!r}: {e}",
                        exc_info=True,
                    )

        # Invalidate the class-keyed cache for the dispatch class so any
        # waterfall reads pick up the change.  Multi-instance rows still
        # share their class with the single-instance default.
        self.get_platform_config_internal_cached.cache_invalidate(class_key)
        _post_commit_router_bust(cls)

    async def delete_config_by_ref(
        self,
        ref_key: str,
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        """F.4c.4 — delete the platform-stored row at ``ref_key``.

        Returns ``True`` when a row was removed, ``False`` for a no-op.
        Tier-local: does NOT cascade to catalog / collection rows that
        share the ref name (per-tier isolation matches single-instance
        ``delete_config``).
        """
        db_resource = ctx.db_resource if ctx else None
        async with managed_transaction(db_resource or self.engine) as conn:
            existing_row = await get_platform_config_by_ref_query.execute(
                conn, ref_key=ref_key
            )
            if not existing_row:
                return False
            stored_class_key = existing_row["class_key"]
            await delete_platform_config_query.execute(conn, ref_key=ref_key)
        # Best-effort cache + router invalidation.  resolve_config_class
        # returns None when the class has been unregistered (warning
        # logged); skip the router bust in that case.
        self.get_platform_config_internal_cached.cache_invalidate(stored_class_key)
        cls = resolve_config_class(stored_class_key)
        if cls is not None:
            _post_commit_router_bust(cls)
        return True
