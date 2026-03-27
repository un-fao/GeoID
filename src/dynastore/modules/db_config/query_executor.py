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
#    See the License for the apecific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

import inspect
import re
import logging
import asyncio
import weakref
import contextvars
import hashlib
import os
import secrets
from uuid import UUID, uuid4
from abc import abstractmethod, ABC
from contextlib import asynccontextmanager, contextmanager
from sqlalchemy import text, DDL
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection as SAConnection
from sqlalchemy.engine.result import Result

# SASession is renamed here to avoid confusion; sessionmaker usually returns it
from sqlalchemy.orm import Session as SASession
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    AsyncEngine,
    AsyncTransaction,
)
from sqlalchemy.exc import (
    ProgrammingError,
    IntegrityError,
    OperationalError,
    PendingRollbackError,
    InvalidRequestError,
)
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKBElement, WKTElement, _SpatialElement
from sqlalchemy import Table, MetaData
from typing import (
    Union,
    List,
    Callable,
    Any,
    Awaitable,
    Tuple,
    TypeAlias,
    TypeVar,
    ParamSpec,
    Optional,
    cast,
    Type,
    Dict,
    Set,
)
from pydantic import BaseModel
from .exceptions import (
    QueryExecutionError,
    PGCODE_EXCEPTION_MAP,
    DatabaseConnectionError,
)

# Re-map asyncpg ConnectionDoesNotExistError if possible
try:
    from asyncpg.exceptions import ConnectionDoesNotExistError as AsyncpgConnectionDoesNotExistError
except ImportError:
    AsyncpgConnectionDoesNotExistError = type("AsyncpgConnectionDoesNotExistError", (Exception,), {})

# --- Type Definitions ---
DbSyncConnection = Union[SAConnection, SASession]
DbAsyncConnection = Union[AsyncConnection, AsyncSession]
DbEngine = Union[Engine, AsyncEngine]
DbConnection = Union[DbSyncConnection, DbAsyncConnection]
DbSyncResource = Union[Engine, DbSyncConnection]
DbAsyncResource = Union[AsyncEngine, DbAsyncConnection]
DbResource = Union[DbSyncResource, DbAsyncResource]
BuilderResult = Tuple[TextClause, dict]
QueryBuilderFunction: TypeAlias = Callable[
    [DbResource, dict], Union[BuilderResult, Awaitable[BuilderResult]]
]

R = TypeVar("R")
P = ParamSpec("P")

logger = logging.getLogger(__name__)

_metadata = MetaData()

# --- Connection Serialization (Re-entrant Async Wire Lock) ---

# Stores one asyncio.Lock per underlying physical connection wire (asyncpg.Connection).
_conn_locks = weakref.WeakKeyDictionary()
# Track which wire is locked by which asyncio task (wire_id -> task_id)
_locked_ids: contextvars.ContextVar[Dict[int, int]] = contextvars.ContextVar(
    "_locked_ids", default={}
)


def _get_wire_identity(conn: Any) -> Any:
    """
    Safely drills down to find a stable identity for the connection wire
    without triggering prohibited SQLAlchemy properties like .connection.

    Uses isinstance checks against concrete SQLAlchemy types instead of
    hasattr duck typing — faster and type-checker friendly.
    """
    curr = conn
    for _ in range(15):
        # 1. Handle Async Wrappers — unwrap to sync counterparts
        if isinstance(curr, AsyncSession):
            curr = curr.sync_session
            continue
        if isinstance(curr, AsyncConnection):
            curr = curr.sync_connection
            continue

        # 2. Handle Session bound to Connection
        if isinstance(curr, SASession) and curr.bind is not None and not isinstance(curr.bind, (Engine, AsyncEngine)):
            if isinstance(curr.bind, (SAConnection, AsyncConnection)):
                curr = curr.bind
                continue

        # 3. Drill to driver connection via standard attributes.
        # driver_connection is the public API; _connection and _proxied
        # are SQLAlchemy internals needed to traverse proxy layers to reach
        # the actual asyncpg wire (required for correct wire-lock identity).
        nxt = (
            getattr(curr, "driver_connection", None)
            or getattr(curr, "_connection", None)
            or getattr(curr, "_proxied", None)
        )

        # 4. Fallback to dbapi_connection / _dbapi_connection (on Connection objects)
        if nxt is None and isinstance(curr, SAConnection):
            nxt = getattr(curr, "dbapi_connection", None) or getattr(curr, "_dbapi_connection", None)

        if nxt is None or nxt is curr:
            break

        # If we hit the asyncpg connection, we're at the bottom
        if type(nxt).__module__.startswith("asyncpg"):
            curr = nxt
            break

        curr = nxt

    return curr


@asynccontextmanager
async def _connection_lock_scope(conn: DbResource):
    """
    Serializes access to the physical connection wire to prevent asyncpg InterfaceErrors.
    Nested sequential calls within the SAME coroutine proceed immediately (re-entrancy).
    Spawning concurrent tasks (e.g. via gather) on same wire will correctly wait.
    """
    # Engines create fresh wires for each request, so we only lock on connection instances.
    if not is_async_resource(conn) or isinstance(conn, (AsyncEngine, Engine)):
        yield
        return

    wire = _get_wire_identity(conn)
    wire_id = id(wire)
    # logger.warning(
    #     f"DEBUG: lock_scope wire_id={wire_id} type={type(wire)} conn_type={type(conn)}"
    # )

    # Identify current execution context

    current_task = asyncio.current_task()
    task_id = id(current_task) if current_task else 0

    locked_map = _locked_ids.get()

    # Re-entrant ONLY if it's the SAME task holding the lock for this wire
    if wire_id in locked_map and locked_map[wire_id] == task_id:
        yield
    else:
        # Use a stable lock for this physical wire instance
        if wire not in _conn_locks:
            _conn_locks[wire] = asyncio.Lock()

        async with _conn_locks[wire]:
            # Register this wire as locked by THIS task
            token = _locked_ids.set({**locked_map, wire_id: task_id})
            try:
                yield
            finally:
                _locked_ids.reset(token)
                # Brief yield to allow driver state to settle
                await asyncio.sleep(0)


# A process-wide global reference to the main application's event loop.
_main_app_loop: Optional[asyncio.AbstractEventLoop] = None


def set_main_app_loop(loop: asyncio.AbstractEventLoop):
    """Sets the main application event loop for thread-safe calls."""
    global _main_app_loop
    _main_app_loop = loop


# --- Helper Functions ---


def is_async_resource(db_resource: DbResource) -> bool:
    """Determines if a resource supports asynchronous operations."""
    return isinstance(db_resource, (AsyncEngine, AsyncConnection, AsyncSession, AsyncTransaction))


def _is_in_transaction(conn: Any) -> bool:
    """Helper to check if a database resource is currently in a transaction."""
    if isinstance(conn, (SAConnection, SASession, AsyncConnection, AsyncSession)):
        return conn.in_transaction()
    return False


def serialize_geom(item):
    """Converts geometry elements to GeoJSON-compatible dictionaries."""
    if not isinstance(item, dict) and not hasattr(item, "_asdict"):
        return item
    data = item if isinstance(item, dict) else item._asdict()
    for geom_col in ["geom", "bbox_geom", "simplified_geom"]:
        if geom_col in data and data[geom_col] is not None:
            if isinstance(data[geom_col], (WKBElement, WKTElement)):
                data[geom_col] = to_shape(data[geom_col]).__geo_interface__
    return data


# --- Result Handling ---


class ResultHandler:
    """Standard recipes for processing SQLAlchemy Results."""

    SCALAR = lambda r: r.scalar()
    SCALAR_ONE = lambda r: r.scalar_one()
    SCALAR_ONE_OR_NONE = lambda r: r.scalar_one_or_none()
    ONE = lambda r: r.one()
    ONE_OR_NONE = lambda r: r.fetchone()
    ALL = lambda r: r.all()
    ALL_SCALARS = lambda r: r.scalars().all()
    ROWCOUNT = lambda r: r.rowcount
    ALL_DICTS = lambda r: [row._asdict() for row in r.all()]
    ONE_DICT = lambda r: row._asdict() if (row := r.fetchone()) else None
    NONE = lambda r: None


class PydanticResultHandler(ResultHandler):
    """Extends ResultHandler to include Pydantic model conversion."""

    @staticmethod
    def pydantic_one(model_class: Type[BaseModel]):
        def handler(result_proxy: Result) -> Optional[BaseModel]:
            row = result_proxy.fetchone()
            if row:
                return model_class.model_validate(row._asdict())
            return None

        return handler

    @staticmethod
    def pydantic_all(model_class: Type[BaseModel]):
        def handler(result_proxy: Result) -> List[BaseModel]:
            return [
                model_class.model_validate(row._asdict())
                for row in result_proxy.fetchall()
            ]

        return handler


# --- Query Builder Strategies ---


class QueryBuilderStrategy(ABC):
    @abstractmethod
    def build(
        self, db_resource: DbResource, raw_params: dict
    ) -> Union[BuilderResult, Awaitable[BuilderResult]]:
        pass


class TemplateQueryBuilder(QueryBuilderStrategy):
    """Builds a query from a string template with {identifier} substitutions."""

    def __init__(self, query_template: Union[str, DDL]):
        self.query_template = query_template

    def build(self, db_resource: DbResource, raw_params: dict):
        is_ddl = isinstance(self.query_template, DDL)
        template_str = str(self.query_template)

        if isinstance(db_resource, (Engine, AsyncEngine, SAConnection, AsyncConnection)):
            dialect = db_resource.dialect
        elif isinstance(db_resource, (SASession, AsyncSession)):
            bind = db_resource.bind
            if bind is not None:
                dialect = bind.dialect
            else:
                raise TypeError(
                    f"TemplateQueryBuilder: Session has no bind, cannot resolve dialect."
                )
        else:
            raise TypeError(
                f"TemplateQueryBuilder: Unable to resolve dialect from {type(db_resource)}."
            )

        template_identifiers = re.findall(r"{(\w+)}", template_str)
        quoted_identifiers, params = {}, {}
        for key, value in raw_params.items():
            if key in template_identifiers:
                val_str = str(value)
                try:
                    quoted_identifiers[key] = dialect.identifier_preparer.quote(val_str)
                except Exception:
                    quoted_identifiers[key] = f'"{val_str.replace('"', '""')}"'
            else:
                params[key] = value
        final_query_str = template_str.format(**quoted_identifiers)
        query_obj = DDL(final_query_str) if is_ddl else text(final_query_str)
        return query_obj, params


class CommentQueryBuilder(TemplateQueryBuilder):
    """Specialized builder for COMMENT ON statements."""

    def build(self, db_resource: DbResource, raw_params: dict):
        comment_text = raw_params.pop("comment", "")
        query_obj, params = super().build(db_resource, raw_params)
        final_sql = f"{str(query_obj)} $${comment_text}$$"
        compiled_sql = text(final_sql).compile(compile_kwargs={"literal_binds": True})
        return compiled_sql, params


class FunctionQueryBuilder(QueryBuilderStrategy):
    def __init__(self, query_builder_func: QueryBuilderFunction):
        self.query_builder_func = query_builder_func

    def build(self, db_resource, raw_params: dict):
        return self.query_builder_func(db_resource, raw_params)


# --- Executors ---


class BaseExecutor:
    """Core executor logic with wire serialization and post-processing."""

    def __init__(
        self,
        query_builder_strategy: QueryBuilderStrategy,
        post_processor: Optional[Callable] = None,
        **kwargs,
    ):
        self.query_builder_strategy = query_builder_strategy
        self.post_processors = [post_processor] if post_processor else []

    @classmethod
    def from_template(cls, query_template: Union[str, DDL], **kwargs):
        return cls(TemplateQueryBuilder(query_template), **kwargs)

    @classmethod
    def from_builder(cls, query_builder: QueryBuilderFunction, **kwargs):
        return cls(FunctionQueryBuilder(query_builder), **kwargs)

    async def __call__(self, db_resource: DbResource, *args, **kwargs):
        raw_params = args[0] if args else kwargs
        if isinstance(db_resource, str):
            raise TypeError(
                f"BaseExecutor: Expected database resource, got string '{db_resource}'."
            )

        if is_async_resource(db_resource):
            return await self._execute_async_workflow(db_resource, raw_params)
        else:
            return self._execute_sync_workflow(db_resource, raw_params)

    def _execute_sync_workflow(self, db_resource, raw_params):
        if isinstance(db_resource, Engine):
            with db_resource.connect() as conn:
                return self._build_and_execute_sync(conn, raw_params)
        return self._build_and_execute_sync(db_resource, raw_params)

    async def _execute_async_workflow(self, db_resource, raw_params):
        if isinstance(db_resource, AsyncEngine):
            # Manual management to extend lock scope over close()
            # This ensures that connection cleanup (rollback/reset) happens while we hold the wire lock.
            conn = await db_resource.connect()
            try:
                async with _connection_lock_scope(conn):
                    result = await self._build_and_execute_async(conn, raw_params)
                    # Paranoid cleanup: ensure no transaction lingers before return to pool
                    # This helps with StaticPool where the wire is reused immediately
                    if conn.in_transaction():
                        await conn.rollback()
                    await conn.close()
                    return result
            except Exception:
                # Ensure closed if something failed
                try:
                    await conn.close()
                except Exception:
                    pass
                raise
        return await self._build_and_execute_async(db_resource, raw_params)

    async def stream_async_workflow(self, db_resource, raw_params):
        if isinstance(db_resource, (AsyncEngine, Engine)):
            raise TypeError(
                "Cannot stream from an Engine. Please acquire a connection first."
            )
        return await self._build_and_stream_async(db_resource, raw_params)

    def _build_and_execute_sync(self, conn, raw_params: dict):
        if inspect.iscoroutinefunction(self.query_builder_strategy.build):
            raise TypeError(
                "Cannot use an async query builder with a synchronous connection."
            )
        # Store raw_params so DDLExecutor existence checks can access
        # identifier values (e.g. schema) that TemplateQueryBuilder consumes.
        self._raw_params = raw_params
        build_result = self.query_builder_strategy.build(conn, raw_params)
        query_obj, params = cast(BuilderResult, build_result)
        return self._execute_sync(conn, query_obj, params)

    async def _build_and_execute_async(self, conn, raw_params: dict):
        async with _connection_lock_scope(conn):
            # Store raw_params so DDLExecutor existence checks can access
            # identifier values (e.g. schema) that TemplateQueryBuilder consumes.
            self._raw_params = raw_params
            build_result = self.query_builder_strategy.build(conn, raw_params)
            query_obj, params = (
                await build_result
                if inspect.isawaitable(build_result)
                else build_result
            )
            return await self._execute_async(conn, query_obj, params)

    async def _build_and_stream_async(self, conn, raw_params: dict):
        async with _connection_lock_scope(conn):
            build_result = self.query_builder_strategy.build(conn, raw_params)
            query_obj, params = (
                await build_result
                if inspect.isawaitable(build_result)
                else build_result
            )
            return self._stream_async(conn, query_obj, params)

    def _handle_db_exception(self, e: Exception) -> None:
        original_exc = getattr(e, "orig", None)
        pgcode = getattr(original_exc, "pgcode", None)
        if pgcode in PGCODE_EXCEPTION_MAP:
            exception_class = PGCODE_EXCEPTION_MAP[pgcode]
            raise exception_class(
                f"Database error ({pgcode})", original_exception=original_exc
            ) from e
        raise QueryExecutionError(
            "Database query failed.", original_exception=original_exc
        ) from e

    @abstractmethod
    def _execute_sync(self, conn, query_obj: TextClause, params: dict):
        pass

    @abstractmethod
    async def _execute_async(self, conn, query_obj: TextClause, params: dict):
        pass

    async def _stream_async(self, conn, query_obj: TextClause, params: dict):
        raise NotImplementedError(
            f"Streaming not supported by {self.__class__.__name__}"
        )

    def _apply_post_processing_sync(self, result: Any) -> Any:
        for p in self.post_processors:
            result = (
                run_in_event_loop(p(result))
                if inspect.iscoroutinefunction(p)
                else p(result)
            )
        return result

    async def _apply_post_processing_async(self, result: Any) -> Any:
        for p in self.post_processors:
            result = await p(result) if inspect.iscoroutinefunction(p) else p(result)
        return result


class DQLExecutor(BaseExecutor):
    def __init__(self, query_builder_strategy, result_handler, **kwargs):
        super().__init__(query_builder_strategy, **kwargs)
        self.result_handler = result_handler

    def _execute_sync(self, conn: DbConnection, query_obj: TextClause, params: dict):
        try:
            result = conn.execute(query_obj, params)
            processed = self.result_handler(result)
            return self._apply_post_processing_sync(processed)
        except Exception as e:
            self._handle_db_exception(e)

    async def _execute_async(
        self, conn: DbConnection, query_obj: TextClause, params: dict
    ):
        try:
            result = await conn.execute(query_obj, params)
            processed = self.result_handler(result)

            return await self._apply_post_processing_async(processed)
        except Exception as e:
            self._handle_db_exception(e)

    async def _stream_async(
        self, conn: DbConnection, query_obj: TextClause, params: dict
    ):
        try:
            stream_result = await conn.stream(query_obj, params)
            async for row in stream_result.mappings():
                yield await self._apply_post_processing_async(dict(row))
        except Exception as e:
            self._handle_db_exception(e)


def split_ddl(ddl_text: str) -> List[str]:
    """
    Smarter split that respects dollar-quoting (e.g. $$, $BODY$) and '' string literals.
    This avoids breaking function bodies or complex DDL containing semicolons.
    """
    if not ddl_text or ";" not in ddl_text:
        return [ddl_text] if ddl_text else []

    statements = []
    parts = re.split(r"(\$[a-zA-Z0-9_]*\$|'|;)", ddl_text)
    current_stmt = []
    active_dollar_tag = None
    in_quote = False

    for part in parts:
        if re.match(r"^\$[a-zA-Z0-9_]*\$$", part):
            if not in_quote:
                if active_dollar_tag is None:
                    active_dollar_tag = part
                elif active_dollar_tag == part:
                    active_dollar_tag = None
            current_stmt.append(part)
        elif part == "'" and active_dollar_tag is None:
            in_quote = not in_quote
            current_stmt.append(part)
        elif part == ";" and active_dollar_tag is None and not in_quote:
            stmt = "".join(current_stmt).strip()
            if stmt:
                statements.append(stmt)
            current_stmt = []
        else:
            current_stmt.append(part)

    final_stmt = "".join(current_stmt).strip()
    if final_stmt:
        statements.append(final_stmt)
    
    return statements


class DDLExecutor(BaseExecutor):
    """
    Transparently implements DDL Coordination:
    1. In-process deduplication (via StartupCoordinator).
    2. DB-level advisory locking (via query hash) for cross-instance safety.
    3. Retries on conflict.
    4. Guarded by existence checks to avoid redundant locking.
    """

    def __init__(self, query_builder_strategy, existence_check=None, **kwargs):
        super().__init__(query_builder_strategy, **kwargs)
        self.existence_check = existence_check

    async def _call_existence_check(self, conn, params):
        """Invoke existence_check, passing raw_params for inferred checks."""
        check = self.existence_check
        if getattr(check, "_needs_raw_params", False):
            res = check(conn, params, self._raw_params)
        else:
            res = check(conn, params)
        if inspect.isawaitable(res):
            res = await res
        return res

    def _execute_sync(self, conn, query_obj: TextClause, params: dict):
        """Execute DDL with centralized coordination and timeout guards."""
        from .locking_tools import sync_acquire_startup_lock
        import json

        # 1. Faster Optimistic Check
        if self.existence_check:
            try:
                # DDLExecutor._execute_sync is used by DDLQuery in sync contexts.
                # If existence_check is a coroutine (from DDLQuery.__init__), skip here.
                if not inspect.iscoroutinefunction(self.existence_check):
                    if self.existence_check(conn, params):
                        return self._apply_post_processing_sync(None)
            except Exception:
                pass

        stmt_text = query_obj.text if isinstance(query_obj, TextClause) else str(query_obj)
        # Include parameters in hash for proper coordination
        param_str = json.dumps(params, sort_keys=True, default=str) if params else ""
        combined = f"{stmt_text.strip()}|{param_str}"
        stmt_hash = hashlib.sha256(combined.encode()).hexdigest()[:16]
        lock_key = f"ddl.{stmt_hash}"

        with sync_managed_transaction(conn) as tx_conn:
            with sync_acquire_startup_lock(
                tx_conn, lock_key, timeout="10s"
            ) as active_conn:
                if active_conn:
                    try:
                        # Timeout guard to prevent deadlocks
                        active_conn.execute(text("SET LOCAL statement_timeout = '30s'"))

                        # Support multi-statement DDL by splitting
                        statements = split_ddl(stmt_text)
                        if len(statements) > 1:
                            for stmt in statements:
                                active_conn.execute(text(stmt), params)
                        else:
                            active_conn.execute(query_obj, params)
                    except Exception as e:
                        self._handle_db_exception(e)
        return self._apply_post_processing_sync(None)

    async def _execute_async(self, conn, query_obj: TextClause, params: dict):
        """Execute DDL with centralized coordination and timeout guards."""
        from .locking_tools import _get_stable_lock_id
        import json

        # 1. Faster Optimistic Check
        # IMPORTANT: must run inside a SAVEPOINT so that any query failure rolls back
        # only to the savepoint and does NOT poison the outer transaction.
        if self.existence_check:
            try:
                # Use a savepoint if we're already inside a transaction to prevent
                # a failed existence check from aborting the outer transaction.
                if isinstance(conn, (AsyncConnection, AsyncSession)) and conn.in_transaction():
                    try:
                        res = False
                        async with conn.begin_nested() as sp:
                            res = await self._call_existence_check(conn, params)
                            # Force a rollback of this savepoint.
                            # If `existence_check` executed a query that failed and swallowed the error,
                            # the asyncpg connection is in an "aborted" state. If we exit the block
                            # gracefully, SQLAlchemy emits RELEASE SAVEPOINT which fails and poisons
                            # everything. Rolling back explicitly guarantees health restoration.
                            await sp.rollback()

                        if res:
                            return await self._apply_post_processing_async(None)
                    except Exception:
                        pass  # SAVEPOINT was rolled back cleanly; outer tx remains healthy
                else:
                    res = await self._call_existence_check(conn, params)
                    if res:
                        return await self._apply_post_processing_async(None)
            except Exception:
                pass

        stmt_text = query_obj.text if isinstance(query_obj, TextClause) else str(query_obj)
        # Include parameters in hash for proper coordination
        param_str = json.dumps(params, sort_keys=True, default=str) if params else ""
        combined = f"{stmt_text.strip()}|{param_str}"
        stmt_hash = hashlib.sha256(combined.encode()).hexdigest()[:16]
        lock_id = _get_stable_lock_id(f"ddl.{stmt_hash}")

        try:
            async with managed_transaction(conn) as tx_conn:
                # 2. Re-check after acquiring transaction but before locking
                if self.existence_check:
                    res = False
                    if isinstance(tx_conn, (AsyncConnection, AsyncSession)) and tx_conn.in_transaction():
                        async with tx_conn.begin_nested() as sp:
                            res = await self._call_existence_check(tx_conn, params)
                            await sp.rollback()
                    else:
                        res = await self._call_existence_check(tx_conn, params)

                    if res:
                        return await self._apply_post_processing_async(None)

                # 3. Try-lock first for fast failure
                result = await tx_conn.execute(
                    text("SELECT pg_try_advisory_xact_lock(:lock_id)"),
                    {"lock_id": lock_id},
                )
                acquired = result.scalar()

                if not acquired:
                    # Another worker holds the lock. Wait for them to finish so that
                    # their transaction is committed before we re-check existence.
                    await tx_conn.execute(text("SET LOCAL lock_timeout = '30s'"))
                    await tx_conn.execute(
                        text("SELECT pg_advisory_xact_lock(:lock_id)"),
                        {"lock_id": lock_id},
                    )
                    # Re-check: the other worker should have committed the object by now.
                    if self.existence_check:
                        res_post = await self._call_existence_check(tx_conn, params)
                        if res_post:
                            return await self._apply_post_processing_async(None)
                    # Object still doesn't exist — fall through and create it.

                # Timeout guard to prevent DDL hangs
                await tx_conn.execute(text("SET LOCAL statement_timeout = '30s'"))

                # Support multi-statement DDL by splitting (asyncpg limitation)
                statements = split_ddl(stmt_text)
                if len(statements) > 1:
                    for i, stmt in enumerate(statements):
                        await tx_conn.execute(text(stmt), params)
                else:
                    await tx_conn.execute(query_obj, params)

            return await self._apply_post_processing_async(None)
        except Exception as e:
            self._handle_db_exception(e)


class GeoDQLExecutor(DQLExecutor):
    def __init__(
        self, query_builder_strategy, result_handler, post_processor=None, **kwargs
    ):
        super().__init__(
            query_builder_strategy, result_handler=result_handler, **kwargs
        )

        def geo_p(data):
            if data is None:
                return None
            items = [data] if not isinstance(data, list) else data
            processed = [serialize_geom(item) for item in items]
            return (
                processed[0] if not isinstance(data, list) and processed else processed
            )

        self.post_processors = [geo_p] + (
            post_processor
            if isinstance(post_processor, list)
            else ([post_processor] if post_processor else [])
        )
        self.post_processors = [geo_p] + (
            post_processor
            if isinstance(post_processor, list)
            else ([post_processor] if post_processor else [])
        )


# --- Public API Functions ---


@contextmanager
def sync_managed_transaction(db_resource: DbResource):
    """Sync re-entrant transaction manager."""
    if isinstance(db_resource, Engine):
        with db_resource.begin() as conn:
            yield conn
        return

    conn = db_resource
    wire_id = id(_get_wire_identity(conn))
    if conn.in_transaction():
        # Check for poisoned state
        if not getattr(conn, "is_active", True):
            raise DatabaseConnectionError(
                f"Cannot start nested transaction on connection {wire_id}: state is poisoned. "
                "The parent transaction must be rolled back."
            )
        with conn.begin_nested():
            yield conn
    else:
        with conn.begin():
            yield conn


@asynccontextmanager
async def managed_transaction(db_resource: DbResource):
    """Async-native re-entrant transaction manager."""
    if db_resource is None:
        raise ValueError("Cannot start managed_transaction: db_resource is None.")
    is_async = is_async_resource(db_resource)
    if isinstance(db_resource, (AsyncEngine, Engine)):
        if is_async:
            async with db_resource.begin() as conn:
                yield conn
        else:
            with db_resource.begin() as conn:
                yield conn
        return

    conn = db_resource
    async with _connection_lock_scope(conn):
        # 0. Check if connection is already closed
        is_closed = False
        wire_id = id(_get_wire_identity(conn))

        # Perform health check
        try:
            # Check common connection-closed attributes (SQLAlchemy + asyncpg)
            if (
                getattr(conn, "closed", False) is True
                or getattr(conn, "invalidated", False) is True
            ):
                is_closed = True
            elif (
                getattr(getattr(conn, "connection", None), "closed", False) is True
            ):
                is_closed = True
            elif isinstance(conn, (SAConnection, AsyncConnection)):
                # Try to access driver state safely
                drv = conn.driver_connection
                if (
                    getattr(drv, "is_closed", lambda: False)
                    if callable(getattr(drv, "is_closed", None))
                    else getattr(drv, "is_closed", False)
                ):
                    is_closed = True
                elif getattr(drv, "_closed", False):  # asyncpg internal
                    is_closed = True
        except Exception:
            # If we can't check, don't assume it's broken yet.
            pass

        if is_closed:
            raise DatabaseConnectionError(
                f"Cannot start transaction: Connection {wire_id} is closed."
            )

        # 1. Transactional State Guard (SQLAlchemy 2.0 re-entrancy)
        # We rely on the connection's own state. If it is already in a transaction,
        # we start a nested SAVEPOINT. If not, we start a new transaction.
        # We NO LONGER attempt to "fix" poisoned state by calling rollback() here,
        # because if this connection belongs to a parent context manager,
        # an explicit rollback would terminate its transaction logically but
        # leave its context manager open, causing subsequent InvalidRequestErrors.
        if is_async:
            if conn.in_transaction():
                # Check for poisoned state (SQLAlchemy 2.0)
                if not getattr(conn, "is_active", True):
                    raise DatabaseConnectionError(
                        f"Cannot start nested transaction on connection {wire_id}: state is poisoned. "
                        "The parent transaction must be rolled back."
                    )
                # Check the asyncpg wire-level state. SQLAlchemy's is_active only tracks
                # its own rollback; asyncpg may independently mark a transaction as aborted
                # (e.g., after a failed DDL or DML statement). If we call begin_nested() on
                # an asyncpg-aborted transaction, the SAVEPOINT statement itself fails with
                # InFailedSQLTransactionError, which poisons the outer transaction further.
                try:
                    drv = getattr(conn, "driver_connection", None) or getattr(
                        getattr(conn, "connection", None), "driver_connection", None
                    )
                    if drv is not None:
                        proto = getattr(drv, "_protocol", None)
                        if proto is not None and hasattr(proto, "_is_in_transaction"):
                            # asyncpg marks this True only if transaction is active and healthy
                            if not proto._is_in_transaction():
                                raise DatabaseConnectionError(
                                    f"Connection {wire_id} has an asyncpg-aborted transaction. "
                                    "Cannot open a nested SAVEPOINT. The outer transaction must be rolled back."
                                )
                except DatabaseConnectionError:
                    raise
                except Exception:
                    pass  # Safe: if we can't inspect, let asyncpg fail naturally
                async with conn.begin_nested():
                    yield conn
            else:
                async with conn.begin():
                    yield conn

        else:
            if conn.in_transaction():
                # Check for poisoned state
                if not getattr(conn, "is_active", True):
                    raise DatabaseConnectionError(
                        f"Cannot start nested transaction on connection {wire_id}: state is poisoned. "
                        "The parent transaction must be rolled back."
                    )
                with conn.begin_nested():
                    yield conn
            else:
                with conn.begin():
                    yield conn


@asynccontextmanager
async def managed_nested_transaction(conn: DbResource):
    """
    Standardizes nested transaction (SAVEPOINT) management.
    Uses managed_transaction internally for robustness.
    """
    async with managed_transaction(conn) as active_conn:
        yield active_conn


def run_in_event_loop(awaitable: Awaitable[R]) -> R:
    """Safely runs awaitable from sync, preventing nested loop deadlocks."""

    async def _wrapper():
        return await awaitable

    try:
        asyncio.get_running_loop()
        raise RuntimeError("Recursive loop entry detected in run_in_event_loop.")
    except RuntimeError as e:
        if "no current event loop" not in str(e):
            raise e
        if _main_app_loop and _main_app_loop.is_running():
            return asyncio.run_coroutine_threadsafe(_wrapper(), _main_app_loop).result()
    return asyncio.run(_wrapper())


async def reflect_table(schema: str, table_name: str, db_resource: DbResource) -> Table:
    def _load(sync_conn):
        return Table(table_name, _metadata, schema=schema, autoload_with=sync_conn)

    async with managed_transaction(db_resource) as conn:
        if is_async_resource(conn):
            return await conn.run_sync(_load)
        return _load(conn)


class BaseQuery:
    def __init__(self, sql_template, executor_class, **kwargs):
        self._executor = executor_class.from_template(sql_template, **kwargs)

    @property
    def template(self) -> str:
        return str(getattr(self._executor.query_builder_strategy, "query_template", ""))

    async def execute(self, conn: DbResource, **kwargs) -> Any:
        return await self._executor(conn, **kwargs)

    async def stream(self, conn: DbResource, **kwargs):
        if not is_async_resource(conn):
            raise TypeError("Async resources only.")
        return await self._executor.stream_async_workflow(conn, kwargs)


class DQLQuery(BaseQuery):
    def __init__(self, sql_template, *, result_handler, post_processor=None):
        super().__init__(
            sql_template,
            executor_class=DQLExecutor,
            result_handler=result_handler,
            post_processor=post_processor,
        )

    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("", result_handler=kwargs.get("result_handler", ResultHandler.NONE))
        inst._executor = DQLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst


from .ddl_inference import _infer_existence_check, _ddl_existence_cache



class DDLQuery(BaseQuery):
    def __init__(self, sql_template, check_query=None, lock_key=None):
        # We wrap check_query into a function that DDLExecutor can use
        existence_check = None
        if check_query:

            async def existence_check(conn, params):
                if callable(check_query):
                    # Handle callable (may be async or sync)
                    res = check_query()
                    if inspect.isawaitable(res):
                        return await res
                    return res
                if isinstance(check_query, BaseQuery):
                    return await check_query.execute(conn, **params)
                # Handle raw SQL string existence check
                from .locking_tools import DQLQuery, ResultHandler

                return await DQLQuery(
                    check_query, result_handler=ResultHandler.SCALAR
                ).execute(conn, **params)

        elif isinstance(sql_template, str):
            # Auto-infer existence check from CREATE DDL patterns.
            # The inferred check has signature (conn, params, raw_params)
            # where raw_params contains identifier values like schema.
            inferred = _infer_existence_check(sql_template)
            if inferred:
                existence_check = inferred
                existence_check._needs_raw_params = True  # type: ignore[attr-defined]

        super().__init__(
            sql_template,
            executor_class=DDLExecutor,
            existence_check=existence_check,
        )
        self.check_query, self.lock_key = check_query, lock_key

    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("")
        inst._executor = DDLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst

    async def execute(self, conn: DbResource, **kwargs):
        # Delegate entirely to the executor which now handles locking and checks
        return await super().execute(conn, **kwargs)


class DDLBatch:
    """Execute a group of DDL statements under a single sentinel check.

    On warm startup (sentinel exists), the entire batch is skipped in
    **one** DB round-trip instead of N individual existence checks.

    Usage::

        batch = DDLBatch(
            sentinel=DDLQuery("CREATE TABLE IF NOT EXISTS {schema}.my_last_table (id INT);"),
            steps=[
                DDLQuery("CREATE TABLE IF NOT EXISTS {schema}.table_a (id INT);"),
                DDLQuery("CREATE INDEX IF NOT EXISTS idx_a ON {schema}.table_a (id);"),
                DDLQuery("CREATE TABLE IF NOT EXISTS {schema}.my_last_table (id INT);"),
            ],
        )
        await batch.execute(conn, schema="myschema")

    The *sentinel* is typically the last object created in the group.
    If it already exists, all *steps* are skipped. Otherwise, each step
    is executed in order (each with its own auto-inferred existence check
    for idempotency).
    """

    def __init__(self, sentinel: DDLQuery, steps: list[DDLQuery]):
        self.sentinel = sentinel
        self.steps = steps

    async def execute(self, conn: DbResource, **kwargs):
        # Fast-path: check if sentinel object already exists
        sentinel_executor = self.sentinel._executor
        if sentinel_executor.existence_check:
            sentinel_executor._raw_params = kwargs
            try:
                res = await sentinel_executor._call_existence_check(conn, kwargs)
                if res:
                    return  # All DDL already applied — skip entire batch
            except Exception:
                pass  # Sentinel check failed — execute all steps

        # Cold path: execute each DDL in order
        for step in self.steps:
            await step.execute(conn, **kwargs)


class GeoDQLQuery(BaseQuery):
    def __init__(self, sql_template, *, result_handler, post_processor=None):
        super().__init__(
            sql_template,
            executor_class=GeoDQLExecutor,
            result_handler=result_handler,
            post_processor=post_processor,
        )

    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("", result_handler=kwargs.get("result_handler", ResultHandler.NONE))
        inst._executor = GeoDQLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst
