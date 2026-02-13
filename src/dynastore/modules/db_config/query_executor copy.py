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
from abc import abstractmethod, ABC
from contextlib import asynccontextmanager
from sqlalchemy import text, DDL
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection as SAConnection
from sqlalchemy.engine.result import Result
from sqlalchemy.orm import Session as SASession
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, AsyncEngine, AsyncTransaction
from sqlalchemy.exc import ProgrammingError, IntegrityError, OperationalError
from geoalchemy2.shape import to_shape
from geoalchemy2.elements import WKBElement, WKTElement, _SpatialElement
from sqlalchemy import Table, MetaData
from typing import Union, List, Callable, Any, Awaitable, Tuple, TypeAlias, TypeVar, ParamSpec, Optional, cast, Type
from pydantic import BaseModel
from .exceptions import QueryExecutionError, PGCODE_EXCEPTION_MAP

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
    [DbResource, dict],
    Union[BuilderResult, Awaitable[BuilderResult]]
    ]

R = TypeVar("R")
P = ParamSpec("P")

logger = logging.getLogger(__name__)

_metadata = MetaData()

# --- Connection Serialization (Re-entrant Async Wire Lock) ---

# Stores one asyncio.Lock per underlying physical connection wire (asyncpg.Connection).
_conn_locks = weakref.WeakKeyDictionary()
# Tracks connection object IDs already locked by the current task to allow re-entrancy.
_locked_ids = contextvars.ContextVar("locked_ids", default=frozenset())

def _get_wire_identity(conn: Any) -> Any:
    """
    Safely drills down to find a stable identity for the connection wire 
    without triggering prohibited SQLAlchemy properties like .connection.
    """
    # 1. Handle AsyncSession
    if hasattr(conn, 'sync_session'): 
        conn = getattr(conn.sync_session, "_connection", conn)
    
    # 2. Drill down through SQLAlchemy wrappers (Connection -> Adapter -> asyncpg)
    # We use a safe depth traversal to reach the physical connection wire.
    curr = conn
    for _ in range(5):
        nxt = getattr(curr, "_connection", None)
        if nxt is None or nxt is curr:
            break
        curr = nxt
    
    return curr

@asynccontextmanager
async def _connection_lock_scope(conn: DbResource):
    """
    Serializes access to the physical connection wire to prevent asyncpg InterfaceErrors.
    Nested calls within the same coroutine proceed immediately (re-entrancy).
    """
    # Engines create fresh wires for each request, so we only lock on connection instances.
    if not is_async_resource(conn) or isinstance(conn, (AsyncEngine, Engine)):
        yield
        return
        
    wire = _get_wire_identity(conn)
    wire_id = id(wire)
    locked_set = _locked_ids.get()
    
    if wire_id in locked_set:
        yield # Wire already locked by this specific coroutine task
    else:
        if wire not in _conn_locks:
            _conn_locks[wire] = asyncio.Lock()
        
        async with _conn_locks[wire]:
            # Register this wire instance as locked for this task context
            token = _locked_ids.set(locked_set | {wire_id})
            try:
                yield
            finally:
                _locked_ids.reset(token)
                # Force a yield to allow internal asyncpg wire cleanup
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
    return (
        isinstance(db_resource, (AsyncEngine, AsyncConnection, AsyncSession)) or
        (hasattr(db_resource, 'begin_nested') and inspect.iscoroutinefunction(db_resource.begin_nested))
    )

def serialize_geom(item):
    """Converts geometry elements to GeoJSON-compatible dictionaries."""
    if not isinstance(item, dict) and not hasattr(item, '_asdict'):
        return item
    data = item if isinstance(item, dict) else item._asdict()
    for geom_col in ['geom', 'bbox_geom', 'simplified_geom']:
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
    ONE_DICT = lambda r: (row._asdict() if (row := r.fetchone()) else None)
    NONE = lambda r: None

class PydanticResultHandler(ResultHandler):
    """Extends ResultHandler to include Pydantic model conversion."""
    @staticmethod
    def pydantic_one(model_class: Type[BaseModel]):
        def handler(result_proxy: Result) -> Optional[BaseModel]:
            row = result_proxy.fetchone()
            if row: return model_class.model_validate(row._asdict())
            return None
        return handler

    @staticmethod
    def pydantic_all(model_class: Type[BaseModel]):
        def handler(result_proxy: Result) -> List[BaseModel]:
            return [model_class.model_validate(row._asdict()) for row in result_proxy.fetchall()]
        return handler

# --- Query Builder Strategies ---

class QueryBuilderStrategy(ABC):
    @abstractmethod
    def build(self, db_resource: DbResource, raw_params: dict) -> Union[BuilderResult, Awaitable[BuilderResult]]:
        pass

class TemplateQueryBuilder(QueryBuilderStrategy):
    """Builds a query from a string template with {identifier} substitutions."""
    def __init__(self, query_template: Union[str, DDL]):
        self.query_template = query_template

    def build(self, db_resource: DbResource, raw_params: dict):
        is_ddl = isinstance(self.query_template, DDL)
        template_str = str(self.query_template)
        
        if hasattr(db_resource, 'engine') and hasattr(db_resource.engine, 'dialect'):
            dialect = db_resource.engine.dialect
        elif hasattr(db_resource, 'dialect'):
            dialect = db_resource.dialect
        else:
            raise TypeError(f"TemplateQueryBuilder: Unable to resolve dialect from {type(db_resource)}.")
        
        template_identifiers = re.findall(r"{(\w+)}", template_str)
        quoted_identifiers, params = {}, {}
        for key, value in raw_params.items():
            if key in template_identifiers:
                val_str = str(value)
                try:
                    quoted_identifiers[key] = dialect.identifier_preparer.quote(val_str)
                except Exception:
                    quoted_identifiers[key] = f'"{val_str.replace("\"", "\"\"")}"'
            else:
                params[key] = value
        final_query_str = template_str.format(**quoted_identifiers)
        query_obj = DDL(final_query_str) if is_ddl else text(final_query_str)
        return query_obj, params

class CommentQueryBuilder(TemplateQueryBuilder):
    """Specialized builder for COMMENT ON statements."""
    def build(self, db_resource: DbResource, raw_params: dict):
        comment_text = raw_params.pop('comment', '')
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
    def __init__(self, query_builder_strategy: QueryBuilderStrategy, post_processor: Optional[Callable] = None):
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
            raise TypeError(f"BaseExecutor: Expected database resource, got string '{db_resource}'.")

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
            async with db_resource.connect() as conn:
                return await self._build_and_execute_async(conn, raw_params)
        return await self._build_and_execute_async(db_resource, raw_params)

    async def stream_async_workflow(self, db_resource, raw_params):
        if isinstance(db_resource, (AsyncEngine, Engine)):
             raise TypeError("Cannot stream from an Engine. Please acquire a connection first.")
        return await self._build_and_stream_async(db_resource, raw_params)

    def _build_and_execute_sync(self, conn, raw_params: dict):
        if inspect.iscoroutinefunction(self.query_builder_strategy.build):
            raise TypeError("Cannot use an async query builder with a synchronous connection.")
        build_result = self.query_builder_strategy.build(conn, raw_params)
        query_obj, params = cast(BuilderResult, build_result)
        return self._execute_sync(conn, query_obj, params)

    async def _build_and_execute_async(self, conn, raw_params: dict):
        async with _connection_lock_scope(conn):
            build_result = self.query_builder_strategy.build(conn, raw_params)
            query_obj, params = await build_result if inspect.isawaitable(build_result) else build_result
            return await self._execute_async(conn, query_obj, params)

    async def _build_and_stream_async(self, conn, raw_params: dict):
        async with _connection_lock_scope(conn):
            build_result = self.query_builder_strategy.build(conn, raw_params)
            query_obj, params = await build_result if inspect.isawaitable(build_result) else build_result
            return self._stream_async(conn, query_obj, params)

    def _handle_db_exception(self, e: Exception) -> None:
        original_exc = getattr(e, 'orig', None)
        pgcode = getattr(original_exc, 'pgcode', None)
        if pgcode in PGCODE_EXCEPTION_MAP:
            exception_class = PGCODE_EXCEPTION_MAP[pgcode]
            raise exception_class(f"Database error ({pgcode})", original_exception=original_exc) from e
        raise QueryExecutionError("Database query failed.", original_exception=original_exc) from e

    @abstractmethod
    def _execute_sync(self, conn, query_obj: TextClause, params: dict): pass

    @abstractmethod
    async def _execute_async(self, conn, query_obj: TextClause, params: dict): pass

    async def _stream_async(self, conn, query_obj: TextClause, params: dict):
        raise NotImplementedError(f"Streaming not supported by {self.__class__.__name__}")

    def _apply_post_processing_sync(self, result: Any) -> Any:
        for p in self.post_processors:
            result = run_in_event_loop(p(result)) if inspect.iscoroutinefunction(p) else p(result)
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
        except Exception as e: self._handle_db_exception(e)

    async def _execute_async(self, conn: DbConnection, query_obj: TextClause, params: dict):
        try:
            result = await conn.execute(query_obj, params)
            processed = self.result_handler(result)
            if inspect.isawaitable(processed): processed = await processed
            return await self._apply_post_processing_async(processed)
        except Exception as e: self._handle_db_exception(e)

    async def _stream_async(self, conn: DbConnection, query_obj: TextClause, params: dict):
        try:
            stream_result = await conn.stream(query_obj, params)
            async for row in stream_result.mappings():
                 yield await self._apply_post_processing_async(dict(row))
        except Exception as e: self._handle_db_exception(e)

class DDLExecutor(BaseExecutor):
    """
    Transparently implements DDL Coordination:
    1. Retries on conflict.
    2. Sharded locking.
    """
    def _execute_sync(self, conn, query_obj: TextClause, params: dict):
        try:
            conn.execute(query_obj, params)
            return self._apply_post_processing_sync(None)
        except Exception as e: self._handle_db_exception(e)

    async def _execute_async(self, conn, query_obj: TextClause, params: dict):
        from .locking_tools import retry_on_lock_conflict, acquire_startup_lock
        
        stmt_text = query_obj.text if hasattr(query_obj, 'text') else str(query_obj)
        stmt_hash = hashlib.md5(stmt_text.encode()).hexdigest()[:16]
        lock_key = f"ddl_coord.{stmt_hash}"

        @retry_on_lock_conflict(max_retries=5)
        async def _run():
            try:
                async with acquire_startup_lock(conn, lock_key) as active_conn:
                    if active_conn:
                        await conn.execute(text("SET LOCAL lock_timeout = '5s'"))
                        await conn.execute(query_obj, params)
                return await self._apply_post_processing_async(None)
            except Exception as e:
                self._handle_db_exception(e)
        return await _run()

class GeoDQLExecutor(DQLExecutor):
    def __init__(self, query_builder_strategy, result_handler, post_processor=None, **kwargs):
        super().__init__(query_builder_strategy, result_handler=result_handler, **kwargs)
        def geo_p(data):
            if data is None: return None
            items = [data] if not isinstance(data, list) else data
            processed = [serialize_geom(item) for item in items]
            return processed[0] if not isinstance(data, list) and processed else processed
        self.post_processors = [geo_p] + (post_processor if isinstance(post_processor, list) else ([post_processor] if post_processor else []))

# --- Public API Functions ---

@asynccontextmanager
async def managed_transaction(db_resource: DbResource):
    """Async-native re-entrant transaction manager."""
    is_async = is_async_resource(db_resource)
    if isinstance(db_resource, (AsyncEngine, Engine)):
        if is_async:
            async with db_resource.begin() as conn: yield conn
        else:
            with db_resource.begin() as conn: yield conn
        return

    conn = db_resource
    async with _connection_lock_scope(conn):
        if is_async:
            if conn.in_transaction():
                async with conn.begin_nested(): yield conn
            else:
                async with conn.begin(): yield conn
        else:
            if conn.in_transaction():
                with conn.begin_nested(): yield conn
            else:
                with conn.begin(): yield conn

@asynccontextmanager
async def managed_nested_transaction(conn: DbResource):
    if is_async_resource(conn):
        if isinstance(conn, AsyncEngine):
            async with conn.begin() as real_conn: yield real_conn
        else:
            async with _connection_lock_scope(conn):
                async with conn.begin_nested(): yield conn
    else:
        if isinstance(conn, Engine):
            with conn.connect() as rc:
                with rc.begin(): yield rc
        else:
            with conn.begin_nested(): yield rc

def run_in_event_loop(awaitable: Awaitable[R]) -> R:
    """Safely runs awaitable from sync, preventing nested loop deadlocks."""
    async def _wrapper(): return await awaitable
    try:
        asyncio.get_running_loop()
        raise RuntimeError("Recursive loop entry detected in run_in_event_loop.")
    except RuntimeError as e:
        if "no current event loop" not in str(e): raise e
        if _main_app_loop and _main_app_loop.is_running():
            return asyncio.run_coroutine_threadsafe(_wrapper(), _main_app_loop).result()
    return asyncio.run(_wrapper())

async def reflect_table(schema: str, table_name: str, db_resource: DbResource) -> Table:
    def _load(sync_conn): return Table(table_name, _metadata, schema=schema, autoload_with=sync_conn)
    async with managed_transaction(db_resource) as conn:
        if is_async_resource(conn): return await conn.run_sync(_load)
        return _load(conn)

class BaseQuery:
    def __init__(self, sql_template, executor_class, **kwargs):
        self._executor = executor_class.from_template(sql_template, **kwargs)
    @property
    def template(self) -> str:
        return str(getattr(self._executor.query_builder_strategy, 'query_template', ""))
    async def execute(self, conn: DbResource, **kwargs) -> Any:
        return await self._executor(conn, **kwargs)
    async def stream(self, conn: DbResource, **kwargs):
        if not is_async_resource(conn): raise TypeError("Async resources only.")
        return await self._executor.stream_async_workflow(conn, kwargs)

class DQLQuery(BaseQuery):
    def __init__(self, sql_template, *, result_handler, post_processor=None):
        super().__init__(sql_template, executor_class=DQLExecutor, result_handler=result_handler, post_processor=post_processor)
    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("", result_handler=kwargs.get('result_handler', ResultHandler.NONE))
        inst._executor = DQLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst

class DDLQuery(BaseQuery):
    def __init__(self, sql_template, check_query=None, lock_key=None):
        super().__init__(sql_template, executor_class=DDLExecutor)
        self.check_query, self.lock_key = check_query, lock_key
    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("")
        inst._executor = DDLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst
    async def execute(self, conn: DbResource, **kwargs):
        if self.check_query and self.lock_key:
            from .locking_tools import acquire_lock_if_needed
            async def cf():
                if isinstance(self.check_query, BaseQuery): return await self.check_query.execute(conn, **kwargs)
                return await DQLQuery(self.check_query, result_handler=ResultHandler.SCALAR).execute(conn, **kwargs)
            async with acquire_lock_if_needed(conn, self.lock_key.format(**kwargs), cf) as sr:
                if sr: return await super().execute(conn, **kwargs)
        else: return await super().execute(conn, **kwargs)

class GeoDQLQuery(BaseQuery):
    def __init__(self, sql_template, *, result_handler, post_processor=None):
        super().__init__(sql_template, executor_class=GeoDQLExecutor, result_handler=result_handler, post_processor=post_processor)
    @classmethod
    def from_builder(cls, builder, **kwargs):
        inst = cls("", result_handler=kwargs.get('result_handler', ResultHandler.NONE))
        inst._executor = GeoDQLExecutor(FunctionQueryBuilder(builder), **kwargs)
        return inst