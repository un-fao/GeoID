# dynastore/modules/spanner/session_manager.py
import asyncio
import logging
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager
from abc import ABC
from google.cloud.spanner_v1.pool import AbstractSessionPool
from google.cloud.spanner_v1.session import Session
from google.cloud.spanner_v1.transaction import Transaction
from google.api_core import exceptions as api_exceptions

logger = logging.getLogger(__name__)

class SpannerSessionManager(ABC):
    """
    Manages Spanner sessions/transactions using BurstyPool asynchronously.
    """
    # _instance: Optional['SpannerSessionManager'] = None
    # _lock = asyncio.Lock()
    _pool = None
    def __init__(self, pool:AbstractSessionPool):
        # if SpannerSessionManager._instance is not None:
        #      raise Exception("SpannerSessionManager is a singleton.")
        self._pool = pool
        logger.info("Initializing Spanner Client and Pool...")

    
    # @classmethod
    # async def get_manager(cls, project_id: str, instance_id: str, database_id: str, **pool_kwargs) -> 'SpannerSessionManager':
    #     if cls._instance is None:
    #         async with cls._lock:
    #             if cls._instance is None:
    #                 cls._instance = cls(project_id, instance_id, database_id, **pool_kwargs)
    #     return cls._instance

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[Session, None]:
        session = self._pool.get()
        try:
            yield session
        finally:
            self._pool.put(session)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Transaction, None]:
        session = self._pool.get()
        try:
            with session.transaction() as txn:
                yield txn
        except api_exceptions.Aborted:
            raise 
        except Exception:
            raise
        finally:
            self._pool.put(session)

    # self.database.pool.clear() is used at shutdown

    # async def close(self):
    #     pass
    

@asynccontextmanager
async def get_or_create_session(manager, session=None):
    if session:
        # If a session object is provided, yield it directly.
        # We do NOT exit/close it here; the caller owns that lifecycle.
        yield session
    else:
        # If no session provided, context manage a new one from the manager.
        # This will automatically release the session back to the pool on exit.
        async with manager.get_session() as new_session:
            yield new_session

@asynccontextmanager
async def get_or_create_transaction(manager, tx=None):
    if tx:
        # If tx was passed, yield it and do nothing on exit 
        # (the caller owns the commit/rollback)
        yield tx
    else:
        # If no tx, create a new one. 
        # This will commit automatically when the block ends.
        async with manager.transaction() as new_tx:
            yield new_tx