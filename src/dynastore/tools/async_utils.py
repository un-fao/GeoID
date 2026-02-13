import logging
import asyncio
from typing import Optional, Any, Iterator, Callable, List, Awaitable, Dict
from dynastore.modules.db_config.query_executor import run_in_event_loop as _real_run

logger = logging.getLogger(__name__)

def run_in_event_loop(awaitable: Awaitable[Any]) -> Any:
    """
    Deprecated bridge: Runs an awaitable from a synchronous context.
    Use `dynastore.modules.db_config.query_executor.run_in_event_loop` directly.
    """
    return _real_run(awaitable)

class SyncQueueIterator:
    """
    Bridge between async queue and sync iterator.
    
    Allows sync code (running in a thread) to consume items from an async queue.
    Used by tasks to bridge async data producers (like DB streaming) and sync consumers (like file writers).
    """
    
    def __init__(self, queue: 'asyncio.Queue', loop: 'asyncio.AbstractEventLoop'):
        self.queue = queue
        self.loop = loop
    
    def __iter__(self) -> Iterator[Any]:
        return self
    
    def __next__(self) -> Any:
        try:
            # We use run_coroutine_threadsafe to get the next item from the queue
            # since this is called from a synchronous context (a thread).
            future = asyncio.run_coroutine_threadsafe(self.queue.get(), self.loop)
            item = future.result()
            if item is None:
                raise StopIteration
            return item
        except StopIteration:
            raise
        except Exception as e:
            logger.error(f"Error retrieving item from queue: {e}")
            raise StopIteration

class AsyncBufferAggregator:
    """
    Generic aggregator that buffers items and flushes them in batches.
    Useful for high-throughput accounting/stats where individual DB writes are too expensive.
    
    Supports:
    - Time-based flushing (e.g. every 5 seconds)
    - Threshold-based flushing (e.g. every 100 items)
    - Key-based aggregation (e.g. summing increments for the same API key)
    """
    def __init__(
        self, 
        flush_callback: Callable[[List[Any]], Awaitable[None]],
        threshold: int = 100,
        interval: float = 5.0,
        name: str = "aggregator"
    ):
        self._callback = flush_callback
        self._threshold = threshold
        self._interval = interval
        self._name = name
        self._buffer: List[Any] = []
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_event = asyncio.Event()
        self._last_flush = asyncio.get_event_loop().time()

    async def add(self, item: Any):
        """Adds an item to the buffer and triggers flush if threshold reached."""
        async with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self._threshold:
                self._flush_event.set()

    async def _trigger_flush(self, wait: bool = False) -> Optional[asyncio.Task]:
        """Internal: Drains buffer and executes callback."""
        if not self._buffer:
            return None

        to_flush = self._buffer[:]
        self._buffer.clear()
        self._last_flush = asyncio.get_event_loop().time()
        
        # We wrap the callback in another lock check if we want to serialize flushes.
        # But wait, run_in_background is decoupled.
        # Let's ensure ONE flush runs at a time for THIS aggregator.
        
        from dynastore.modules.concurrency import run_in_background
        
        async def _flush_locked():
            # This second internal lock ensures that callback executions for this
            # specific aggregator are serialized.
            if not hasattr(self, '_flush_exec_lock'):
                self._flush_exec_lock = asyncio.Lock()
                
            async with self._flush_exec_lock:
                await self._callback(to_flush)
                
        task = run_in_background(_flush_locked(), name=f"flush_{self._name}")
        if wait:
             await task
        return task

    async def start(self):
        """Starts the periodic flush loop."""
        if self._flush_task:
            return
        self._flush_task = asyncio.create_task(self._loop())

    async def stop(self):
        """Stops the loop and performs a final flush."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        await self._trigger_flush(wait=True)

    async def _loop(self):
        """Background loop for time-based and event-based flushing."""
        while True:
            try:
                # Wait for either the event (threshold reached) or the timeout (interval reached)
                try:
                    await asyncio.wait_for(self._flush_event.wait(), timeout=self._interval)
                except asyncio.TimeoutError:
                    pass # Interval reached

                async with self._lock:
                    self._flush_event.clear()
                    await self._trigger_flush(wait=True)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in {self._name} loop: {e}")

class KeyValueAggregator(AsyncBufferAggregator):
    """
    Specialized aggregator that sums values for the same key.
    Useful for usage counters.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._kv_buffer: Dict[Any, int] = {}

    async def add_increment(self, key: Any, amount: int = 1):
        async with self._lock:
            self._kv_buffer[key] = self._kv_buffer.get(key, 0) + amount
            if len(self._kv_buffer) >= self._threshold:
                 self._flush_event.set()

    async def _trigger_flush(self, wait: bool = False) -> Optional[asyncio.Task]:
        if not self._kv_buffer:
            return None

        to_flush = list(self._kv_buffer.items())
        self._kv_buffer.clear()
        self._last_flush = asyncio.get_event_loop().time()

        from dynastore.modules.concurrency import run_in_background

        async def _flush_locked():
            if not hasattr(self, '_flush_exec_lock'):
                self._flush_exec_lock = asyncio.Lock()
            async with self._flush_exec_lock:
                await self._callback(to_flush)
                
        task = run_in_background(_flush_locked(), name=f"flush_{self._name}")
        if wait:
             await task
        return task
