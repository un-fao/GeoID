import asyncio
import functools
import logging
import inspect
from typing import Any, Callable, TypeVar, Optional, List, Tuple

from async_lru import alru_cache as async_lru_cache

logger = logging.getLogger(__name__)

T = TypeVar("T")

def alru_cache(
    maxsize: int = 128,
    typed: bool = False,
    exclude_kwargs: Optional[List[str]] = None,
) -> Callable:
    """
    Extensible async LRU cache decorator.
    Acts as a wrapper around async_lru.alru_cache.

    This permits overriding the behavior (e.g., using Redis or Memorystore
    for central distributed caching) seamlessly in the future without changing
    the method signatures of cached functions.

    It also provides advanced functionality like `exclude_kwargs`, which drops
    specified arguments (e.g., `db_resource`) from the cache key generation.

    Args:
        maxsize: Maximum size of the cache.
        typed: Whether to cache differently based on argument types.
        exclude_kwargs: List of argument names to exclude from the cache key computation.
                        This is critical for excluding DB connections that change
                        across requests but yield the same deterministic results.

    Returns:
        A decorated async function that supports caching.
    """

    def decorator(func: Callable) -> Callable:
        # We need to inspect the signature to handle positional arguments 
        # dynamically when creating the cache key if we drop certain params.
        sig = inspect.signature(func)
        excluded = set(exclude_kwargs) if exclude_kwargs else set()

        @async_lru_cache(maxsize=maxsize, typed=typed)
        async def _cached_func_wrapper(*args, **kwargs):
            # This wrapper is used purely to be targeted by async_lru.
            # The actual execution happens inside the wrapper logic below.
            return await func(*args, **kwargs)

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if not excluded:
                return await _cached_func_wrapper(*args, **kwargs)

            # Bind the arguments to the function signature
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # Create a tuple of arguments excluding the drop_params for the cache
            cache_args_dict = {
                k: v for k, v in bound_args.arguments.items() if k not in excluded
            }

            # We use a secondary function bound to async_lru that only takes the filtered args.
            # However, `async_lru` needs to call the ORIGINAL function with ALL args (including DB conn).
            # To achieve this cleanly with async_lru, we cache a factory that wraps the call
            # or we modify the cache key. async_lru doesn't easily let us change the cache key.
            # A common approach is to pass the excluded kwargs into an un-cached closure, but
            # since the DB connection is required to fetch, we MUST pass it to the real function.
            
            # Since async_lru uses all passed args for the cache key, we can't easily 
            # pass `db_resource` to `_cached_func_wrapper` without it becoming part of the key.
            # Therefore, we use a custom closure cache method:
            raise NotImplementedError("Exclusion of kwargs in async_lru requires custom key generation, which is complex. Using a custom memory dict for now if exclude_kwargs is used.")

        # If we have excluded kwargs, we implement a custom dictionary cache
        # until a full Redis/Memorystore backend handles it.
        # Otherwise, we use the standard async_lru_cache wrapper.
        
        if not excluded:
            return _cached_func_wrapper

        # Custom Memory Cache Implementation for Excluded Params
        cache: dict[tuple, Any] = {}
        locks: dict[tuple, asyncio.Lock] = {}

        @functools.wraps(func)
        async def custom_cache_wrapper(*args, **kwargs):
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Build cache key
            cache_key_elements = []
            for k, v in bound_args.arguments.items():
                if k not in excluded:
                    cache_key_elements.append((k, v))
            cache_key = tuple(cache_key_elements)

            if cache_key in cache:
                return cache[cache_key]

            # Lock to prevent Thundering Herd
            if cache_key not in locks:
                locks[cache_key] = asyncio.Lock()
            
            async with locks[cache_key]:
                # Check again after acquiring lock
                if cache_key in cache:
                    return cache[cache_key]
                
                result = await func(*args, **kwargs)
                
                # Check maxsize
                if len(cache) >= maxsize:
                    # Very crude LRU eviction (pops random/oldest)
                    cache.pop(next(iter(cache)))
                    
                cache[cache_key] = result
                return result

        def cache_clear():
            cache.clear()
            locks.clear()

        custom_cache_wrapper.cache_clear = cache_clear
        return custom_cache_wrapper

    return decorator
