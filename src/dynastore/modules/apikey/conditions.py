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

# File: dynastore/modules/apikey/conditions.py

import abc
import re
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from starlette.requests import Request
from cachetools import LRUCache

from .models import Condition
from .exceptions import RateLimitExceededError, QuotaExceededError, ApiKeyInvalidError
from dynastore.modules.apikey.apikey_storage import AbstractApiKeyStorage

logger = logging.getLogger(__name__)

@dataclass
class EvaluationContext:
    request: Optional[Request] 
    storage: AbstractApiKeyStorage
    usage_cache: LRUCache
    manager: Optional[Any] = None # ApiKeyService for buffered increments
    api_key_hash: Optional[str] = None # The ID of the API Key (Parent)
    token_identifier: Optional[str] = None # The ID of the specific Token (Child)
    principal_id: Optional[str] = None # The User ID
    path: str = ""
    method: str = ""
    query_params: Dict[str, str] = None
    requested_ttl: int = 0 
    schema: str = "catalog" # Default to global/catalog schema
    catalog_id: Optional[str] = None
    extras: Dict[str, Any] = field(default_factory=dict)

class ConditionHandler(abc.ABC):
    @property
    @abc.abstractmethod
    def type(self) -> str: pass

    @abc.abstractmethod
    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        pass

    async def inspect(self, config: Dict[str, Any], ctx: EvaluationContext) -> Optional[Dict[str, Any]]:
        """
        Returns the current status of the condition (e.g., current usage vs limit).
        Returns None if the condition is stateless or not trackable.
        """
        return None

# --- Helper for Scoped Identity Resolution ---

def resolve_tracking_key(ctx: EvaluationContext, scope: str, metric_name: str) -> Optional[str]:
    """
    Determines the database key for tracking usage based on the requested scope.
    """
    identity = None
    if scope == "token":
        identity = ctx.token_identifier or ctx.api_key_hash
    elif scope == "apikey":
        identity = ctx.api_key_hash
    elif scope == "principal":
        identity = ctx.principal_id
    elif scope == "catalog":
        # The key should be unique per logical catalog for a given principal/key
        if not ctx.api_key_hash:
            return None # Cannot track catalog scope without a key context
        # Use the logical catalog_id for tracking, not the physical schema
        logical_id = ctx.catalog_id or "global"
        identity = f"{ctx.api_key_hash}:{logical_id}"
    elif scope == "global":
        identity = "global"
    
    # Auto-detect if scope not specified
    if not identity:
        if ctx.api_key_hash: identity = ctx.api_key_hash
        elif ctx.principal_id: identity = f"principal:{ctx.principal_id}"

    if not identity:
        return None
        
    return f"{metric_name}:{scope}:{identity}"

# --- Existing Handlers ---

BATCH_SIZE = 10  # Local batching threshold

class RateLimitHandler(ConditionHandler):
    @property
    def type(self) -> str: return "rate_limit"

    def _get_window_start(self, period_sec: int, now: datetime) -> datetime:
        """Calculates the start of the time window based on the period duration."""
        timestamp = now.timestamp()
        
        # For standard small periods (minute, hour, day), simple division works
        if period_sec <= 86400: # <= 1 Day
            window_key = int(timestamp / period_sec)
            return datetime.fromtimestamp(window_key * period_sec, tz=timezone.utc)
        
        # For longer periods (month, year), we align to calendar boundaries
        # This is crucial because "1 year" is not exactly 365*24*60*60 seconds (leap years)
        # and users expect quotas to reset on Jan 1st.
        if period_sec >= 31536000: # ~1 Year
            return datetime(now.year, 1, 1, tzinfo=timezone.utc)
        elif period_sec >= 2592000: # ~1 Month
            return datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        
        # Fallback
        window_key = int(timestamp / period_sec)
        return datetime.fromtimestamp(window_key * period_sec, tz=timezone.utc)

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        limit = config.get("limit") or config.get("max_requests") or 1000
        period_sec = config.get("period_seconds", 60)
        scope = config.get("scope", "apikey")
        
        tracking_key = resolve_tracking_key(ctx, scope, "rl")
        if not tracking_key: 
            return True

        now_utc = datetime.now(timezone.utc)
        
        # Calculate the bucket start time based on the period logic
        period_start = self._get_window_start(period_sec, now_utc)
        
        # Cache key includes the specific window start time
        cache_key = f"{tracking_key}:{period_start.isoformat()}"
        current_count = ctx.usage_cache.get(cache_key)

        if current_count is None:
             # Cache miss: fetch from DB (sum of shards)
             # Note: get_usage sums up the counts for this key and period_start
             db_count = await ctx.storage.get_usage(tracking_key, period_start, schema=ctx.schema)
             current_count = db_count
             ctx.usage_cache[cache_key] = current_count

        if current_count >= limit: 
            raise RateLimitExceededError(f"Rate limit of {limit} requests per {period_sec}s exceeded for scope '{scope}'.")
        
        # Increment local cache
        ctx.usage_cache[cache_key] = current_count + 1
        
        # Optimized: uses internal buffering and aggregation
        if (current_count + 1) % BATCH_SIZE == 0:
            if ctx.manager:
                await ctx.manager.increment_usage(
                    key_hash=tracking_key, 
                    period_start=period_start, 
                    amount=BATCH_SIZE,
                    schema=ctx.schema
                )
            else:
                # Fallback to fire-and-forget background task
                from dynastore.modules.concurrency import run_in_background
                run_in_background(ctx.storage.increment_usage(
                    key_hash=tracking_key, 
                    period_start=period_start, 
                    amount=BATCH_SIZE,
                    last_access=now_utc,
                    schema=ctx.schema
                ), name="fallback_rl_increment")
            
        return True

class MaxCountHandler(ConditionHandler):
    @property
    def type(self) -> str: return "max_count"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        max_count = config.get("max_count", 0)
        scope = config.get("scope", "apikey")
        if max_count <= 0: return True
        
        tracking_key = resolve_tracking_key(ctx, scope, "quota")
        if not tracking_key: return True

        now_utc = datetime.now(timezone.utc)
        epoch_start = datetime.fromtimestamp(0, tz=timezone.utc)
        cache_key = f"quota:{tracking_key}"
        usage_data = ctx.usage_cache.get(cache_key)
        
        if not usage_data:
             db_count = await ctx.storage.get_usage(tracking_key, epoch_start, schema=ctx.schema)
             usage_data = [db_count]
             ctx.usage_cache[cache_key] = usage_data

        # If current usage is already >= limit, reject.
        # This logic assumes we check BEFORE incrementing or increment is separate.
        # But here we are incrementing as well.
        # The key question: Is 'max_count' the number of ALLOWED requests?
        # If max=5, can I make 5 requests? Yes.
        # If I have made 5 requests, used=5. Next request (6th) should be rejected.
        # So: if usage >= max_count -> Reject.
        if usage_data[0] >= max_count: 
            raise QuotaExceededError(f"Total quota of {max_count} requests exceeded for scope '{scope}'.")

        # Increment local cache
        usage_data[0] += 1

        # Optimized: uses internal buffering and aggregation
        # We flush periodically based on BATCH_SIZE or via background aggregator.
        # If this is the Nth request, we add 1 to pending buffer.
        
        # NOTE: If we are running in a test where we manually incremented using increment_key_usage,
        # the DB count might be ahead of local cache if cache was not refreshed.
        # However, increment_key_usage goes through storage/aggregator directly.
        # ctx.usage_cache is local to this context (if created per request) or shared (if passed from manager).
        # ApiKeyService passes self.usage_cache.
        
        if ctx.manager:
            await ctx.manager.increment_usage(
                key_hash=tracking_key, 
                period_start=epoch_start, 
                amount=1, # Increment by 1 for this request
                schema=ctx.schema
            )
        else:
             # Fallback to fire-and-forget background task
            from dynastore.modules.concurrency import run_in_background
            run_in_background(ctx.storage.increment_usage(
                key_hash=tracking_key, 
                period_start=epoch_start, 
                amount=1,
                last_access=now_utc,
                schema=ctx.schema
            ), name="fallback_quota_increment")
            
        return True

    async def inspect(self, config: Dict[str, Any], ctx: EvaluationContext) -> Optional[Dict[str, Any]]:
        max_count = config.get("max_count", 0)
        scope = config.get("scope", "apikey")
        tracking_key = resolve_tracking_key(ctx, scope, "quota")
        
        if not tracking_key: return None

        epoch_start = datetime.fromtimestamp(0, tz=timezone.utc)
        
        # For inspection, force a DB read if not in cache to be accurate
        db_count = await ctx.storage.get_usage(tracking_key, epoch_start, schema=ctx.schema)
        
        return {
            "type": "max_count",
            "scope": scope,
            "limit": max_count,
            "used": db_count,
            "remaining": max(0, max_count - db_count)
        }

class QueryParamHandler(ConditionHandler):
    @property
    def type(self) -> str: return "query_match"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        if not ctx.query_params: return True
        param_key = config.get("param")
        pattern = config.get("pattern")
        if not param_key or not pattern: return True
        
        val = ctx.query_params.get(param_key)
        if val is None: return False
        if not re.fullmatch(pattern, val): return False
        return True

class TimeWindowHandler(ConditionHandler):
    @property
    def type(self) -> str: return "time_window"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        now = datetime.now(timezone.utc)
        
        # Absolute ISO windows
        start_str = config.get("start")
        end_str = config.get("end")
        if start_str:
            try:
                start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                if now < start_dt:
                    raise ApiKeyInvalidError(f"Key is outside of its valid time window (valid from {start_dt.isoformat()}).")
            except ValueError: pass
        if end_str:
            try:
                end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                if now > end_dt:
                    raise ApiKeyInvalidError(f"Key is outside of its valid time window (expired at {end_dt.isoformat()}).")
            except ValueError: pass

        # Hour-based recurring windows
        start_h = config.get("start_hour", 0)
        end_h = config.get("end_hour", 24)
        weekdays_only = config.get("weekdays_only", False)
        
        if weekdays_only and now.weekday() > 4:
            raise ApiKeyInvalidError("Key is outside of its valid time window (only valid on weekdays).")
        if not (start_h <= now.hour < end_h):
            raise ApiKeyInvalidError(f"Key is outside of its valid time window (only valid between {start_h}:00 and {end_h}:00 UTC).")
        return True

class TimeExpirationHandler(ConditionHandler):
    @property
    def type(self) -> str: return "expiration"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        exp_str = config.get("expires_at")
        if not exp_str: return True
        try:
            exp_date = datetime.fromisoformat(exp_str.replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            if now > exp_date:
                raise ApiKeyInvalidError(f"Key expired at {exp_date.isoformat()}.")
        except ValueError:
            logger.error(f"Invalid date format in policy: {exp_str}")
            raise ApiKeyInvalidError("Invalid expiration date format in policy.")
        return True
    
    async def inspect(self, config: Dict[str, Any], ctx: EvaluationContext) -> Optional[Dict[str, Any]]:
        exp_str = config.get("expires_at")
        return {"type": "expiration", "expires_at": exp_str}

class MaxTokenTTLHandler(ConditionHandler):
    @property
    def type(self) -> str: return "max_token_ttl"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        if ctx.requested_ttl <= 0: return True
        max_ttl = config.get("max_ttl_seconds", 3600)
        if ctx.requested_ttl > max_ttl: return False
        return True

# --- Logical and Support Handlers ---

class AttributeMatchHandler(ConditionHandler):
    """
    Matches an attribute from the context against a value.
    Syntax for attribute: 'query.param_name', 'header.header-name', 'path', 'method', 'principal.id', 'principal.attributes.key'.
    """
    @property
    def type(self) -> str: return "match"

    def _get_value(self, path: str, ctx: EvaluationContext) -> Any:
        try:
            if path.startswith("query."):
                return ctx.query_params.get(path[6:]) if ctx.query_params else None
            elif path.startswith("header."):
                return ctx.request.headers.get(path[7:]) if ctx.request and ctx.request.headers else None
            elif path == "path":
                return ctx.path
            elif path == "method":
                return ctx.method
            elif path == "principal.id":
                return ctx.principal_id
            elif path.startswith("principal.attributes."):
                # We need to access principal object from extras or somewhere
                # For now assume it's in extras['principal_obj']
                p_obj = ctx.extras.get("principal_obj")
                if p_obj and hasattr(p_obj, "attributes"):
                     return p_obj.attributes.get(path[21:])
                return None
            elif path.startswith("extras."):
                return ctx.extras.get(path[7:])
        except Exception:
            return None
        return None

    def _compare(self, actual: Any, operator: str, expected: Any) -> bool:
        if operator == "eq": return str(actual) == str(expected)
        if operator == "neq": return str(actual) != str(expected)
        if operator == "contains": return str(expected) in str(actual)
        if operator == "regex": return bool(re.match(str(expected), str(actual)))
        if operator == "gt": 
            try: return float(actual) > float(expected)
            except Exception: return False
        if operator == "lt":
            try: return float(actual) < float(expected)
            except Exception: return False
        if operator == "in":
            if isinstance(expected, list): return actual in expected
            return actual in str(expected).split(",")
        return False

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        attr_path = config.get("attribute")
        if not attr_path: return True
        
        actual_value = self._get_value(attr_path, ctx)
        if actual_value is None: return False
        
        operator = config.get("operator", "eq")
        expected_value = config.get("value")
        
        return self._compare(actual_value, operator, expected_value)

class LogicalAndHandler(ConditionHandler):
    @property
    def type(self) -> str: return "and"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        conditions_cfg = config.get("conditions", [])
        for c_cfg in conditions_cfg:
            c = Condition(**c_cfg)
            if not await evaluate_condition(c, ctx):
                return False
        return True

class LogicalOrHandler(ConditionHandler):
    @property
    def type(self) -> str: return "or"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        conditions_cfg = config.get("conditions", [])
        if not conditions_cfg: return True
        for c_cfg in conditions_cfg:
            c = Condition(**c_cfg)
            if await evaluate_condition(c, ctx):
                return True
        return False

class LogicalNotHandler(ConditionHandler):
    @property
    def type(self) -> str: return "not"

    async def evaluate(self, config: Dict[str, Any], ctx: EvaluationContext) -> bool:
        c_cfg = config.get("condition")
        if not c_cfg: return True
        c = Condition(**c_cfg)
        return not await evaluate_condition(c, ctx)

# --- Registry ---

class ConditionRegistry:
    def __init__(self):
        self._handlers: Dict[str, ConditionHandler] = {}
        self.register(RateLimitHandler())
        self.register(MaxCountHandler())
        self.register(QueryParamHandler())
        self.register(TimeWindowHandler())
        self.register(TimeExpirationHandler())
        self._handlers["time_expiration"] = self._handlers["expiration"] # Alias
        self.register(MaxTokenTTLHandler())
        self.register(AttributeMatchHandler())
        self.register(LogicalAndHandler())
        self.register(LogicalOrHandler())
        self.register(LogicalNotHandler())

    def register(self, handler: ConditionHandler):
        self._handlers[handler.type] = handler

    async def evaluate_all(self, conditions: List[Condition], ctx: EvaluationContext) -> bool:
        if not conditions: return True
        for condition in conditions:
            handler = self._handlers.get(condition.type)
            if not handler:
                logger.warning(f"Unknown condition type: {condition.type}. Skipping.")
                continue
            allowed = await handler.evaluate(condition.config, ctx)
            if not allowed: return False
        return True
    
    async def inspect_all(self, conditions: List[Condition], ctx: EvaluationContext) -> List[Dict[str, Any]]:
        results = []
        if not conditions: return results
        
        for condition in conditions:
            handler = self._handlers.get(condition.type)
            if handler:
                status = await handler.inspect(condition.config, ctx)
                if status:
                    results.append(status)
        return results

condition_registry = ConditionRegistry()

# --- Legacy Aliases ---
ConditionManager = ConditionRegistry
condition_manager = condition_registry

async def evaluate_condition(condition: Condition, ctx: EvaluationContext) -> bool:
    """Helper to evaluate a single condition."""
    return await condition_registry.evaluate_all([condition], ctx)

async def evaluate_conditions(conditions: List[Condition], ctx: EvaluationContext) -> bool:
    """Helper to evaluate a list of conditions."""
    return await condition_registry.evaluate_all(conditions, ctx)

def register_condition_handler(handler: ConditionHandler):
    """
    Public SPI to register custom condition handlers.
    Example:
        from dynastore.modules.apikey.conditions import register_condition_handler, ConditionHandler
        class MyHandler(ConditionHandler): ...
        register_condition_handler(MyHandler())
    """
    condition_registry.register(handler)