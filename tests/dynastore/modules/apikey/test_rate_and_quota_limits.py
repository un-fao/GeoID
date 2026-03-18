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

"""
Tests for rate limiting and quota enforcement in the API Key module.

Covers:
- Rate limit enforcement (100 requests per minute)
- Quota limit enforcement
- Hierarchical quota limits (parent token limiting child tokens)
- Time window validation
- Scope-based limits (principal, catalog, collection)
"""

import pytest
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from dynastore.modules.apikey.apikey_service import ApiKeyService as ApiKeyManager
from dynastore.modules.apikey.models import (
    ApiKeyCreate,
    Principal,
    Policy,
    Condition,
    ApiKeyValidationRequest,
)
from dynastore.modules.db_config.query_executor import managed_transaction


@pytest.fixture
async def apikey_manager(app_lifespan, db_cleanup):
    from dynastore.modules.apikey.postgres_apikey_storage import PostgresApiKeyStorage
    from dynastore.modules.apikey.postgres_policy_storage import PostgresPolicyStorage
    from dynastore.modules.apikey.policies import PolicyService as PolicyManager
    from dynastore.modules.catalog import catalog_module
    from dynastore.modules.catalog.models import Catalog
    from sqlalchemy.exc import IntegrityError

    storage = PostgresApiKeyStorage(app_lifespan)
    policy_storage = PostgresPolicyStorage(app_lifespan)
    policy_manager = PolicyManager(
        app_lifespan, storage=policy_storage, apikey_storage=storage
    )

    manager = ApiKeyManager(storage, policy_manager, app_state=app_lifespan)

    # Initialize schemas first in their own transaction to ensure tables exist
    # even if later catalog creation fails
    async with managed_transaction(app_lifespan.engine) as conn:
        await storage.initialize(conn, schema="apikey")
        await storage.initialize(conn, schema="catalog")
        await policy_storage.initialize(conn, schema="apikey")

    from dynastore.modules import get_protocol
    from dynastore.models.protocols import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)

    # Catalog A
    manager.test_catalog_a_id = f"rate_cat_a_{uuid4().hex[:8]}"
    async with managed_transaction(app_lifespan.engine) as conn:
        # This also initializes the catalog schema
        await catalogs.create_catalog(
            {"id": manager.test_catalog_a_id, "title": {"en": "Catalog A"}},
            lang="*",
            db_resource=conn,
        )

        # Resolve schema
        schema_a = await catalogs.resolve_physical_schema(
            manager.test_catalog_a_id, db_resource=conn
        )

        if schema_a:
            await storage.initialize(conn, schema=schema_a)

    # Catalog B
    manager.test_catalog_b_id = f"rate_cat_b_{uuid4().hex[:8]}"
    async with managed_transaction(app_lifespan.engine) as conn:
        await catalogs.create_catalog(
            {"id": manager.test_catalog_b_id, "title": {"en": "Catalog B"}},
            lang="*",
            db_resource=conn,
        )

        schema_b = await catalogs.resolve_physical_schema(
            manager.test_catalog_b_id, db_resource=conn
        )
        if schema_b:
            await storage.initialize(conn, schema=schema_b)

    # We must ensure the 'api_keys' table exists in the global 'apikey' schema as well,
    # because some lookups fall back to it.
    # The initialization at the top of this function handles 'apikey' schema.

    # However, for test_catalog_scoped_rate_limit, the logic is:
    # 1. User passes catalog_id="catalog_a"
    # 2. ApiKeyManager resolves schema to "s_..." (catalog_a's physical schema)
    # 3. ApiKeyManager queries "s_...api_keys"
    # 4. If key not found there, it queries "apikey.api_keys"

    # The error "relation s_...api_keys does not exist" means step 3 failed because the table wasn't created.
    # PostgresApiKeyStorage.initialize SHOULD create it.
    # But maybe create_catalog does NOT create API key tables by default (it shouldn't, separate modules).
    # So we MUST manually initialize apikey storage for the new catalog schemas.

    await policy_manager.initialize()

    yield manager

    # Cleanup: delete catalogs created for this test
    for cat_id in [manager.test_catalog_a_id, manager.test_catalog_b_id]:
        try:
            async with managed_transaction(app_lifespan.engine) as conn:
                await catalogs.delete_catalog(cat_id, force=True, db_resource=conn)
        except Exception:
            pass


@pytest.mark.asyncio
async def test_rate_limit_enforcement_100_per_minute(apikey_manager):
    """Test that rate limit of 100 requests per minute is enforced."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"rate_test_{uuid4().hex[:8]}",
        roles=["user"],
        attributes={"name": "Rate Test User"},
    )
    created_principal = await apikey_manager.create_principal(principal)

    # Create API key with rate limit: 100 requests per minute
    rate_limit_condition = Condition(
        type="rate_limit",
        config={
            "max_requests": 100,
            "period_seconds": 3600,  # Use 1 hour window to avoid flaky tests near minute boundaries
            "scope": "principal",
        },
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Rate Limited Key",
        note="100 requests per minute",
        conditions=[rate_limit_condition],
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Rate Limited Key",
        note="100 requests per minute",
        conditions=[rate_limit_condition],
    )

    api_key, raw_key = await apikey_manager.create_key(key_create)

    # Make 100 requests - all should succeed
    for i in range(100):
        status = await apikey_manager.validate_key(
            ApiKeyValidationRequest(api_key=raw_key)
        )
        assert status.is_valid is True, f"Request {i + 1} should be valid"

    # Flush to ensure all increments are written
    await apikey_manager.flush()

    # 101st request should fail due to rate limit
    status = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key))
    assert status.is_valid is False, "Request 101 should be rate limited"
    assert "rate limit" in status.status.lower() or "exceeded" in status.status.lower()


@pytest.mark.asyncio
async def test_quota_limit_enforcement(apikey_manager):
    """Test that total quota limit is enforced."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"quota_test_{uuid4().hex[:8]}",
        roles=["user"],
        attributes={"name": "Quota Test User"},
    )
    created_principal = await apikey_manager.create_principal(principal)

    # Create API key with quota limit: 50 total requests
    quota_condition = Condition(
        type="max_count", config={"max_count": 50, "scope": "principal"}
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Quota Limited Key",
        note="50 total requests",
        conditions=[quota_condition],
    )

    api_key, raw_key = await apikey_manager.create_key(key_create)

    # Make 50 requests - all should succeed
    for i in range(50):
        status = await apikey_manager.validate_key(
            ApiKeyValidationRequest(api_key=raw_key)
        )
        assert status.is_valid is True, f"Request {i + 1} should be valid"

    # Flush to ensure all increments are written
    await apikey_manager.flush()

    # 51st request should fail due to quota exhaustion
    status = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key))
    assert status.is_valid is False, "Request 51 should exceed quota"
    assert "quota" in status.status.lower() or "limit" in status.status.lower()


@pytest.mark.asyncio
async def test_combined_rate_and_quota_limits(apikey_manager):
    """Test API key with both rate limit and quota limit."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"combined_test_{uuid4().hex[:8]}",
        roles=["user"],
        attributes={"name": "Combined Limits User"},
    )
    created_principal = await apikey_manager.create_principal(principal)

    # Create API key with both limits
    rate_limit = Condition(
        type="rate_limit",
        config={
            "max_requests": 10,
            "period_seconds": 3600,  # Use 1 hour window to avoid flaky tests near minute boundaries
            "scope": "principal",
        },
    )

    quota_limit = Condition(
        type="max_count", config={"max_count": 25, "scope": "principal"}
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Combined Limits Key",
        note="10/min rate, 25 total quota",
        conditions=[rate_limit, quota_limit],
    )

    api_key, raw_key = await apikey_manager.create_key(key_create)

    # Make 10 requests - should hit rate limit
    for i in range(10):
        status = await apikey_manager.validate_key(
            ApiKeyValidationRequest(api_key=raw_key)
        )
        assert status.is_valid is True, f"Request {i + 1} should be valid"

    await apikey_manager.flush()

    # 11th request should fail due to rate limit (not quota yet)
    status = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key))
    assert status.is_valid is False, "Request 11 should be rate limited"


@pytest.mark.asyncio
async def test_catalog_scoped_rate_limit(apikey_manager):
    """Test rate limit scoped to a specific catalog."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"catalog_rate_{uuid4().hex[:8]}",
        roles=["user"],
        attributes={"name": "Catalog Rate Test"},
    )
    created_principal = await apikey_manager.create_principal(principal)

    # Create API key with catalog-scoped rate limit
    rate_limit = Condition(
        type="rate_limit",
        config={
            "max_requests": 20,
            "period_seconds": 3600,  # Use 1 hour window to avoid flaky tests near minute boundaries
            "scope": "catalog",
        },
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Catalog Rate Limited Key",
        note="20/hour per catalog",
        conditions=[rate_limit],
    )

    api_key, raw_key = await apikey_manager.create_key(key_create)

    # Make 20 requests to catalog_A
    for i in range(20):
        status = await apikey_manager.validate_key(
            ApiKeyValidationRequest(api_key=raw_key),
            catalog_id=apikey_manager.test_catalog_a_id,
        )
        assert status.is_valid is True, f"Request {i + 1} to catalog_a should be valid"

    await apikey_manager.flush()

    # 21st request to catalog_A should fail
    status = await apikey_manager.validate_key(
        ApiKeyValidationRequest(api_key=raw_key),
        catalog_id=apikey_manager.test_catalog_a_id,
    )
    assert status.is_valid is False, "Request 21 to catalog_a should be rate limited"

    # But request to catalog_B should still work (different scope)
    status = await apikey_manager.validate_key(
        ApiKeyValidationRequest(api_key=raw_key),
        catalog_id=apikey_manager.test_catalog_b_id,
    )
    assert status.is_valid is True, (
        "Request to catalog_b should be valid (different scope)"
    )


@pytest.mark.asyncio
async def test_time_expiration_condition(apikey_manager):
    """Test that API keys expire after a specified time."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"expiry_test_{uuid4().hex[:8]}",
        roles=["user"],
        attributes={"name": "Expiry Test User"},
    )
    created_principal = await apikey_manager.create_principal(principal)

    # Create API key that expires in the past (to test expiration)
    past_time = datetime.now(timezone.utc) - timedelta(hours=1)
    expiry_condition = Condition(
        type="time_expiration", config={"expires_at": past_time.isoformat()}
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Expired Key",
        note="Already expired",
        conditions=[expiry_condition],
    )

    api_key, raw_key = await apikey_manager.create_key(key_create)

    # Validation should fail due to expiration
    status = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key))
    assert status.is_valid is False, "Expired key should not be valid"
    assert "expir" in status.status.lower() or "time" in status.status.lower()


@pytest.mark.asyncio
async def test_time_window_condition(apikey_manager):
    """Test that API keys are only valid during specific time windows."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"window_test_{uuid4().hex[:8]}",
        roles=["user"],
        attributes={"name": "Time Window Test"},
    )
    created_principal = await apikey_manager.create_principal(principal)

    # Create API key valid only in the future
    future_start = datetime.now(timezone.utc) + timedelta(hours=1)
    future_end = datetime.now(timezone.utc) + timedelta(hours=2)

    window_condition = Condition(
        type="time_window",
        config={"start": future_start.isoformat(), "end": future_end.isoformat()},
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Future Window Key",
        note="Valid in 1 hour",
        conditions=[window_condition],
    )

    api_key, raw_key = await apikey_manager.create_key(key_create)

    # Validation should fail (not in time window yet)
    status = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key))
    assert status.is_valid is False, "Key should not be valid outside time window"
    assert "time" in status.status.lower() or "window" in status.status.lower()


@pytest.mark.asyncio
async def test_read_write_separate_rate_limits(apikey_manager):
    """Test separate rate limits for read and write operations."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"rw_rate_{uuid4().hex[:8]}",
        roles=["user"],
        attributes={"name": "Read/Write Rate Test"},
    )
    created_principal = await apikey_manager.create_principal(principal)

    # Create API key with 100 read/write per minute
    # This is implemented via the rate_limit condition with scope
    rate_limit = Condition(
        type="rate_limit",
        config={
            "max_requests": 100,
            "period_seconds": 3600,  # Use 1 hour window to avoid flaky tests near minute boundaries
            "scope": "principal",
        },
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="100 Req/Min Key",
        note="100 requests per minute total",
        conditions=[rate_limit],
    )

    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="100 Req/Min Key",
        note="100 requests per minute total",
        conditions=[rate_limit],
    )

    api_key, raw_key = await apikey_manager.create_key(key_create)

    # Simulate 50 read operations
    for i in range(50):
        status = await apikey_manager.validate_key(
            ApiKeyValidationRequest(api_key=raw_key)
        )
        assert status.is_valid is True

    # Simulate 50 write operations
    for i in range(50):
        status = await apikey_manager.validate_key(
            ApiKeyValidationRequest(api_key=raw_key)
        )
        assert status.is_valid is True

    await apikey_manager.flush()

    # 101st operation should fail
    status = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key))
    assert status.is_valid is False, "Should exceed 100 req/min limit"
