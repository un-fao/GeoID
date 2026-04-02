import pytest
import os
from dynastore.tools.identifiers import generate_id_hex
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine
from types import SimpleNamespace
from dynastore.modules.apikey.apikey_service import ApiKeyService as ApiKeyManager
from dynastore.modules.apikey.models import ApiKeyCreate, Principal
from dynastore.modules.db_config.query_executor import managed_transaction

@pytest.fixture
async def apikey_manager(app_lifespan):
    from dynastore.modules.apikey.postgres_apikey_storage import PostgresApiKeyStorage
    from dynastore.modules.apikey.postgres_policy_storage import PostgresPolicyStorage
    from dynastore.modules.apikey.policies import PolicyService as PolicyManager
    
    storage = PostgresApiKeyStorage(app_lifespan)
    policy_storage = PostgresPolicyStorage(app_lifespan)
    policy_manager = PolicyManager(app_lifespan, storage=policy_storage)
    
    manager = ApiKeyManager(storage, policy_manager, app_state=app_lifespan)
    async with managed_transaction(app_lifespan.engine) as conn:
        await storage.initialize(conn, schema="apikey")
    await policy_manager.initialize()
    return manager

@pytest.mark.asyncio
async def test_apikey_v3_features(apikey_manager):
    # 1. Create Principal with Roles
    test_subject = f"test_{generate_id_hex()[:8]}"
    principal = Principal(
        provider="local",
        subject_id=test_subject,
        roles=["admin", "special_viewer"],
        attributes={"name": "Test User"}
    )
    created_principal = await apikey_manager.create_principal(principal)
    assert created_principal.id is not None
    assert "special_viewer" in created_principal.roles

    # 2. Create Key with New Constraints
    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Test V3 Key",
        note="Testing new fields",
        max_usage=5,
        allowed_domains=["example.com"],
        catalog_match="^cat_.*",
        collection_match=".*_data"
    )
    api_key, raw_key = await apikey_manager.create_key(key_create)
    
    # Verify Metadata
    assert api_key.note == "Testing new fields"
    # The max_usage field is now converted to a condition.
    # We verify the condition exists and the legacy field is None.
    assert api_key.max_usage is None
    assert any(c.type == 'max_count' and c.config.get('max_count') == 5 for c in api_key.conditions)
    assert "example.com" in api_key.allowed_domains

    from dynastore.modules.apikey.models import ApiKeyValidationRequest
    
    # 3. Validate Context (Catalog Scope)
    # Correct scope
    status = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key), catalog_id="cat_001")
    assert status.is_valid is True
    
    # Incorrect scope
    status_fail = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key), catalog_id="dog_001")
    assert status_fail.is_valid is False
    assert "Catalog scope" in status_fail.status

    # 4. Domain Check
    # Passing context with domain
    status_domain = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key), origin="example.com")
    assert status_domain.is_valid is True
    
    status_domain_fail = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key), origin="malicious.com")
    assert status_domain_fail.is_valid is False

    # 5. Usage Limit
    # Note: Usage limit (max_count) is a hard limit in V2.
    # It doesn't rely on background flushing for ENFORCEMENT if checked synchronously,
    # but the aggregator handles increments.
    # We must ensure the aggregator flushes BEFORE validation check.
    
    # We need to simulate usage increment. The manager has increment_key_usage method.
    
    # Clear local usage cache to force fetch from DB, as increment_key_usage updates DB directly
    apikey_manager.usage_cache.clear()

    for i in range(10):
        # We need to increment usage. The test helper increments 'quota'.
        await apikey_manager.increment_key_usage(raw_key)
    
    # Force flush to persistence
    await apikey_manager.flush()
        
    # The 6th request should fail if enforcement works correctly.
    # However, validate_key performs authentication, which CHECKS conditions.
    # The max_count condition checks total usage.
    
    # Wait, 'max_usage' in create_key converted to condition.
    # Condition: type='max_count', config={'max_count': 5, 'scope': 'apikey'}
    # This checks against QUOTA counter (scope='apikey' implies global/key quota).
    
    # increment_key_usage increments quota for the key.
    # So if count >= 5, it should be invalid.
    
    # Clear cache again to ensure validate_key fetches the updated usage from DB
    apikey_manager.usage_cache.clear()

    status_limit = await apikey_manager.validate_key(ApiKeyValidationRequest(api_key=raw_key))
    assert status_limit.is_valid is False
    # Check for specific quota message
    assert "quota" in status_limit.status.lower() or "limit" in status_limit.status.lower()

    # 6. Hierarchy Check (Manual Test)
    # We verify if we can add a parent-child relationship
    from dynastore.modules.apikey.models import Role
    try:
        await apikey_manager.storage.create_role(Role(name="viewer", level=10), schema="apikey")
    except Exception:
        # Ignore if role already exists (from other tests)
        pass
        
    try:
        await apikey_manager.storage.create_role(Role(name="editor", level=20), schema="apikey")
    except Exception:
        pass
        
    await apikey_manager.storage.add_role_hierarchy("editor", "viewer", schema="apikey")
    
    # Resolve hierarchy for an editor
    effective = await apikey_manager.storage.get_role_hierarchy(["editor"], schema="apikey")
    assert "viewer" in effective
    assert "editor" in effective # My recent change included the role itself
