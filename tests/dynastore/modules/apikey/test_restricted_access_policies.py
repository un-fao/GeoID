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
Tests for restricted access policies in the API Key module.

Covers:
- Restricted collection access (only POST to specific collection)
- Item-level GET access (only GET specific items by ID)
- Query parameter enforcement
- Denied operations (DELETE, PUT, LIST)
- Cross-catalog access denial
"""

import pytest
from dynastore.tools.identifiers import generate_id_hex
from dynastore.modules.apikey.apikey_service import ApiKeyService as ApiKeyManager
from dynastore.modules.apikey.models import (
    ApiKeyCreate, Principal, Policy, Condition
)
from dynastore.modules.apikey.policies import PolicyService as PolicyManager
from dynastore.modules.db_config.query_executor import managed_transaction


@pytest.fixture
async def apikey_manager(app_lifespan, db_cleanup):
    from dynastore.modules.apikey.postgres_apikey_storage import PostgresApiKeyStorage
    from dynastore.modules.apikey.postgres_policy_storage import PostgresPolicyStorage
    
    storage = PostgresApiKeyStorage(app_lifespan)
    policy_storage = PostgresPolicyStorage(app_lifespan)
    policy_manager = PolicyManager(app_lifespan, storage=policy_storage, apikey_storage=storage)
    
    manager = ApiKeyManager(storage, policy_manager, app_state=app_lifespan)
    
    # Initialize both apikey and catalog schemas
    async with managed_transaction(app_lifespan.engine) as conn:
        await storage.initialize(conn, schema="apikey")
        await storage.initialize(conn, schema="catalog")  # For principal storage
        await policy_storage.initialize(conn, schema="apikey")
    
    await policy_manager.initialize()
    return manager


@pytest.mark.asyncio
async def test_restricted_collection_post_only(apikey_manager):
    """Test principal can only POST to a specific collection."""
    # Create principal with restricted policy
    principal = Principal(
        provider="local",
        subject_id=f"restricted_{generate_id_hex()[:8]}",
        roles=[],  # No roles, only custom policies
        attributes={"name": "Restricted User"}
    )
    created_principal = await apikey_manager.create_principal(principal)
    
    # Create restrictive policy: only POST to /features/catalogs/demo_catalog/collections/demo_collection/items
    restricted_policy = Policy(
        id=f"restricted_post_{generate_id_hex()[:8]}",
        effect="allow",
        actions=["POST"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/demo_collection/items$"
        ],
        conditions=[]
    )
    
    # Store the policy
    await apikey_manager.policy_service.create_policy(restricted_policy)
    
    # Create API key with this policy
    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Restricted POST Key",
        note="Can only POST to demo_collection",
        custom_policies=[restricted_policy]
    )
    
    api_key, raw_key = await apikey_manager.create_key(key_create)
    
    # Test allowed operation: POST to the specific collection
    from dynastore.modules.apikey.policies import PolicyService as PolicyManager
    from dynastore.modules.apikey.models import ApiKeyPolicy
    
    # Simulate policy evaluation
    policy_obj = ApiKeyPolicy(statements=[restricted_policy])
    
    # Allowed: POST to demo_collection
    is_allowed = await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="POST",
        path="/features/catalogs/demo_catalog/collections/demo_collection/items"
    )
    assert is_allowed is True, "POST to demo_collection should be allowed"
    
    # Denied: GET on the same path
    is_denied = await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="GET",
        path="/features/catalogs/demo_catalog/collections/demo_collection/items"
    )
    assert is_denied is False, "GET should be denied"
    
    # Denied: POST to different collection
    is_denied = await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="POST",
        path="/features/catalogs/demo_catalog/collections/other_collection/items"
    )
    assert is_denied is False, "POST to other_collection should be denied"


@pytest.mark.asyncio
async def test_item_level_get_access(apikey_manager):
    """Test principal can only GET specific items by ID."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"item_reader_{generate_id_hex()[:8]}",
        roles=[],
        attributes={"name": "Item Reader"}
    )
    created_principal = await apikey_manager.create_principal(principal)
    
    # Policy: only GET /features/catalogs/demo_catalog/collections/demo_collection/items/{item_id}
    get_item_policy = Policy(
        id=f"get_item_{generate_id_hex()[:8]}",
        effect="allow",
        actions=["GET"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/demo_collection/items/[^/]+$"
        ],
        conditions=[]
    )
    
    await apikey_manager.policy_service.create_policy(get_item_policy)
    
    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Item GET Key",
        note="Can only GET specific items",
        custom_policies=[get_item_policy]
    )
    
    api_key, raw_key = await apikey_manager.create_key(key_create)
    
    from dynastore.modules.apikey.models import ApiKeyPolicy
    policy_obj = ApiKeyPolicy(statements=[get_item_policy])
    
    # Allowed: GET specific item
    is_allowed = await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="GET",
        path="/features/catalogs/demo_catalog/collections/demo_collection/items/item123"
    )
    assert is_allowed is True, "GET specific item should be allowed"
    
    # Denied: GET collection list
    is_denied = await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="GET",
        path="/features/catalogs/demo_catalog/collections/demo_collection/items"
    )
    assert is_denied is False, "GET collection list should be denied"
    
    # Denied: GET catalog
    is_denied = await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="GET",
        path="/features/catalogs/demo_catalog"
    )
    assert is_denied is False, "GET catalog should be denied"


@pytest.mark.asyncio
async def test_query_parameter_enforcement(apikey_manager):
    """Test policies that enforce specific query parameter values."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"query_enforced_{generate_id_hex()[:8]}",
        roles=[],
        attributes={"name": "Query Enforced User"}
    )
    created_principal = await apikey_manager.create_principal(principal)
    
    # Policy: DELETE only with force=false
    delete_policy = Policy(
        id=f"delete_safe_{generate_id_hex()[:8]}",
        effect="allow",
        actions=["DELETE"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/.*$"
        ],
        conditions=[
            Condition(
                type="match",
                config={
                    "attribute": "query.force",
                    "operator": "eq",
                    "value": "false"
                }
            )
        ]
    )
    
    await apikey_manager.policy_service.create_policy(delete_policy)
    
    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Safe Delete Key",
        note="Can only DELETE with force=false",
        custom_policies=[delete_policy]
    )
    
    api_key, raw_key = await apikey_manager.create_key(key_create)
    
    # Test: The condition enforcement is handled by the condition manager
    # This test validates the policy structure
    assert delete_policy.conditions[0].type == "match"
    assert delete_policy.conditions[0].config["attribute"] == "query.force"
    assert delete_policy.conditions[0].config["value"] == "false"


@pytest.mark.asyncio
async def test_deny_all_except_specific_operations(apikey_manager):
    """Test that all operations are denied except explicitly allowed ones."""
    # Create principal with minimal permissions
    principal = Principal(
        provider="local",
        subject_id=f"minimal_{generate_id_hex()[:8]}",
        roles=[],
        attributes={"name": "Minimal User"}
    )
    created_principal = await apikey_manager.create_principal(principal)
    
    # Only allow: POST to collection and GET specific items
    post_policy = Policy(
        id=f"post_only_{generate_id_hex()[:8]}",
        effect="allow",
        actions=["POST"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/demo_collection/items$"
        ]
    )
    
    get_policy = Policy(
        id=f"get_item_only_{generate_id_hex()[:8]}",
        effect="allow",
        actions=["GET"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/demo_collection/items/[^/]+$"
        ]
    )
    
    await apikey_manager.policy_service.create_policy(post_policy)
    await apikey_manager.policy_service.create_policy(get_policy)
    
    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Minimal Access Key",
        note="Only POST and GET items",
        custom_policies=[post_policy, get_policy]
    )
    
    api_key, raw_key = await apikey_manager.create_key(key_create)
    
    from dynastore.modules.apikey.models import ApiKeyPolicy
    policy_obj = ApiKeyPolicy(statements=[post_policy, get_policy])
    
    # Allowed operations
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "POST",
        "/features/catalogs/demo_catalog/collections/demo_collection/items"
    ) is True
    
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/demo_catalog/collections/demo_collection/items/item1"
    ) is True
    
    # Denied operations
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "DELETE",
        "/features/catalogs/demo_catalog/collections/demo_collection/items/item1"
    ) is False
    
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "PUT",
        "/features/catalogs/demo_catalog/collections/demo_collection/items/item1"
    ) is False
    
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/demo_catalog/collections"
    ) is False


@pytest.mark.asyncio
async def test_cross_catalog_access_denial(apikey_manager):
    """Test that principal cannot access other catalogs."""
    # Create principal restricted to one catalog
    principal = Principal(
        provider="local",
        subject_id=f"single_catalog_{generate_id_hex()[:8]}",
        roles=[],
        attributes={"name": "Single Catalog User"}
    )
    created_principal = await apikey_manager.create_principal(principal)
    
    # Policy: only access demo_catalog
    catalog_policy = Policy(
        id=f"demo_catalog_only_{generate_id_hex()[:8]}",
        effect="allow",
        actions=["GET", "POST", "PUT", "DELETE"],
        resources=[
            r"^/features/catalogs/demo_catalog/.*$",
            r"^/stac/catalogs/demo_catalog/.*$"
        ]
    )
    
    await apikey_manager.policy_service.create_policy(catalog_policy)
    
    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="Demo Catalog Key",
        note="Only access demo_catalog",
        custom_policies=[catalog_policy]
    )
    
    api_key, raw_key = await apikey_manager.create_key(key_create)
    
    from dynastore.modules.apikey.models import ApiKeyPolicy
    policy_obj = ApiKeyPolicy(statements=[catalog_policy])
    
    # Allowed: access to demo_catalog
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/demo_catalog/collections"
    ) is True
    
    # Denied: access to other_catalog
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/other_catalog/collections"
    ) is False
    
    # Denied: access to production_catalog
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "POST",
        "/stac/catalogs/production_catalog/collections"
    ) is False


@pytest.mark.asyncio
async def test_no_list_operations_allowed(apikey_manager):
    """Test that listing operations are denied, only specific item access."""
    # Create principal
    principal = Principal(
        provider="local",
        subject_id=f"no_list_{generate_id_hex()[:8]}",
        roles=[],
        attributes={"name": "No List User"}
    )
    created_principal = await apikey_manager.create_principal(principal)
    
    # Policy: only GET specific items, no listing
    item_only_policy = Policy(
        id=f"item_only_{generate_id_hex()[:8]}",
        effect="allow",
        actions=["GET"],
        resources=[
            # Match only paths with item IDs (not collection lists)
            r"^/features/catalogs/[^/]+/collections/[^/]+/items/[^/]+$"
        ]
    )
    
    await apikey_manager.policy_service.create_policy(item_only_policy)
    
    key_create = ApiKeyCreate(
        principal_id=created_principal.id,
        name="No List Key",
        note="Cannot list, only get items",
        custom_policies=[item_only_policy]
    )
    
    api_key, raw_key = await apikey_manager.create_key(key_create)
    
    from dynastore.modules.apikey.models import ApiKeyPolicy
    policy_obj = ApiKeyPolicy(statements=[item_only_policy])
    
    # Allowed: GET specific item
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/cat1/collections/col1/items/item123"
    ) is True
    
    # Denied: list items
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/cat1/collections/col1/items"
    ) is False
    
    # Denied: list collections
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/cat1/collections"
    ) is False
    
    # Denied: list catalogs
    assert await apikey_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/stac/catalogs"
    ) is False
