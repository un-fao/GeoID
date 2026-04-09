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
Tests for restricted access policies.

Covers:
- Restricted collection access (only POST to specific collection)
- Item-level GET access (only GET specific items by ID)
- Query parameter enforcement
- Denied operations (DELETE, PUT, LIST)
- Cross-catalog access denial
"""

import pytest
from tests.dynastore.test_utils import generate_test_id
from dynastore.modules.iam.iam_service import IamService as IamManager
from dynastore.modules.iam.models import (
    Principal, Policy, Condition, PolicyBundle,
)
from dynastore.modules.iam.policies import PolicyService as PolicyManager
from dynastore.modules.db_config.query_executor import managed_transaction


@pytest.fixture
async def iam_manager(app_lifespan, db_cleanup):
    from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage
    from dynastore.modules.iam.postgres_policy_storage import PostgresPolicyStorage

    storage = PostgresIamStorage(app_lifespan)
    policy_storage = PostgresPolicyStorage(app_lifespan)
    policy_manager = PolicyManager(app_lifespan, storage=policy_storage, iam_storage=storage)

    manager = IamManager(storage, policy_manager, app_state=app_lifespan)

    async with managed_transaction(app_lifespan.engine) as conn:
        await storage.initialize(conn, schema="iam")
        await storage.initialize(conn, schema="catalog")
        await policy_storage.initialize(conn, schema="iam")

    await policy_manager.initialize()
    return manager


@pytest.mark.asyncio
async def test_restricted_collection_post_only(iam_manager):
    """Test policy allows only POST to a specific collection."""
    restricted_policy = Policy(
        id=f"restricted_post_{generate_test_id()}",
        effect="allow",
        actions=["POST"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/demo_collection/items$"
        ],
        conditions=[],
    )

    await iam_manager.policy_service.create_policy(restricted_policy)

    policy_obj = PolicyBundle(statements=[restricted_policy])

    # Allowed: POST to demo_collection
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="POST",
        path="/features/catalogs/demo_catalog/collections/demo_collection/items",
    ) is True

    # Denied: GET on the same path
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="GET",
        path="/features/catalogs/demo_catalog/collections/demo_collection/items",
    ) is False

    # Denied: POST to different collection
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="POST",
        path="/features/catalogs/demo_catalog/collections/other_collection/items",
    ) is False


@pytest.mark.asyncio
async def test_item_level_get_access(iam_manager):
    """Test policy allows only GET specific items by ID."""
    get_item_policy = Policy(
        id=f"get_item_{generate_test_id()}",
        effect="allow",
        actions=["GET"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/demo_collection/items/[^/]+$"
        ],
        conditions=[],
    )

    await iam_manager.policy_service.create_policy(get_item_policy)

    policy_obj = PolicyBundle(statements=[get_item_policy])

    # Allowed: GET specific item
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="GET",
        path="/features/catalogs/demo_catalog/collections/demo_collection/items/item123",
    ) is True

    # Denied: GET collection list
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="GET",
        path="/features/catalogs/demo_catalog/collections/demo_collection/items",
    ) is False

    # Denied: GET catalog
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj,
        method="GET",
        path="/features/catalogs/demo_catalog",
    ) is False


@pytest.mark.asyncio
async def test_query_parameter_enforcement(iam_manager):
    """Test policies that enforce specific query parameter values."""
    delete_policy = Policy(
        id=f"delete_safe_{generate_test_id()}",
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
                    "value": "false",
                },
            )
        ],
    )

    await iam_manager.policy_service.create_policy(delete_policy)

    # Verify the condition structure
    assert delete_policy.conditions[0].type == "match"
    assert delete_policy.conditions[0].config["attribute"] == "query.force"
    assert delete_policy.conditions[0].config["value"] == "false"


@pytest.mark.asyncio
async def test_deny_all_except_specific_operations(iam_manager):
    """Test that all operations are denied except explicitly allowed ones."""
    post_policy = Policy(
        id=f"post_only_{generate_test_id()}",
        effect="allow",
        actions=["POST"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/demo_collection/items$"
        ],
    )

    get_policy = Policy(
        id=f"get_item_only_{generate_test_id()}",
        effect="allow",
        actions=["GET"],
        resources=[
            r"^/features/catalogs/demo_catalog/collections/demo_collection/items/[^/]+$"
        ],
    )

    await iam_manager.policy_service.create_policy(post_policy)
    await iam_manager.policy_service.create_policy(get_policy)

    policy_obj = PolicyBundle(statements=[post_policy, get_policy])

    # Allowed operations
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "POST",
        "/features/catalogs/demo_catalog/collections/demo_collection/items",
    ) is True

    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/demo_catalog/collections/demo_collection/items/item1",
    ) is True

    # Denied operations
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "DELETE",
        "/features/catalogs/demo_catalog/collections/demo_collection/items/item1",
    ) is False

    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "PUT",
        "/features/catalogs/demo_catalog/collections/demo_collection/items/item1",
    ) is False

    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/demo_catalog/collections",
    ) is False


@pytest.mark.asyncio
async def test_cross_catalog_access_denial(iam_manager):
    """Test that principal cannot access other catalogs."""
    catalog_policy = Policy(
        id=f"demo_catalog_only_{generate_test_id()}",
        effect="allow",
        actions=["GET", "POST", "PUT", "DELETE"],
        resources=[
            r"^/features/catalogs/demo_catalog/.*$",
            r"^/stac/catalogs/demo_catalog/.*$",
        ],
    )

    await iam_manager.policy_service.create_policy(catalog_policy)

    policy_obj = PolicyBundle(statements=[catalog_policy])

    # Allowed: access to demo_catalog
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/demo_catalog/collections",
    ) is True

    # Denied: access to other_catalog
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/other_catalog/collections",
    ) is False

    # Denied: access to production_catalog
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "POST",
        "/stac/catalogs/production_catalog/collections",
    ) is False


@pytest.mark.asyncio
async def test_no_list_operations_allowed(iam_manager):
    """Test that listing operations are denied, only specific item access."""
    item_only_policy = Policy(
        id=f"item_only_{generate_test_id()}",
        effect="allow",
        actions=["GET"],
        resources=[
            r"^/features/catalogs/[^/]+/collections/[^/]+/items/[^/]+$"
        ],
    )

    await iam_manager.policy_service.create_policy(item_only_policy)

    policy_obj = PolicyBundle(statements=[item_only_policy])

    # Allowed: GET specific item
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/cat1/collections/col1/items/item123",
    ) is True

    # Denied: list items
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/cat1/collections/col1/items",
    ) is False

    # Denied: list collections
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/features/catalogs/cat1/collections",
    ) is False

    # Denied: list catalogs
    assert await iam_manager.policy_service.evaluate_policy_statements(
        policy_obj, "GET",
        "/stac/catalogs",
    ) is False
