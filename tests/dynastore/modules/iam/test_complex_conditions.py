import pytest
from unittest.mock import MagicMock
from types import SimpleNamespace
from datetime import datetime, timezone

from dynastore.modules.iam.conditions import (
    ConditionManager, EvaluationContext, 
    AttributeMatchHandler, LogicalAndHandler, LogicalOrHandler, LogicalNotHandler
)
from dynastore.modules.iam.models import Condition
@pytest.fixture
def condition_manager():
    # The global instance already has handlers registered in __init__
    from dynastore.modules.iam.conditions import condition_manager as cm
    return cm

@pytest.fixture
def eval_context():
    # Mock Principal Object
    principal_mock = SimpleNamespace(
        id="user_123",
        attributes={"department": "science", "clearance": "top_secret"}
    )

    request_mock = MagicMock()
    request_mock.headers = {"user-agent": "test-agent", "x-custom-header": "custom-value"}

    return EvaluationContext(
        request=request_mock,
        storage=MagicMock(),
        query_params={"force": "false", "debug": "true", "limit": "100"},
        path="/api/v1/resource",
        method="GET",
        principal_id="user_123",
        extras={
            "client_ip": "127.0.0.1", 
            "principal_obj": principal_mock
        }
    )

@pytest.mark.asyncio
async def test_attribute_match_query(condition_manager, eval_context):
    """Test matching query parameters."""
    # Match: force == false
    c1 = Condition(type="match", config={"attribute": "query.force", "operator": "eq", "value": "false"})
    assert await condition_manager.evaluate_all([c1], eval_context) is True

    # Mismatch: force == true
    c2 = Condition(type="match", config={"attribute": "query.force", "operator": "eq", "value": "true"})
    assert await condition_manager.evaluate_all([c2], eval_context) is False

@pytest.mark.asyncio
async def test_attribute_match_headers(condition_manager, eval_context):
    """Test matching headers."""
    c = Condition(type="match", config={"attribute": "header.x-custom-header", "operator": "eq", "value": "custom-value"})
    assert await condition_manager.evaluate_all([c], eval_context) is True

@pytest.mark.asyncio
async def test_attribute_match_principal(condition_manager, eval_context):
    """Test matching principal attributes."""
    # Principal Attribute Match
    c1 = Condition(type="match", config={"attribute": "principal.attributes.department", "operator": "eq", "value": "science"})
    assert await condition_manager.evaluate_all([c1], eval_context) is True

    # Principal ID Match
    c2 = Condition(type="match", config={"attribute": "principal.id", "operator": "eq", "value": "user_123"})
    assert await condition_manager.evaluate_all([c2], eval_context) is True

@pytest.mark.asyncio
async def test_logical_operators(condition_manager, eval_context):
    """Test AND, OR, NOT operators."""
    
    # AND: (query.force=false) AND (method=GET)
    c_and = Condition(
        type="and",
        config={
            "conditions": [
                {"type": "match", "config": {"attribute": "query.force", "value": "false"}},
                {"type": "match", "config": {"attribute": "method", "value": "GET"}}
            ]
        }
    )
    assert await condition_manager.evaluate_all([c_and], eval_context) is True

    # OR: (query.force=true [False]) OR (method=GET [True]) -> True
    c_or = Condition(
        type="or",
        config={
            "conditions": [
                {"type": "match", "config": {"attribute": "query.force", "value": "true"}},
                {"type": "match", "config": {"attribute": "method", "value": "GET"}}
            ]
        }
    )
    assert await condition_manager.evaluate_all([c_or], eval_context) is True

    # NOT: NOT(query.debug=false) -> (Since debug=true, inner is false? No, debug=true. Condition: debug=false is False. NOT(False) -> True)
    # Wait, Context debug is "true".
    # Inner Condition: query.debug == "false" -> False.
    # NOT(False) -> True.
    c_not = Condition(
        type="not",
        config={
            "condition": {"type": "match", "config": {"attribute": "query.debug", "value": "false"}}
        }
    )
    assert await condition_manager.evaluate_all([c_not], eval_context) is True

@pytest.mark.asyncio
async def test_complex_nested_policy(condition_manager, eval_context):
    """Test complex nested policy logic."""
    # Logic: (Department=Science AND Method=POST) OR (Method=GET AND Limit<=100)
    # Context: Department=Science, Method=GET, Limit=100.
    # Left side: (True AND False) -> False
    # Right side: (True AND 100<=100) -> True
    # Result: True
    
    # We need a generic numeric comparison for the limit. AttributeMatchHandler supports 'lte'/'gt' etc?
    # Checking implementation: 'gt', 'lt'. Let's use 'eq' or 'in' or 'regex' as per current impl.
    # Or strict 'eq'.
    
    complex_c = Condition(
        type="or",
        config={
            "conditions": [
                {
                    "type": "and",
                    "config": {
                        "conditions": [
                             {"type": "match", "config": {"attribute": "principal.attributes.department", "value": "science"}},
                             {"type": "match", "config": {"attribute": "method", "value": "POST"}}
                        ]
                    }
                },
                {
                    "type": "and",
                    "config": {
                        "conditions": [
                             {"type": "match", "config": {"attribute": "method", "value": "GET"}},
                             {"type": "match", "config": {"attribute": "query.limit", "value": "100"}}
                        ]
                    }
                }
            ]
        }
    )
    assert await condition_manager.evaluate_all([complex_c], eval_context) is True
