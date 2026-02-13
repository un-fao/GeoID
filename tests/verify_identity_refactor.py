import asyncio
import pytest
from typing import Dict, Any, Optional
from unittest.mock import MagicMock, AsyncMock
from dynastore.modules.catalog.sidecars.attributes import FeatureAttributeSidecar, AttributeStorageMode
from dynastore.modules.catalog.sidecars.attributes_config import FeatureAttributeSidecarConfig

def test_prepare_upsert_payload_default():
    """Test default configuration (maps 'id' to 'external_id')."""
    config = FeatureAttributeSidecarConfig(enable_external_id=True)
    sidecar = FeatureAttributeSidecar(config)
    
    feature = {"id": "test-id-1", "properties": {"foo": "bar"}}
    context = {"geoid": "some-uuid"}
    
    payload = sidecar.prepare_upsert_payload(feature, context)
    assert payload["external_id"] == "test-id-1"

def test_prepare_upsert_payload_custom_field():
    """Test custom field mapping."""
    config = FeatureAttributeSidecarConfig(
        enable_external_id=True,
        external_id_field="properties.code"
    )
    sidecar = FeatureAttributeSidecar(config)
    
    feature = {
        "id": "ignored-id", 
        "properties": {"code": "custom-code-123"}
    }
    context = {"geoid": "some-uuid"}
    
    payload = sidecar.prepare_upsert_payload(feature, context)
    assert payload["external_id"] == "custom-code-123"

def test_prepare_upsert_payload_fallback():
    """Test fallback to properties if ID is missing at root (for default 'id' field)."""
    config = FeatureAttributeSidecarConfig(enable_external_id=True) # default field="id"
    sidecar = FeatureAttributeSidecar(config)
    
    # "id" is inside properties, not at root
    feature = {"properties": {"id": "fallback-id"}}
    context = {"geoid": "some-uuid"}
    
    payload = sidecar.prepare_upsert_payload(feature, context)
    assert payload["external_id"] == "fallback-id"

def test_prepare_upsert_payload_asset_id_default():
    """Test default configuration for asset_id."""
    config = FeatureAttributeSidecarConfig(enable_asset_id=True)
    sidecar = FeatureAttributeSidecar(config)
    
    feature = {"asset_id": "test-asset-1", "properties": {"foo": "bar"}}
    context = {"geoid": "some-uuid"}
    
    payload = sidecar.prepare_upsert_payload(feature, context)
    assert payload["asset_id"] == "test-asset-1"

def test_prepare_upsert_payload_asset_id_custom():
    """Test custom configuration for asset_id."""
    config = FeatureAttributeSidecarConfig(
        enable_asset_id=True,
        asset_id_field="properties.my_asset"
    )
    sidecar = FeatureAttributeSidecar(config)
    
    feature = {"properties": {"my_asset": "custom-asset-1"}}
    context = {"geoid": "some-uuid"}
    
    payload = sidecar.prepare_upsert_payload(feature, context)
    assert payload["asset_id"] == "custom-asset-1"

def test_prepare_upsert_payload_asset_id_context_priority():
    """Test that context asset_id takes precedence over feature property."""
    config = FeatureAttributeSidecarConfig(enable_asset_id=True)
    sidecar = FeatureAttributeSidecar(config)
    
    # Feature has one ID, Context has another
    feature = {"asset_id": "feature-asset-1", "properties": {"foo": "bar"}}
    context = {"geoid": "some-uuid", "asset_id": "SYSTEM-ASSIGNED-ID"}
    
    payload = sidecar.prepare_upsert_payload(feature, context)
    assert payload["asset_id"] == "SYSTEM-ASSIGNED-ID"

def test_select_fields_hidden():
    """Verify external_id and asset_id are NOT in select fields."""
    config = FeatureAttributeSidecarConfig(
        enable_external_id=True,
        enable_asset_id=True,
        storage_mode=AttributeStorageMode.COLUMNAR # Force non-JSONB to test specific fields if any
    )
    sidecar = FeatureAttributeSidecar(config)
    
    fields = sidecar.get_select_fields(hub_alias="h", sidecar_alias="sc")
    print(f"DEBUG SELECT FIELDS: {fields}")
    
    # Needs to ensure external_id/asset_id are NOT present
    for f in fields:
        if "external_id" in f:
            raise AssertionError(f"external_id exposed in select fields: {f}")
        if "asset_id" in f:
            raise AssertionError(f"asset_id exposed in select fields: {f}")

def test_prepare_upsert_payload_populates_context():
    """Verify that sidecar populates context with resolved IDs."""
    config = FeatureAttributeSidecarConfig(
        enable_external_id=True,
        enable_asset_id=True
    )
    sidecar = FeatureAttributeSidecar(config)
    
    feature = {
        "id": "my-external-id",
        "asset_id": "my-asset-id",
        "properties": {}
    }
    context = {"geoid": "some-uuid"}
    
    payload = sidecar.prepare_upsert_payload(feature, context)
    
    # Assert payload has them
    assert payload["external_id"] == "my-external-id"
    assert payload["asset_id"] == "my-asset-id"
    
    # Assert CONTEXT has them (Decentralization check)
    assert context["external_id"] == "my-external-id"
    assert context["asset_id"] == "my-asset-id"

def test_is_acceptable():
    """Verify sidecar can refuse features missing required IDs."""
    config = FeatureAttributeSidecarConfig(enable_external_id=True)
    sidecar = FeatureAttributeSidecar(config)
    
    # Valid feature
    assert sidecar.is_acceptable({"id": "valid"}, {}) is True
    
    # Missing ID
    assert sidecar.is_acceptable({"properties": {}}, {}) is False
    
    # ID in context is also acceptable
    assert sidecar.is_acceptable({}, {"external_id": "resolved-elsewhere"}) is True

async def test_check_collision_mocked():
    """Verify check_collision logic (mocked DB)."""
    from sqlalchemy import text
    config = FeatureAttributeSidecarConfig(enable_asset_id=True)
    sidecar = FeatureAttributeSidecar(config)
    
    conn = MagicMock()
    # Mock result
    res = MagicMock()
    res.scalar.return_value = True
    conn.execute = AsyncMock(return_value=res)
    
    # Correct call should trigger collision
    collided = await sidecar.check_collision(conn, "schema", "table", "asset_id", "duplicate")
    assert collided is True
    
    # Check if correct SQL was generated (roughly)
    call_args = conn.execute.call_args[0]
    sql = str(call_args[0])
    assert 'asset_id = :val' in sql

if __name__ == "__main__":
    # Manually run tests if executed as script
    try:
        test_prepare_upsert_payload_default()
        print("test_prepare_upsert_payload_default PASSED")
        test_prepare_upsert_payload_custom_field()
        print("test_prepare_upsert_payload_custom_field PASSED")
        test_prepare_upsert_payload_fallback()
        print("test_prepare_upsert_payload_fallback PASSED")
        
        test_prepare_upsert_payload_asset_id_default()
        print("test_prepare_upsert_payload_asset_id_default PASSED")
        
        test_prepare_upsert_payload_asset_id_custom()
        print("test_prepare_upsert_payload_asset_id_custom PASSED")
        
        test_prepare_upsert_payload_asset_id_context_priority()
        print("test_prepare_upsert_payload_asset_id_context_priority PASSED")
        
        test_select_fields_hidden()
        print("test_select_fields_hidden PASSED")
        
        test_prepare_upsert_payload_populates_context()
        print("test_prepare_upsert_payload_populates_context PASSED")
        
        test_is_acceptable()
        print("test_is_acceptable PASSED")
        
        asyncio.run(test_check_collision_mocked())
        print("test_check_collision_mocked PASSED")
        
        print("\nAll verification tests passed!")
    except Exception as e:
        print(f"\nFAILED: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
