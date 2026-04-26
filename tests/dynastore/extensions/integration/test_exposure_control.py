"""End-to-end coverage for the Service Exposure Control Panel.

Covers the full stack wired in Tasks 3-7:
- PATCH /configs invalidates the ExposureMatrix and resets app.openapi_schema
- Platform-disabled extensions are filtered from /openapi.json
- Platform-disabled extensions return 503 on their routes
- Re-enable restores OpenAPI visibility + route serving
- Unknown plugin_id → 404; invalid data → 422
"""

import pytest


@pytest.fixture
def dynastore_extensions():
    # Override integration/conftest.py default ("features") — we also need the
    # configs extension so /configs PATCH routes are mounted.
    return ["configs", "features"]


@pytest.mark.asyncio
async def test_platform_disable_filters_openapi_and_gates_routes(
    sysadmin_in_process_client,
):
    client = sysadmin_in_process_client

    # Baseline: /features/* is visible in OpenAPI.
    schema0 = (await client.get("/openapi.json")).json()
    assert any(p.startswith("/features") for p in schema0["paths"]), (
        "features routes should be visible before disable"
    )

    # Disable FeaturesPluginConfig at platform scope.
    r1 = await client.patch(
        "/configs/", json={"FeaturesPluginConfig": {"enabled": False}}
    )
    assert r1.status_code == 200, r1.text

    # OpenAPI schema should no longer advertise /features/* paths.
    schema1 = (await client.get("/openapi.json")).json()
    assert not any(p.startswith("/features") for p in schema1["paths"]), (
        "features routes must be filtered from OpenAPI after platform disable"
    )

    # A live route under /features/* returns 503 with the expected detail.
    r2 = await client.get("/features/catalogs/nonexistent/collections")
    assert r2.status_code == 503, r2.text
    assert "disabled on this platform" in r2.json()["detail"]

    # Re-enable → OpenAPI + routes restored.
    r3 = await client.patch(
        "/configs/", json={"FeaturesPluginConfig": {"enabled": True}}
    )
    assert r3.status_code == 200, r3.text

    schema2 = (await client.get("/openapi.json")).json()
    assert any(p.startswith("/features") for p in schema2["paths"]), (
        "features routes should reappear after re-enable"
    )

    r4 = await client.get("/features/catalogs/nonexistent/collections")
    assert r4.status_code != 503, (
        f"after re-enable, route should not return 503 (got {r4.status_code})"
    )


@pytest.mark.asyncio
async def test_patch_unknown_plugin_returns_404(sysadmin_in_process_client):
    client = sysadmin_in_process_client
    r = await client.patch(
        "/configs/", json={"NoSuchPluginConfig": {"enabled": False}}
    )
    assert r.status_code == 404, r.text
    # Error message wording changed in the configs API reshape (April 2026):
    # "Unknown plugin_id" → "Unknown config class".
    assert "Unknown config class" in r.json()["detail"]


@pytest.mark.asyncio
async def test_patch_validation_error_returns_422(sysadmin_in_process_client):
    client = sysadmin_in_process_client
    r = await client.patch(
        "/configs/", json={"FeaturesPluginConfig": {"enabled": "not-a-bool"}}
    )
    assert r.status_code == 422, r.text
