import pytest


@pytest.mark.asyncio
async def test_phase3_installs_matrix_and_openapi_override(app_fixture):
    app = app_fixture
    assert hasattr(app.state, "exposure_matrix")
    assert app.openapi.__name__ == "custom_openapi"
