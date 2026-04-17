"""Unit tests for exposure-matrix invalidation on config writes."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_configs_service_with_matrix():
    """
    Build a ConfigsService-like object with a mock exposure matrix and FastAPI app.
    Returns (configs_service_instance, matrix_mock, app_mock).
    """
    from fastapi import FastAPI
    from starlette.datastructures import State

    # Minimal FastAPI app mock
    app = FastAPI()
    app.state = State()
    app.openapi_schema = {"info": {"title": "test"}}  # simulates cached schema

    # Mock ExposureMatrix — invalidate is sync, get is async (awaited by
    # _invalidate_exposure to reload the snapshot).
    matrix = MagicMock()
    matrix.invalidate = MagicMock()
    matrix.get = AsyncMock()
    app.state.exposure_matrix = matrix

    return app, matrix


@pytest.mark.asyncio
async def test_invalidate_exposure_clears_matrix_and_schema():
    """_invalidate_exposure() must call matrix.invalidate() and reset openapi_schema."""
    from dynastore.extensions.configs.service import ConfigsService

    app, matrix = _make_configs_service_with_matrix()

    # Build a ConfigsService instance but don't call lifespan/setup_routes side effects
    svc = ConfigsService.__new__(ConfigsService)
    svc.app = app

    await svc._invalidate_exposure()

    matrix.invalidate.assert_called_once()
    matrix.get.assert_awaited_once()
    assert app.openapi_schema is None


@pytest.mark.asyncio
async def test_invalidate_exposure_no_matrix_does_not_raise():
    """_invalidate_exposure() must not raise when no matrix is attached to app.state."""
    from dynastore.extensions.configs.service import ConfigsService
    from fastapi import FastAPI
    from starlette.datastructures import State

    app = FastAPI()
    app.state = State()
    # No exposure_matrix on state
    app.openapi_schema = {"info": {"title": "test"}}

    svc = ConfigsService.__new__(ConfigsService)
    svc.app = app

    # Should not raise
    await svc._invalidate_exposure()
    assert app.openapi_schema is None


@pytest.mark.asyncio
async def test_patch_platform_config_calls_invalidate():
    """_patch_platform_config() handler must call _invalidate_exposure() on success."""
    from dynastore.extensions.configs.service import ConfigsService
    from dynastore.extensions.configs.config_api_dto import PatchConfigBody

    app, matrix = _make_configs_service_with_matrix()

    svc = ConfigsService.__new__(ConfigsService)
    svc.app = app

    # Mock _config_api.patch_config to return a canned result
    mock_api = MagicMock()
    mock_api.patch_config = AsyncMock(return_value={"updated": ["TilesConfig"]})

    with patch.object(type(svc), "_config_api", new_callable=lambda: property(lambda self: mock_api)):
        body = PatchConfigBody(root={"TilesConfig": {"enabled": False}})
        result = await svc._patch_platform_config(body=body)

    matrix.invalidate.assert_called_once()
    assert app.openapi_schema is None
    assert result == {"updated": ["TilesConfig"]}


@pytest.mark.asyncio
async def test_patch_catalog_config_calls_invalidate():
    """_patch_catalog_config() handler must call _invalidate_exposure() on success."""
    from dynastore.extensions.configs.service import ConfigsService
    from dynastore.extensions.configs.config_api_dto import PatchConfigBody

    app, matrix = _make_configs_service_with_matrix()

    svc = ConfigsService.__new__(ConfigsService)
    svc.app = app

    mock_api = MagicMock()
    mock_api.patch_config = AsyncMock(return_value={"updated": ["TilesConfig"]})

    with patch.object(type(svc), "_config_api", new_callable=lambda: property(lambda self: mock_api)):
        body = PatchConfigBody(root={"TilesConfig": None})
        await svc._patch_catalog_config(catalog_id="my-catalog", body=body)

    matrix.invalidate.assert_called_once()
    assert app.openapi_schema is None
