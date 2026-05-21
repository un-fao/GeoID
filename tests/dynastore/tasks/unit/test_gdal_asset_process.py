"""Regression tests — GdalAssetProcess satisfies AssetProcessProtocol and
dispatches GDAL work to a backend runner *without* requiring GDAL locally.

Two concerns are covered:

1. Protocol/describe discovery (the original Phase D regression): without the
   duck-typed ``GdalAssetProcess`` wrapper registered in ``GdalModule.lifespan``,
   ``POST /assets/.../assets/{aid}/processes/gdal/execution`` returned 404
   "Asset process 'gdal' is not registered.".

2. Dispatch (#1145): the catalog API service has no GDAL runtime. It must still
   dispatch the gdal task asynchronously to a GDAL-equipped backend (Cloud Run
   Job via GcpJobRunner, or an in-process worker via BackgroundRunner). The
   earlier implementation gated ``execute()`` on the local ``GDAL_AVAILABLE``
   flag and returned 503 before ever delegating — these tests pin the corrected
   behaviour: applicability and availability are decided by *runner* presence.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.models.protocols.asset_process import AssetProcessProtocol
from dynastore.tasks.gdal.asset_process import GdalAssetProcess

_HELPER = "dynastore.tasks.gdal.asset_process.gdal_backend_available"


def test_gdal_asset_process_conforms_to_protocol() -> None:
    instance = GdalAssetProcess()
    assert isinstance(instance, AssetProcessProtocol)
    assert instance.process_id == "gdal"
    assert instance.http_method == "POST"


def _mock_asset(asset_type: str = "RASTER", uri: str = "gs://b/o.tif") -> SimpleNamespace:
    """Build a duck-typed Asset for describe/execute."""
    return SimpleNamespace(
        asset_id="a1",
        catalog_id="cat_a",
        collection_id="col_a",
        uri=uri,
        asset_type=SimpleNamespace(value=asset_type, name=asset_type),
    )


def _raster_asset() -> SimpleNamespace:
    from dynastore.modules.catalog.asset_service import AssetTypeEnum

    asset = _mock_asset(asset_type=AssetTypeEnum.RASTER.value)
    asset.asset_type = AssetTypeEnum.RASTER  # type: ignore[assignment]
    return asset


# ---------------------------------------------------------------------------
# describe()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_describe_applicable_for_raster_with_backend() -> None:
    """A raster asset is applicable when a GDAL backend runner is available —
    even though the catalog API service itself has no local GDAL."""
    asset = _raster_asset()
    instance = GdalAssetProcess()
    with patch(_HELPER, return_value=True):
        descriptor = await instance.describe(asset)
    assert descriptor.process_id == "gdal"
    assert descriptor.applicable is True
    assert descriptor.reason is None


@pytest.mark.asyncio
async def test_describe_inapplicable_when_no_backend() -> None:
    """No runner can execute gdal anywhere → not applicable, with a backend
    (not "this service") explanation."""
    asset = _raster_asset()
    instance = GdalAssetProcess()
    with patch(_HELPER, return_value=False):
        descriptor = await instance.describe(asset)
    assert descriptor.applicable is False
    assert "backend" in (descriptor.reason or "").lower()


@pytest.mark.asyncio
async def test_describe_inapplicable_for_table_asset() -> None:
    from dynastore.modules.catalog.asset_service import AssetTypeEnum

    # Use a non-raster, non-vectorial asset type
    other = next(
        (t for t in AssetTypeEnum if t not in (AssetTypeEnum.RASTER, AssetTypeEnum.VECTORIAL)),
        None,
    )
    if other is None:
        pytest.skip("No non-raster/non-vector AssetTypeEnum value to test against")
    asset = _mock_asset(asset_type=other.value)
    asset.asset_type = other  # type: ignore[assignment]
    instance = GdalAssetProcess()
    # Backend present, so the only reason for inapplicability is the asset type.
    with patch(_HELPER, return_value=True):
        descriptor = await instance.describe(asset)
    assert descriptor.applicable is False
    assert other.value in (descriptor.reason or "")


# ---------------------------------------------------------------------------
# execute()  — #1145 regression: must dispatch without local GDAL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_dispatches_async_returns_job_handle() -> None:
    """The core #1145 fix: with no local GDAL runtime, execute() must delegate
    to the OGC processes layer (ASYNC) and surface a job handle — NOT 503."""
    asset = _raster_asset()
    instance = GdalAssetProcess()

    status_info = SimpleNamespace(jobID="job-123")
    fake_execute = AsyncMock(return_value=status_info)

    # GDAL_AVAILABLE=False reproduces the catalog API service exactly — the
    # previous guard raised 503 here before delegating. The fix must dispatch.
    with patch(
        "dynastore.modules.gdal.service.GDAL_AVAILABLE", False
    ), patch(
        "dynastore.modules.processes.processes_module.execute_process", fake_execute
    ), patch(
        "dynastore.tools.protocol_helpers.get_engine", return_value=object()
    ):
        output = await instance.execute(asset, {})

    assert output.type == "job"
    assert output.job_id == "job-123"
    # Dispatched asynchronously, with the asset locators injected into inputs.
    fake_execute.assert_awaited_once()
    kwargs = fake_execute.await_args.kwargs
    assert kwargs["process_id"] == "gdal"
    assert kwargs["catalog_id"] == "cat_a"
    assert kwargs["collection_id"] == "col_a"
    sent_inputs = kwargs["execution_request"].inputs
    assert sent_inputs["asset_id"] == "a1"
    assert sent_inputs["asset_uri"] == "gs://b/o.tif"


@pytest.mark.asyncio
async def test_execute_409_for_non_raster_vector_asset() -> None:
    from fastapi import HTTPException

    from dynastore.modules.catalog.asset_service import AssetTypeEnum

    other = next(
        (t for t in AssetTypeEnum if t not in (AssetTypeEnum.RASTER, AssetTypeEnum.VECTORIAL)),
        None,
    )
    if other is None:
        pytest.skip("No non-raster/non-vector AssetTypeEnum value to test against")
    asset = _mock_asset(asset_type=other.value)
    asset.asset_type = other  # type: ignore[assignment]
    instance = GdalAssetProcess()

    with pytest.raises(HTTPException) as exc_info:
        await instance.execute(asset, {})
    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_execute_503_when_no_runner_available() -> None:
    """When ExecutionEngine has no runner for 'gdal' (no Cloud Run Job, no local
    worker) it raises NotImplementedError → execute() maps to a 503 that names
    the backend, not "GDAL not available in this service"."""
    from fastapi import HTTPException

    asset = _raster_asset()
    instance = GdalAssetProcess()

    fake_execute = AsyncMock(
        side_effect=NotImplementedError("No available runner for task 'gdal'")
    )
    with patch(
        "dynastore.modules.processes.processes_module.execute_process", fake_execute
    ), patch(
        "dynastore.tools.protocol_helpers.get_engine", return_value=object()
    ):
        with pytest.raises(HTTPException) as exc_info:
            await instance.execute(asset, {})

    assert exc_info.value.status_code == 503
    assert "backend" in str(exc_info.value.detail).lower()
