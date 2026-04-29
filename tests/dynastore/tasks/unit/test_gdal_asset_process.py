"""Phase D regression test — GdalAssetProcess satisfies AssetProcessProtocol.

Without this, the asset router at
`POST /assets/.../assets/{aid}/processes/gdal/execution` returns 404
"Asset process 'gdal' is not registered." even though the task is in the
OGC Processes registry. The fix is the duck-typed `GdalAssetProcess`
wrapper registered in `GdalModule.lifespan()`.
"""
from types import SimpleNamespace

import pytest

from dynastore.models.protocols.asset_process import AssetProcessProtocol
from dynastore.tasks.gdal.asset_process import GdalAssetProcess


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


@pytest.mark.asyncio
async def test_describe_applicable_for_raster() -> None:
    from unittest.mock import patch

    from dynastore.modules.catalog.asset_service import AssetTypeEnum

    asset = _mock_asset(asset_type=AssetTypeEnum.RASTER.value)
    asset.asset_type = AssetTypeEnum.RASTER  # type: ignore[assignment]
    instance = GdalAssetProcess()
    with patch("dynastore.modules.gdal.service.GDAL_AVAILABLE", True):
        descriptor = await instance.describe(asset)
    assert descriptor.process_id == "gdal"
    assert descriptor.applicable is True
    assert descriptor.reason is None


@pytest.mark.asyncio
async def test_describe_inapplicable_when_gdal_missing() -> None:
    from unittest.mock import patch

    from dynastore.modules.catalog.asset_service import AssetTypeEnum

    asset = _mock_asset(asset_type=AssetTypeEnum.RASTER.value)
    asset.asset_type = AssetTypeEnum.RASTER  # type: ignore[assignment]
    instance = GdalAssetProcess()
    with patch("dynastore.modules.gdal.service.GDAL_AVAILABLE", False):
        descriptor = await instance.describe(asset)
    assert descriptor.applicable is False
    assert "not available" in (descriptor.reason or "").lower()


@pytest.mark.asyncio
async def test_describe_inapplicable_for_table_asset() -> None:
    from unittest.mock import patch

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
    with patch("dynastore.modules.gdal.service.GDAL_AVAILABLE", True):
        descriptor = await instance.describe(asset)
    assert descriptor.applicable is False
    assert other.value in (descriptor.reason or "")
