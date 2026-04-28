"""Unit tests for UploadAssetProcess (Phase 3)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from dynastore.extensions.assets.asset_processes import UploadAssetProcess


def _asset(uri="", asset_id="aid-1", catalog_id="cat-1", collection_id="coll-1"):
    a = MagicMock()
    a.uri = uri
    a.asset_id = asset_id
    a.catalog_id = catalog_id
    a.collection_id = collection_id
    from dynastore.modules.catalog.asset_service import AssetTypeEnum
    a.asset_type = AssetTypeEnum.ASSET
    a.metadata = {}
    a.owned_by = None
    return a


class TestDescribe:
    @pytest.mark.asyncio
    async def test_applicable_when_uri_blank(self):
        proc = UploadAssetProcess()
        d = await proc.describe(_asset(uri=""))
        assert d.applicable is True
        assert d.process_id == "upload"
        assert d.http_method == "POST"

    @pytest.mark.asyncio
    async def test_not_applicable_when_uri_set(self):
        proc = UploadAssetProcess()
        d = await proc.describe(_asset(uri="gs://b/k"))
        assert d.applicable is False
        assert "already" in (d.reason or "").lower()


class TestExecute:
    @pytest.mark.asyncio
    async def test_409_when_uri_already_set(self):
        proc = UploadAssetProcess()
        with pytest.raises(HTTPException) as exc:
            await proc.execute(_asset(uri="gs://b/k"), {})
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_503_when_no_upload_backend(self):
        proc = UploadAssetProcess()
        with patch(
            "dynastore.modules.storage.router.get_asset_upload_driver",
            new=AsyncMock(return_value=None),
        ):
            with pytest.raises(HTTPException) as exc:
                await proc.execute(_asset(uri=""), {})
        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_returns_signed_url_from_routed_driver(self):
        proc = UploadAssetProcess()
        ticket = MagicMock()
        ticket.upload_url = "https://storage.googleapis.com/sig?upload_id=x"
        ticket.method = "PUT"
        ticket.headers = {"x-goog-resumable": "start"}
        ticket.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        ticket.backend = "gcs"

        provider = MagicMock()
        provider.initiate_upload = AsyncMock(return_value=ticket)

        with patch(
            "dynastore.modules.storage.router.get_asset_upload_driver",
            new=AsyncMock(return_value=provider),
        ):
            out = await proc.execute(_asset(uri=""), {"filename": "x.tif"})

        assert out.type == "signed_url"
        assert out.url == ticket.upload_url
        assert out.method == "PUT"
        assert out.headers == {"x-goog-resumable": "start"}
        assert out.expires_at is not None
        provider.initiate_upload.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_falls_back_method_to_post_when_unrecognised(self):
        proc = UploadAssetProcess()
        ticket = MagicMock()
        ticket.upload_url = "/local-upload/abc"
        ticket.method = "PATCH"  # not in HTTPMethod literal
        ticket.headers = None
        ticket.expires_at = None
        ticket.backend = "local"

        provider = MagicMock()
        provider.initiate_upload = AsyncMock(return_value=ticket)

        with patch(
            "dynastore.modules.storage.router.get_asset_upload_driver",
            new=AsyncMock(return_value=provider),
        ):
            out = await proc.execute(_asset(uri=""), {})

        assert out.method == "POST"
        assert out.expires_at is None
