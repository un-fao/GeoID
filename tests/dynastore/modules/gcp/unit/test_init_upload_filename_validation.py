"""Strict filename validation on the init-upload request.

Without an extension on the uploaded filename, downstream ingestion
cannot pick a reader by URI suffix.  Reject at the API boundary.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.modules.gcp.gcp_config import InitiateUploadRequest
from dynastore.modules.catalog.asset_service import AssetUploadDefinition, AssetTypeEnum


def _asset() -> AssetUploadDefinition:
    return AssetUploadDefinition(
        asset_id="aoi_oasis",
        asset_type=AssetTypeEnum.VECTORIAL,
    )


def test_filename_with_extension_accepted():
    req = InitiateUploadRequest(
        filename="aoi_oasis.zip",
        content_type="application/zip",
        catalog_id="t1",
        collection_id="c1",
        asset=_asset(),
    )
    assert req.filename == "aoi_oasis.zip"


def test_bare_filename_rejected_with_422_friendly_error():
    """The original bug: filename='aoi_oasis' with no extension."""
    with pytest.raises(ValidationError) as ei:
        InitiateUploadRequest(
            filename="aoi_oasis",
            content_type="application/zip",
            catalog_id="t1",
            collection_id="c1",
            asset=_asset(),
        )
    msg = str(ei.value)
    assert "aoi_oasis" in msg
    assert "extension" in msg.lower()


def test_empty_filename_rejected():
    with pytest.raises(ValidationError):
        InitiateUploadRequest(
            filename="",
            content_type="application/zip",
            catalog_id="t1",
            collection_id="c1",
            asset=_asset(),
        )


def test_dotfile_with_no_extension_rejected():
    """A leading-dot 'extension' isn't a real extension (PurePosixPath
    treats e.g. '.gitignore' as having no suffix). Reject."""
    with pytest.raises(ValidationError):
        InitiateUploadRequest(
            filename=".aoi_oasis",
            content_type="application/zip",
            catalog_id="t1",
            collection_id="c1",
            asset=_asset(),
        )


def test_filename_with_double_extension_accepted():
    req = InitiateUploadRequest(
        filename="archive.tar.gz",
        content_type="application/gzip",
        catalog_id="t1",
        collection_id="c1",
        asset=_asset(),
    )
    assert req.filename == "archive.tar.gz"
