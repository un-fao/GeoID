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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for AssetBlobReaper — GCS blob removal on asset hard-delete."""

from unittest.mock import MagicMock, patch

import pytest

from dynastore.modules.gcp.asset_sync import AssetBlobReaper


class TestAssetBlobReaperSkips:
    """Reaper must short-circuit (no GCS calls) on inapplicable payloads."""

    @pytest.mark.asyncio
    async def test_skips_when_owned_by_not_gcs(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"owned_by": "local", "uri": "gs://b/k"},
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_owned_by_missing(self):
        """Virtual assets carry no owned_by — never touch external hrefs."""
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"href": "https://example.com/x.tif"},
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_uri_not_gs(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"owned_by": "gcs", "uri": "https://example.com/x"},
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_uri_missing(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"owned_by": "gcs"},
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_payload_missing(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", asset_id="aid-1", payload=None,
            )
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_catalog_id_missing(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol"
        ) as mock_get:
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id=None, asset_id="aid-1",
                payload={"owned_by": "gcs", "uri": "gs://b/k"},
            )
            mock_get.assert_not_called()


class TestAssetBlobReaperDeletes:
    """Reaper must call blob.delete() on a GCS-backed hard-deleted asset."""

    @pytest.mark.asyncio
    async def test_deletes_collection_asset_blob(self):
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_gcp = MagicMock()
        mock_gcp.get_storage_client.return_value = mock_client

        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=mock_gcp,
        ):
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", collection_id="coll-1", asset_id="aid-1",
                payload={
                    "owned_by": "gcs",
                    "uri": "gs://my-bucket/collections/coll-1/file.tif",
                },
            )

        mock_client.bucket.assert_called_once_with("my-bucket")
        mock_bucket.blob.assert_called_once_with("collections/coll-1/file.tif")
        mock_blob.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_deletes_catalog_tier_asset_blob(self):
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_gcp = MagicMock()
        mock_gcp.get_storage_client.return_value = mock_client

        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=mock_gcp,
        ):
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", collection_id=None, asset_id="aid-1",
                payload={
                    "owned_by": "gcs",
                    "uri": "gs://my-bucket/catalog/file.tif",
                },
            )

        mock_blob.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_swallows_gcs_exceptions(self):
        """Any GCS API error must be logged and swallowed, never raised."""
        mock_blob = MagicMock()
        mock_blob.delete.side_effect = RuntimeError("GCS 503")
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_gcp = MagicMock()
        mock_gcp.get_storage_client.return_value = mock_client

        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=mock_gcp,
        ):
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"owned_by": "gcs", "uri": "gs://my-bucket/blob.tif"},
            )

    @pytest.mark.asyncio
    async def test_skips_when_storage_client_uninitialized(self):
        mock_gcp = MagicMock()
        mock_gcp.get_storage_client.side_effect = RuntimeError("uninit")

        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=mock_gcp,
        ):
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"owned_by": "gcs", "uri": "gs://my-bucket/blob.tif"},
            )

    @pytest.mark.asyncio
    async def test_skips_when_gcp_module_unregistered(self):
        with patch(
            "dynastore.modules.gcp.asset_sync.get_protocol",
            return_value=None,
        ):
            await AssetBlobReaper.on_asset_hard_delete(
                catalog_id="cat-1", asset_id="aid-1",
                payload={"owned_by": "gcs", "uri": "gs://my-bucket/blob.tif"},
            )
