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

"""Local-disk asset download.

Implements :class:`AssetDownloadProtocol` for assets whose ``owned_by ==
"local"`` and whose ``uri`` is a ``file://`` path: returns the bearer-auth
protected ``GET /local-download/{catalog_id}/{asset_id}`` route served by
the assets extension. Local bytes never leave the auth perimeter, so no
signed-URL machinery is needed — authentication is enforced by the ``auth``
extension's middleware on the download route.
"""
from __future__ import annotations

import logging
from typing import Optional

from dynastore.modules.catalog.asset_service import Asset

logger = logging.getLogger(__name__)


class LocalAssetDownload:
    """Resolves the bearer-auth local-download route for locally-owned assets."""

    def applies_to(self, asset: Asset) -> bool:
        return (
            asset.owned_by == "local"
            and bool(asset.uri)
            and asset.uri.startswith("file://")
        )

    async def resolve_download_url(
        self, asset: Asset, ttl: Optional[int] = None
    ) -> str:
        if not self.applies_to(asset):
            # Not a locally-owned asset — signal caller to try another provider.
            raise ValueError(
                "download not applicable: asset is not locally owned."
            )
        return f"/local-download/{asset.catalog_id}/{asset.asset_id}"
