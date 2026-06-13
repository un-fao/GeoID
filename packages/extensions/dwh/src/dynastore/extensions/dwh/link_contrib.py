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

"""DwhLinkContributor — advertises legacy /dwh join endpoint in collection responses.

Emits a ``resource_root`` link per collection so consumers can discover the
DWH join surface alongside the OGC API - Joins surface. Remove once clients
have migrated to /join and DwhService is retired.
"""

from __future__ import annotations

from typing import AsyncIterator

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.models.protocols.link_contrib import AnchoredLink


class DwhLinkContributor:
    """LinkContributor for the legacy Data Warehouse join surface (/dwh).

    Contributes ``rel="dwh-join"`` (POST) at ``resource_root`` — the
    per-catalog join endpoint that accepts a DWHJoinRequestBase body.
    """

    priority: int = 100  # matches DwhService.priority

    async def contribute_links(self, ref: ResourceRef) -> AsyncIterator[AnchoredLink]:
        if ref.item_id is not None:
            return  # join is collection-scoped

        base = (ref.base_url or "").rstrip("/")
        href = f"{base}/dwh/catalogs/{ref.catalog_id}/join"

        yield AnchoredLink(
            anchor="resource_root",
            rel="dwh-join",
            href=href,
            title="Data Warehouse join (legacy — use /join instead)",
            media_type="application/json",
            extras={"method": "POST"},
        )
