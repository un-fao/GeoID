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

"""JoinsLinkContributor — advertises OGC API - Joins endpoints in collection responses.

Emits two ``resource_root`` links per collection so OGC Features, STAC, and
other consumers surface the /join surface without callers needing to know
the join extension's prefix.

Links are collection-scoped (skipped for individual item refs).
"""

from __future__ import annotations

from typing import AsyncIterator

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.models.protocols.link_contrib import AnchoredLink


class JoinsLinkContributor:
    """LinkContributor for OGC API - Joins.

    Contributes:
    - ``rel="join"`` (GET)  — describe endpoint (capabilities + supported drivers)
    - ``rel="join"`` (POST) — execute endpoint
    """

    priority: int = 180  # matches JoinsService.priority

    async def contribute_links(self, ref: ResourceRef) -> AsyncIterator[AnchoredLink]:
        if ref.item_id is not None:
            return  # join is collection-scoped

        base = (ref.base_url or "").rstrip("/")
        href = (
            f"{base}/join/catalogs/{ref.catalog_id}"
            f"/collections/{ref.collection_id}/join"
        )

        yield AnchoredLink(
            anchor="resource_root",
            rel="join",
            href=href,
            title="OGC API - Joins: describe",
            media_type="application/json",
            extras={"method": "GET"},
        )
        yield AnchoredLink(
            anchor="resource_root",
            rel="join",
            href=href,
            title="OGC API - Joins: execute",
            media_type="application/json",
            extras={"method": "POST"},
        )
