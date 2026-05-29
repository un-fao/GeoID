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

"""``geoid`` routing preset (#847) — flagship FAO GeoID profile.

Composes the ``private_catalog`` bundle (PG-first storage, per-tenant
private ES indexers on the items tier, IAM DENY on the catalog's routes)
and opts the catalog in to a single anonymous audience:

  * ``CatalogLookupAudience.is_public=True`` — opens the anonymous
    lookup-only needle endpoint ``POST /search/catalogs/{cat}/items-search``
    (resolve one item by exact ``geoid`` / ``external_id``, gated by the
    ``lookup_only_search`` handler so a broadening request never matches),
    and arms the geoid extension's anonymous STAC/Features enumeration DENY
    so the catalog is reachable only by exact lookup — never by browsing,
    listing, or filterable search.

``on_applied`` installs the per-catalog IAM policies in the catalog's
tenant schema (see ``catalog_policies.register_geoid_policies_for_catalog``):
the lookup ALLOW, the STAC + Features enumeration DENYs, and a dormant
per-collection anonymous-create ALLOW.

Anonymous *writes* are off by default: no collection opts in, so the catalog
refuses anonymous inserts (un-fao/GeoID#1204 — public users must not insert
by default). A catalog admin turns a single intake collection into a "blind
dropbox" by setting ``CollectionWriteAudience.allow_anonymous_create=True``
on it (PUT ``/configs/catalogs/{cat}/collections/{col}/plugins/collection_write_audience``).
For that collection only, anonymous ``POST .../items`` is then allowed: the
create ALLOW carries a higher priority than the enumeration DENY, so it wins
the #915 ranking (highest priority wins; DENY only wins on a tie) — while
every browse / list / search verb stays denied because the ALLOW is
POST-only. The two halves compose on one catalog: contribute an item and
resolve it later by a known ``geoid``, but never enumerate what is there.

Auto-registers on extension import.
"""
from __future__ import annotations

from dynastore.modules.iam.audience_configs import CatalogLookupAudience
from typing import ClassVar

from dynastore.modules.storage.presets import BundlePreset, get_preset, register_preset
from dynastore.modules.storage.presets.protocol import PresetBundle, PresetBundleEntry, PresetTier


class GeoidPreset(BundlePreset):
    """Flagship FAO GeoID profile: private storage + anonymous lookup-only."""

    name = "geoid"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False
    description = (
        "FAO GeoID flagship profile. Composes the private_catalog bundle "
        "(PG-first storage + per-tenant private ES indexers, IAM DENY on "
        "default routes) with one anonymous opt-in: catalog-level "
        "lookup_audience.is_public=True. That arms lookup-only mode — "
        "anonymous callers can resolve a single record by geoid or "
        "external_id but cannot enumerate. Anonymous insert stays off unless "
        "a catalog admin opts a specific collection in via "
        "CollectionWriteAudience.allow_anonymous_create (per-collection "
        "blind-dropbox intake). Use for catalogs that serve anonymous lookup "
        "— and optionally anonymous contribution to chosen collections — "
        "while keeping the read/index/browse surface private."
    )

    def build(self, catalog_id: str, **_scope: str) -> PresetBundle:
        base = get_preset("private_catalog").build(catalog_id=catalog_id)
        return PresetBundle(
            entries=(
                *base.entries,
                PresetBundleEntry(
                    slot="audience:catalog_lookup_audience",
                    config_cls=CatalogLookupAudience,
                    instance=CatalogLookupAudience(is_public=True),
                    rollback_priority=100,
                ),
            )
        )

    async def on_applied(self, catalog_id: str, **_scope: str) -> None:
        """Called after the preset bundle is applied to a catalog.
        
        Registers per-catalog geoid IAM policies for the lookup-only access model.
        """
        from .catalog_policies import register_geoid_policies_for_catalog
        
        await register_geoid_policies_for_catalog(catalog_id)


register_preset(GeoidPreset())
