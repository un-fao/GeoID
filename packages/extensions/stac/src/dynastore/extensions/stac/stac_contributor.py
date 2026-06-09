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

"""STAC-specific extension-enrichment contribution protocol.

Distinct from ``AssetContributor`` / ``LinkContributor`` (which are
consumer-agnostic — STAC, Records, Features, Coverages all consume them):
``StacContributor`` produces STAC-only enrichment — extension URIs for the
``stac_extensions`` array plus top-level STAC fields. Producers register an
instance via ``register_plugin`` and consumers iterate
``get_protocols(StacContributor)``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol, Set, Tuple, runtime_checkable

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.extensions.stac.stac_models import inject_stac_language_fields


@dataclass(frozen=True)
class StacContribution:
    """Neutral STAC enrichment: extension URIs to declare plus top-level
    STAC fields to merge onto the target document."""

    stac_extensions: Tuple[str, ...] = ()
    extra_fields: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class StacContributor(Protocol):
    """Producer of STAC extension enrichment for a Catalog/Collection/Item.

    Optional producer: if the module isn't loaded, ``get_protocols()``
    returns an empty iteration and generation proceeds without it.
    """

    priority: int

    def contribute_stac(self, ref: ResourceRef) -> Iterable[StacContribution]: ...


class LanguageStacContributor:
    """First ``StacContributor`` — declares the STAC Language extension and
    its ``language`` / ``languages`` fields, reusing the canonical
    ``inject_stac_language_fields`` logic (including the en-US fallback).

    Baseline capability: always registered, never gated by
    ``auto_render_extensions``. Yields nothing when no languages are
    available (matching ``inject_stac_language_fields``' own early return).
    """

    priority: int = 10  # baseline; runs before default-100 contributors

    def contribute_stac(self, ref: ResourceRef) -> Iterable[StacContribution]:
        available: Set[str] = set(ref.extras.get("available_languages") or [])
        if not available:
            return
        lang = ref.lang or "en"
        enriched = inject_stac_language_fields({}, available, lang)
        extra = {
            k: enriched[k] for k in ("language", "languages") if k in enriched
        }
        yield StacContribution(
            stac_extensions=tuple(enriched.get("stac_extensions", ())),
            extra_fields=extra,
        )
