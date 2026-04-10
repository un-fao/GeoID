#    Copyright 2025 FAO
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

# dynastore/tools/conformance.py

import re
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Set
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# --- Data Model ---

# Canonical Conformance is defined in ogc_common_models; re-export for compatibility.
from dynastore.extensions.tools.ogc_common_models import Conformance


class StandardSummary(BaseModel):
    name: str
    implemented: int
    uris: List[str]
    status: str


class ConformanceSummary(BaseModel):
    timestamp: str
    total_conformance_classes: int
    standards: List[StandardSummary]
    not_implemented: List[str]


# --- In-Memory Registry ---

_REGISTERED_URIS: Set[str] = set()

# Pattern map: standard display name -> regex matching its conformance URIs
_STANDARD_PATTERNS: Dict[str, str] = {
    "OGC API Features": r"ogcapi-features",
    "STAC API": r"stacspec\.org|stac-api",
    "OGC API Processes": r"ogcapi-processes",
    "OGC API Records": r"ogcapi-records",
    "OGC API Tiles": r"ogcapi-tiles",
    "OGC API Maps": r"ogcapi-maps",
    "OGC API Coverages": r"ogcapi-coverages",
    "OGC API EDR": r"ogcapi-edr",
    "OGC Dimensions": r"ogc-dimensions",
    "OGC API Styles": r"ogcapi-styles",
}

# Standards in the OGC REST API family that DynaStore could potentially implement
_ALL_OGC_STANDARDS = [
    "OGC API Common",
    "OGC API Features",
    "OGC API Tiles",
    "OGC API Maps",
    "OGC API Processes",
    "OGC API Records",
    "OGC API Coverages",
    "OGC API EDR",
    "OGC API DGGS",
    "OGC API Routes",
    "OGC API Joins",
    "OGC API Styles",
    "OGC API 3D GeoVolumes",
    "OGC API Moving Features",
    "OGC API Connected Systems",
    "SensorThings API",
    "STAC API",
    "OGC Dimensions",
]

# --- Public API for Registration and Retrieval ---

def register_conformance_uris(uris: List[str]):
    """
    Allows a DynaStore extension to register its supported conformance classes.
    This is called by each extension once at application startup.
    """
    _REGISTERED_URIS.update(uris)
    logger.info(f"Registered {len(uris)} new conformance URIs.")

def get_active_conformance() -> Conformance:
    """
    Returns the dynamically-built list of all registered conformance classes.
    """
    return Conformance(conformsTo=sorted(list(_REGISTERED_URIS)))


def get_conformance_summary() -> ConformanceSummary:
    """
    Returns a structured summary of OGC API compliance grouped by standard family.
    Classifies each registered conformance URI into its parent standard using
    regex patterns, computes per-standard counts, and lists standards with
    zero registered URIs as not_implemented.
    """
    all_uris = sorted(_REGISTERED_URIS)

    # Bucket URIs into standard families
    buckets: Dict[str, List[str]] = {name: [] for name in _STANDARD_PATTERNS}
    unclassified: List[str] = []

    for uri in all_uris:
        matched = False
        for name, pattern in _STANDARD_PATTERNS.items():
            if re.search(pattern, uri):
                buckets[name].append(uri)
                matched = True
                break
        if not matched:
            unclassified.append(uri)

    # Build per-standard summaries
    standards: List[StandardSummary] = []
    implemented_names: Set[str] = set()

    for name, uris in sorted(buckets.items()):
        if uris:
            implemented_names.add(name)
            standards.append(StandardSummary(
                name=name,
                implemented=len(uris),
                uris=uris,
                status="implemented",
            ))

    # Add unclassified URIs as a bucket if any exist
    if unclassified:
        standards.append(StandardSummary(
            name="Other / OGC Common",
            implemented=len(unclassified),
            uris=unclassified,
            status="implemented",
        ))
        implemented_names.add("OGC API Common")

    # Determine which standards have zero coverage
    not_implemented = sorted(
        s for s in _ALL_OGC_STANDARDS if s not in implemented_names
    )

    return ConformanceSummary(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_conformance_classes=len(all_uris),
        standards=standards,
        not_implemented=not_implemented,
    )
