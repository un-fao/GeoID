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

"""OGC Dimensions extension — exposes dimension generators as REST API.

Wraps the ogc-dimensions package (pip dependency) into a Dynastore
ExtensionProtocol so it can be deployed on the tools Cloud Run service.
"""

import logging

from fastapi import APIRouter, FastAPI

from dynastore.extensions.protocols import ExtensionProtocol

logger = logging.getLogger(__name__)


class DimensionsExtension(ExtensionProtocol):
    priority: int = 200

    def __init__(self, app: FastAPI):
        self.app = app

        from ogc_dimensions.api.routes import DIMENSIONS, DimensionConfig, router as dimensions_router
        from ogc_dimensions.generators import (
            DekadalGenerator,
            IntegerRangeGenerator,
            PentadalAnnualGenerator,
            PentadalMonthlyGenerator,
            StaticTreeGenerator,
        )
        from ogc_dimensions.generators.tree import LeveledTreeGenerator

        from .use_cases import ADMIN_NODES, INDICATOR_NODES, SPECIES_NODES

        # -- Temporal pagination demos (100-year extents) ----------------------
        #
        # Three non-Gregorian calendars in wide operational use for agri/climate
        # monitoring demonstrate both pagination (large member counts) and a
        # real interoperability problem: the same "5-day period" concept is
        # encoded with two incompatible calendar systems depending on the data
        # producer.  Clients cannot combine pentadal datasets without knowing
        # which system was used.
        #
        # Reference: https://github.com/ccancellieri/ogc-dimensions/tree/main/spec

        DIMENSIONS["temporal-dekadal"] = DimensionConfig(
            generator=DekadalGenerator(),
            description=(
                "Dekadal temporal dimension — 10-day periods, 36 per year, "
                "month-aligned (D1=1–10, D2=11–20, D3=remainder). "
                "Widely used for agricultural monitoring and early warning systems. "
                "100-year extent (1950–2050) demonstrates OGC-style pagination over "
                "large temporal dimensions (3 600+ members)."
            ),
            extent_min="1950-01-01",
            extent_max="2050-12-31",
        )
        DIMENSIONS["temporal-pentadal-monthly"] = DimensionConfig(
            generator=PentadalMonthlyGenerator(),
            description=(
                "Pentadal-monthly temporal dimension — 5-day periods, 72 per year, "
                "month-aligned (P1=1–5, P2=6–10, …, P6=26–EOM). "
                "Used by rainfall estimation products that align dekads and pentads "
                "to the same month boundaries (e.g. CHIRPS, CDT). "
                "100-year extent yields 7 200+ members; illustrates that pentadal "
                "and dekadal periods from the same producer are directly comparable."
            ),
            extent_min="1950-01-01",
            extent_max="2050-12-31",
        )
        DIMENSIONS["temporal-pentadal-annual"] = DimensionConfig(
            generator=PentadalAnnualGenerator(),
            description=(
                "Pentadal-annual temporal dimension — 5-day periods, 73 per year, "
                "year-start-aligned (P1=Jan 1–5, …, P73=Dec 27–31). "
                "Used by global precipitation climatology products that count pentads "
                "from January 1 regardless of month boundaries (e.g. GPCP, CPC/NOAA). "
                "⚠ Interoperability note: pentad #12 in the monthly system and pentad "
                "#12 in the annual system refer to different calendar intervals — "
                "a client must know which encoding was used before combining datasets."
            ),
            extent_min="1950-01-01",
            extent_max="2050-12-31",
        )

        # -- Hierarchical dimensions -------------------------------------------
        DIMENSIONS["indicator-tree"] = DimensionConfig(
            generator=StaticTreeGenerator(nodes=INDICATOR_NODES),
            description=(
                "Statistical indicator tree. "
                "Recursive hierarchy: Domain → Group → Indicator."
            ),
            extent_min="",
            extent_max="",
        )
        DIMENSIONS["admin-boundaries"] = DimensionConfig(
            generator=LeveledTreeGenerator(nodes=ADMIN_NODES),
            description=(
                "Administrative boundaries. Leveled hierarchy: "
                "Continent (L0) → Country (L1) → Region (L2). "
                "Supports ?level= filter."
            ),
            extent_min="",
            extent_max="",
        )
        DIMENSIONS["forestry-species"] = DimensionConfig(
            generator=StaticTreeGenerator(nodes=SPECIES_NODES),
            description=(
                "Forestry species classification. "
                "Recursive hierarchy with search (exact + like)."
            ),
            extent_min="",
            extent_max="",
        )

        # -- Integer range demo ------------------------------------------------
        DIMENSIONS["elevation-bands"] = DimensionConfig(
            generator=IntegerRangeGenerator(step=50),
            description=(
                "Elevation bands (50 m step, 0–8848 m). "
                "Bijective, searchable, supports /inverse."
            ),
            extent_min="0",
            extent_max="8848",
        )

        self.router = APIRouter(prefix="/dimensions", tags=["OGC Dimensions"])
        self.router.include_router(dimensions_router)

        logger.info(
            "OGC Dimensions extension loaded — %d dimensions registered",
            len(DIMENSIONS),
        )
