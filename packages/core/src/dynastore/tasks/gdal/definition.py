#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
"""Lightweight OGC Process definition for the GDAL info task.

Lives separately from ``gdalinfo_task.py`` so services that don't have GDAL
installed (e.g. the catalog when it only dispatches GDAL work to a Cloud Run
worker) can still surface the Process in ``/processes`` via the tasks'
definition-only fallback loader.
"""

from dynastore.modules.gdal.models import RasterInfo, VectorInfo
from dynastore.modules.processes.models import (
    JobControlOptions,
    Process,
    ProcessInput,
    ProcessOutput,
    ProcessScope,
)

GDALINFO_PROCESS_DEFINITION = Process(
    id="gdal",
    title="GDAL Info Task",
    description=(
        "Calculates GDAL/OGR information for an asset and enriches its metadata. "
        "Targets a single asset identified by ``asset_id`` in ``inputs``. Run it "
        "at the catalog mount for a catalog-level asset, or the collection mount "
        "for a collection-level asset; ``catalog_id`` (and ``collection_id`` at "
        "the collection mount) are taken from the URL path. The task resolves the "
        "asset's URI itself — callers do not supply it."
    ),
    version="1.0.0",
    scopes=[ProcessScope.CATALOG, ProcessScope.COLLECTION],
    jobControlOptions=[
        JobControlOptions.SYNC_EXECUTE,
        JobControlOptions.ASYNC_EXECUTE,
    ],
    inputs={
        "asset_id": ProcessInput(
            title="Asset ID",
            description="Identifier of the asset to inspect (required).",
            schema={"type": "string"},
        ),
        "catalog_id": ProcessInput(
            title="Catalog ID",
            description=(
                "Catalog owning the asset (required). Taken from the URL path."
            ),
            schema={"type": "string"},
        ),
        "collection_id": ProcessInput(
            title="Collection ID",
            description=(
                "Collection owning the asset, when collection-level. Taken from "
                "the URL path at the collection mount."
            ),
            schema={"type": ["string", "null"]},
        ),
        "asset_metadata": ProcessInput(
            title="Asset Metadata Override",
            description=(
                "Optional metadata patch merged onto the asset alongside the "
                "computed ``gdalinfo`` block."
            ),
            schema={"type": "object", "additionalProperties": True},
        ),
    },
    outputs={
        "info": ProcessOutput(
            title="Result Info",
            description="The calculated GDAL/OGR information.",
            schema={
                "oneOf": [
                    RasterInfo.model_json_schema(),
                    VectorInfo.model_json_schema(),
                ]
            },
        )
    },
)
