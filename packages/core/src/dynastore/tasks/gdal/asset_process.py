"""``gdal`` asset process — wraps the GDAL OGC Process for asset-scoped invocation.

Implements ``AssetProcessProtocol`` (duck-typed) so that the asset router at
``POST /assets/catalogs/{c}/[collections/{c}/]assets/{a}/processes/gdal/execution``
can find and execute it. Without this wrapper the underlying ``GdalInfoTask``
exists in the OGC Processes registry but the asset router returns
``Asset process 'gdal' is not registered.``

Delegates to ``processes_module.execute_process`` so the standard OGC pipeline
(input validation, mode selection, ExecutionEngine routing) is reused — no
duplicate dispatch code here.

GDAL itself is **not** required in the service that hosts this process. The
catalog API only *dispatches* the work; a GDAL-equipped backend executes it:

* the deployed ``dynastore-gdal-job`` Cloud Run Job (via ``GcpJobRunner``), or
* an in-process GDAL worker (via ``BackgroundRunner``, when ``osgeo`` is
  installed in that service).

Applicability and availability are therefore decided by *runner* presence, not
by ``GDAL_AVAILABLE`` in the local process.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import HTTPException, status

from dynastore.models.protocols.asset_process import (
    AssetProcessDescriptor,
    AssetProcessOutput,
    HTTPMethod,
)
from dynastore.modules.catalog.asset_service import Asset, AssetTypeEnum

logger = logging.getLogger(__name__)


def gdal_backend_available() -> bool:
    """True when some runner can execute the ``gdal`` task.

    Covers both delivery paths without requiring GDAL in *this* process:

    * ``GcpJobRunner`` — advertises ``gdal`` when the ``dynastore-gdal-job``
      Cloud Run Job is configured in the job map.
    * ``BackgroundRunner`` — advertises ``gdal`` when the ``GdalInfoTask`` is
      loaded (i.e. ``osgeo`` is installed in this service).

    Re-uses ``ExecutionEngine.get_runners_for`` so the availability check and the
    actual ``execute()`` dispatch resolve runners through identical logic.
    """
    from dynastore.modules.tasks.execution import execution_engine
    from dynastore.modules.tasks.models import TaskExecutionMode

    runners = execution_engine.get_runners_for(
        "gdal", TaskExecutionMode.ASYNCHRONOUS
    )
    return bool(runners)


class GdalAssetProcess:
    """Asset-scoped invocation surface for the GDAL info task.

    Applicable to RASTER and VECTORIAL assets whenever a GDAL execution backend
    is reachable (Cloud Run Job or in-process worker). The execute path delegates
    to the OGC Processes layer so a single canonical execution code path runs the
    GdalInfoTask — both via this asset surface and via direct OGC
    ``/processes/gdal/execution`` calls.
    """

    process_id: str = "gdal"
    http_method: HTTPMethod = "POST"
    title: str = "GDAL Info"
    description: str = (
        "Calculates GDAL/OGR information (driver, bands, projection, extent) "
        "for a raster or vector asset and enriches its metadata."
    )

    async def describe(self, asset: Asset) -> AssetProcessDescriptor:
        asset_type_ok = asset.asset_type in (
            AssetTypeEnum.RASTER,
            AssetTypeEnum.VECTORIAL,
        )
        if not asset_type_ok:
            applicable = False
            reason = (
                f"GDAL info applies only to RASTER/VECTORIAL assets; "
                f"this asset is {asset.asset_type.value}."
            )
        elif not gdal_backend_available():
            applicable = False
            reason = (
                "No GDAL execution backend available: neither an in-process "
                "GDAL worker nor a deployed GDAL Cloud Run Job is registered."
            )
        else:
            applicable = True
            reason = None
        return AssetProcessDescriptor(
            process_id=self.process_id,
            title=self.title,
            description=self.description,
            http_method=self.http_method,
            applicable=applicable,
            reason=reason,
            parameters_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": True,
                "description": (
                    "GDAL info takes no required parameters; the asset's URI "
                    "and metadata are derived from the asset row itself."
                ),
            },
        )

    async def execute(
        self, asset: Asset, params: Dict[str, Any]
    ) -> AssetProcessOutput:
        if asset.asset_type not in (AssetTypeEnum.RASTER, AssetTypeEnum.VECTORIAL):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"GDAL info not applicable to asset_type={asset.asset_type.value}."
                ),
            )

        # No local GDAL guard: this service dispatches; a GDAL-equipped backend
        # executes. ExecutionEngine picks the right runner — GcpJobRunner spawns
        # the deployed dynastore-gdal-job Cloud Run Job, or BackgroundRunner runs
        # the GdalInfoTask in-process where osgeo is installed. The GdalInfoTask
        # uses the AssetTasksSPI to inject asset metadata into inputs, so we only
        # supply caller-provided extras plus the asset locators here.
        from dynastore.modules.processes import processes_module
        from dynastore.modules.processes.models import (
            ExecuteRequest,
            JobControlOptions,
        )
        from dynastore.tools.protocol_helpers import get_engine

        engine = get_engine()
        if engine is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No database engine available for GDAL job dispatch.",
            )

        execution_request = ExecuteRequest(
            inputs={
                **params,
                "asset_id": asset.asset_id,
                "asset_uri": str(asset.uri) if asset.uri else None,
                "catalog_id": asset.catalog_id,
                "collection_id": asset.collection_id,
                "asset_type": asset.asset_type.value,
            },
            outputs={},
        )

        try:
            # ASYNC_EXECUTE so the dispatcher routes to a backend runner rather
            # than attempting in-process execution: GcpJobRunner → the deployed
            # dynastore-gdal-job Cloud Run Job (which has GDAL/OGR installed), or
            # BackgroundRunner in services that ship osgeo. The catalog API
            # service carries neither GDAL nor the gdal task, which is fine — it
            # only needs a capable runner registered, not GDAL itself.
            result = await processes_module.execute_process(
                process_id="gdal",
                execution_request=execution_request,
                engine=engine,
                preferred_mode=JobControlOptions.ASYNC_EXECUTE,
                catalog_id=asset.catalog_id,
                collection_id=asset.collection_id,
            )
        except NotImplementedError as exc:
            # ExecutionEngine.execute raises this when no runner can handle the
            # 'gdal' task in the requested mode — i.e. neither a GDAL Cloud Run
            # Job nor an in-process GDAL worker is deployed anywhere. This is a
            # capability/deployment gap, distinct from a per-request error.
            logger.warning(
                "GdalAssetProcess.execute: no GDAL execution backend available "
                "for asset=%s/%s/%s: %s",
                asset.catalog_id,
                asset.collection_id,
                asset.asset_id,
                exc,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "No GDAL execution backend available: deploy the GDAL "
                    "Cloud Run Job or a GDAL-equipped worker to run this process."
                ),
            ) from exc
        except Exception as exc:
            logger.exception(
                "GdalAssetProcess.execute: dispatch failed for "
                "asset=%s/%s/%s: %s",
                asset.catalog_id,
                asset.collection_id,
                asset.asset_id,
                exc,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"GDAL info dispatch failed: {exc}",
            ) from exc

        # Async dispatch returns a job handle (StatusInfo); expose as `job`
        # so the asset-process REST surface advertises the polling shape
        # rather than pretending the result is inline.
        if hasattr(result, "jobID") or (isinstance(result, dict) and result.get("jobID")):
            job_id = (
                getattr(result, "jobID", None)
                or (result.get("jobID") if isinstance(result, dict) else None)
            )
            return AssetProcessOutput(type="job", job_id=str(job_id))

        # Sync fallback (defensive): keep the inline shape for cases where
        # ExecutionEngine completed synchronously despite the async preference.
        # `result` is `Any` from execute_process(); use getattr-then-call so
        # Pyright doesn't carry a stale dict-narrowing from the if-branch
        # above into this otherwise-unrelated fallback.
        model_dump = getattr(result, "model_dump", None)
        return AssetProcessOutput(
            type="inline",
            data=model_dump(mode="json") if callable(model_dump) else result,
        )
