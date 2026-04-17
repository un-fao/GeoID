"""Custom APIRoute that rejects requests to disabled extensions."""

from typing import Callable, cast

from fastapi import HTTPException, Request
from fastapi.responses import Response
from fastapi.routing import APIRoute

from dynastore.extensions.tools.exposure_matrix import ExposureMatrix
from dynastore.extensions.tools.exposure_mixin import (
    ALWAYS_ON_EXTENSIONS,
    KNOWN_EXTENSION_IDS,
)


def make_exposure_gated_route(matrix: ExposureMatrix) -> type[APIRoute]:
    class ExposureGatedRoute(APIRoute):
        def get_route_handler(self) -> Callable:
            original = super().get_route_handler()
            ext_ids = set(self.tags or []) & KNOWN_EXTENSION_IDS
            if not ext_ids or ext_ids & ALWAYS_ON_EXTENSIONS:
                return original
            ext_id = cast(str, next(iter(ext_ids)))

            async def custom_handler(request: Request) -> Response:
                snap = await matrix.get()
                if not snap.platform.get(ext_id, True):
                    raise HTTPException(
                        status_code=503,
                        detail=f"Service '{ext_id}' is disabled on this platform. Contact the administrator.",
                    )
                catalog_id = request.path_params.get("catalog_id")
                if catalog_id and snap.catalogs.get(catalog_id, {}).get(ext_id, True) is False:
                    raise HTTPException(
                        status_code=503,
                        detail=f"Service '{ext_id}' is disabled for catalog '{catalog_id}'. Contact the administrator.",
                    )
                return await original(request)

            return custom_handler

    return ExposureGatedRoute
