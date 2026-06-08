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

"""FastAPI dependency that rejects requests to disabled extensions.

Implemented as a request dependency instead of a custom `APIRoute` subclass
because `FastAPI.include_router` re-creates routes via the source route's
`type(route)`, so `router.route_class = ...` set after routes exist has no
effect. A dependency attached at `include_router` time runs for every included
operation, which is exactly the cross-cutting check this feature needs.
"""

from typing import Callable

from fastapi import HTTPException, Request

from dynastore.extensions.tools.exposure_matrix import ExposureMatrix


def make_exposure_dependency(matrix: ExposureMatrix, ext_id: str) -> Callable:
    """Build an async dependency that 503s when `ext_id` is disabled.

    Pass via `app.include_router(..., dependencies=[Depends(dep)])` so the
    check runs before any route in the extension's router.
    """

    async def _check(request: Request) -> None:
        snap = await matrix.get()
        if not snap.platform.get(ext_id, True):
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Service '{ext_id}' is disabled on this platform. "
                    "Contact the administrator."
                ),
            )
        catalog_id = request.path_params.get("catalog_id")
        if catalog_id and snap.catalogs.get(catalog_id, {}).get(ext_id, True) is False:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Service '{ext_id}' is disabled for catalog '{catalog_id}'. "
                    "Contact the administrator."
                ),
            )

    return _check
