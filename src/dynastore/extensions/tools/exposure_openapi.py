"""Install a filtered `app.openapi` that omits platform-disabled extensions."""

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from dynastore.extensions.tools.exposure_matrix import ExposureMatrix


def install_filtered_openapi(app: FastAPI, matrix: ExposureMatrix) -> None:
    # Capture any previous override (e.g., IAM's securitySchemes injection) so
    # this wrapper composes on top of it rather than silently replacing it.
    previous_openapi = app.openapi

    def custom_openapi():
        if app.openapi_schema is not None:
            return app.openapi_schema
        # Let the previous override build the base schema, then filter paths.
        # previous_openapi may cache on app.openapi_schema; clear it so our
        # filter output is the cached value, not the unfiltered one.
        app.openapi_schema = None
        if previous_openapi is not None:
            schema = previous_openapi()
        else:
            schema = get_openapi(
                title=app.title, version=app.version,
                description=app.description, routes=app.routes,
            )
        app.openapi_schema = None
        snap = matrix.get_sync()
        disabled = {e for e, on in snap.platform.items() if not on}
        if disabled:
            paths = {}
            for path, methods in schema.get("paths", {}).items():
                kept = {
                    m: op for m, op in methods.items()
                    if not (set(op.get("tags", [])) & disabled)
                }
                if kept:
                    paths[path] = kept
            schema["paths"] = paths
        app.openapi_schema = schema
        return schema

    app.openapi = custom_openapi
