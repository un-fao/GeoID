"""Install a filtered `app.openapi` that omits platform-disabled extensions."""

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from dynastore.extensions.tools.exposure_matrix import ExposureMatrix


def install_filtered_openapi(app: FastAPI, matrix: ExposureMatrix) -> None:
    def custom_openapi():
        if app.openapi_schema is not None:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title, version=app.version,
            description=app.description, routes=app.routes,
        )
        snap = matrix.get_sync()
        disabled = {e for e, on in snap.platform.items() if not on}
        if not disabled:
            app.openapi_schema = schema
            return schema
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
