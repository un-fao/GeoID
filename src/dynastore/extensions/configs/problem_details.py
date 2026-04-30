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
"""RFC 9457 Problem Details for the configs extension.

Replaces ``raise HTTPException(detail=str(e))`` with structured error
responses that match the RFC 9457 envelope:

    {
      "type":     "https://errors.dynastore.fao.org/config/<category>",
      "title":    "<short human label>",
      "status":   422,
      "detail":   "<longer human prose>",
      "instance": "/configs/.../GeometryStorage",
      "errors":   [{"field": "...", "message": "...", "value": ...}]
    }

Rendered with media type ``application/problem+json`` per the spec.
The handler is registered in ``ConfigsService.lifespan`` so the wrapper
is local to the configs extension; no global side effects.
"""

from typing import Any, Dict, List, Optional, Sequence

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError


_TYPE_BASE = "https://errors.dynastore.fao.org/config"


class ProblemError(BaseModel):
    """Per-field validation error (RFC 9457 ``errors`` extension member).

    Mirrors the structure operators expect when a request body fails
    validation: which field, why, and the offending value.
    """

    field: str = Field(
        ..., description="Dotted path to the failing field, e.g. 'target_srid'."
    )
    message: str = Field(
        ..., description="Human-readable validation message."
    )
    value: Optional[Any] = Field(
        default=None,
        description="The offending value, when serialisable.",
    )


class ProblemDetails(BaseModel):
    """RFC 9457 Problem Details for HTTP APIs.

    `type` is a URI identifying the problem class (clients can dispatch on
    it without parsing `title`); `title` is a short human label; `detail`
    is the longer prose; `instance` identifies this specific occurrence
    (typically the request path).
    """

    type: str = Field(
        ...,
        description=(
            "URI identifying the problem class — clients should dispatch on this, "
            "not on `title`. Format: 'https://errors.dynastore.fao.org/config/<category>'."
        ),
    )
    title: str = Field(
        ..., description="Short, human-readable summary of the problem class."
    )
    status: int = Field(
        ..., description="HTTP status code for this occurrence."
    )
    detail: Optional[str] = Field(
        default=None,
        description="Human-readable explanation specific to this occurrence.",
    )
    instance: Optional[str] = Field(
        default=None,
        description="URI identifying this specific occurrence — typically the request path.",
    )
    errors: Optional[List[ProblemError]] = Field(
        default=None,
        description=(
            "Per-field validation errors (RFC 9457 extension). Populated for "
            "422 responses arising from Pydantic validation."
        ),
    )


class ProblemException(Exception):
    """Raised to surface a ``ProblemDetails`` response.

    Subclasses :class:`Exception` (not :class:`HTTPException`) so the
    custom handler picks it up before FastAPI's default exception path
    can convert it to a plain JSON body.
    """

    def __init__(self, problem: ProblemDetails) -> None:
        self.problem = problem
        super().__init__(problem.title)


# ---------------------------------------------------------------------------
# Constructors — one per problem class kept in this extension's surface
# ---------------------------------------------------------------------------

def plugin_not_registered(plugin_id: str, *, instance: Optional[str] = None) -> ProblemException:
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/plugin-not-registered",
        title="Configuration plugin not registered",
        status=404,
        detail=f"Configuration plugin '{plugin_id}' is not registered.",
        instance=instance,
    ))


def validation_failed(
    exc: ValidationError, *, instance: Optional[str] = None,
) -> ProblemException:
    """Wrap a Pydantic ``ValidationError`` as a 422 Problem Details."""
    errors: List[ProblemError] = []
    for err in exc.errors():
        loc = err.get("loc", ())
        field = ".".join(str(p) for p in loc) or "<root>"
        errors.append(ProblemError(
            field=field,
            message=str(err.get("msg", "")),
            value=err.get("input"),
        ))
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/validation-failed",
        title="Configuration payload failed validation",
        status=422,
        detail=str(exc),
        instance=instance,
        errors=errors,
    ))


def value_error(
    exc: ValueError, *, instance: Optional[str] = None,
) -> ProblemException:
    """Wrap a domain-layer ``ValueError`` (e.g. unknown plugin) as 404."""
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/not-found",
        title="Configuration resource not found",
        status=404,
        detail=str(exc),
        instance=instance,
    ))


def unexpected_failure(
    exc: BaseException, *, instance: Optional[str] = None,
) -> ProblemException:
    """Wrap an otherwise-uncaught exception as a 500 Problem Details.

    The body intentionally carries the exception's str() in `detail` so
    operators can debug; the `type` URI lets clients dispatch on the
    problem class without parsing the message.
    """
    return ProblemException(ProblemDetails(
        type=f"{_TYPE_BASE}/internal",
        title="Configuration backend failure",
        status=500,
        detail=str(exc),
        instance=instance,
    ))


# ---------------------------------------------------------------------------
# Exception handler — rendered by FastAPI per registration in lifespan
# ---------------------------------------------------------------------------

async def problem_exception_handler(
    request: Request, exc: Exception,
) -> JSONResponse:
    """FastAPI exception handler for :class:`ProblemException`.

    Renders the body as ``application/problem+json`` per RFC 9457 §3.
    `instance` defaults to the request path when the raiser didn't set
    one explicitly (most common case).
    """
    if not isinstance(exc, ProblemException):
        raise exc  # pragma: no cover — registered only for ProblemException
    problem = exc.problem
    if problem.instance is None:
        # Default instance to the request path (RFC 9457 §3.1.5)
        problem = problem.model_copy(update={"instance": str(request.url.path)})
    return JSONResponse(
        content=problem.model_dump(exclude_none=True),
        status_code=problem.status,
        media_type="application/problem+json",
    )


def register(app: FastAPI) -> None:
    """Wire the handler. Called from ``ConfigsService.lifespan``."""
    app.add_exception_handler(ProblemException, problem_exception_handler)
