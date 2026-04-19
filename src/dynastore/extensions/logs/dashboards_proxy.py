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

"""
Reverse-proxy OpenSearch Dashboards / Kibana under the geoid origin.

Why: end-users authenticate to geoid with Keycloak, not with Elastic/OpenSearch
directly. Embedding the dashboard via a naive cross-origin iframe would hit a
Kibana login the user can't pass (in prod against Elastic Cloud) or be blocked
by CSP ``frame-ancestors``. This proxy solves three problems at once:

1. Auth translation — IamMiddleware enforces the sysadmin policy on the proxy
   paths; geoid then injects an Elastic API key on the forwarded request.
2. Same-origin — the iframe can reference the proxy path, so X-Frame-Options,
   CSP, and cookies all just work.
3. Single code path for dev (OpenSearch Dashboards, no auth) and prod (Elastic
   Cloud Kibana, ApiKey).

Scope: HTTP only. OpenSearch Dashboards dashboard viewing does not require
WebSockets, so we deliberately skip the WS upgrade to keep the proxy simple.
Features that require WS (Dev Tools console, some ML UIs) will degrade but
dashboard rendering is unaffected.
"""

import logging
import os
from typing import AsyncIterator, Optional

import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)


# Response headers we must strip before returning upstream content to the
# browser. These would otherwise (a) break iframe embedding even though we
# are same-origin post-proxy (Kibana's CSP treats its upstream origin as
# authoritative), or (b) leak upstream HSTS / cookie semantics into the
# geoid origin.
_STRIP_RESPONSE_HEADERS = frozenset(
    h.lower() for h in (
        "x-frame-options",
        "content-security-policy",
        "content-security-policy-report-only",
        "strict-transport-security",
        # Hop-by-hop — httpx already handles these but be explicit:
        "connection",
        "keep-alive",
        "transfer-encoding",
        "te",
        "trailers",
        "proxy-authenticate",
        "proxy-authorization",
        "upgrade",
    )
)

# Request headers we must NOT forward to upstream. Host must match the
# upstream, not the geoid ingress; Authorization is replaced with the
# API-key we inject (so we don't leak the user's Keycloak Bearer to
# Kibana); hop-by-hop fields per RFC 7230 §6.1.
_STRIP_REQUEST_HEADERS = frozenset(
    h.lower() for h in (
        "host",
        "authorization",
        "cookie",  # Kibana cookies are session state we don't want to round-trip
        "content-length",  # httpx will set this correctly
        "connection",
        "keep-alive",
        "transfer-encoding",
        "te",
        "trailers",
        "proxy-authenticate",
        "proxy-authorization",
        "upgrade",
    )
)


def _upstream_url() -> Optional[str]:
    raw = os.environ.get("KIBANA_UPSTREAM_URL", "").strip().rstrip("/")
    return raw or None


def _api_key() -> Optional[str]:
    key = os.environ.get("KIBANA_UPSTREAM_API_KEY", "").strip()
    return key or None


def _public_path() -> str:
    """The path under which the proxy is mounted on the geoid origin."""
    raw = os.environ.get("KIBANA_PUBLIC_PATH", "/dashboards").strip()
    if not raw.startswith("/"):
        raw = "/" + raw
    return raw.rstrip("/") or "/dashboards"


def _forward_request_headers(request: Request) -> dict:
    """Build headers to send upstream: strip sensitive, inject ApiKey."""
    out: dict[str, str] = {}
    for name, value in request.headers.items():
        if name.lower() in _STRIP_REQUEST_HEADERS:
            continue
        out[name] = value
    # XSRF bypass for OpenSearch Dashboards / Kibana
    out.setdefault("osd-xsrf", "true")
    out.setdefault("kbn-xsrf", "true")
    key = _api_key()
    if key:
        out["Authorization"] = f"ApiKey {key}"
    return out


def _filter_response_headers(headers: httpx.Headers, public_path: str) -> dict:
    """Strip disallowed headers; rewrite Set-Cookie Path into our mount."""
    out: dict[str, str] = {}
    for name, value in headers.multi_items():
        low = name.lower()
        if low in _STRIP_RESPONSE_HEADERS:
            continue
        if low == "set-cookie":
            # Constrain cookies to our mount path so Kibana's session state
            # does not leak into the rest of the geoid origin.
            value = _rewrite_cookie_path(value, public_path)
        # StreamingResponse headers dict only keeps last value; if the
        # upstream sends multiple Set-Cookies, we concatenate them with
        # newlines so Starlette re-emits each. Fall back to last-wins
        # otherwise.
        if low == "set-cookie" and name in out:
            out[name] = out[name] + "\n" + value
        else:
            out[name] = value
    return out


def _rewrite_cookie_path(cookie_header: str, public_path: str) -> str:
    """Replace ``Path=<x>`` with ``Path=<public_path>`` in a Set-Cookie header."""
    parts = [p.strip() for p in cookie_header.split(";")]
    rewritten = []
    found = False
    for part in parts:
        if part.lower().startswith("path="):
            rewritten.append(f"Path={public_path}")
            found = True
        else:
            rewritten.append(part)
    if not found:
        rewritten.append(f"Path={public_path}")
    return "; ".join(rewritten)


async def _stream_body(
    upstream_response: httpx.Response,
) -> AsyncIterator[bytes]:
    """Stream the upstream body to the client, closing the response at end."""
    try:
        async for chunk in upstream_response.aiter_raw():
            yield chunk
    finally:
        await upstream_response.aclose()


def build_dashboards_proxy_router() -> APIRouter:
    """
    Build the FastAPI router that proxies under ``${KIBANA_PUBLIC_PATH}``.

    The router is always mounted so that the path exists and the in-page
    Setup Guide can probe it; when ``KIBANA_UPSTREAM_URL`` is unset the
    handler returns a clear 503 explaining what to configure.

    Authorization on the proxy path is enforced by ``IamMiddleware`` against
    the ``logs_dashboard_sysadmin_access`` policy registered by
    :mod:`dynastore.extensions.logs.log_extension`.
    """
    public_path = _public_path()
    router = APIRouter(prefix=public_path, tags=["Logs Dashboard (proxy)"])

    @router.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
        include_in_schema=False,
    )
    async def _proxy(path: str, request: Request) -> Response:
        upstream = _upstream_url()
        if not upstream:
            return Response(
                content=(
                    "Logs dashboard proxy is not configured. "
                    "Set KIBANA_UPSTREAM_URL to enable it."
                ),
                status_code=503,
                media_type="text/plain",
            )

        # Compose the upstream URL. The leading slash is important so Kibana
        # recognises this as its own basePath-aware path if SERVER_BASEPATH
        # is configured; when basePath is empty, a leading slash is still
        # required by the server.
        target = f"{upstream}/{path}"
        qs = request.url.query
        if qs:
            target = f"{target}?{qs}"

        headers = _forward_request_headers(request)

        # Stream the request body through — small POSTs (saved-object edits)
        # are the common case, but avoid buffering in case of file uploads
        # via the saved-objects _import endpoint.
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=False) as client:
            try:
                upstream_response = await client.send(
                    client.build_request(
                        method=request.method,
                        url=target,
                        headers=headers,
                        content=request.stream(),
                    ),
                    stream=True,
                )
            except httpx.RequestError as exc:
                logger.warning(
                    "dashboards_proxy: upstream request to %s failed: %s",
                    target, exc,
                )
                return Response(
                    content=f"Dashboards upstream unreachable: {exc}",
                    status_code=502,
                    media_type="text/plain",
                )

            out_headers = _filter_response_headers(
                upstream_response.headers, public_path
            )
            # Small responses: return immediately; large responses (JS
            # bundles, map tiles): stream chunks. httpx's stream=True gives
            # us the latter for free.
            return StreamingResponse(
                _stream_body(upstream_response),
                status_code=upstream_response.status_code,
                headers=out_headers,
                media_type=upstream_response.headers.get("content-type"),
            )

    return router
