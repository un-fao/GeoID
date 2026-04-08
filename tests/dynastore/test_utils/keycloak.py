"""
Keycloak test utilities — obtain real tokens from the Keycloak service.

Requires Keycloak to be running (via docker-compose) with the ``geoid``
realm imported.  All helpers use httpx to call the token endpoint.

Environment variables (read from docker/.env via compose):
    KEYCLOAK_ISSUER_URL  — e.g. http://keycloak:8080/realms/geoid
    KEYCLOAK_CLIENT_ID   — e.g. geoid-api
    KEYCLOAK_CLIENT_SECRET — secret for the confidential client
"""

from __future__ import annotations

import os
import logging

import httpx

logger = logging.getLogger(__name__)

# When running outside Docker (local pytest), Keycloak is on localhost:8180
_DEFAULT_ISSUER = os.getenv(
    "KEYCLOAK_ISSUER_URL", "http://localhost:8180/realms/geoid"
)
_DEFAULT_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "geoid-api")
_DEFAULT_CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", "geoid-api-secret")


def _token_url(issuer_url: str | None = None) -> str:
    base = (issuer_url or _DEFAULT_ISSUER).rstrip("/")
    return f"{base}/protocol/openid-connect/token"


async def get_user_token(
    username: str,
    password: str,
    *,
    issuer_url: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> str:
    """
    Obtain an access token via the Resource Owner Password Credentials
    (direct access) grant.

    Requires ``directAccessGrantsEnabled=true`` on the client.
    """
    url = _token_url(issuer_url)
    data = {
        "grant_type": "password",
        "client_id": client_id or _DEFAULT_CLIENT_ID,
        "client_secret": client_secret or _DEFAULT_CLIENT_SECRET,
        "username": username,
        "password": password,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]


async def get_service_account_token(
    client_id: str | None = None,
    client_secret: str | None = None,
    *,
    issuer_url: str | None = None,
) -> str:
    """
    Obtain an access token via the Client Credentials grant
    (service account flow).

    The resulting JWT will have ``client_id`` claim and no ``email``,
    identifying it as a service account.
    """
    url = _token_url(issuer_url)
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id or _DEFAULT_CLIENT_ID,
        "client_secret": client_secret or _DEFAULT_CLIENT_SECRET,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]


def is_keycloak_available(issuer_url: str | None = None) -> bool:
    """
    Synchronous check whether Keycloak is reachable.

    Use as a pytest skip marker::

        pytestmark = pytest.mark.skipif(
            not is_keycloak_available(),
            reason="Keycloak not available"
        )
    """
    url = (issuer_url or _DEFAULT_ISSUER).rstrip("/")
    try:
        resp = httpx.get(f"{url}/.well-known/openid-configuration", timeout=5.0)
        return resp.status_code == 200
    except Exception:
        return False
