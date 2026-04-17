"""
Session-scoped fixtures for notebook integration tests.

Creates the standard demo infrastructure that most notebooks depend on:
- demo-catalog
- sentinel2-l2a collection with PostgreSQL driver + routing configs

Notebooks that *create* infrastructure (catalog/01, catalog/02, storage_drivers/*)
use their own unique IDs and are unaffected.
"""
import os
import pytest
import httpx

BASE_URL = os.environ.get("DYNASTORE_BASE_URL", "http://localhost:8080")
CATALOG_ID = os.environ.get("CATALOG_ID", "demo-catalog")
COLLECTION_ID = os.environ.get("COLLECTION_ID", "sentinel2-l2a")

# Keycloak defaults match docker/keycloak/realm-export.json for the dev stack.
KEYCLOAK_INTERNAL_TOKEN_URL = os.environ.get(
    "DYNASTORE_TOKEN_URL_INTERNAL",
    "http://keycloak:8080/realms/geoid/protocol/openid-connect/token",
)
KEYCLOAK_CLIENT_ID = os.environ.get("DYNASTORE_OIDC_CLIENT_ID", "geoid-api")
KEYCLOAK_CLIENT_SECRET = os.environ.get("DYNASTORE_OIDC_CLIENT_SECRET", "geoid-api-secret")
KEYCLOAK_USERNAME = os.environ.get("DYNASTORE_OIDC_USERNAME", "testadmin")
KEYCLOAK_PASSWORD = os.environ.get("DYNASTORE_OIDC_PASSWORD", "testpassword")
WEB_CONTAINER = os.environ.get("DYNASTORE_WEB_CONTAINER", "geoid_web")


def _fetch_token_via_docker(
    max_retries: int = 6,
    retry_delay_s: float = 5.0,
    username: str | None = None,
    password: str | None = None,
) -> str:
    """Get a token from keycloak using the internal docker-network issuer URL.

    The backend validates the token's `iss` claim against the internal URL
    (http://keycloak:8080/...). When run from the host, we have to route the
    token request through the web container so the issuer matches.

    Retries on empty output / connection refused, since keycloak can take
    30–60s to finish realm initialization after a cold container restart.

    Pass ``username`` / ``password`` to fetch a non-admin token (e.g.
    ``testuser`` for the 401/403 rejection notebooks). Defaults to the
    configured admin credentials when not supplied.
    """
    import shutil
    import subprocess
    import time

    if not shutil.which("docker"):
        return ""
    user = username or KEYCLOAK_USERNAME
    passwd = password or KEYCLOAK_PASSWORD
    last_err = ""
    for attempt in range(1, max_retries + 1):
        try:
            result = subprocess.run(
                [
                    "docker", "exec", WEB_CONTAINER,
                    "curl", "-s", "-X", "POST", KEYCLOAK_INTERNAL_TOKEN_URL,
                    "-H", "Content-Type: application/x-www-form-urlencoded",
                    "-d",
                    (
                        f"grant_type=password&client_id={KEYCLOAK_CLIENT_ID}"
                        f"&client_secret={KEYCLOAK_CLIENT_SECRET}"
                        f"&username={user}&password={passwd}"
                    ),
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode == 0 and result.stdout.strip():
                import json
                try:
                    data = json.loads(result.stdout)
                    tok = data.get("access_token", "") or ""
                    if tok:
                        return tok
                    last_err = f"no access_token in response: {result.stdout[:200]}"
                except json.JSONDecodeError as e:
                    last_err = f"JSON decode: {e}; body={result.stdout[:200]}"
            else:
                last_err = (
                    f"rc={result.returncode} "
                    f"stdout={result.stdout[:200]!r} "
                    f"stderr={result.stderr[:200]!r}"
                )
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        if attempt < max_retries:
            time.sleep(retry_delay_s)
    print(
        f"[notebook_showcase.conftest] WARN token fetch failed after "
        f"{max_retries} attempts — notebooks will run unauthenticated. "
        f"Last error: {last_err}",
        flush=True,
    )
    return ""


def _resolve_token() -> str:
    """Resolve a sysadmin token: prefer env var, else fetch from keycloak."""
    token = os.environ.get("DYNASTORE_SYSADMIN_TOKEN", "")
    if not token:
        token = _fetch_token_via_docker()
    if token:
        # Admin-tier tokens: testadmin holds sysadmin/admin/writer roles.
        for var in (
            "DYNASTORE_SYSADMIN_TOKEN",
            "DYNASTORE_ADMIN_TOKEN",
            "DYNASTORE_WRITE_TOKEN",
        ):
            os.environ.setdefault(var, token)

    # Non-admin token — notebooks that test 401/403 rejection of non-admin
    # requests read DYNASTORE_TOKEN. Fetch testuser separately so those
    # checks don't pass by accident against a broadcast sysadmin token.
    if not os.environ.get("DYNASTORE_TOKEN"):
        user_token = _fetch_token_via_docker(
            max_retries=2, retry_delay_s=2.0,
            username="testuser", password="testpassword",
        )
        if user_token:
            os.environ["DYNASTORE_TOKEN"] = user_token
    return token


def _admin_headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _ensure_catalog(client: httpx.Client) -> bool:
    """POST-first idempotent create. Returns True if this call created it."""
    r = client.post("/stac/catalogs", json={
        "id": CATALOG_ID,
        "type": "Catalog",
        "title": "Demo Catalog",
        "description": "Standard demo catalog for integration tests.",
        "stac_version": "1.0.0",
    })
    if r.status_code in (200, 201):
        _mark_catalog_ready()
        return True
    if r.status_code == 409:
        _mark_catalog_ready()
        return False
    raise RuntimeError(
        f"Failed to create {CATALOG_ID}: {r.status_code} {r.text[:400]}"
    )


def _mark_catalog_ready() -> None:
    """Flip ``provisioning_status`` to ``ready`` via direct DB update.

    Catalogs start in ``provisioning`` whenever a StorageProtocol implementer
    is registered (GcpModule always is, even in dev). The async provisioning
    task then calls ``setup_catalog_gcp_resources`` — which fails in dev
    without real GCP credentials — so the catalog is stuck in ``provisioning``
    and upload/ingest endpoints 503. For the showcase we bypass that.
    """
    import shutil
    import subprocess

    if not shutil.which("docker"):
        return
    try:
        subprocess.run(
            [
                "docker", "exec", "geoid_db",
                "psql", "-U", "testuser", "-d", "gis_dev", "-v", "ON_ERROR_STOP=1",
                "-c",
                f"UPDATE catalog.catalogs SET provisioning_status = 'ready' "
                f"WHERE id = '{CATALOG_ID}';",
            ],
            check=False,
            capture_output=True,
            timeout=10,
        )
    except Exception:
        pass


def _ensure_collection(client: httpx.Client) -> bool:
    r = client.post(f"/stac/catalogs/{CATALOG_ID}/collections", json={
        "id": COLLECTION_ID,
        "type": "Collection",
        "stac_version": "1.0.0",
        "title": "Sentinel-2 L2A",
        "description": "Demo Sentinel-2 collection for integration tests.",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [],
    })
    if r.status_code in (200, 201):
        return True
    if r.status_code == 409:
        return False
    raise RuntimeError(
        f"Failed to create {COLLECTION_ID}: {r.status_code} {r.text[:400]}"
    )


def _apply_driver_configs(client: httpx.Client):
    base = f"/configs/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}"
    # NOTE: config class_keys default to the class __qualname__, not user-friendly
    # aliases like "driver:postgresql". The registered keys for the storage
    # drivers are CollectionPostgresqlDriverConfig / CollectionRoutingConfig.
    client.put(f"{base}/configs/CollectionPostgresqlDriverConfig", json={
        "enabled": True,
        "collection_type": "VECTOR",
    })
    client.put(f"{base}/configs/CollectionRoutingConfig", json={
        "enabled": True,
        "operations": {
            "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
            "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
        },
    })


def _delete_catalog(client: httpx.Client):
    client.delete(f"/stac/catalogs/{CATALOG_ID}", params={"force": "true"})


_SESSION_STATE: dict = {}


def pytest_sessionstart(session):
    """Set up demo infrastructure at session start.

    We use a session-start hook (not an autouse fixture) because nbval's
    IPyNbCell collection items don't reliably trigger function-level autouse
    fixtures, but session hooks always fire.
    """
    token = _resolve_token()
    _SESSION_STATE["token"] = token
    if not token:
        return

    client = httpx.Client(
        base_url=BASE_URL,
        headers=_admin_headers(token),
        timeout=30.0,
        follow_redirects=True,
    )
    _SESSION_STATE["client"] = client
    _SESSION_STATE["catalog_created"] = _ensure_catalog(client)
    collection_created = _ensure_collection(client)
    if collection_created:
        _apply_driver_configs(client)


def pytest_sessionfinish(session, exitstatus):
    """Tear down only what this session created."""
    client = _SESSION_STATE.get("client")
    if client is None:
        return
    try:
        if _SESSION_STATE.get("catalog_created"):
            _delete_catalog(client)
    finally:
        client.close()


@pytest.fixture(scope="session")
def demo_infrastructure():
    """Expose the demo catalog/collection identifiers to tests that want them."""
    return {
        "base_url": BASE_URL,
        "catalog_id": CATALOG_ID,
        "collection_id": COLLECTION_ID,
        "token": _SESSION_STATE.get("token", ""),
    }
