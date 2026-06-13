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

"""Unit tests for the dev-compose ``idp-config.json`` overlay seed files.

Validates that every dev config overlay for IdpConfig:
- has the correct ``class_key`` (resolves to IdpConfig via the registry);
- has a ``value`` that parses via ``IdpConfig.model_validate`` without error;
- produces ``is_configured == True`` (i.e. type=oidc and issuer_url is set),
  confirming the first-boot chicken-and-egg issue (geoid#2042) is addressed.

Three services are covered: catalog, tools, maps — the only three that have
per-service config dirs bind-mounted in docker-compose.dev.yml.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

os.environ.setdefault(
    "JWT_SECRET", "test-secret-padded-to-enough-chars-for-fernet-xx"
)

from dynastore.models.plugin_config import resolve_config_class
from dynastore.modules.iam.idp_config import IdpConfig

# Path to the docker/config dir under the package (sibling of this test's repo root).
_DOCKER_CONFIG_DIR = (
    Path(__file__).parents[5]  # repo root: worktrees/cc-2042/
    / "packages" / "core" / "src" / "dynastore" / "docker" / "config"
)

_DEV_SERVICES = ("catalog", "tools", "maps")


def _seed_path(svc: str) -> Path:
    return _DOCKER_CONFIG_DIR / svc / "defaults" / "idp-config.json"


@pytest.mark.parametrize("svc", _DEV_SERVICES)
def test_seed_file_exists(svc: str) -> None:
    assert _seed_path(svc).exists(), (
        f"Missing dev seed: {_seed_path(svc)}. "
        "Each service config dir needs an idp-config.json overlay."
    )


@pytest.mark.parametrize("svc", _DEV_SERVICES)
def test_seed_class_key_resolves_to_idp_config(svc: str) -> None:
    payload = json.loads(_seed_path(svc).read_text())
    class_key = payload.get("class_key")
    assert class_key == "idp_config", f"{svc}: expected class_key='idp_config', got {class_key!r}"
    cls = resolve_config_class(class_key)
    assert cls is IdpConfig, f"{svc}: resolve_config_class({class_key!r}) returned {cls!r}"


@pytest.mark.parametrize("svc", _DEV_SERVICES)
def test_seed_value_validates_and_is_configured(svc: str) -> None:
    payload = json.loads(_seed_path(svc).read_text())
    value = payload["value"]
    cfg = IdpConfig.model_validate(value)
    assert cfg.is_configured, (
        f"{svc}: IdpConfig.is_configured is False after model_validate — "
        "check that type='oidc' and issuer_url is set in the seed file."
    )


@pytest.mark.parametrize("svc", _DEV_SERVICES)
def test_seed_issuer_url_is_internal_keycloak(svc: str) -> None:
    payload = json.loads(_seed_path(svc).read_text())
    cfg = IdpConfig.model_validate(payload["value"])
    assert cfg.issuer_url == "http://keycloak:8080/realms/geoid", (
        f"{svc}: issuer_url must point to the in-cluster Keycloak address"
    )


@pytest.mark.parametrize("svc", _DEV_SERVICES)
def test_seed_public_url_matches_host_port(svc: str) -> None:
    payload = json.loads(_seed_path(svc).read_text())
    cfg = IdpConfig.model_validate(payload["value"])
    # HOST_PORT_KEYCLOAK=8180 in docker/.env; compose default is 8181 but .env wins.
    assert cfg.public_url == "http://localhost:8180/realms/geoid", (
        f"{svc}: public_url must match HOST_PORT_KEYCLOAK=8180 from docker/.env"
    )


@pytest.mark.parametrize("svc", _DEV_SERVICES)
def test_seed_no_client_secret(svc: str) -> None:
    """geoid-web is a public PKCE client — no client_secret should be seeded."""
    payload = json.loads(_seed_path(svc).read_text())
    value = payload["value"]
    assert "client_secret" not in value, (
        f"{svc}: client_secret must not appear in the dev seed (public PKCE client)"
    )
