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

"""Behavioural tests for ``IamModule._register_identity_provider`` (geoid#1500).

The IdP factory is now config-first with a one-release deprecated ENV fallback:

- when ``IdpConfig`` selects an implemented + addressed backend, the provider
  is registered from the config and the ENV is never read;
- when the config does not select one (no row → zero-arg default, or a row
  with ``issuer_url`` unset), the lifespan falls back to ``IDP_*`` /
  ``KEYCLOAK_*`` env vars and emits a deprecation WARNING;
- ``type=saml2`` registers nothing (reserved placeholder).

``IamModule._register_identity_provider`` only touches module-level
``get_protocol`` / ``register_plugin`` (not instance state), so we exercise it
on a bare ``object.__new__(IamModule)`` with those two names monkeypatched.
"""

import os

import pytest

os.environ.setdefault(
    "JWT_SECRET", "test-secret-padded-to-enough-chars-for-fernet-xx"
)

import dynastore.modules.iam.module as iam_module
from dynastore.modules.iam.idp_config import IdpConfig
from dynastore.modules.iam.module import IamModule


class _FakeConfigs:
    """PlatformConfigsProtocol stub returning a pinned IdpConfig."""

    def __init__(self, cfg) -> None:
        self._cfg = cfg

    async def get_config(self, cls):
        return self._cfg


def _patch_runtime(monkeypatch, configs, captured):
    """Route get_protocol → configs (or None) and capture register_plugin."""
    monkeypatch.setattr(
        iam_module, "get_protocol", lambda _proto: configs, raising=True
    )
    monkeypatch.setattr(
        iam_module, "register_plugin", lambda obj: captured.append(obj), raising=True
    )


def _clear_idp_env(monkeypatch):
    for key in (
        "IDP_TYPE", "IDP_ISSUER_URL", "IDP_CLIENT_ID", "IDP_CLIENT_SECRET",
        "IDP_AUDIENCE", "IDP_PUBLIC_URL", "IDP_ROLES_CLAIM_PATH",
        "KEYCLOAK_ISSUER_URL", "KEYCLOAK_CLIENT_ID", "KEYCLOAK_CLIENT_SECRET",
        "KEYCLOAK_AUDIENCE", "KEYCLOAK_PUBLIC_URL",
    ):
        monkeypatch.delenv(key, raising=False)


@pytest.mark.asyncio
async def test_registers_from_config_when_configured(monkeypatch, caplog):
    captured: list = []
    cfg = IdpConfig(
        issuer_url="https://kc.example.org/realms/fao",
        client_id="my-client",
        client_secret="topsecret",
        roles_claim_path="realm_access.roles",
    )
    _patch_runtime(monkeypatch, _FakeConfigs(cfg), captured)
    _clear_idp_env(monkeypatch)  # prove ENV is not consulted

    mod = object.__new__(IamModule)
    with caplog.at_level("WARNING"):
        await mod._register_identity_provider()

    assert len(captured) == 1
    provider = captured[0]
    assert provider.issuer_url == "https://kc.example.org/realms/fao"
    assert provider.client_id == "my-client"
    assert provider.client_secret == "topsecret"  # revealed for the provider
    assert provider.roles_claim_path == "realm_access.roles"
    # Config path must NOT emit the ENV deprecation warning.
    assert "DEPRECATED" not in caplog.text


@pytest.mark.asyncio
async def test_falls_back_to_env_when_config_unconfigured(monkeypatch, caplog):
    captured: list = []
    _patch_runtime(monkeypatch, _FakeConfigs(IdpConfig()), captured)
    _clear_idp_env(monkeypatch)
    monkeypatch.setenv("IDP_ISSUER_URL", "https://env-issuer/realms/legacy")
    monkeypatch.setenv("IDP_CLIENT_ID", "env-client")

    mod = object.__new__(IamModule)
    with caplog.at_level("WARNING"):
        await mod._register_identity_provider()

    assert len(captured) == 1
    assert captured[0].issuer_url == "https://env-issuer/realms/legacy"
    assert captured[0].client_id == "env-client"
    # The ENV fallback must announce its deprecation.
    assert "DEPRECATED" in caplog.text


@pytest.mark.asyncio
async def test_registers_nothing_when_unconfigured_and_no_env(monkeypatch):
    captured: list = []
    _patch_runtime(monkeypatch, _FakeConfigs(IdpConfig()), captured)
    _clear_idp_env(monkeypatch)

    mod = object.__new__(IamModule)
    await mod._register_identity_provider()

    assert captured == []


@pytest.mark.asyncio
async def test_saml2_config_registers_nothing(monkeypatch, caplog):
    captured: list = []
    cfg = IdpConfig(type="saml2", issuer_url="https://kc/x")
    _patch_runtime(monkeypatch, _FakeConfigs(cfg), captured)
    _clear_idp_env(monkeypatch)

    mod = object.__new__(IamModule)
    with caplog.at_level("WARNING"):
        await mod._register_identity_provider()

    assert captured == []
    assert "saml2" in caplog.text


@pytest.mark.asyncio
async def test_no_configs_protocol_uses_env_fallback(monkeypatch, caplog):
    """When PlatformConfigsProtocol is unregistered (get_protocol → None),
    the helper still falls back to ENV rather than crashing."""
    captured: list = []
    _patch_runtime(monkeypatch, None, captured)
    _clear_idp_env(monkeypatch)
    monkeypatch.setenv("IDP_ISSUER_URL", "https://env-only/realms/x")

    mod = object.__new__(IamModule)
    with caplog.at_level("WARNING"):
        await mod._register_identity_provider()

    assert len(captured) == 1
    assert captured[0].issuer_url == "https://env-only/realms/x"
    assert "DEPRECATED" in caplog.text
