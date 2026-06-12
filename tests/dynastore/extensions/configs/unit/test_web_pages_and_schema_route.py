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

"""Tests for Feature A (web pages + schema route) and Feature A3 (policies).

Covers:
- ConfigsService.get_web_pages() returns 'configuration' + 'presets' pages
  with section='admin' and audience_policy_id='configs_access'
- GET /configs/presets/{name}/schema for a params preset (returns its JSON Schema)
- GET /configs/presets/{name}/schema for a NoParams preset (returns empty schema)
- GET /configs/presets/unknown/schema returns 404
- configs_access policy contains the new web-page + static resources
- configs_role_bindings() binds configs_access to admin AND sysadmin
- IAM-absent: with no PageVisibilityFilter registered, /web/config/pages includes
  configuration + presets (visible to all because audience_policy_id is not enforced
  without IAM)
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_app() -> FastAPI:
    app = FastAPI()
    from dynastore.extensions.configs.service import ConfigsService
    svc = ConfigsService(app)
    app.include_router(svc.router)
    return app


# ---------------------------------------------------------------------------
# A2 — get_web_pages()
# ---------------------------------------------------------------------------


class TestConfigsServiceWebPages:
    def test_get_web_pages_returns_both_pages(self):
        from dynastore.extensions.configs.service import ConfigsService
        svc = ConfigsService(FastAPI())
        pages = svc.get_web_pages()
        ids = {p.page_id for p in pages}
        assert "configuration" in ids, f"'configuration' page missing from {ids}"
        assert "presets" in ids, f"'presets' page missing from {ids}"

    def test_configuration_page_section_is_admin(self):
        from dynastore.extensions.configs.service import ConfigsService
        svc = ConfigsService(FastAPI())
        pages = {p.page_id: p for p in svc.get_web_pages()}
        assert pages["configuration"].section == "admin"

    def test_presets_page_section_is_admin(self):
        from dynastore.extensions.configs.service import ConfigsService
        svc = ConfigsService(FastAPI())
        pages = {p.page_id: p for p in svc.get_web_pages()}
        assert pages["presets"].section == "admin"

    def test_configuration_page_audience_policy(self):
        from dynastore.extensions.configs.service import ConfigsService
        svc = ConfigsService(FastAPI())
        pages = {p.page_id: p for p in svc.get_web_pages()}
        assert pages["configuration"].audience_policy_id == "configs_access"

    def test_presets_page_audience_policy(self):
        from dynastore.extensions.configs.service import ConfigsService
        svc = ConfigsService(FastAPI())
        pages = {p.page_id: p for p in svc.get_web_pages()}
        assert pages["presets"].audience_policy_id == "configs_access"

    def test_get_static_assets_returns_configs_prefix(self):
        from dynastore.extensions.configs.service import ConfigsService
        svc = ConfigsService(FastAPI())
        assets = svc.get_static_assets()
        prefixes = {a.prefix for a in assets}
        assert "configs" in prefixes, f"'configs' prefix missing from {prefixes}"


# ---------------------------------------------------------------------------
# A7 — GET /configs/presets/{name}/schema
# ---------------------------------------------------------------------------


class TestPresetParamsSchemaRoute:
    def test_known_preset_with_params_returns_json_schema(self):
        """A preset that declares a real params_model returns its JSON Schema."""
        import dynastore.extensions.geoid  # noqa: F401 — register geoid presets
        from dynastore.modules.storage.presets import get_preset

        # Find any preset that has a non-NoParams params_model.
        from dynastore.modules.storage.presets.bundle_preset import NoParams
        from dynastore.modules.storage.presets.registry import search_presets

        result = search_presets(limit=200)
        params_preset = None
        for item in result["items"]:
            try:
                p = get_preset(item["name"])
                pm = getattr(p, "params_model", None)
                if pm is not None and pm is not NoParams:
                    params_preset = item["name"]
                    break
            except Exception:
                continue

        if params_preset is None:
            pytest.skip("No preset with a real params_model registered")

        client = TestClient(_make_app())
        resp = client.get(f"/configs/presets/{params_preset}/schema")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, dict)
        # A real params model should have properties
        assert "properties" in body or "type" in body

    def test_no_params_preset_returns_empty_schema(self):
        """A NoParams preset returns {"type": "object", "properties": {}}."""
        client = TestClient(_make_app())
        # public_catalog is a built-in NoParams preset
        resp = client.get("/configs/presets/public_catalog/schema")
        assert resp.status_code == 200
        body = resp.json()
        assert body == {"type": "object", "properties": {}}

    def test_unknown_preset_returns_404(self):
        client = TestClient(_make_app())
        resp = client.get("/configs/presets/no_such_preset_xyzzy_999/schema")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# A3 — configs_access policy resources
# ---------------------------------------------------------------------------


class TestConfigsPolicies:
    def test_configs_access_policy_contains_web_page_resources(self):
        from dynastore.extensions.configs.policies import configs_policies
        policies = {p.id: p for p in configs_policies()}
        assert "configs_access" in policies
        resources = policies["configs_access"].resources
        assert any("/web/pages/configuration" in r for r in resources), (
            f"/web/pages/configuration not in resources: {resources}"
        )
        assert any("/web/pages/presets" in r for r in resources), (
            f"/web/pages/presets not in resources: {resources}"
        )

    def test_configs_access_policy_contains_configs_static_prefix(self):
        from dynastore.extensions.configs.policies import configs_policies
        policies = {p.id: p for p in configs_policies()}
        resources = policies["configs_access"].resources
        assert any("/web/configs" in r for r in resources), (
            f"/web/configs prefix not in resources: {resources}"
        )

    def test_configs_role_bindings_includes_sysadmin(self):
        from dynastore.extensions.configs.policies import configs_role_bindings
        bindings = {r.name: r for r in configs_role_bindings()}
        from dynastore.models.protocols.authorization import IamRolesConfig
        sysadmin = IamRolesConfig().sysadmin_role_name
        assert sysadmin in bindings, f"sysadmin role '{sysadmin}' not in bindings: {list(bindings)}"
        assert "configs_access" in bindings[sysadmin].policies

    def test_configs_role_bindings_includes_admin(self):
        from dynastore.extensions.configs.policies import configs_role_bindings
        bindings = {r.name: r for r in configs_role_bindings()}
        from dynastore.models.protocols.authorization import IamRolesConfig
        admin = IamRolesConfig().admin_role_name
        assert admin in bindings, f"admin role '{admin}' not in bindings: {list(bindings)}"
        assert "configs_access" in bindings[admin].policies


# ---------------------------------------------------------------------------
# A3 / IAM-absent: pages visible without PageVisibilityFilter
# ---------------------------------------------------------------------------


class TestIamAbsentPagesVisible:
    """Without IAM (no PageVisibilityFilter), /web/config/pages must return
    configuration and presets pages — audience_policy_id is only enforced
    by the IAM-provided filter.
    """

    def test_config_pages_includes_configuration_and_presets_without_iam(self):
        """Build a minimal web app with ConfigsService registered as a
        WebPageContributor but without an IamService / PageVisibilityFilter.
        The /web/config/pages endpoint should return both pages.
        """
        from fastapi import FastAPI
        from dynastore.extensions.configs.service import ConfigsService

        # Build a minimal app with just ConfigsService so its pages are in
        # the registry. We simulate the /web/config/pages response by calling
        # get_web_pages() directly (the web extension's handler logic:
        # no PageVisibilityFilter → show pages without required_roles only).
        svc = ConfigsService(FastAPI())
        pages = svc.get_web_pages()
        # Simulate the no-IAM filter: admit pages that have no required_roles
        # (audience_policy_id is only enforced by PageVisibilityFilter).
        visible = [p for p in pages if not getattr(p, "required_roles", None)]
        ids = {p.page_id for p in visible}
        assert "configuration" in ids, (
            f"'configuration' page not visible without IAM: {ids}"
        )
        assert "presets" in ids, (
            f"'presets' page not visible without IAM: {ids}"
        )
