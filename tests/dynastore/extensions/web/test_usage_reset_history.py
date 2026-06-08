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

"""Source-level pins for the usage-counter reset *history* view (#1791).

The reset-history feature deliberately reuses the existing logs subsystem
instead of building a bespoke audit-read endpoint:

- the reset handler mirrors every quota reset into the logs subsystem as a
  ``usage_counter_reset`` event (while keeping the platform-only
  ``iam.audit_log`` write as the always-on security source of truth), and
- the Access & Bindings page reads those events back through the existing
  ``GET /logs/...`` API, filtered by ``event_type``.

These guards are DB-free (pure source pins, mirroring
``test_grant_usage_panel.py``) so they never touch the live ``gis_dev`` DB.
"""

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[3].parent


def _read(rel: str) -> str:
    return (_ROOT / rel).read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def admin_service_py() -> str:
    return _read(
        "packages/extensions/admin/src/dynastore/extensions/admin/admin_service.py"
    )


@pytest.fixture(scope="module")
def api_js() -> str:
    return _read(
        "packages/extensions/web/src/dynastore/extensions/web/static/common/api.js"
    )


@pytest.fixture(scope="module")
def access_bindings_js() -> str:
    return _read(
        "packages/extensions/web/src/dynastore/extensions/web/static/admin/access-bindings.js"
    )


@pytest.fixture(scope="module")
def access_bindings_html() -> str:
    return _read(
        "packages/extensions/web/src/dynastore/extensions/web/static/admin/access-bindings.html"
    )


@pytest.fixture(scope="module")
def catalog_logs_html() -> str:
    return _read(
        "packages/extensions/logs/src/dynastore/extensions/logs/static/catalog-logs.html"
    )


# --- Backend: reset mirrors into the logs subsystem ----------------------


def test_reset_handler_dual_writes_to_logs(admin_service_py: str) -> None:
    """The reset handler emits a ``usage_counter_reset`` log event via
    LogsProtocol so the history is visible through the /logs read API."""
    reset_block = admin_service_py.split("async def reset_policy_usage", 1)[-1]
    # Scope to the handler body (cut at the next route's separator banner).
    reset_block = reset_block.split("Per-binding live counter view", 1)[0]
    assert "from dynastore.models.protocols.logs import LogsProtocol" in reset_block
    assert "get_protocol(LogsProtocol)" in reset_block
    assert "await logs.log_event(" in reset_block
    assert 'event_type="usage_counter_reset"' in reset_block


def test_reset_log_scopes_to_catalog_or_system(admin_service_py: str) -> None:
    """Catalog-tier resets log under their catalog; platform-tier resets
    fall back to the ``_system_`` sentinel (native multi-level scoping)."""
    reset_block = admin_service_py.split("async def reset_policy_usage", 1)[-1]
    reset_block = reset_block.split("Per-binding live counter view", 1)[0]
    assert "from dynastore.models.shared_models import SYSTEM_CATALOG_ID" in reset_block
    assert "catalog_id=catalog_id or SYSTEM_CATALOG_ID" in reset_block
    # Instantly queryable after the reset returns.
    assert "immediate=True" in reset_block


def test_reset_log_carries_structured_detail(admin_service_py: str) -> None:
    """The log details echo the fields the history view renders: which
    policy/grant, which subject, the window, and the cleared count."""
    reset_block = admin_service_py.split("async def reset_policy_usage", 1)[-1]
    reset_block = reset_block.split("Per-binding live counter view", 1)[0]
    for key in (
        '"policy_id": policy_id',
        '"subject_principal_key": principal_key',
        '"window_seconds": window_seconds',
        '"previous_count": int(before or 0)',
        '"actor_principal_id"',
    ):
        assert key in reset_block, key


def test_reset_logs_writes_are_failsoft(admin_service_py: str) -> None:
    """A logs failure must never break the reset — the mirror is wrapped
    and surfaced at WARNING, same posture as the audit write."""
    reset_block = admin_service_py.split("async def reset_policy_usage", 1)[-1]
    reset_block = reset_block.split("Per-binding live counter view", 1)[0]
    assert "logs dual-write for usage_counter_reset failed" in reset_block


def test_reset_keeps_audit_log_write(admin_service_py: str) -> None:
    """The platform-only audit_log write stays the security source of
    truth — the logs mirror is *additive*, not a replacement."""
    reset_block = admin_service_py.split("async def reset_policy_usage", 1)[-1]
    reset_block = reset_block.split("Per-binding live counter view", 1)[0]
    assert "log_audit_event(" in reset_block
    assert 'event_type="usage_counter_reset"' in reset_block


# --- api.js: history reads the logs endpoint -----------------------------


def test_api_helper_reads_logs_endpoint(api_js: str) -> None:
    """``listUsageResetHistory`` queries the logs read API filtered by the
    reset event type — no bespoke audit endpoint."""
    assert "export const listUsageResetHistory" in api_js
    block = api_js.split("listUsageResetHistory", 1)[-1].split("export const", 1)[0]
    assert 'event_type: "usage_counter_reset"' in block
    assert "/logs/catalogs/" in block
    assert "/logs/system" in block
    assert "encodeURIComponent(catalogId)" in block


# --- access-bindings.html: history scaffold ------------------------------


def test_history_scaffold_present(access_bindings_html: str) -> None:
    assert 'id="usage-history-btn"' in access_bindings_html
    assert 'id="usage-history-panel"' in access_bindings_html
    assert 'id="usage-history-table"' in access_bindings_html
    assert 'id="usage-history-csv"' in access_bindings_html
    assert 'id="usage-history-json"' in access_bindings_html
    assert 'id="usage-history-close"' in access_bindings_html


# --- access-bindings.js: wiring + safe rendering -------------------------


def test_history_imports_and_wires(access_bindings_js: str) -> None:
    assert "listUsageResetHistory" in access_bindings_js
    assert "loadResetHistory" in access_bindings_js
    # The button is bound in the page wiring.
    assert '$("#usage-history-btn")' in access_bindings_js
    assert '$("#usage-history-csv")' in access_bindings_js
    assert '$("#usage-history-json")' in access_bindings_js


def test_history_export_reuses_download_util(access_bindings_js: str) -> None:
    """CSV/JSON export reuses the #1790 client-side download helper."""
    assert "handleHistoryExportCsv" in access_bindings_js
    assert "handleHistoryExportJson" in access_bindings_js
    assert "rowsToCsv(" in access_bindings_js
    assert "downloadAsFile(" in access_bindings_js


def test_history_render_never_uses_innerhtml(access_bindings_js: str) -> None:
    """Server-derived log fields are rendered through textContent only —
    no innerHTML injection (same XSS posture as the rest of the page)."""
    render_block = access_bindings_js.split("function renderHistory", 1)[-1]
    render_block = render_block.split("function setHistoryExportEnabled", 1)[0]
    assert ".innerHTML" not in render_block
    assert ".textContent" in render_block


def test_history_reads_detail_fields(access_bindings_js: str) -> None:
    """The flattener pulls the structured fields from the log ``details``
    blob the backend stamps."""
    block = access_bindings_js.split("function historyRowFrom", 1)[-1]
    block = block.split("function renderHistory", 1)[0]
    assert "entry.details" in block
    assert "d.policy_id" in block
    assert "d.subject_principal_key" in block
    assert "d.previous_count" in block


# --- catalog-logs.html: standalone logs page can filter resets ----------


def test_catalog_logs_dropdown_has_reset_option(catalog_logs_html: str) -> None:
    assert '<option value="usage_counter_reset">' in catalog_logs_html
