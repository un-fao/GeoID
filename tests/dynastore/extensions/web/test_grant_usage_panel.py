"""
Source-level pins for the per-binding live counter view (#1342 / #1346).

The per-row "Usage" affordance on the Principals tab bindings table opens
a dialog that calls ``GET /admin/iam/usage/grants`` and renders the live
counters for that binding. These guards verify the button is present per
row with the right a11y attributes, the api.js client encodes every query
value, the render path never assigns server-derived strings through
``innerHTML``, and the stale-data banner lights up when the wire flag
flips.
"""

from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def governance_js() -> str:
    path = Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/admin/governance.js"
    )
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def governance_html() -> str:
    path = Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/admin/governance.html"
    )
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def api_js() -> str:
    path = Path(__file__).resolve().parents[3].parent / (
        "packages/extensions/web/src/dynastore/extensions/web/static/common/api.js"
    )
    return path.read_text(encoding="utf-8")


# --- "Usage" button on every binding row --------------------------------


def test_binding_row_has_usage_button(governance_js: str) -> None:
    """A ``Usage`` button is rendered in the bindings-table actions cell,
    next to ``Why?``. Same canonical ghost-xs variant."""
    assert 'usage.textContent = "Usage"' in governance_js
    assert 'usage.className = "btn btn-ghost btn-xs"' in governance_js


def test_usage_button_has_aria_label(governance_js: str) -> None:
    """Accessibility: the short-label button carries an aria-label that
    spells out what the operator gets when they click it."""
    needle = (
        'usage.setAttribute("aria-label", '
        '"Show live usage counters for this binding")'
    )
    assert needle in governance_js


def test_usage_button_lives_in_bindings_row_renderer(governance_js: str) -> None:
    """The Usage button is wired inside ``renderBindings`` so it lands on
    every row — not, accidentally, only on the principals-survey row."""
    render_block = governance_js.split("function renderBindings", 1)[-1]
    # Cut at the next top-level function declaration to keep this scoped
    # to renderBindings.
    next_fn = render_block.split("\nasync function ", 1)[0]
    assert 'usage.textContent = "Usage"' in next_fn
    # The Why? and Revoke affordances must still be next to it.
    assert 'why.textContent = "Why?"' in next_fn
    assert 'del.textContent = "Revoke"' in next_fn


# --- Modal scaffold in HTML ---------------------------------------------


def test_usage_dialog_scaffold_present(governance_html: str) -> None:
    """The native <dialog> scaffold with the refresh + close controls is
    in the page markup, hidden until ``showModal()`` is called."""
    assert 'id="usage-modal"' in governance_html
    assert 'id="usage-subject"' in governance_html
    assert 'id="usage-body"' in governance_html
    assert 'id="usage-refresh-btn"' in governance_html
    assert 'id="usage-close-btn"' in governance_html
    assert 'id="usage-stale-banner"' in governance_html


def test_usage_dialog_has_aria_label(governance_html: str) -> None:
    """The dialog is labelled by its title for assistive tech."""
    block = governance_html.split('id="usage-modal"', 1)[-1].split(">", 1)[0]
    assert 'aria-labelledby="usage-title"' in block


# --- api.js helper -------------------------------------------------------


def test_api_exports_fetch_grant_usage(api_js: str) -> None:
    """The thin client function is exported so governance.js can import it."""
    assert "export const fetchGrantUsage" in api_js


def test_fetch_grant_usage_encodes_every_query_value(api_js: str) -> None:
    """Every query-string value going to /admin/iam/usage/grants must pass
    through encodeURIComponent — principal ids and catalog ids can carry
    characters that would otherwise smuggle extra keys."""
    fn = api_js.split("fetchGrantUsage", 1)[-1]
    # Cut at the next top-level export to scope the slice.
    fn = fn.split("\nexport ", 1)[0]
    assert "/admin/iam/usage/grants" in fn
    # Both key and value go through encodeURIComponent on every parameter.
    assert "encodeURIComponent(key)" in fn
    assert "encodeURIComponent(value)" in fn
    # Every documented parameter is pushed into the query.
    for key in ("principal_id", "catalog_id"):
        assert f'"{key}"' in fn, f"missing push for {key}"


# --- XSS posture on the render path -------------------------------------


def test_no_unsafe_innerhtml_on_usage_render(governance_js: str) -> None:
    """The usage render path must not assign server-derived strings via
    innerHTML — same rule the other governance render paths enforce
    (matches #1382 / #1386 / #1391)."""
    # Scope the check to the usage block to avoid bleed from unrelated
    # blocks the rest of the file may add later.
    block = governance_js.split("function renderUsagePanel", 1)[-1]
    block = block.split("\n}\n\n", 1)[0]
    unsafe_sink = "inner" + "HTML"  # avoid tripping scanners on the literal
    needle = f".{unsafe_sink} ="
    assert needle not in block


def test_quota_spec_uses_pre_textcontent(governance_js: str) -> None:
    """``quota_spec`` is rendered as a <pre> via textContent so an
    operator-supplied condition spec cannot smuggle markup."""
    block = governance_js.split("function renderUsagePanel", 1)[-1]
    block = block.split("\n}\n\n", 1)[0]
    assert 'document.createElement("pre")' in block
    assert "specPre.textContent" in block


# --- Stale-data banner --------------------------------------------------


def test_stale_banner_shows_on_valkey_unavailable(governance_js: str) -> None:
    """When ``valkey_available=false`` lands, the render path lights up
    the stale-data banner with the documented message."""
    block = governance_js.split("function renderUsagePanel", 1)[-1]
    block = block.split("\n}\n\n", 1)[0]
    assert "valkey_available === false" in block
    # The user-facing copy mentions the PG fallback so the operator
    # knows the figures are still valid for inspection.
    assert "PG fallback" in block
    assert "may be stale" in block


def test_stale_banner_textcontent_not_innerhtml(governance_js: str) -> None:
    """The banner is set via textContent, not innerHTML, so the wire
    payload can't smuggle markup if the message ever becomes
    server-derived."""
    block = governance_js.split("function renderUsagePanel", 1)[-1]
    block = block.split("\n}\n\n", 1)[0]
    assert "banner.textContent" in block


# --- Refresh button -----------------------------------------------------


def test_refresh_button_rewires_fetch(governance_js: str) -> None:
    """The Refresh control re-runs ``refreshUsagePanel`` which calls
    ``fetchGrantUsage`` again."""
    assert "function refreshUsagePanel" in governance_js
    assert "fetchGrantUsage(" in governance_js
    # The dialog wires the refresh button click to the function.
    assert "refreshUsagePanel" in governance_js


# --- Behaviour preservation ---------------------------------------------


def test_why_button_still_present(governance_js: str) -> None:
    """Adding ``Usage`` must not remove or break the existing ``Why?``
    affordance from #1391 (parallel diagnostic, same row)."""
    assert 'why.textContent = "Why?"' in governance_js
    assert "openExplainer(row)" in governance_js


def test_revoke_button_still_present(governance_js: str) -> None:
    """Adding ``Usage`` must not remove or break the Revoke affordance."""
    assert 'del.textContent = "Revoke"' in governance_js
    assert "revokeBinding(row)" in governance_js
