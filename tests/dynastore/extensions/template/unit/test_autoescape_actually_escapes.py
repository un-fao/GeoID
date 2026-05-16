"""Regression test for the template extension's ``?ae=true`` (autoescape) flag.

Background: ``templating.py`` used to build one module-level Jinja2
``Environment()`` (autoescape=False — Jinja2 default), compile each
incoming template against it with ``env.from_string(template)``, and
then attempt to opt into escaping by setting ``jinja_template.autoescape
= autoEscape`` AFTER compilation. That assignment is a no-op: Jinja2
emits escape calls at compile time based on the Environment's setting,
so flipping ``template.autoescape`` after the bytecode is built does
not change render output. Empirically::

    env = Environment()             # autoescape=False (default)
    t = env.from_string("Hi {{x}}")
    t.autoescape = True
    t.render(x="<script>alert(1)</script>")
    # 'Hi <script>alert(1)</script>'  ← raw, not escaped

The ``# type: ignore[reportAttributeAccessIssue]`` comment that used to
sit on the assignment was the smoking gun — pyright was already saying
``Template.autoescape`` is not the right knob.

User impact: ``GET|POST /template/interpolate?ae=true&r_mt=text/html``
with a model containing HTML/script payload returned raw HTML with a
text/html content type → reflected XSS in any pipeline where the
template or model originate from untrusted input. The OpenAPI surface
of ``ae`` advertised escaping that did not happen.

Fix: two long-lived ``Environment`` instances (autoescape on / off),
selected per request before ``from_string``. Compilation now happens
against the environment whose escaping bytecode matches caller intent.

This test pins both halves: ``ae=true`` must escape, ``ae=false`` must
not.
"""
from __future__ import annotations

import pytest


def test_autoescape_true_escapes_html_in_model_value():
    pytest.importorskip("jinja2")
    from dynastore.extensions.template.templating import _interpolate

    out = _interpolate(
        template="Hello {{name}}!",
        model={"name": "<script>alert(1)</script>"},
        autoEscape=True,
    )
    assert "<script>" not in out, (
        "ae=true must escape HTML in the rendered model — got raw script "
        f"tag in output: {out!r}. Regression of the bug fixed by selecting "
        "the autoescape=True environment at from_string time instead of "
        "setting template.autoescape after compilation (which is a no-op)."
    )
    assert "&lt;script&gt;" in out, (
        f"ae=true must produce HTML-escaped output; got {out!r}"
    )


def test_autoescape_false_passes_html_through_unchanged():
    """The autoescape=False env is what JSON / text-template callers
    expect — values render verbatim. We pin this so a future
    'always escape' overcorrection cannot break callers that legitimately
    template HTML/JSON fragments."""
    pytest.importorskip("jinja2")
    from dynastore.extensions.template.templating import _interpolate

    out = _interpolate(
        template="Hello {{name}}!",
        model={"name": "<b>bob</b>"},
        autoEscape=False,
    )
    assert out == "Hello <b>bob</b>!", (
        f"ae=false must render values verbatim; got {out!r}"
    )


def test_module_holds_two_distinct_environments():
    """Source-pin: the two long-lived environments must coexist so
    callers can switch escaping per request without paying a
    per-request ``Environment()`` construction cost. A regression that
    collapsed them back into one env would re-introduce the bug above."""
    pytest.importorskip("jinja2")
    from dynastore.extensions.template import templating

    plain = templating._jinja_env_plain
    autoesc = templating._jinja_env_autoescape

    assert plain is not autoesc, "envs must be distinct objects"
    assert plain.autoescape is False, (
        f"_jinja_env_plain.autoescape must be False; got {plain.autoescape!r}"
    )
    # In Jinja2, Environment(autoescape=True) sets the attribute to True.
    assert autoesc.autoescape is True, (
        f"_jinja_env_autoescape.autoescape must be True; got "
        f"{autoesc.autoescape!r}"
    )


def test_streaming_path_also_escapes():
    """The streaming variant ``_interpolate_streaming`` shares the same
    env-selection helper — make sure the fix covers it too. Callers
    hit this branch via ``?stream=true``."""
    pytest.importorskip("jinja2")
    import asyncio

    from dynastore.extensions.template.templating import _interpolate_streaming

    async def _collect() -> bytes:
        chunks: list[bytes] = []
        async for chunk in _interpolate_streaming(
            template="Hello {{name}}!",
            model={"name": "<script>alert(1)</script>"},
            autoEscape=True,
        ):
            chunks.append(chunk)
        return b"".join(chunks)

    out = asyncio.run(_collect())
    assert b"<script>" not in out, (
        "streaming path must also escape under ae=true; got raw script "
        f"tag in output: {out!r}"
    )
    assert b"&lt;script&gt;" in out, (
        f"streaming path with ae=true must produce escaped output; got {out!r}"
    )
