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

"""Unit tests for OGC API Processes i18n link-title resolution.

Covers ``_localize_process_list`` and ``_localize_status_info`` for:
- default ``en`` resolution (plain string on the wire)
- unknown-language fallback to ``en``
- ``lang='*'`` full multi-language passthrough
for both the process-list links and the job-status links.
"""
from __future__ import annotations

import uuid

from dynastore.modules.processes import models
from dynastore.extensions.processes.processes_service import (
    _localize_process_list,
    _localize_status_info,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_process_list() -> models.ProcessList:
    """ProcessList with one process that has titled links."""
    self_link = models.Link(
        href="http://example.com/processes/ingest",
        rel="self",
        type="application/json",
        title="Detailed process description",  # type: ignore[arg-type]
        hreflang=None,
    )
    execute_link = models.Link.model_validate(
        {
            "href": "http://example.com/processes/ingest/execution",
            "rel": "http://www.opengis.net/def/rel/ogc/1.0/execute",
            "type": "application/json",
            "title": "Execute at this collection",
            "method": "POST",
            "templated": False,
        }
    )
    top_link = models.Link(
        href="http://example.com/processes",
        rel="self",
        type="application/json",
        hreflang=None,
    )
    summary = models.ProcessSummary.model_validate(
        {
            "id": "ingest",
            "title": "Data Ingestion",
            "version": "1.0.0",
            "scopes": ["collection"],
            "jobControlOptions": ["async-execute"],
            "outputTransmission": ["reference"],
            "links": [self_link, execute_link],
        }
    )
    return models.ProcessList(processes=[summary], links=[top_link])


def _make_status_info() -> models.StatusInfo:
    """StatusInfo with titled links (self + results)."""
    links = [
        models.Link(
            href="http://example.com/jobs/abc",
            rel="self",
            type="application/json",
            title="This document",  # type: ignore[arg-type]
        ),
        models.Link(
            href="http://example.com/jobs/abc/results",
            rel="http://www.opengis.net/def/rel/ogc/1.0/results",
            type="application/json",
            title="Job results",  # type: ignore[arg-type]
        ),
    ]
    return models.StatusInfo(
        jobID=uuid.UUID("550e8400-e29b-41d4-a716-446655440000"),
        status="successful",
        type="process",
        links=links,
    )


# ---------------------------------------------------------------------------
# ProcessList link-title resolution
# ---------------------------------------------------------------------------


def test_process_list_link_title_default_en_is_plain_string():
    """Default language 'en': link titles must be plain strings on the wire."""
    pl = _make_process_list()
    result = _localize_process_list(pl, "en")
    for process in result["processes"]:
        for link in process.get("links", []):
            title = link.get("title")
            if title is not None:
                assert isinstance(title, str), (
                    f"Expected plain str, got {type(title).__name__!r}: {title!r}"
                )


def test_process_list_link_title_default_en_content():
    """Default language 'en': the resolved title matches the English value."""
    pl = _make_process_list()
    result = _localize_process_list(pl, "en")
    titles = [
        lnk["title"]
        for p in result["processes"]
        for lnk in p.get("links", [])
        if "title" in lnk
    ]
    assert "Detailed process description" in titles
    assert "Execute at this collection" in titles


def test_process_list_link_title_unknown_lang_falls_back_to_en():
    """An unknown language code must fall back to the 'en' value."""
    pl = _make_process_list()
    result = _localize_process_list(pl, "de")
    for process in result["processes"]:
        for link in process.get("links", []):
            title = link.get("title")
            if title is not None:
                assert isinstance(title, str), (
                    f"Expected plain str fallback, got {type(title).__name__!r}: {title!r}"
                )


def test_process_list_link_title_star_preserves_full_object():
    """lang='*' must keep the full multi-language dict on the wire."""
    pl = _make_process_list()
    result = _localize_process_list(pl, "*")
    for process in result["processes"]:
        for link in process.get("links", []):
            title = link.get("title")
            if title is not None:
                assert isinstance(title, dict), (
                    f"Expected full dict for lang='*', got {type(title).__name__!r}: {title!r}"
                )
                assert "en" in title, f"Expected 'en' key in title dict: {title!r}"


# ---------------------------------------------------------------------------
# StatusInfo link-title resolution
# ---------------------------------------------------------------------------


def test_status_info_link_title_default_en_is_plain_string():
    """Default language 'en': StatusInfo link titles must be plain strings."""
    si = _make_status_info()
    result = _localize_status_info(si, "en")
    for link in result.get("links", []):
        title = link.get("title")
        if title is not None:
            assert isinstance(title, str), (
                f"Expected plain str, got {type(title).__name__!r}: {title!r}"
            )


def test_status_info_link_title_default_en_content():
    """Default language 'en': resolved titles match the English values."""
    si = _make_status_info()
    result = _localize_status_info(si, "en")
    titles = [lnk["title"] for lnk in result["links"] if "title" in lnk]
    assert "This document" in titles
    assert "Job results" in titles


def test_status_info_link_title_unknown_lang_falls_back_to_en():
    """An unknown language code falls back to the 'en' value for StatusInfo."""
    si = _make_status_info()
    result = _localize_status_info(si, "de")
    for link in result.get("links", []):
        title = link.get("title")
        if title is not None:
            assert isinstance(title, str), (
                f"Expected plain str fallback, got {type(title).__name__!r}: {title!r}"
            )


def test_status_info_link_title_star_preserves_full_object():
    """lang='*' must keep the full multi-language dict on the wire for StatusInfo."""
    si = _make_status_info()
    result = _localize_status_info(si, "*")
    for link in result.get("links", []):
        title = link.get("title")
        if title is not None:
            assert isinstance(title, dict), (
                f"Expected full dict for lang='*', got {type(title).__name__!r}: {title!r}"
            )
            assert "en" in title, f"Expected 'en' key in title dict: {title!r}"


def test_status_info_required_fields_present():
    """The serialized StatusInfo must always carry the mandatory OGC fields."""
    si = _make_status_info()
    result = _localize_status_info(si, "en")
    for key in ("jobID", "status", "type", "links"):
        assert key in result, f"Missing required field {key!r}"
