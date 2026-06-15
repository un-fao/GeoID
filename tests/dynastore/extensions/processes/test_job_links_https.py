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

"""HATEOAS job links must carry the external https scheme.

The app runs behind an inner load balancer that terminates TLS and forwards
plain http, so ``request.url_for`` yields an ``http://`` origin. Advertising
that to external clients produces mixed-content links. ``_get_job_links`` must
upgrade the scheme through ``enforce_https`` when ``FORCE_HTTPS`` is set, and
leave it untouched otherwise (local/dev, inner hop).
"""
from __future__ import annotations

import dynastore.extensions.tools.url as url_mod
from dynastore.extensions.processes import processes_service as ps
from dynastore.modules.tasks.models import Task, TaskStatusEnum


class _FakeRequest:
    """Minimal Request stand-in: only ``scope`` and ``url_for`` are used."""

    def __init__(self, scheme: str = "http") -> None:
        self.scope = {"path_params": {}}
        self._scheme = scheme

    def url_for(self, name: str, **params: object) -> str:
        job_id = params.get("job_id", "j")
        return f"{self._scheme}://inner.local/processes/{name}/{job_id}"


def test_job_links_upgraded_to_https_when_forced(monkeypatch):
    monkeypatch.setattr(url_mod, "FORCE_HTTPS", True)
    task = Task(task_type="ingest", status=TaskStatusEnum.COMPLETED, outputs={"ok": True})

    links = ps._get_job_links(task, _FakeRequest("http"))

    assert links, "expected a self link (+ results link for a completed job)"
    assert {link.rel for link in links} >= {"self"}
    for link in links:
        assert link.href.startswith("https://"), link.href


def test_job_links_left_untouched_when_not_forced(monkeypatch):
    monkeypatch.setattr(url_mod, "FORCE_HTTPS", False)
    task = Task(task_type="ingest", status=TaskStatusEnum.RUNNING)

    links = ps._get_job_links(task, _FakeRequest("http"))

    assert links
    for link in links:
        assert link.href.startswith("http://"), link.href


def test_external_url_helper_is_noop_without_force(monkeypatch):
    monkeypatch.setattr(url_mod, "FORCE_HTTPS", False)
    assert ps._external_url("http://inner.local/x") == "http://inner.local/x"


def test_external_url_helper_upgrades_with_force(monkeypatch):
    monkeypatch.setattr(url_mod, "FORCE_HTTPS", True)
    assert ps._external_url("http://inner.local/x") == "https://inner.local/x"
