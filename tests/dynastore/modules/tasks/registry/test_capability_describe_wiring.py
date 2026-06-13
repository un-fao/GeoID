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

"""describe()-driven enrichment of the capability registry rows + SQL columns."""
from __future__ import annotations

import json
from typing import ClassVar, Optional

from pydantic import BaseModel

import dynastore.tasks as tasks_pkg
from dynastore.tasks import TaskConfig
from dynastore.tasks.protocols import TaskProtocol
from dynastore.modules.tasks.registry import publisher, repository
from dynastore.modules.tasks.registry.model import CapabilityRow


class _PayloadModel(BaseModel):
    indexer_id: str


class _DescribedTask(TaskProtocol):
    """Reindex things."""
    mandatory: ClassVar[bool] = True
    affinity_tier: ClassVar[Optional[str]] = "worker"
    payload_model: ClassVar[Optional[type]] = _PayloadModel

    async def run(self, payload):  # pragma: no cover
        return None


def test_collect_local_inventory_populates_description_and_schema(monkeypatch):
    monkeypatch.setattr(
        tasks_pkg, "_DYNASTORE_TASKS",
        {"reindex": TaskConfig(cls=_DescribedTask, module_name="m", name="reindex")},
    )
    # Identity is stubbed so the test is hermetic.
    monkeypatch.setattr(publisher, "get_service_name", lambda: "catalog")
    monkeypatch.setattr(publisher, "get_git_commit", lambda: "abc123")
    monkeypatch.setattr(publisher, "get_version", lambda: "0.1.0")

    service, commit, version, rows = publisher.collect_local_inventory()
    assert len(rows) == 1
    row = rows[0]
    assert row.task_key == "reindex"
    assert row.description == "Reindex things."
    assert row.payload_schema is not None
    assert "indexer_id" in row.payload_schema["properties"]


def test_capability_row_defaults_keep_new_fields_optional():
    r = CapabilityRow(
        service="s", task_key="t", kind="task",
        service_version="v", service_commit="c", version="c",
    )
    assert r.description == ""
    assert r.payload_schema is None


def test_upsert_and_list_sql_carry_new_columns():
    assert "description" in repository._UPSERT_SQL
    assert "payload_schema" in repository._UPSERT_SQL
    assert "CAST(:payload_schema AS jsonb)" in repository._UPSERT_SQL
    assert "description" in repository._LIST_SQL
    assert "payload_schema" in repository._LIST_SQL


def test_list_all_coerces_jsonb_string_to_dict(monkeypatch):
    # asyncpg may return jsonb as a JSON string under a raw text() query;
    # list_all must coerce it back to a dict for callers.
    coerced = repository._coerce_payload_schema({"payload_schema": json.dumps({"a": 1})})
    assert coerced["payload_schema"] == {"a": 1}
    passthru = repository._coerce_payload_schema({"payload_schema": {"a": 1}})
    assert passthru["payload_schema"] == {"a": 1}
    none_ok = repository._coerce_payload_schema({"payload_schema": None})
    assert none_ok["payload_schema"] is None
