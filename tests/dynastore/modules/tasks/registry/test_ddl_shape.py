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

"""The registry DDL is idempotent (CREATE ... IF NOT EXISTS) and matches the
qualified table name the repository reads. No ALTER/DROP (hard invariant)."""
from __future__ import annotations

from dynastore.modules.db_config.typed_store.ddl import TASK_CAPABILITY_REGISTRY_DDL
from dynastore.modules.tasks.registry.model import TASK_CAPABILITY_REGISTRY_TABLE


def test_ddl_is_idempotent_create_only():
    sql = TASK_CAPABILITY_REGISTRY_DDL.lower()
    assert "create table if not exists" in sql
    assert "create index if not exists" in sql
    for forbidden in (" alter table ", " drop table ", " drop column ", " rename "):
        assert forbidden not in f" {sql} ", f"forbidden in-place DDL: {forbidden!r}"


def test_ddl_targets_the_repository_table():
    assert TASK_CAPABILITY_REGISTRY_TABLE in TASK_CAPABILITY_REGISTRY_DDL
    assert "primary key (service, task_key)" in TASK_CAPABILITY_REGISTRY_DDL.lower()
