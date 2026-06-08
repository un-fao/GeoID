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

"""DDL tests for the AccessEnvelope sidecar's optional per-attr btree expression
indexes (#1453).

These indexes match the read filter's JSONB access pattern
``access_envelope->'attrs'->>'<key>'`` and are opt-in via ``btree_attrs_index``
+ ``known_attrs_keys``. ``get_ddl`` is a pure string builder (no DB), so these
assert on the emitted SQL directly.
"""
from dynastore.modules.storage.drivers.pg_sidecars.access_envelope import (
    AccessEnvelopeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.access_envelope_config import (
    AccessEnvelopeSidecarConfig,
)

_TABLE = "items_demo"


def _ddl(**cfg) -> str:
    sidecar = AccessEnvelopeSidecar(AccessEnvelopeSidecarConfig(**cfg))
    return sidecar.get_ddl(physical_table=_TABLE)


def test_ddl_gin_only_by_default():
    """Defaults: sub-table + FK + GIN, and NO btree expression indexes."""
    ddl = _ddl()
    assert f'CREATE TABLE IF NOT EXISTS {{schema}}."{_TABLE}_access_envelope"' in ddl
    assert f'"fk_{_TABLE}_access_envelope_hub"' in ddl
    assert f'"idx_{_TABLE}_access_envelope_gin"' in ddl
    assert "_attrs_" not in ddl  # no per-attr btree index emitted


def test_ddl_emits_btree_expression_index_per_known_key():
    """btree_attrs_index + known_attrs_keys → one expression index per key,
    indexing the exact JSONB path the read filter queries."""
    ddl = _ddl(btree_attrs_index=True, known_attrs_keys=["dept", "region"])
    for key in ("dept", "region"):
        assert f'"idx_{_TABLE}_access_envelope_attrs_{key}"' in ddl
        assert f"((access_envelope->'attrs'->>'{key}'))" in ddl


def test_ddl_skips_invalid_attr_key():
    """A key with non-identifier characters is skipped — never emitted as DDL."""
    ddl = _ddl(btree_attrs_index=True, known_attrs_keys=["ok_key", "dept; DROP TABLE x"])
    assert f'"idx_{_TABLE}_access_envelope_attrs_ok_key"' in ddl
    assert "DROP TABLE" not in ddl
    assert "ok_key" in ddl


def test_ddl_btree_flag_gates_key_list():
    """known_attrs_keys without btree_attrs_index emits no btree indexes."""
    ddl = _ddl(btree_attrs_index=False, known_attrs_keys=["dept"])
    assert "_attrs_dept" not in ddl
