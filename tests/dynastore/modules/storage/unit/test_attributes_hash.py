"""Unit coverage for ``IdentityMatcher.ATTRIBUTES_HASH`` plumbing.

The matcher relies on a STORED GENERATED column ``attributes_hash`` on the
attributes sidecar (Mode B / JSONB only) — ``encode(digest(attributes::text,
'sha256'), 'hex')``. Two items with byte-equal canonicalised JSONB attributes
produce the same hash regardless of geometry, supporting "same attribute
combination, different geometry" detection.

These tests cover the surface that is verifiable without a live DB:

* DDL emits the GENERATED column in Mode B but NOT in Mode A.
* Mode B includes ``attributes_hash`` in ``known_columns`` / internal-columns
  set so it never leaks into ``Feature.properties``.
* The application-side hash matches what PG would compute on insert
  (Python's ``CustomJSONEncoder`` output → SHA256 ↔ PG ``encode(digest(jsonb::text,
  'sha256'), 'hex')``).  Equivalence is exercised against canonicalised JSON.

DB-side parity (the matcher actually finding the row by hash) is a separate
integration test that requires PG with pgcrypto — tracked under #221's
"matcher-chain ordering" sub-task.
"""
from __future__ import annotations

import hashlib
import json

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.tools.json import CustomJSONEncoder


def _jsonb_mode_sidecar() -> FeatureAttributeSidecar:
    return FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB)
    )


def _columnar_mode_sidecar() -> FeatureAttributeSidecar:
    """Force Mode A. Mode A requires a non-empty schema; pass a minimal one
    via the config override so the resolved mode comes back COLUMNAR."""
    return FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.COLUMNAR)
    )


class TestStorageModeResolution:
    def test_explicit_jsonb_resolves_to_jsonb(self):
        sidecar = _jsonb_mode_sidecar()
        assert sidecar.resolved_storage_mode is AttributeStorageMode.JSONB

    def test_explicit_columnar_resolves_to_columnar(self):
        sidecar = _columnar_mode_sidecar()
        assert sidecar.resolved_storage_mode is AttributeStorageMode.COLUMNAR

    def test_automatic_with_no_schema_is_jsonb(self):
        sidecar = FeatureAttributeSidecar(
            FeatureAttributeSidecarConfig(
                storage_mode=AttributeStorageMode.AUTOMATIC,
                attribute_schema=None,
            )
        )
        assert sidecar.resolved_storage_mode is AttributeStorageMode.JSONB


class TestDdlEmission:
    def test_jsonb_mode_emits_generated_attributes_hash_column(self):
        sidecar = _jsonb_mode_sidecar()
        ddl = sidecar.get_ddl(physical_table="features")
        assert "attributes_hash CHAR(64) GENERATED ALWAYS AS" in ddl
        assert "encode(digest(attributes::text, 'sha256'), 'hex')" in ddl
        assert "STORED" in ddl

    def test_jsonb_mode_emits_btree_index_on_attributes_hash(self):
        sidecar = _jsonb_mode_sidecar()
        ddl = sidecar.get_ddl(physical_table="features")
        assert 'idx_features_attributes_attributes_hash' in ddl

    def test_columnar_mode_does_not_emit_attributes_hash(self):
        sidecar = _columnar_mode_sidecar()
        ddl = sidecar.get_ddl(physical_table="features")
        assert "attributes_hash" not in ddl


class TestInternalColumnsHidesAttributesHash:
    """``attributes_hash`` is plumbing for the matcher; if it ever bled into
    ``Feature.properties`` the field would surface in API responses. The
    sidecar's internal-columns set is what protects against that leak."""

    def test_jsonb_mode_includes_attributes_hash_in_internal_columns(self):
        sidecar = _jsonb_mode_sidecar()
        assert "attributes_hash" in sidecar.get_internal_columns()

    def test_columnar_mode_excludes_attributes_hash(self):
        sidecar = _columnar_mode_sidecar()
        assert "attributes_hash" not in sidecar.get_internal_columns()


class TestPythonPgHashParity:
    """The matcher's ``_resolve_by_attributes_hash`` constructs a query that
    asks PG to compute ``encode(digest(CAST(:attrs AS jsonb)::text, 'sha256'),
    'hex')``. The application must serialise the inbound feature attributes
    via ``CustomJSONEncoder`` so PG sees the same string. These tests pin the
    invariant that two attribute dicts that should match have the same Python
    serialisation, regardless of in-memory key order, decimal vs int, etc.

    PG itself canonicalises ``jsonb`` on write (sorted keys, normalised
    numbers), so the *real* parity check is integration-level — but at least
    we lock the application side to a deterministic byte stream."""

    def _python_hash(self, attrs: dict) -> str:
        s = json.dumps(attrs, cls=CustomJSONEncoder, sort_keys=True)
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def test_same_attrs_same_hash(self):
        a = {"foo": "bar", "baz": 1}
        b = {"foo": "bar", "baz": 1}
        assert self._python_hash(a) == self._python_hash(b)

    def test_different_key_order_same_hash(self):
        a = {"foo": "bar", "baz": 1}
        b = {"baz": 1, "foo": "bar"}
        assert self._python_hash(a) == self._python_hash(b)

    def test_different_values_distinct_hashes(self):
        a = {"foo": "bar", "baz": 1}
        b = {"foo": "bar", "baz": 2}
        assert self._python_hash(a) != self._python_hash(b)

    def test_added_key_distinct_hash(self):
        a = {"foo": "bar"}
        b = {"foo": "bar", "extra": None}
        assert self._python_hash(a) != self._python_hash(b)

    def test_nested_dict_canonicalised(self):
        a = {"meta": {"x": 1, "y": 2}}
        b = {"meta": {"y": 2, "x": 1}}
        assert self._python_hash(a) == self._python_hash(b)

    def test_hash_is_64_hex_chars(self):
        h = self._python_hash({"foo": "bar"})
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


class TestMatcherGatedOnStorageMode:
    """``_resolve_by_attributes_hash`` returns ``None`` early in Mode A
    because the GENERATED column doesn't exist there. Verifies the gate
    without invoking PG."""

    async def test_columnar_mode_short_circuits_to_none(self):
        sidecar = _columnar_mode_sidecar()
        result = await sidecar._resolve_by_attributes_hash(
            conn=None,  # never reached
            physical_schema="s",
            physical_table="t",
            processing_context={"feature_attributes": {"foo": "bar"}},
        )
        assert result is None

    async def test_jsonb_mode_no_attrs_returns_none(self):
        sidecar = _jsonb_mode_sidecar()
        result = await sidecar._resolve_by_attributes_hash(
            conn=None,  # would be reached if attrs were present
            physical_schema="s",
            physical_table="t",
            processing_context={},  # no feature_attributes
        )
        assert result is None
