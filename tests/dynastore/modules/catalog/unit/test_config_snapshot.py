"""Unit tests for the schema-id-gated catalog defaults snapshot (#1079 c).

Pure (no DB): exercises the snapshot scope predicate, serialization, and the
schema-id gate that makes the snapshot safe across config-class evolution.
"""

from dynastore.modules.catalog.config_snapshot import (
    SNAPSHOT_REF_KEY,
    is_snapshottable_config,
    select_snapshot_base,
    serialize_for_snapshot,
    snapshottable_config_classes,
)
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
    ItemsWritePolicy,
)
from dynastore.modules.storage.routing_config import ItemsRoutingConfig


def test_value_config_is_snapshottable():
    # ItemsWritePolicy is a stable behaviour/shape value config → frozen.
    assert is_snapshottable_config(ItemsWritePolicy) is True


def test_driver_and_routing_configs_excluded():
    # Driver/routing configs are evolution-sensitive + already Immutable →
    # left on live inheritance, never snapshotted.
    assert is_snapshottable_config(ItemsPostgresqlDriverConfig) is False
    assert is_snapshottable_config(ItemsRoutingConfig) is False


def test_snapshottable_set_includes_value_excludes_driver_routing():
    classes = set(snapshottable_config_classes())
    assert ItemsWritePolicy in classes
    assert ItemsPostgresqlDriverConfig not in classes
    assert ItemsRoutingConfig not in classes


def test_serialize_captures_schema_id_and_full_data():
    entry = serialize_for_snapshot(ItemsWritePolicy())
    assert entry["schema_id"] == ItemsWritePolicy.schema_id()
    # Every field is present (full shadow), not just explicitly-set ones.
    assert set(entry["data"]).issuperset(set(ItemsWritePolicy.model_fields))


def test_select_base_returns_instance_on_matching_schema_id():
    snap = {ItemsWritePolicy.class_key(): serialize_for_snapshot(ItemsWritePolicy())}
    base = select_snapshot_base(snap, ItemsWritePolicy)
    assert isinstance(base, ItemsWritePolicy)


def test_select_base_falls_back_on_schema_id_mismatch():
    # Simulate the class having evolved since the snapshot was taken: the stored
    # schema_id no longer matches → entry ignored, caller uses the live default.
    entry = serialize_for_snapshot(ItemsWritePolicy())
    entry["schema_id"] = "stale-deadbeef"
    snap = {ItemsWritePolicy.class_key(): entry}
    assert select_snapshot_base(snap, ItemsWritePolicy) is None


def test_select_base_none_when_absent_or_empty():
    assert select_snapshot_base(None, ItemsWritePolicy) is None
    assert select_snapshot_base({}, ItemsWritePolicy) is None
    assert select_snapshot_base({"other_key": {}}, ItemsWritePolicy) is None


def test_snapshot_ref_key_is_iterator_safe():
    # The reserved key must not collide with any real config class_key and must
    # carry the ``__`` prefix the per-class iterators skip.
    assert SNAPSHOT_REF_KEY.startswith("__")
    assert SNAPSHOT_REF_KEY not in {
        c.class_key() for c in snapshottable_config_classes()
    }
