from dynastore.extensions.volumes.config import VolumesConfig
from dynastore.modules.volumes.bounds import FeatureBounds
from dynastore.modules.volumes.tileset_builder import build_tileset


def test_empty_bounds_emits_skeleton():
    ts = build_tileset([], VolumesConfig())
    assert ts["asset"]["version"] == "1.0"
    assert "root" in ts
    assert ts["root"]["geometricError"] == 0.0
    assert ts["root"]["boundingVolume"]["box"] == [0.0] * 12


def test_single_feature_is_a_leaf():
    b = [FeatureBounds("f", 0, 0, 0, 1, 1, 1)]
    ts = build_tileset(b, VolumesConfig())
    root = ts["root"]
    assert "children" not in root
    assert root["content"]["uri"].endswith(".b3dm")
    assert root["_feature_ids"] == ["f"]


def test_root_geometric_error_from_config():
    cfg = VolumesConfig(root_geometric_error=42.0)
    ts = build_tileset([FeatureBounds("f", 0, 0, 0, 1, 1, 1)], cfg)
    assert ts["geometricError"] == 42.0
    # Leaf node halves per depth — root is depth 0.
    assert ts["root"]["geometricError"] == 42.0


def test_two_features_are_split_at_pivot():
    # Far-apart features so the partition succeeds.
    b = [
        FeatureBounds("left",  0, 0, 0, 1, 1, 1),
        FeatureBounds("right", 10, 0, 0, 11, 1, 1),
    ]
    cfg = VolumesConfig(max_features_per_tile=1, max_tree_depth=5)
    ts = build_tileset(b, cfg)
    assert "children" in ts["root"]
    assert len(ts["root"]["children"]) == 2
    # Each child is a leaf.
    for child in ts["root"]["children"]:
        assert "children" not in child
        assert len(child["_feature_ids"]) == 1


def test_max_tree_depth_forces_leaf():
    # Two overlapping features with a depth cap of 0 → root must be a leaf
    # (cannot split into deeper tree).
    b = [
        FeatureBounds("a", 0, 0, 0, 1, 1, 1),
        FeatureBounds("b", 0.1, 0.1, 0.1, 1.1, 1.1, 1.1),
    ]
    cfg = VolumesConfig(max_features_per_tile=1, max_tree_depth=0)
    ts = build_tileset(b, cfg)
    assert "children" not in ts["root"]
    assert sorted(ts["root"]["_feature_ids"]) == ["a", "b"]


def test_degenerate_partition_collapses_to_leaf():
    # Three features identically placed — partition can't separate them;
    # builder should collapse to a leaf rather than recurse forever.
    b = [FeatureBounds(f"f{i}", 0, 0, 0, 1, 1, 1) for i in range(3)]
    cfg = VolumesConfig(max_features_per_tile=1, max_tree_depth=20)
    ts = build_tileset(b, cfg)
    # Top-level should be either a leaf, or one child is a leaf — walk
    # down and confirm no infinite recursion happened (depth < tree-cap).
    depth = 0
    node = ts["root"]
    while "children" in node:
        node = node["children"][0]
        depth += 1
        assert depth <= cfg.max_tree_depth + 1


def test_content_uri_template_is_formatted():
    b = [FeatureBounds("f", 0, 0, 0, 1, 1, 1)]
    ts = build_tileset(b, VolumesConfig(), content_uri_template="X/{tile_id}.bin")
    assert ts["root"]["content"]["uri"].startswith("X/")
    assert ts["root"]["content"]["uri"].endswith(".bin")
