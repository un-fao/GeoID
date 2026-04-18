import pytest
from pydantic import ValidationError

from dynastore.extensions.volumes.config import VolumesConfig


def test_defaults():
    cfg = VolumesConfig()
    assert cfg.max_features_per_tile == 10_000
    assert cfg.max_tree_depth == 20
    assert cfg.on_demand_cache_ttl_s == 3600
    assert cfg.default_height_attr == "height"
    assert cfg.root_geometric_error == 500.0
    assert cfg.refinement_ratio == 4.0


def test_rejects_negative_depth():
    with pytest.raises(ValidationError):
        VolumesConfig(max_tree_depth=-1)


def test_rejects_non_positive_error():
    with pytest.raises(ValidationError):
        VolumesConfig(root_geometric_error=0)


def test_override_values():
    cfg = VolumesConfig(max_features_per_tile=50, max_tree_depth=3)
    assert cfg.max_features_per_tile == 50
    assert cfg.max_tree_depth == 3
