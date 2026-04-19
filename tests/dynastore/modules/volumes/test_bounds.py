import pytest
from dynastore.modules.volumes.bounds import FeatureBounds, merge_bounds


def test_bounds_volume_and_center():
    b = FeatureBounds("f", 0, 0, 0, 2, 4, 6)
    assert b.volume() == 48.0
    assert b.center() == (1.0, 2.0, 3.0)


def test_bounds_rejects_inverted_axis():
    with pytest.raises(ValueError):
        FeatureBounds("bad", min_x=10, min_y=0, min_z=0, max_x=0, max_y=1, max_z=1)


def test_merge_single_bound_roundtrips_extent():
    b = FeatureBounds("a", 0, 0, 0, 1, 1, 1)
    m = merge_bounds([b])
    assert (m.min_x, m.min_y, m.min_z, m.max_x, m.max_y, m.max_z) == (0, 0, 0, 1, 1, 1)


def test_merge_multiple_bounds_takes_outer_hull():
    a = FeatureBounds("a", 0, 0, 0, 1, 1, 1)
    b = FeatureBounds("b", -1, 0.5, -2, 0.5, 3, 5)
    m = merge_bounds([a, b])
    assert m.min_x == -1 and m.max_x == 1
    assert m.min_y == 0 and m.max_y == 3
    assert m.min_z == -2 and m.max_z == 5


def test_merge_empty_raises():
    with pytest.raises(ValueError):
        merge_bounds([])
