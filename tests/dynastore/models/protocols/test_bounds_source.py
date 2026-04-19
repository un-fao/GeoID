import pytest

from dynastore.models.protocols.bounds_source import (
    BoundsSourceProtocol,
    EmptyBoundsSource,
)


def test_empty_source_is_protocol_compliant():
    assert isinstance(EmptyBoundsSource(), BoundsSourceProtocol)


@pytest.mark.asyncio
async def test_empty_source_returns_empty_for_any_inputs():
    src = EmptyBoundsSource()
    assert list(await src.get_bounds("c", "l")) == []
    assert list(await src.get_bounds("c", "l", limit=10)) == []


class _Fixture:
    """Concrete impl returning a fixed list. Confirms a non-EmptyBoundsSource
    also satisfies the Protocol via duck typing."""

    def __init__(self, items):
        self._items = items

    async def get_bounds(self, catalog_id, collection_id, *, limit=None):
        return self._items if limit is None else self._items[:limit]


@pytest.mark.asyncio
async def test_fixture_impl_satisfies_protocol():
    from dynastore.modules.volumes.bounds import FeatureBounds
    items = [FeatureBounds("a", 0, 0, 0, 1, 1, 1)]
    fx = _Fixture(items)
    assert isinstance(fx, BoundsSourceProtocol)
    assert (await fx.get_bounds("c", "l")) == items
    assert (await fx.get_bounds("c", "l", limit=0)) == []
