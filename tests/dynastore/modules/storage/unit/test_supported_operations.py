"""supported_operations — capability-derived + protocol-derived union.

Combines ``derive_supported_operations(driver.capabilities)`` with the
TRANSFORM bit added when the driver implements EntityTransformProtocol.
"""

from __future__ import annotations

from dynastore.models.protocols.storage_driver import Capability
from dynastore.modules.storage.routing_config import (
    Operation,
    supported_operations,
)


class _CapsOnly:
    capabilities = frozenset({Capability.WRITE, Capability.READ})


class _TransformOnly:
    """No capabilities, but implements EntityTransformProtocol shape."""
    capabilities = frozenset()
    async def transform_for_index(self, entity, **_): return entity
    async def restore_from_index(self, doc, **_): return doc


class _CapsAndTransform:
    capabilities = frozenset({Capability.READ})
    async def transform_for_index(self, entity, **_): return entity
    async def restore_from_index(self, doc, **_): return doc


def test_capabilities_only_no_TRANSFORM_bit():
    """Driver with caps but not implementing the protocol → no TRANSFORM."""
    ops = supported_operations(_CapsOnly())
    assert Operation.TRANSFORM not in ops


def test_protocol_only_adds_TRANSFORM_bit():
    """Driver with only the protocol shape → TRANSFORM present."""
    ops = supported_operations(_TransformOnly())
    assert Operation.TRANSFORM in ops


def test_caps_and_protocol_combine():
    """A driver with both caps and the protocol gets the union."""
    ops = supported_operations(_CapsAndTransform())
    assert Operation.TRANSFORM in ops
    assert any(o for o in ops if o != Operation.TRANSFORM), \
        "expected at least one capability-derived op alongside TRANSFORM"


def test_returns_frozenset():
    ops = supported_operations(_CapsOnly())
    assert isinstance(ops, frozenset)
