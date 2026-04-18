"""Regression tests for ``_dimension_fingerprint``.

The dimension materialisation sentinel
(``dimensions_extension._dimension_fingerprint``) gates each per-dimension
upsert pass on a SHA-256 hash of (provider class, ``provider.model_dump()``,
extent bounds). The skip path only fires when the freshly-computed hash
matches the previously-stored one byte-for-byte. If the hash is
non-deterministic across pod boots — e.g. ``model_dump()`` emits a field
whose iteration order is hash-randomised, or includes a memory address —
the comparison silently fails and every reboot re-materialises every
dimension. That regression is exactly what we just shipped a fix for; this
test guards against it returning.
"""
from fastapi import FastAPI

from dynastore.extensions.dimensions.dimensions_extension import (
    DimensionsExtension,
    _dimension_fingerprint,
)


def _load_dimensions_registry():
    """Instantiate the extension to populate the upstream ``DIMENSIONS``
    dict from ``ogc_dimensions.api.routes`` with the *production*
    configurations the lifespan would materialise."""
    DimensionsExtension(FastAPI())
    from ogc_dimensions.api.routes import DIMENSIONS
    return DIMENSIONS


# ---------------------------------------------------------------------------
# Pure determinism — same instance, same hash on repeated calls.
# ---------------------------------------------------------------------------


def test_fingerprint_is_pure_for_same_instance():
    dims = _load_dimensions_registry()
    cfg = next(iter(dims.values()))
    assert _dimension_fingerprint(cfg) == _dimension_fingerprint(cfg)


# ---------------------------------------------------------------------------
# Cross-instance determinism — two structurally identical cfgs hash equal.
# Catches reliance on ``id()`` / per-instance state.
# ---------------------------------------------------------------------------


def test_fingerprint_is_stable_across_independent_instances():
    from ogc_dimensions.api.routes import DimensionConfig
    from ogc_dimensions.providers import IntegerRangeProvider

    cfg_a = DimensionConfig(
        provider=IntegerRangeProvider(step=10),
        description="x",
        extent_min="0",
        extent_max="100",
    )
    cfg_b = DimensionConfig(
        provider=IntegerRangeProvider(step=10),
        description="x",
        extent_min="0",
        extent_max="100",
    )
    assert _dimension_fingerprint(cfg_a) == _dimension_fingerprint(cfg_b)


# ---------------------------------------------------------------------------
# Production registry — every dimension the lifespan materialises must
# fingerprint identically on two consecutive calls. This catches
# non-determinism that only surfaces with real Pydantic providers (e.g. a
# field whose iteration order is hash-randomised).
# ---------------------------------------------------------------------------


def test_fingerprint_is_deterministic_for_every_production_dimension():
    dims = _load_dimensions_registry()
    assert dims, "DIMENSIONS registry is empty — extension didn't populate it"

    drift: list[str] = []
    for name, cfg in dims.items():
        first = _dimension_fingerprint(cfg)
        second = _dimension_fingerprint(cfg)
        if first != second:
            drift.append(f"{name}: {first} != {second}")

    assert not drift, (
        "Non-deterministic fingerprint(s) detected — every reboot will "
        "re-materialise the listed dimensions:\n  " + "\n  ".join(drift)
    )


# ---------------------------------------------------------------------------
# Sensitivity — a real change in inputs must change the hash. Guards
# against a future refactor that accidentally makes the function constant.
# ---------------------------------------------------------------------------


def test_fingerprint_changes_when_extent_changes():
    from ogc_dimensions.api.routes import DimensionConfig
    from ogc_dimensions.providers import IntegerRangeProvider

    base = DimensionConfig(
        provider=IntegerRangeProvider(step=10),
        description="x",
        extent_min="0",
        extent_max="100",
    )
    changed = DimensionConfig(
        provider=IntegerRangeProvider(step=10),
        description="x",
        extent_min="50",   # ← only this differs
        extent_max="100",
    )
    assert _dimension_fingerprint(base) != _dimension_fingerprint(changed)
