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

"""Cycle F.7b — closure check on the F.2 engine-binding contract.

The hardcoded ``ENGINE_COMPATIBILITY`` table in
``test_engine_ref_field.py`` exercises every driver config we know about
TODAY, but a future driver-config addition that forgets
``required_engine_class`` (or typos the value) lands silently because
the parametrised table doesn't cover unknown classes.

This file walks every concrete ``_PluginDriverConfig`` subclass (the
same machinery ``test_no_orphan_driver_configs.py`` uses for the
``assert_bound`` guard) and pins two F.2 invariants:

1. Every concrete bound driver config declares a non-empty
   ``required_engine_class``.

2. That ``required_engine_class`` resolves to a registered engine via
   ``engine_registry.list_registered_engines()`` — so a typo'd value
   surfaces as a clear failure here, not as a 422 at first PATCH.
"""
from __future__ import annotations

import pytest

# Mirror the import block from test_no_orphan_driver_configs.py so every
# driver module's class-creation side effect fires before we walk
# ``__subclasses__()``.  Order doesn't matter — class registration is
# import-time only.
import dynastore.modules.storage.driver_config  # noqa: F401
import dynastore.modules.storage.drivers.catalog_postgresql  # noqa: F401
import dynastore.modules.storage.drivers.collection_postgresql  # noqa: F401
import dynastore.modules.storage.drivers.postgresql  # noqa: F401
import dynastore.modules.storage.drivers.duckdb  # noqa: F401
import dynastore.modules.storage.drivers.iceberg  # noqa: F401
import dynastore.modules.storage.drivers.bigquery  # noqa: F401
import dynastore.modules.storage.drivers.elasticsearch  # noqa: F401
import dynastore.modules.storage.drivers.elasticsearch_private.driver  # noqa: F401
import dynastore.modules.storage.drivers.core_postgresql  # noqa: F401
import dynastore.modules.stac.drivers.postgresql  # noqa: F401
import dynastore.modules.elasticsearch.collection_es_driver  # noqa: F401
import dynastore.modules.elasticsearch.catalog_es_driver  # noqa: F401
import dynastore.modules.catalog.drivers.pg_asset_driver  # noqa: F401

from dynastore.models.protocols.typed_driver import (
    _PluginDriverConfig,
    registered_pairs,
)
from dynastore.modules.db_config.engine_registry import (
    list_registered_engines,
)


# Drivers whose engine kind hasn't shipped yet — exempt from the F.2
# binding contract until their corresponding ``EngineConfig`` lands.
# Recorded by class name (not by class identity) so this guard surfaces
# obviously when an exempt driver disappears or gets renamed without
# the corresponding engine.
#
# - ItemsBigQueryDriver: BigQueryEngineConfig deferred per
#   project_geoid_cycle_f2_fixup_followup.md.  Add the engine and drop
#   this exemption together.
DEFERRED_ENGINE_BINDING: frozenset[str] = frozenset({
    "ItemsBigQueryDriverConfig",
})


def _concrete_bound_configs() -> list[type[_PluginDriverConfig]]:
    """Return every production-grade concrete ``_PluginDriverConfig``
    subclass that is bound to a :class:`TypedDriver`.

    Filters mirror :func:`_all_concrete_subclasses` in
    ``test_no_orphan_driver_configs.py``: skip abstract bases, skip
    test-local subclasses (``<locals>``), then intersect with the
    bound-pair registry so orphans don't double-fail through this
    test (the orphan guard already covers them).
    """
    bound = set(registered_pairs())
    seen: set[type] = set()
    out: list[type[_PluginDriverConfig]] = []
    stack: list[type] = list(_PluginDriverConfig.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls in seen:
            continue
        seen.add(cls)
        stack.extend(cls.__subclasses__())
        if cls.__dict__.get("is_abstract_base", False):
            continue
        if "<locals>" in cls.__qualname__:
            continue
        if cls not in bound:
            continue  # orphan — separate guard owns this
        out.append(cls)
    return out


def test_every_concrete_driver_config_declares_required_engine_class():
    """F.2 contract pin: every concrete bound driver config declares a
    non-empty ``required_engine_class``.  An unset value silently drops
    the engine-compatibility check in ``_default_and_validate_engine_ref``,
    letting any ``engine_ref`` (or none) slip through.
    """
    configs = _concrete_bound_configs()
    # Sanity: import block at the top of this file must surface all
    # production driver configs.  If this trips, a new module landed
    # without being added to the imports — fix the import block first,
    # not this assertion.
    assert len(configs) >= 15, (
        f"Only {len(configs)} concrete bound _PluginDriverConfig "
        "subclasses found — the import block at the top of this test "
        "is stale.  Add the new driver module so its config class is "
        "reachable via __subclasses__()."
    )

    missing: list[str] = []
    for cls in configs:
        if cls.__name__ in DEFERRED_ENGINE_BINDING:
            continue
        if not cls.required_engine_class:
            missing.append(
                f"  - {cls.__qualname__}: required_engine_class is empty"
            )
    if missing:
        pytest.fail(
            "F.2 contract violation — concrete bound driver config(s) "
            "do not declare ``required_engine_class``.  Add e.g. "
            "``required_engine_class: ClassVar[str] = \"postgresql_engine\"`` "
            "matching the platform engine your driver consumes:\n"
            + "\n".join(missing),
        )


def test_every_required_engine_class_resolves_to_registered_engine():
    """Cross-check: every driver config's ``required_engine_class`` must
    point at an :class:`EngineConfig` that is actually registered.

    A typo (e.g. ``"postgresql_engien"``) silently passes
    :func:`test_every_concrete_driver_config_declares_required_engine_class`
    above (the value is non-empty) and only surfaces at first
    operator PATCH.  Catching it here gives a single failure naming
    the offending class instead of N PATCH-time errors.
    """
    engines = list_registered_engines()
    valid_kinds = {cls.engine_class for cls in engines.values()}
    assert valid_kinds, (
        "engine_registry returned zero registered engines — F.1 engine "
        "modules failed to load.  Fix engine_config.py imports first."
    )

    mismatches: list[str] = []
    for cls in _concrete_bound_configs():
        if cls.__name__ in DEFERRED_ENGINE_BINDING:
            continue
        required = cls.required_engine_class
        if not required:
            continue  # covered by the prior test
        if required not in valid_kinds:
            mismatches.append(
                f"  - {cls.__qualname__}: required_engine_class="
                f"{required!r} not in registered engines "
                f"{sorted(valid_kinds)!r}"
            )
    if mismatches:
        pytest.fail(
            "F.2 contract violation — driver config(s) declare "
            "``required_engine_class`` that does not resolve to any "
            "registered EngineConfig.  Either fix the typo or "
            "register the engine kind:\n"
            + "\n".join(mismatches),
        )
