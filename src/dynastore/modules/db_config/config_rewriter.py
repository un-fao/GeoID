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

"""
Legacy-name → canonical-name rewriter for driver and config renames.

When a driver class or its :class:`PluginConfig` subclass is renamed, any
persisted rows / YAML configs / routing entries keyed on the old name must
still resolve. The rewriter keeps a legacy → canonical map and normalises
at lookup time only.

Concurrency model
-----------------
All *mutations* (register, reset, restore) acquire ``_LOCK`` — under
CPython this is enough to keep the map in a self-consistent state during
the mutation itself.  Readers (``normalise_driver_id``,
``normalise_class_key``) are lock-free; they rely on ``dict.get`` being
atomic under the GIL.  That guarantee holds for CPython up to 3.13 and
for 3.14+ only when the GIL is enabled (``python --enable-gil``).  If a
caller deploys dynastore on a no-GIL interpreter or PyPy and intends to
register renames concurrently with reads, wrap read sites in ``_LOCK``
or register all renames before any concurrent reader starts.  The
current usage — registrations happen at module-import time (effectively
serialised) and reads happen afterwards at request time — is safe
everywhere.

Two separate maps because they live in distinct namespaces:

- ``driver_id`` — ``type(driver).__name__``, the key in
  ``routing_config._validate_routing_entries``'s ``driver_index``.
- ``config class_key`` — ``PluginConfig.class_key()`` (defaults to
  ``__qualname__``), used by :func:`resolve_config_class` /
  ``TypedModelRegistry.get()`` / the ``collection_configs`` waterfall.

A driver class and its config class can be renamed independently, so the
maps are kept separate.

Registration is done at driver-module import time, next to the module-
level alias that keeps the legacy Python symbol importable:

    CollectionPostgresqlDriver = ItemsPostgresqlDriver  # back-compat alias
    register_driver_id_rename(
        legacy="CollectionPostgresqlDriver",
        canonical="ItemsPostgresqlDriver",
    )

Idempotency
-----------
Re-registering the same (legacy, canonical) pair is a no-op. Conflicting
re-registration (same legacy pointing at a different canonical) raises
``ValueError`` to surface bugs — it's never what the caller wants.

Non-goals
---------
The rewriter normalises ONLY at registry-lookup time. It does NOT mutate
Pydantic-validated values, persisted JSON, or anything else that would
break equality semantics downstream. If a row stored ``driver_id``:
``"CollectionPostgresqlDriver"``, that literal value stays on disk — the
rewriter just makes lookup resolve to the renamed class.
"""

from __future__ import annotations

import logging
import threading
from typing import Dict

logger = logging.getLogger(__name__)


_DRIVER_ID_RENAMES: Dict[str, str] = {}
_CONFIG_CLASS_KEY_RENAMES: Dict[str, str] = {}
_LOCK = threading.Lock()


def _register(
    store: Dict[str, str],
    legacy: str,
    canonical: str,
    label: str,
) -> None:
    """Core register routine, shared between the two namespaces.

    Enforces:
    - ``legacy != canonical`` (registering a no-op rename is a bug)
    - Same (legacy, canonical) pair is idempotent
    - Different canonical for the same legacy raises ``ValueError``
    """
    if not legacy or not canonical:
        raise ValueError(
            f"{label} rename: legacy and canonical must both be non-empty "
            f"(got legacy={legacy!r}, canonical={canonical!r})."
        )
    if legacy == canonical:
        raise ValueError(
            f"{label} rename: legacy == canonical ({legacy!r}); "
            f"register only real renames."
        )
    with _LOCK:
        existing = store.get(legacy)
        if existing is None:
            store[legacy] = canonical
            logger.debug(
                "Registered %s rename: %s -> %s", label, legacy, canonical
            )
            return
        if existing == canonical:
            return  # idempotent re-register
        raise ValueError(
            f"{label} rename conflict: {legacy!r} already maps to "
            f"{existing!r}, cannot re-register as {canonical!r}."
        )


def register_driver_id_rename(*, legacy: str, canonical: str) -> None:
    """Map a legacy driver_id (driver class ``__name__``) to its canonical form.

    Called at driver-module import time when a driver class is renamed.
    The ``driver_index`` in ``_validate_routing_entries`` keys on
    ``type(driver).__name__``, so a legacy routing entry with
    ``driver_id="CollectionPostgresqlDriver"`` won't resolve to the
    renamed ``ItemsPostgresqlDriver`` without a rewriter entry.
    """
    _register(_DRIVER_ID_RENAMES, legacy, canonical, "driver_id")


def register_config_class_key_rename(*, legacy: str, canonical: str) -> None:
    """Map a legacy PluginConfig ``class_key()`` to its canonical form.

    Called at config-module import time when a :class:`PluginConfig`
    subclass is renamed. The ``TypedModelRegistry`` keys on
    ``class_key()`` (``__qualname__`` by default), so persisted rows in
    ``collection_configs`` / ``platform_configs`` with the legacy key
    won't resolve to the renamed class without a rewriter entry.
    """
    _register(_CONFIG_CLASS_KEY_RENAMES, legacy, canonical, "config class_key")


def normalise_driver_id(driver_id: str) -> str:
    """Return the canonical ``driver_id`` for a possibly-legacy one.

    Unregistered values pass through unchanged — callers never need to
    know whether a rename is in play.
    """
    return _DRIVER_ID_RENAMES.get(driver_id, driver_id)


def normalise_class_key(class_key: str) -> str:
    """Return the canonical ``class_key`` for a possibly-legacy one.

    Unregistered values pass through unchanged.
    """
    return _CONFIG_CLASS_KEY_RENAMES.get(class_key, class_key)


def list_driver_id_renames() -> Dict[str, str]:
    """Return a snapshot of registered driver_id renames (legacy → canonical).

    Primarily for diagnostics / CLI ``--list-drivers`` output.
    """
    with _LOCK:
        return dict(_DRIVER_ID_RENAMES)


def list_class_key_renames() -> Dict[str, str]:
    """Return a snapshot of registered class_key renames (legacy → canonical)."""
    with _LOCK:
        return dict(_CONFIG_CLASS_KEY_RENAMES)


def _reset_for_tests() -> None:
    """Test-only: wipe all registered renames.

    Production code should never call this. Tests use it via fixtures
    to isolate rename registrations between test cases.

    WARNING: Calling this without :func:`_snapshot_for_tests` /
    :func:`_restore_for_tests` pairing is destructive beyond the test
    that called it — module-level driver registrations
    (``register_driver_id_rename`` at import time) cannot be
    re-registered because Python caches imports.  Downstream tests in
    the same pytest session that depend on production renames (e.g. a
    persisted routing config with ``driver_id="CollectionPostgresqlDriver"``)
    will then see passthrough rather than the canonical name.  Always
    pair with the snapshot helpers below.
    """
    with _LOCK:
        _DRIVER_ID_RENAMES.clear()
        _CONFIG_CLASS_KEY_RENAMES.clear()


def _snapshot_for_tests() -> "tuple[Dict[str, str], Dict[str, str]]":
    """Test-only: return a deep copy of both rename maps.

    Use with :func:`_restore_for_tests` in a ``pytest.fixture`` teardown
    to isolate test-level registrations without destroying the
    production registrations that loaded at import time.
    """
    with _LOCK:
        return (dict(_DRIVER_ID_RENAMES), dict(_CONFIG_CLASS_KEY_RENAMES))


def _restore_for_tests(snapshot: "tuple[Dict[str, str], Dict[str, str]]") -> None:
    """Test-only: replace both rename maps with ``snapshot``.

    Complement to :func:`_snapshot_for_tests`.
    """
    driver_snap, config_snap = snapshot
    with _LOCK:
        _DRIVER_ID_RENAMES.clear()
        _DRIVER_ID_RENAMES.update(driver_snap)
        _CONFIG_CLASS_KEY_RENAMES.clear()
        _CONFIG_CLASS_KEY_RENAMES.update(config_snap)
