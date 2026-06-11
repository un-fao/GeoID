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

"""Single-boolean platform bootstrap guard backed by ``catalog.shared_properties``.

One property in the DB records that the one-time platform initialisation has
completed for this deployment.  On subsequent cold-boots (e.g. Cloud Run
scaling events) both initialisers — JSON-config seeding and IAM preset
bootstrapping — skip their guarded work entirely, making reboot cheap.

Property key: ``platform.bootstrap_initialized``
Property value when set: ``"true"``

Usage pattern inside a boot sequence (advisory lock already held externally
by the caller)::

    if await is_initialized():
        return  # fast path — nothing to do
    # ... run idempotent initialisation ...
    await mark_initialized()

The guard is intentionally thin and dependency-free: it only talks to
``PropertiesProtocol`` which is provided by ``CatalogModule`` and contains no
IAM or storage driver logic.  Both consumers can import from here without
creating cross-module cycles.

Concurrency: callers MUST wrap the check-run-mark sequence inside the
existing ``acquire_startup_lock`` advisory lock (same lock used by
``config_seeder`` and ``bootstrap_preset_if_absent``).  The re-check after
lock acquisition (double-checked locking) prevents duplicate work when two
pods race to first boot.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

BOOTSTRAP_GUARD_KEY = "platform.bootstrap_initialized"
_BOOTSTRAP_OWNER = "platform"


async def is_initialized(db_resource: Optional[Any] = None) -> bool:
    """Return ``True`` if the platform bootstrap has already completed.

    Consults ``catalog.shared_properties`` via ``PropertiesProtocol``.  Returns
    ``False`` — not initialised — when:
    * ``PropertiesProtocol`` is not yet registered (very early boot);
    * the property row does not exist;
    * any DB error (degrade safely; the caller will re-run initialisation,
      which is idempotent).
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.properties import PropertiesProtocol

    props = get_protocol(PropertiesProtocol)
    if props is None:
        logger.debug("bootstrap_guard: PropertiesProtocol not registered — treating as uninitialised.")
        return False

    try:
        value = await props.get_property(BOOTSTRAP_GUARD_KEY, db_resource=db_resource)
        return value == "true"
    except Exception as exc:
        logger.warning(
            "bootstrap_guard: could not read %r (%s) — treating as uninitialised.",
            BOOTSTRAP_GUARD_KEY,
            exc,
        )
        return False


async def mark_initialized(db_resource: Optional[Any] = None) -> None:
    """Persist the bootstrap-complete marker to ``catalog.shared_properties``.

    Raises if ``PropertiesProtocol`` is not registered or the write fails —
    the caller should propagate the exception so the boolean remains unset and
    the next boot retries initialisation rather than silently skipping it.
    """
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols.properties import PropertiesProtocol

    props = get_protocol(PropertiesProtocol)
    if props is None:
        raise RuntimeError(
            "bootstrap_guard.mark_initialized: PropertiesProtocol not registered; "
            "cannot persist bootstrap marker."
        )

    await props.set_property(
        BOOTSTRAP_GUARD_KEY,
        "true",
        _BOOTSTRAP_OWNER,
        db_resource=db_resource,
    )
    logger.info("bootstrap_guard: platform bootstrap marked as complete (%r = 'true').", BOOTSTRAP_GUARD_KEY)
