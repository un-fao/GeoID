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

"""Cascade cleanup registry — in-process, startup-populated, freeze-after-init.

Usage
-----
At application startup, each module, driver, or sidecar that owns external
resources registers its ``ResourceOwnerProtocol`` implementation::

    from dynastore.modules.catalog.cascade_registry import cascade_cleanup_registry

    cascade_cleanup_registry.register(my_owner)

After all modules have been loaded, the startup sequence calls::

    cascade_cleanup_registry.freeze()

From that point, ``register`` and ``unregister`` raise ``RuntimeError``.

The orchestrator queries the registry at delete time::

    owners = cascade_cleanup_registry.owners_for_scope(ResourceScope.CATALOG)

and dispatches ``cleanup_one`` using::

    owner = cascade_cleanup_registry.get(ref.owner_id)
    outcome = await owner.cleanup_one(ref, mode)

Thread safety
-------------
Registration may happen from multiple module-import threads during startup.
A ``threading.Lock`` guards all mutating operations.  ``owners_for_scope``
returns a stable-ordered copy (insertion order preserved); mutating the
returned list does not affect the registry.

Module-level singleton
----------------------
``cascade_cleanup_registry`` is the singleton used by app code.  Tests
should construct fresh ``CascadeCleanupRegistry()`` instances to avoid
cross-test contamination.
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from typing import Iterator

from dynastore.modules.catalog.resource_owner import ResourceOwnerProtocol, ResourceScope

logger = logging.getLogger(__name__)


class CascadeCleanupRegistry:
    """Thread-safe, freeze-after-init registry of ``ResourceOwnerProtocol`` instances."""

    def __init__(self) -> None:
        self._by_owner_id: dict[str, ResourceOwnerProtocol] = {}
        self._by_scope: dict[ResourceScope, list[ResourceOwnerProtocol]] = defaultdict(list)
        self._frozen: bool = False
        self._lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def register(self, owner: ResourceOwnerProtocol) -> None:
        """Add *owner* to the registry.

        Raises ``RuntimeError`` if the registry is frozen.
        Raises ``ValueError`` if another owner with the same ``owner_id``
        is already registered.
        """
        with self._lock:
            if self._frozen:
                # ERROR (not WARNING): a register() after the freeze fence means
                # the owning module/extension started too late, so its resources
                # silently leak on cascade delete. Surface it as a CI-detectable
                # signal. See geoid#1468.
                logger.error(
                    "CascadeCleanupRegistry.register(owner_id=%r) called AFTER "
                    "freeze — owner NOT registered; %s resources will leak on "
                    "cascade delete. The owning module/extension lifespan must "
                    "run before the application-level finalize_cascade_registry() "
                    "fence (main.py).",
                    getattr(owner, "owner_id", "<unknown>"),
                    getattr(owner, "owner_id", "<unknown>"),
                )
                raise RuntimeError(
                    "CascadeCleanupRegistry is frozen — register() is not allowed "
                    "after startup has completed."
                )
            oid = owner.owner_id
            if oid in self._by_owner_id:
                raise ValueError(
                    f"An owner with owner_id={oid!r} is already registered. "
                    "Each owner_id must be globally unique."
                )
            self._by_owner_id[oid] = owner
            for scope in owner.supported_scopes():
                self._by_scope[scope].append(owner)

    def unregister(self, owner_id: str) -> None:
        """Remove the owner with *owner_id* from the registry.

        No-op if the owner is not present.
        Raises ``RuntimeError`` if the registry is frozen.
        """
        with self._lock:
            if self._frozen:
                raise RuntimeError(
                    "CascadeCleanupRegistry is frozen — unregister() is not allowed "
                    "after startup has completed."
                )
            owner = self._by_owner_id.pop(owner_id, None)
            if owner is None:
                return
            for scope_list in self._by_scope.values():
                try:
                    scope_list.remove(owner)
                except ValueError:
                    pass

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get(self, owner_id: str) -> ResourceOwnerProtocol:
        """Return the owner registered under *owner_id*.

        Raises ``KeyError`` if no owner with that id is registered.
        """
        try:
            return self._by_owner_id[owner_id]
        except KeyError:
            raise KeyError(
                f"No resource owner registered with owner_id={owner_id!r}. "
                "Ensure the owning module is installed and has called register()."
            ) from None

    def owners_for_scope(self, scope: ResourceScope) -> list[ResourceOwnerProtocol]:
        """Return a stable-ordered copy of all owners that support *scope*.

        Registration order is preserved.  Mutating the returned list does not
        affect the registry.
        """
        with self._lock:
            return list(self._by_scope.get(scope, []))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def freeze(self) -> None:
        """Prevent further registration.

        Call once after all modules have loaded.  Subsequent calls to
        ``register`` or ``unregister`` will raise ``RuntimeError``.
        Calling ``freeze`` on an already-frozen registry is a no-op.
        """
        with self._lock:
            self._frozen = True

    @property
    def is_frozen(self) -> bool:
        """``True`` after ``freeze()`` has been called."""
        return self._frozen

    # ------------------------------------------------------------------
    # Iteration (for testing / introspection)
    # ------------------------------------------------------------------

    def __iter__(self) -> Iterator[ResourceOwnerProtocol]:
        return iter(list(self._by_owner_id.values()))

    def __len__(self) -> int:
        return len(self._by_owner_id)


# ---------------------------------------------------------------------------
# Module-level singleton — used by app code.
# Tests should construct fresh CascadeCleanupRegistry() instances.
# ---------------------------------------------------------------------------
_default_registry = CascadeCleanupRegistry()
cascade_cleanup_registry = _default_registry


def finalize_cascade_registry() -> None:
    """Freeze the application singleton — the cascade registry startup fence.

    Call exactly once, as the **last** startup step, from the application
    composition root (``main.py``), after *all* module and extension lifespans
    have run.  Idempotent.

    Until this runs, any module or extension may register a cascade owner during
    its own lifespan.  Previously the freeze lived inside ``CatalogModule.lifespan``,
    which locked out any owner whose lifespan happened to run later — a fragile,
    registration-order-dependent failure that silently leaked resources on
    cascade delete.  Moving the fence here removes that ordering coupling.
    See geoid#1468.
    """
    cascade_cleanup_registry.freeze()
    logger.info(
        "CascadeCleanupRegistry frozen by application finalize fence "
        "(%d owner(s) registered).",
        len(cascade_cleanup_registry),
    )
