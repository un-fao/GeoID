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

"""Shared durable-work primitives (WorkClass unification, #1807 Phase A).

Three durable-work subsystems — the ``events.events`` outbox, the per-tenant
``storage_outbox`` ES drain, and the ``tasks.tasks`` queue — independently
reinvented the same machinery: advisory-lock key derivation, leader election,
retry/backoff curves, and ``pg_notify`` channel naming. Each private copy is
a drift surface: the all-PENDING outbox drain traced to a fail-closed vs
fail-open election mismatch (#1801) and a producer/consumer scope-string
mismatch (#1804) were both cross-copy divergence, not logic bugs.

This package is the single home for those primitives. Extraction contract:

* **Byte-equivalent.** Moved helpers preserve their exact outputs (lock-key
  integers, channel strings, backoff delays). Golden-value tests pin them.
* **Zero DDL.** Nothing here touches table shape; SQL/plpgsql bodies that
  embed the same values (triggers, reaper functions) are unchanged and must
  agree with the constants here — the tests cross-check the literals.
* **No new behavior.** Call-sites keep their existing names via aliases;
  this slice only relocates and documents.

Modules
-------
``locks``
    Stable advisory-lock key derivations (two distinct, both load-bearing).
``election``
    Canonical import surface for leader election + leader loops.
``backoff``
    Retry/backoff curve shared by the durable drains.
``notify``
    ``pg_notify`` channel names and builders.

Still owned by the subsystems (later Phase A slices): claim-batch SQL
(three structurally different ``FOR UPDATE SKIP LOCKED`` shapes), routing
(``resolved_targets`` / capability registry), and status lifecycles.
"""

from dynastore.durable.backoff import DEFAULT_BACKOFF_SECONDS, backoff_delay_seconds
from dynastore.durable.locks import (
    stable_lock_key_signed64,
    stable_lock_key_uint63,
)
from dynastore.durable.notify import (
    EVENTS_CHANNEL,
    NEW_TASK_QUEUED,
    TASK_STATUS_CHANGED,
    outbox_channel,
)

_ELECTION_EXPORTS = frozenset({"pg_advisory_leadership", "run_leader_loop"})


def __getattr__(name: str):
    # ``election`` re-exports from db_config.locking_tools, which itself
    # imports ``durable.locks`` — eager import here would be circular when
    # locking_tools is imported first. Resolved lazily on first access.
    if name in _ELECTION_EXPORTS:
        from dynastore.durable import election

        return getattr(election, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "DEFAULT_BACKOFF_SECONDS",
    "backoff_delay_seconds",
    "pg_advisory_leadership",
    "run_leader_loop",
    "stable_lock_key_signed64",
    "stable_lock_key_uint63",
    "EVENTS_CHANNEL",
    "NEW_TASK_QUEUED",
    "TASK_STATUS_CHANGED",
    "outbox_channel",
]
