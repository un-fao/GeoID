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

"""``pg_notify`` channel names and builders — the single source of truth.

A NOTIFY channel only works if the producer (usually a plpgsql trigger)
and every consumer spell it identically; a one-character drift silently
matches zero listeners (the same failure shape as the #1804 scope-string
mismatch). Channel names were previously declared independently in the
tasks queue, the events models, and an f-string inside the outbox store —
this module replaces those with one declaration each.

The plpgsql sides (the ``new_task_queued`` trigger + reaper, the
``dynastore_events_channel`` trigger, the ``outbox_<driver>_<schema>``
trigger) necessarily embed the same literals in SQL; they are pinned to
these constants by DB-free regression tests, so renaming a channel here
without touching the DDL fails the suite instead of shipping a dead drain.
Wakeups stay sharded — per-(driver, tenant) for the outbox, per-shard pull
loops for events — so unifying the *naming* must never collapse distinct
channels into one (thundering-herd risk called out on the WorkClass plan).
"""

from dynastore.tools.db import validate_sql_identifier

#: Fired by the tasks AFTER INSERT trigger (payload: task_type) and by the
#: stuck-row reaper/redispatch paths (payload: a reason marker). Wakes every
#: pod's QueueListener; ``claim_*`` SKIP LOCKED dedupes the actual work.
NEW_TASK_QUEUED = "new_task_queued"

#: Fired on task status transitions; consumed by in-process waiters (e.g.
#: catalog lifecycle polling for completion).
TASK_STATUS_CHANGED = "task_status_changed"

#: Fired by the events AFTER INSERT trigger (payload: event_type). Wakes the
#: event consumer's shard pull loops.
EVENTS_CHANNEL = "dynastore_events_channel"


def outbox_channel(driver_id: str, catalog_id: str) -> str:
    """Per-(driver, tenant) storage-outbox channel name.

    Must mirror the plpgsql trigger expression
    ``'outbox_' || NEW.driver_id || '_' || cat`` (see ``outbox_ddl``).
    Both parts are validated as SQL identifiers — the same guard the
    listener applied — so a hostile or malformed id can't smuggle
    characters into a channel name.
    """
    validate_sql_identifier(driver_id)
    validate_sql_identifier(catalog_id)
    return f"outbox_{driver_id}_{catalog_id}"
