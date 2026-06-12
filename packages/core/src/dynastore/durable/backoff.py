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

"""Retry/backoff curve shared by the durable drains.

The per-tenant ``storage_outbox`` drain schedules retries with this curve
(applied SQL-side inside ``mark_retry`` so concurrent retries can't de-sync
on a stale Python-side ``attempts``). The events outbox and the tasks queue
currently use bare counters with terminal DEAD_LETTER thresholds; as they
gain scheduled retries (Phase A2+) they must reuse this curve rather than
introduce a third one.
"""

from typing import List, Sequence

# Per-attempt retry backoff in seconds. Index by ``attempts`` (0-based);
# the last entry is reused for any attempt at or beyond its position so
# the backoff caps at ~30min instead of growing unbounded.
DEFAULT_BACKOFF_SECONDS: List[int] = [1, 5, 30, 5 * 60, 30 * 60]


def backoff_delay_seconds(
    attempts: int, curve: Sequence[int] = DEFAULT_BACKOFF_SECONDS,
) -> int:
    """Delay for a retry after ``attempts`` prior failures (0-based).

    Python mirror of the SQL CASE applied by ``PgOutboxStore.mark_retry``:
    index the curve by ``attempts``, clamping to the last entry so the
    delay plateaus instead of growing unbounded.
    """
    if attempts < 0:
        raise ValueError(f"attempts must be >= 0, got {attempts}")
    if not curve:
        raise ValueError("backoff curve must not be empty")
    return curve[min(attempts, len(curve) - 1)]
