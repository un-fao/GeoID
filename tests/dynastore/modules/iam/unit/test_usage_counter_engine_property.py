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

"""Pin the engine-resolution contract for ``PostgresUsageCounter``.

Gap #7 of issue #800: prior to this hardening the driver captured
``self._engine = get_engine()`` at ``__init__``. That worked in the
current bootstrap ordering, but two failure modes were latent:

1. **Construction reordering** — if a future refactor moved driver
   instantiation earlier than ``DatabaseProtocol`` registration (e.g.
   into module import or a lighter pre-lifespan hook), the captured
   reference would be ``None`` forever, even after DB came up.
2. **Test fixture engine swap** — fixtures that point the global
   provider at a fresh in-memory engine mid-run would silently leave
   the driver pinned to the previous engine.

The fix promotes ``_engine`` from a captured attribute to a property
that resolves via ``get_engine()`` on every access. These tests pin
that behaviour so a future "optimisation" can't quietly re-introduce
the capture.
"""

from __future__ import annotations

from unittest.mock import patch

from dynastore.modules.iam.usage_counter_pg import PostgresUsageCounter


def test_engine_is_a_property_not_a_captured_attribute() -> None:
    """The class descriptor for ``_engine`` must be a property —
    otherwise the driver is back to capturing at ``__init__`` and gap #7
    is silently reopened."""
    descriptor = PostgresUsageCounter.__dict__.get("_engine")
    assert isinstance(descriptor, property), (
        f"PostgresUsageCounter._engine must be a property; "
        f"found {type(descriptor).__name__}"
    )


def test_init_does_not_call_get_engine() -> None:
    """Constructing the driver must not trigger engine discovery — that
    decouples driver instantiation from DB-provider registration order
    and lets the bootstrap call site freely move."""
    with patch(
        "dynastore.modules.iam.usage_counter_pg.get_engine"
    ) as mocked:
        PostgresUsageCounter()
        assert mocked.call_count == 0, (
            f"PostgresUsageCounter.__init__ called get_engine "
            f"{mocked.call_count} times — must be lazy"
        )


def test_engine_property_resolves_live_each_access() -> None:
    """Two successive property reads must each invoke ``get_engine`` —
    proves the value is not memoised in an instance attribute, so a
    test fixture that swaps the registered engine mid-run is picked up
    on the next operation."""
    with patch(
        "dynastore.modules.iam.usage_counter_pg.get_engine"
    ) as mocked:
        mocked.side_effect = ["engine-A", "engine-B"]
        driver = PostgresUsageCounter()
        first = driver._engine
        second = driver._engine
        assert first == "engine-A"
        assert second == "engine-B"
        assert mocked.call_count == 2
