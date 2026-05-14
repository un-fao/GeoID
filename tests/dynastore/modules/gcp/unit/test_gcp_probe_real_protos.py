"""Liveness probe tests exercised against *real* ``run_v2`` proto objects.

#741 (followup to #735): the original #735 probe tests in ``test_gcp_probe.py``
use ``SimpleNamespace`` stand-ins for ``run_v2.Execution`` and the RunJob
``Operation``. That leaves the load-bearing assumption untested — that the real
proto-plus objects expose the attributes the probe mines:

* ``extract_execution_name`` reads ``operation.metadata.name``. The RunJob LRO's
  ``metadata_type`` is ``run_v2.Execution`` — if its ``.name`` were shaped
  differently, ``runner_ref`` would never persist, every probe would return
  ``UNKNOWN``, and #726 would be effectively un-fixed.
* ``_completion_time_set`` assumes an unset ``completion_time`` reads as ``None``
  (or epoch) and a set one as a ``datetime`` with ``.year``.
* ``_map_execution_state`` derives a verdict from the per-attempt count fields.

These tests build genuine ``run_v2`` objects (no network — proto construction is
pure) so a future proto-plus version bump that changes those shapes fails here
loudly instead of silently degrading the reconciler in production.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from google.cloud import run_v2


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py — these
    tests are pure in-memory proto construction."""
    return None


def _verdict():
    from dynastore.modules.tasks.liveness import LivenessVerdict
    return LivenessVerdict


_EXEC_NAME = "projects/fao-aip-geospatial-review/locations/europe-west1/jobs/ingest/executions/ingest-xyz"


# --- extract_execution_name() against the real RunJob LRO metadata ----------

def test_extract_execution_name_from_real_execution_metadata():
    """The RunJob LRO's ``metadata`` is a real ``run_v2.Execution``; its
    ``.name`` is the execution resource path the probe needs."""
    from dynastore.modules.gcp.tools.jobs import extract_execution_name

    metadata = run_v2.Execution(name=_EXEC_NAME)

    # proto-plus mirrors the AsyncOperation surface: ``.metadata`` is the
    # deserialized Execution. A minimal stand-in for the operation wrapper that
    # carries the *real* proto as metadata is sufficient and faithful.
    class _Operation:
        def __init__(self, md):
            self.metadata = md

    assert extract_execution_name(_Operation(metadata)) == _EXEC_NAME


def test_extract_execution_name_real_execution_without_name_is_none():
    """A real ``Execution`` with an unset ``name`` (proto default ``""``) is
    not a usable handle — must yield ``None``, not the empty string."""
    from dynastore.modules.gcp.tools.jobs import extract_execution_name

    nameless = run_v2.Execution()  # name defaults to ""
    assert nameless.name == ""

    class _Operation:
        def __init__(self, md):
            self.metadata = md

    assert extract_execution_name(_Operation(nameless)) is None


# --- _completion_time_set() against real run_v2.Execution -------------------

def test_completion_time_unset_on_real_execution_reads_as_not_completed():
    """A freshly built ``run_v2.Execution`` has ``completion_time is None`` —
    the heuristic must read that as 'not completed'."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    ex = run_v2.Execution(name=_EXEC_NAME, running_count=1)
    assert ex.completion_time is None
    assert GcpJobRunner._completion_time_set(ex) is False


def test_completion_time_set_on_real_execution_reads_as_completed():
    """A real ``completion_time`` is a proto-plus ``DatetimeWithNanoseconds``
    (has ``.year``) — the heuristic must read that as 'completed'."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    ex = run_v2.Execution(name=_EXEC_NAME, succeeded_count=1)
    ex.completion_time = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
    assert ex.completion_time.year == 2026
    assert GcpJobRunner._completion_time_set(ex) is True


# --- _map_execution_state() against real run_v2.Execution -------------------

def test_map_state_real_running_execution_is_alive():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    ex = run_v2.Execution(name=_EXEC_NAME, running_count=1)
    assert GcpJobRunner._map_execution_state(ex) == _verdict().ALIVE


def test_map_state_real_pending_execution_is_alive():
    """No counts yet, no completion_time — cold start / scheduling. The
    execution exists, so ALIVE — this is exactly the window the fixed
    spawn-lease used to mis-handle."""
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    ex = run_v2.Execution(name=_EXEC_NAME)
    assert GcpJobRunner._map_execution_state(ex) == _verdict().ALIVE


def test_map_state_real_succeeded_execution_is_terminal_succeeded():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    ex = run_v2.Execution(name=_EXEC_NAME, succeeded_count=1)
    ex.completion_time = datetime(2026, 5, 14, tzinfo=timezone.utc)
    assert GcpJobRunner._map_execution_state(ex) == _verdict().TERMINAL_SUCCEEDED


def test_map_state_real_failed_execution_is_terminal_failed():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    ex = run_v2.Execution(name=_EXEC_NAME, failed_count=1)
    ex.completion_time = datetime(2026, 5, 14, tzinfo=timezone.utc)
    assert GcpJobRunner._map_execution_state(ex) == _verdict().TERMINAL_FAILED


def test_map_state_real_cancelled_execution_is_dead():
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    ex = run_v2.Execution(name=_EXEC_NAME, cancelled_count=1)
    ex.completion_time = datetime(2026, 5, 14, tzinfo=timezone.utc)
    assert GcpJobRunner._map_execution_state(ex) == _verdict().DEAD


# --- #741 item 4: the mid-retry edge case, pinned against a real proto ------

def test_map_state_real_midretry_execution_is_alive():
    """#741 item 4 — an execution mid-retry: a prior attempt has ``failed_count
    > 0`` but ``running_count`` is momentarily 0 and ``completion_time`` is
    still unset (Cloud Run is scheduling the next retry attempt).

    ``_map_execution_state`` falls through to ALIVE. This is the *intended*
    behaviour — the execution as a whole has not terminated, so it must not be
    failed out from under Cloud Run's own retry. The reconciler keeps extending
    the lease; when the execution genuinely terminates, ``completion_time`` is
    set and the verdict flips to TERMINAL_FAILED. A genuinely-stuck execution
    is bounded by Cloud Run's own ``maxRetries`` / task timeout, after which it
    completes (failed) and the probe reports TERMINAL_FAILED — it is not held
    alive indefinitely by lease extensions.

    This test pins that contract so a future change to the count-based mapping
    is a deliberate, reviewed decision rather than a silent regression.
    """
    from dynastore.modules.gcp.gcp_runner import GcpJobRunner

    ex = run_v2.Execution(name=_EXEC_NAME, failed_count=1, running_count=0)
    assert ex.completion_time is None
    assert GcpJobRunner._map_execution_state(ex) == _verdict().ALIVE
