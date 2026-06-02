"""Regression test for #1743: select_lapsed_gcp_tasks SELECT must include every
column that GcpLivenessReconciler reads when forwarding context to
apply_terminal_action.

Before the fix, inputs / caller_id / collection_id / scope were absent from the
SELECT, so ROUTE continuations (on_success pipelines, on_failure compensation)
launched with empty inputs, a SYSTEM caller, and a NULL collection — failing
silently because apply_terminal_action is fail-soft.

DB-free: inspects the SQL string built by select_lapsed_gcp_tasks and the
apply_terminal_action call-sites in liveness_reconciler.py.  Run with
``--noconftest`` to skip the live-database conftest.
"""

from __future__ import annotations

import inspect
import re


def _get_select_columns() -> set[str]:
    """Extract the column names from select_lapsed_gcp_tasks' SQL string.

    Parses the SELECT clause between ``SELECT`` and ``FROM``, stripping
    whitespace and trailing comma/newline noise, and returns the bare column
    names (no table prefix, no aliases).
    """
    from dynastore.modules.tasks import tasks_module

    src = inspect.getsource(tasks_module.select_lapsed_gcp_tasks)

    # Grab everything between 'SELECT' and 'FROM' (first occurrence)
    m = re.search(r"SELECT\s+(.*?)\s+FROM\b", src, re.DOTALL | re.IGNORECASE)
    assert m, "Could not locate the SELECT … FROM block in select_lapsed_gcp_tasks"

    raw_cols = m.group(1)
    cols: set[str] = set()
    for token in raw_cols.split(","):
        name = token.strip().rstrip(";").strip()
        if name:
            cols.add(name)
    return cols


def test_select_lapsed_gcp_tasks_includes_route_context_columns() -> None:
    """select_lapsed_gcp_tasks must SELECT inputs, caller_id, collection_id, scope.

    These four columns are read by GcpLivenessReconciler._reconcile_row and
    forwarded to apply_terminal_action for every terminal verdict
    (TERMINAL_SUCCEEDED / TERMINAL_FAILED / DEAD).  Omitting any one of them
    causes ROUTE continuations to launch with missing context.
    """
    cols = _get_select_columns()

    required = {"inputs", "caller_id", "collection_id", "scope"}
    missing = required - cols
    assert not missing, (
        f"select_lapsed_gcp_tasks is missing column(s) {sorted(missing)} from "
        f"its SELECT.  GcpLivenessReconciler reads these to forward ROUTE "
        f"continuation context; omitting them silently breaks pipelines."
    )


def test_select_lapsed_gcp_tasks_covers_all_reconciler_row_reads() -> None:
    """Every row.get() key in _reconcile_row must be present in the SELECT.

    Parses the reconciler source for ``row.get("<key>")`` calls, derives the
    full read-set, and asserts it is a subset of the SELECT column list.  This
    catches future drift without requiring a DB.
    """
    from dynastore.modules.gcp import liveness_reconciler

    reconciler_src = inspect.getsource(liveness_reconciler.GcpLivenessReconciler._reconcile_row)

    # Collect all row.get("...") / row.get('...') calls
    row_reads = set(re.findall(r'row\.get\(["\'](\w+)["\']', reconciler_src))
    assert row_reads, "Could not find any row.get(...) calls in _reconcile_row — source changed?"

    selected_cols = _get_select_columns()

    missing_from_select = row_reads - selected_cols
    assert not missing_from_select, (
        f"GcpLivenessReconciler._reconcile_row reads column(s) "
        f"{sorted(missing_from_select)} via row.get() but they are NOT in "
        f"select_lapsed_gcp_tasks's SELECT clause.  Add them to the SELECT so "
        f"the reconciler always receives the full row context."
    )
