"""Phase E + F regression test — unscoped task lookup is uncached.

The OGC Processes API job-status route (`GET /processes/jobs/{job_id}`)
must:
- Find tasks regardless of which tenant `schema_name` they were created
  with (collection-scoped POST writes a non-public schema_name).
- Return fresh data on every poll — Cloud Run Job containers update the
  status row from a different process, so the API process's cache cannot
  be relied on.

`get_task_by_id_unscoped` satisfies both: drops the `schema_name` filter
and is intentionally not `@cached`.
"""
import inspect

from dynastore.modules.tasks import tasks_module


def test_get_task_by_id_unscoped_exists() -> None:
    assert hasattr(tasks_module, "get_task_by_id_unscoped"), (
        "get_task_by_id_unscoped is missing — OGC unscoped job-poll route "
        "would 404 for collection-scoped jobs."
    )


def test_get_task_by_id_unscoped_is_not_cached() -> None:
    """Cache wraps `get_task` (scoped). The unscoped variant must be a plain
    coroutine — caching here would mask cross-process status writes from a
    Cloud Run Job container, leaving the OGC poll stuck at 'running'."""
    fn = tasks_module.get_task_by_id_unscoped
    # `@cached` decorator attaches a `cache_invalidate` attribute; plain
    # async functions do not.
    assert not hasattr(fn, "cache_invalidate"), (
        "get_task_by_id_unscoped must NOT be @cached — status polls need to "
        "see cross-process writes from Cloud Run Job containers."
    )


def test_get_task_by_id_unscoped_signature() -> None:
    """Signature: (conn, task_id) — no `schema` parameter. The caller must
    not be able to accidentally re-introduce schema filtering."""
    sig = inspect.signature(tasks_module.get_task_by_id_unscoped)
    params = list(sig.parameters.keys())
    assert params == ["conn", "task_id"], (
        f"unexpected signature {params}; should be (conn, task_id) so the "
        "scope filter can't be silently re-added"
    )


def test_get_task_still_cached_for_scoped_path() -> None:
    """The original `get_task` keeps its `@cached` decorator — that path is
    still the right choice for hot reads inside a known schema (e.g.
    catalog-scoped routes that already resolved the schema)."""
    assert hasattr(tasks_module.get_task, "cache_invalidate"), (
        "get_task lost its @cached decorator — scoped reads will now hit DB "
        "on every call"
    )
