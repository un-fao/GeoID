"""Regression test — get_task_instance must skip DefinitionOnlyTask placeholders.

DefinitionOnlyTask is registered in services that DON'T carry a task's
heavy deps (so /processes can still surface metadata). It has no `run`
method. If `get_task_instance(task_type)` returns the placeholder, then
`Sync/BackgroundRunner.can_handle()` returns True and the dispatcher
tries to run it in-process — which crashes at first dispatch with
`'DefinitionOnlyTask' object has no attribute 'run'`.

Confirmed against `019ddaa7-0ee8-...` (dwh_join) and `019ddaa7-1209-...`
(export_features) on review env 2026-04-29: the catalog API service
(which lacks the workers' heavy deps) registered both as placeholders;
the dispatcher claimed them via BackgroundRunner; the OGC poll showed
"Asynchronous execution failed: 'DefinitionOnlyTask' object has no
attribute 'run'" — the Cloud Run Job that DID have the deps was never
spawned.

Fix: ``get_task_instance`` returns None for placeholders →
``BackgroundRunner.can_handle()`` returns False → ``GcpJobRunner`` (which
keys off Cloud Run job_map_sync independently of placeholder status)
takes the task instead.
"""
from dynastore.tasks import _DYNASTORE_TASKS, TaskConfig, get_task_instance


class _RealTask:
    """A normal TaskProtocol-shaped class — has run + no placeholder marker."""
    async def run(self, payload):
        return None


class _PlaceholderTask:
    """Mirrors `_register_definition_only_placeholders`'s synthetic class."""
    is_placeholder = True

    @staticmethod
    def get_definition():
        return None


def _register(name: str, cls: type) -> None:
    _DYNASTORE_TASKS[name] = TaskConfig(
        cls=cls, type="task", module_name="test",
        name=name, definition=None, instance=None,
    )


def _unregister(name: str) -> None:
    _DYNASTORE_TASKS.pop(name, None)


def test_get_task_instance_skips_placeholder() -> None:
    _register("test_placeholder_x", _PlaceholderTask)
    try:
        result = get_task_instance("test_placeholder_x")
        assert result is None, (
            f"DefinitionOnlyTask placeholder was returned ({type(result).__name__}). "
            "BackgroundRunner.can_handle would then claim it and crash on "
            ".run(). Returning None forces GcpJobRunner to take the task."
        )
    finally:
        _unregister("test_placeholder_x")


def test_get_task_instance_returns_real_task() -> None:
    """Negative case: real (non-placeholder) tasks must still resolve."""
    _register("test_real_x", _RealTask)
    try:
        result = get_task_instance("test_real_x")
        assert isinstance(result, _RealTask), (
            f"Expected _RealTask instance, got {type(result).__name__}"
        )
    finally:
        _unregister("test_real_x")
