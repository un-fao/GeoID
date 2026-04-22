"""Unit tests for the protocol-gate dispatch mechanism.

Regression guard ensuring that services missing required protocols do not claim
task types they cannot execute.  Covers:
- TaskProtocol.are_protocols_satisfied()
- CapabilityMap.refresh() skipping task types with unmet requirements
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.tasks.protocols import TaskProtocol, requires
from dynastore.modules.tasks.runners import CapabilityMap


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _NoReqTask(TaskProtocol):
    task_type = "test_no_req"

    async def run(self, payload):
        return {}


@requires(object)  # use `object` as a stand-in protocol type
class _RequiresObjTask(TaskProtocol):
    task_type = "test_requires_obj"

    async def run(self, payload):
        return {}


# ---------------------------------------------------------------------------
# are_protocols_satisfied()
# ---------------------------------------------------------------------------


def test_empty_required_protocols_always_satisfied():
    task = _NoReqTask()
    assert task.required_protocols == ()
    assert task.are_protocols_satisfied() is True


def test_satisfied_when_protocol_available():
    task = _RequiresObjTask()
    mock_provider = MagicMock()
    with patch("dynastore.tools.discovery.get_all_protocols", return_value=[mock_provider]):
        assert task.are_protocols_satisfied() is True


def test_unsatisfied_when_protocol_missing():
    task = _RequiresObjTask()
    with patch("dynastore.tools.discovery.get_all_protocols", return_value=[]):
        assert task.are_protocols_satisfied() is False


def test_requires_decorator_sets_attribute():
    assert _RequiresObjTask.required_protocols == (object,)


def test_requires_decorator_does_not_affect_other_tasks():
    assert _NoReqTask.required_protocols == ()


# ---------------------------------------------------------------------------
# CapabilityMap.refresh() — protocol gate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capability_map_excludes_task_with_missing_protocol():
    """Task with unmet required_protocols must NOT appear in the capability map."""
    mock_instance = MagicMock()
    mock_instance.required_protocols = (object,)
    mock_instance.are_protocols_satisfied.return_value = False

    cap = CapabilityMap()

    with (
        patch("dynastore.tasks.get_loaded_task_types", return_value=["test_requires_obj"]),
        patch("dynastore.tasks.get_task_instance", return_value=mock_instance),
        patch("dynastore.tools.discovery.get_all_protocols", return_value=[]),
        patch("dynastore.modules.tasks.runners.get_runners", return_value=[]),
    ):
        await cap.refresh()

    assert "test_requires_obj" not in cap.async_types
    assert "test_requires_obj" not in cap.sync_types


@pytest.mark.asyncio
async def test_capability_map_includes_task_with_met_protocol():
    """Task with satisfied required_protocols must appear in the capability map."""
    mock_instance = MagicMock()
    mock_instance.required_protocols = (object,)
    mock_instance.are_protocols_satisfied.return_value = True

    mock_runner = MagicMock()
    mock_runner.can_handle.return_value = True

    cap = CapabilityMap()

    with (
        patch("dynastore.tasks.get_loaded_task_types", return_value=["test_requires_obj"]),
        patch("dynastore.tasks.get_task_instance", return_value=mock_instance),
        patch("dynastore.modules.tasks.runners.get_runners", return_value=[mock_runner]),
    ):
        await cap.refresh()

    assert "test_requires_obj" in cap.async_types


@pytest.mark.asyncio
async def test_capability_map_includes_task_with_no_requirements():
    """Task with no required_protocols is always included if a runner handles it."""
    mock_instance = MagicMock()
    mock_instance.required_protocols = ()
    mock_instance.are_protocols_satisfied.return_value = True

    mock_runner = MagicMock()
    mock_runner.can_handle.return_value = True

    cap = CapabilityMap()

    with (
        patch("dynastore.tasks.get_loaded_task_types", return_value=["test_no_req"]),
        patch("dynastore.tasks.get_task_instance", return_value=mock_instance),
        patch("dynastore.modules.tasks.runners.get_runners", return_value=[mock_runner]),
    ):
        await cap.refresh()

    assert "test_no_req" in cap.async_types


@pytest.mark.asyncio
async def test_capability_map_tolerates_definition_only_placeholder():
    """Regression: DefinitionOnlyTask-shaped placeholders must not crash refresh().

    The placeholder is registered when a task's definition module is importable but its
    heavy execution deps are missing. It is intentionally not a TaskProtocol subclass
    (it is cast to one at registration). Before the fix, refresh() called
    instance.are_protocols_satisfied() on the placeholder and crashed with
    AttributeError, killing the dispatcher at startup so no service claimed any task
    (observed on dynastore-catalog rev 00132-qsp, 2026-04-22 — carlo_catalog task
    stayed PENDING, GCS bucket never provisioned).

    This test mirrors the placeholder shape from
    ``dynastore/tasks/__init__.py::_register_missing_task_definitions`` and asserts
    the refresh() contract without having to exercise the entry-points machinery.
    """
    class _Placeholder:
        """Shape-matches the real DefinitionOnlyTask."""
        _task_type = "test_placeholder"
        is_placeholder = True
        required_protocols: tuple = ()

        def are_protocols_satisfied(self) -> bool:
            return True

    instance = _Placeholder()
    assert instance.are_protocols_satisfied() is True
    assert instance.required_protocols == ()

    cap = CapabilityMap()
    with (
        patch("dynastore.tasks.get_loaded_task_types", return_value=["test_placeholder"]),
        patch("dynastore.tasks.get_task_instance", return_value=instance),
        # No runner handles the placeholder → it should be silently excluded,
        # not crash the refresh.
        patch("dynastore.modules.tasks.runners.get_runners", return_value=[]),
    ):
        await cap.refresh()  # must not raise

    assert "test_placeholder" not in cap.async_types
    assert "test_placeholder" not in cap.sync_types
