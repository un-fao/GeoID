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

"""Unit tests for the typed task result envelope (#1807 P2).

No live Postgres required — all tests are pure-Python / Pydantic.
"""

from dynastore.models.tasks import TaskStatusEnum
from dynastore.tasks.report import (
    OperationError,
    OperationResult,
    TaskReport,
    normalize_task_result,
)


# ---------------------------------------------------------------------------
# OperationError
# ---------------------------------------------------------------------------


class TestOperationError:
    def test_defaults(self):
        err = OperationError(code="e", message="m")
        assert err.retryable is False
        assert err.details == {}

    def test_retryable(self):
        err = OperationError(code="transient", message="try again", retryable=True)
        assert err.retryable is True

    def test_details_preserved(self):
        err = OperationError(code="c", message="m", details={"k": "v"})
        assert err.details == {"k": "v"}


# ---------------------------------------------------------------------------
# OperationResult
# ---------------------------------------------------------------------------


class TestOperationResult:
    def test_passed(self):
        r = OperationResult(op_id="abc", status="passed")
        assert r.status == "passed"
        assert r.error is None

    def test_retried_with_error(self):
        r = OperationResult(
            op_id="x",
            status="retried",
            error=OperationError(code="timeout", message="timed out", retryable=True),
        )
        assert r.status == "retried"
        assert r.error is not None
        assert r.error.retryable is True

    def test_poison(self):
        r = OperationResult(status="poison", entity_id="item-42")
        assert r.entity_id == "item-42"

    def test_custom_status_string(self):
        # The open str fallback allows extension without breaking the type.
        r = OperationResult(status="no_op")
        assert r.status == "no_op"


# ---------------------------------------------------------------------------
# TaskReport.to_outputs — OGC message contract
# ---------------------------------------------------------------------------


class TestTaskReportToOutputs:
    def test_message_is_top_level_key(self):
        """The OGC ``outputs["message"]`` contract must be preserved."""
        report = TaskReport.completed(message="done!")
        out = report.to_outputs()
        assert out["message"] == "done!"

    def test_message_none_absent_from_outputs(self):
        report = TaskReport.completed(message=None)
        out = report.to_outputs()
        assert "message" not in out

    def test_outputs_dict_merged_flat(self):
        report = TaskReport.completed(
            message="ok",
            outputs={"url": "https://example.org/result.csv"},
        )
        out = report.to_outputs()
        assert out["url"] == "https://example.org/result.csv"
        assert out["message"] == "ok"

    def test_message_overwrites_outputs_conflict(self):
        # ``message`` in ``outputs`` is overwritten by ``self.message``.
        report = TaskReport.completed(
            message="correct",
            outputs={"message": "stale"},
        )
        out = report.to_outputs()
        assert out["message"] == "correct"

    def test_metrics_included_when_nonempty(self):
        report = TaskReport.completed(metrics={"drained": 5})
        out = report.to_outputs()
        assert out["metrics"] == {"drained": 5}

    def test_metrics_absent_when_empty(self):
        report = TaskReport.completed()
        out = report.to_outputs()
        assert "metrics" not in out

    def test_operations_included_when_nonempty(self):
        ops = [OperationResult(op_id="1", status="passed")]
        report = TaskReport.completed(operations=ops)
        out = report.to_outputs()
        assert "operations" in out
        assert len(out["operations"]) == 1
        assert out["operations"][0]["status"] == "passed"

    def test_operations_absent_when_empty(self):
        report = TaskReport.completed()
        out = report.to_outputs()
        assert "operations" not in out

    def test_correlation_included_when_nonempty(self):
        report = TaskReport.completed(correlation={"task_id": "t1"})
        out = report.to_outputs()
        assert out["correlation"] == {"task_id": "t1"}

    def test_correlation_absent_when_empty(self):
        report = TaskReport.completed()
        out = report.to_outputs()
        assert "correlation" not in out

    def test_error_included_in_failed_report(self):
        report = TaskReport.failed(code="oops", message="something went wrong")
        out = report.to_outputs()
        assert out["error"]["code"] == "oops"
        assert out["message"] == "something went wrong"

    def test_result_is_plain_dict(self):
        report = TaskReport.completed(message="x")
        out = report.to_outputs()
        assert isinstance(out, dict)


# ---------------------------------------------------------------------------
# TaskReport.error_message_str
# ---------------------------------------------------------------------------


class TestErrorMessageStr:
    def test_returns_none_when_no_error(self):
        report = TaskReport.completed(message="ok")
        assert report.error_message_str() is None

    def test_returns_error_message(self):
        report = TaskReport.failed(code="e", message="boom")
        assert report.error_message_str() == "boom"


# ---------------------------------------------------------------------------
# TaskReport.log_details
# ---------------------------------------------------------------------------


class TestLogDetails:
    def test_status_always_present(self):
        report = TaskReport.completed(message="done")
        details = report.log_details()
        assert details["status"] == TaskStatusEnum.COMPLETED.value

    def test_error_details_included(self):
        report = TaskReport.failed(code="bad", message="err")
        details = report.log_details()
        assert details["error"]["code"] == "bad"
        assert details["error"]["retryable"] is False

    def test_metrics_included(self):
        report = TaskReport.completed(metrics={"drained": 3})
        details = report.log_details()
        assert details["metrics"] == {"drained": 3}

    def test_metrics_absent_when_empty(self):
        report = TaskReport.completed()
        details = report.log_details()
        assert "metrics" not in details

    def test_correlation_included(self):
        report = TaskReport.completed(correlation={"owner_id": "x"})
        details = report.log_details()
        assert details["correlation"] == {"owner_id": "x"}


# ---------------------------------------------------------------------------
# TaskReport factory helpers
# ---------------------------------------------------------------------------


class TestFactories:
    def test_completed_factory_status(self):
        r = TaskReport.completed(message="ok")
        assert r.status == TaskStatusEnum.COMPLETED

    def test_failed_factory_status(self):
        r = TaskReport.failed(code="c", message="m")
        assert r.status == TaskStatusEnum.FAILED
        assert r.error is not None
        assert r.error.code == "c"

    def test_failed_retryable(self):
        r = TaskReport.failed(code="t", message="try", retryable=True)
        assert r.error is not None
        assert r.error.retryable is True


# ---------------------------------------------------------------------------
# normalize_task_result — verbatim passthrough contract
# ---------------------------------------------------------------------------


class TestNormalizeTaskResult:
    def test_none_passthrough(self):
        out, err = normalize_task_result(None)
        assert out is None
        assert err is None

    def test_dict_passthrough(self):
        d = {"message": "hello", "data": [1, 2]}
        out, err = normalize_task_result(d)
        assert out is d  # same object, no copy
        assert err is None

    def test_statusinfo_like_passthrough(self):
        """Any non-TaskReport value must come through verbatim."""

        class FakeStatusInfo:
            pass

        obj = FakeStatusInfo()
        out, err = normalize_task_result(obj)
        assert out is obj
        assert err is None

    def test_int_passthrough(self):
        out, err = normalize_task_result(42)
        assert out == 42
        assert err is None

    def test_task_report_completed_converts(self):
        report = TaskReport.completed(message="done", metrics={"n": 7})
        out, err = normalize_task_result(report)
        assert isinstance(out, dict)
        assert out["message"] == "done"
        assert out["metrics"] == {"n": 7}
        assert err is None

    def test_task_report_failed_converts(self):
        report = TaskReport.failed(code="c", message="boom")
        out, err = normalize_task_result(report)
        assert isinstance(out, dict)
        assert err == "boom"

    def test_task_report_outputs_message_preserved(self):
        """The ``message`` key in to_outputs() must survive normalize_task_result."""
        report = TaskReport.completed(message="job result URL: https://example.org/x")
        out, _ = normalize_task_result(report)
        assert out["message"] == "job result URL: https://example.org/x"

    def test_task_report_no_message_no_key(self):
        """When message is None, the key must be absent (not set to None)."""
        report = TaskReport.completed(message=None)
        out, _ = normalize_task_result(report)
        assert "message" not in out


# ---------------------------------------------------------------------------
# Round-trip: drain report shape
# ---------------------------------------------------------------------------


class TestDrainReportShape:
    """Verify the shape a storage/event drain returns without hitting PG."""

    def _make_drain_report(self, total: int, owner_id: str) -> TaskReport:
        return TaskReport.completed(
            message=f"storage drain completed: {total} row(s) processed",
            metrics={"drained": total},
            correlation={"owner_id": owner_id},
        )

    def test_drain_report_message_key(self):
        r = self._make_drain_report(10, "owner-abc")
        out = r.to_outputs()
        assert "drained" in out["metrics"]
        assert out["metrics"]["drained"] == 10
        assert "10 row(s)" in out["message"]

    def test_drain_report_correlation(self):
        r = self._make_drain_report(0, "my-owner")
        out = r.to_outputs()
        assert out["correlation"]["owner_id"] == "my-owner"

    def test_drain_report_zero_drained(self):
        r = self._make_drain_report(0, "o")
        out, err = normalize_task_result(r)
        assert out["metrics"]["drained"] == 0
        assert err is None
