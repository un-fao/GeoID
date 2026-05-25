"""``task_to_status_info`` HATEOAS link handling.

Regression: a freshly created async job has an empty ``task.links``. The
StatusInfo builder used to read ``links or task.links.copy() if task.links
else []`` which — because the conditional bound looser than ``or`` — collapsed
to ``[]`` whenever ``task.links`` was empty, discarding the caller-supplied
links and leaving an ``href=""`` ``self`` placeholder. That empty href is the
"status link is empty" symptom seen on job start.
"""
from __future__ import annotations

from dynastore.models.shared_models import Link
from dynastore.models.tasks import Task, TaskStatusEnum
from dynastore.modules.processes.models import task_to_status_info


def _self_links(info):
    return [link for link in info.links if link.rel == "self"]


def test_passed_links_preserved_when_task_has_none():
    """The canonical case: caller computes the status URL, task.links is empty."""
    task = Task(task_type="ingest", status=TaskStatusEnum.PENDING)
    assert task.links == []  # fresh async job
    status_url = "https://data.example.org/processes/jobs/abc-123"

    info = task_to_status_info(
        task,
        links=[Link(href=status_url, rel="self", type="application/json")],
    )

    selfs = _self_links(info)
    assert len(selfs) == 1
    assert selfs[0].href == status_url  # NOT "" — the bug


def test_self_backfilled_only_when_no_links_supplied():
    """With neither caller links nor task links, the placeholder self link is
    still added (so the field is never absent) — but it is the only one."""
    task = Task(task_type="ingest", status=TaskStatusEnum.PENDING)

    info = task_to_status_info(task, links=None)

    selfs = _self_links(info)
    assert len(selfs) == 1
    assert selfs[0].href == ""  # backfill placeholder, unchanged behaviour


def test_task_links_used_when_no_caller_links():
    """If the task already carries links and the caller passes none, keep the
    task's links rather than discarding them for a placeholder."""
    existing = Link(href="https://data.example.org/jobs/xyz", rel="self",
                    type="application/json")
    task = Task(task_type="ingest", status=TaskStatusEnum.RUNNING, links=[existing])

    info = task_to_status_info(task, links=None)

    selfs = _self_links(info)
    assert len(selfs) == 1
    assert selfs[0].href == "https://data.example.org/jobs/xyz"
