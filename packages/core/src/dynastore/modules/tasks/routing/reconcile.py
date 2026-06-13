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

"""Fail-open reconciliation of the task catalogue against live runners (#1675).

The routing config says *where* each task/process should run; this module
checks whether the runner it is routed to is actually able to claim it on this
service, and emits a WARNING for every gap.  It is the consumer side of the
"a runner can declare what it can run" loop: a task routed to ``gcp_cloud_run``
whose Cloud Run Job is not deployed, or to ``background`` on a service that did
not register a background runner, would sit unclaimable — the exact silent
starvation behind #1647.

Pure read, never raises (a degraded registry must not block startup or the
discovery endpoint).  Returns the gap list so ``/configs/tasks/capabilities``
can surface the same information it logs.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)


def _declared_keys(runner: Any) -> Set[str]:
    """task_key set a runner explicitly declares; empty set = declares nothing.

    ``declared_tasks()`` returns a list of dicts (``task_key`` + ``runner_type``
    + optional ``job``).  A runner that declares a concrete manifest (e.g.
    ``GcpJobRunner`` enumerating deployed Cloud Run Jobs) covers ONLY those
    keys; a runner that declares nothing (most in-process runners) covers by
    ``can_handle``.
    """
    try:
        declared = runner.declared_tasks() or []
    except Exception:  # noqa: BLE001 — declaration is best-effort
        return set()
    keys: Set[str] = set()
    for d in declared:
        if isinstance(d, dict) and d.get("task_key"):
            keys.add(d["task_key"])
    return keys


def _covered_here(task_key: str, runners: List[Any]) -> bool:
    """True if some registered runner on this process can actually claim ``task_key``.

    A runner covers the key when it ``can_handle`` it AND either declares
    nothing (handles by capability) or lists the key in its declared manifest.
    The manifest check is what catches "routed to gcp_cloud_run but the Job is
    not deployed": ``GcpJobRunner.declared_tasks()`` would not list it.
    """
    for r in runners:
        try:
            if not r.can_handle(task_key):
                continue
        except Exception:  # noqa: BLE001
            continue
        declared = _declared_keys(r)
        if not declared or task_key in declared:
            return True
    return False


async def reconcile_routing_capabilities() -> Dict[str, Any]:
    """Return ``{service, starving: [...]}`` and WARN on every starvation gap.

    A catalogued task/process is "starving" on this service when it is routed
    here (its target consumers include this service, or impose no constraint)
    yet no registered runner here can claim it.  Each gap is logged at WARNING
    with the routed runner(s) so an operator can see immediately whether the
    fix is a routing edit or a missing deployment.
    """
    from dynastore.modules.tasks.dispatcher import _SERVICE_NAME
    from dynastore.modules.tasks.models import TaskExecutionMode
    from dynastore.modules.tasks.routing import resolver as routing_resolver
    from dynastore.modules.tasks.runners import get_runners

    service = _SERVICE_NAME or ""
    runners = list(get_runners(TaskExecutionMode.ASYNCHRONOUS)) + list(
        get_runners(TaskExecutionMode.SYNCHRONOUS)
    )

    try:
        from dynastore.tasks import describe_all

        catalogue = describe_all()
    except Exception:  # noqa: BLE001 — registry read is best-effort
        logger.warning("routing reconcile: describe_all() failed — skipping", exc_info=True)
        catalogue = []

    starving: List[Dict[str, Any]] = []
    for item in catalogue:
        key = item.get("task_key")
        if not key:
            continue
        try:
            targets = await routing_resolver.resolved_targets(key)
        except Exception:  # noqa: BLE001
            targets = []

        # Routed here? An empty/absent consumer list means "any capable service",
        # which includes this one; a concrete list must contain this service.
        if targets:
            routed_here = any(
                (not t.consumers) or (service and service in t.consumers)
                for t in targets
            )
        else:
            routed_here = True  # no routing opinion → any capable service
        if not routed_here:
            continue

        if _covered_here(key, runners):
            continue

        routed_runners = sorted({t.runner for t in targets}) or ["<unrouted>"]
        starving.append(
            {"task_key": key, "kind": item.get("kind"), "routed_runners": routed_runners}
        )
        logger.warning(
            "routing reconcile: %s %r is routed to runner(s) %s on service %r but no "
            "registered runner here can claim it — it will STARVE. Check "
            "TaskRoutingConfig and that the runner / Cloud Run Job is deployed (#1647/#1675).",
            item.get("kind", "task"), key, routed_runners, service or "<unset>",
        )

    return {"service": service, "starving": starving}
