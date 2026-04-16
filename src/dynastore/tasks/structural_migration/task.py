#    Copyright 2025 FAO
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

"""
Structural Migration Task.

Wraps ``migration_runner.run_migrations()`` as a durable task so it can be
triggered from the admin API, the task queue, or a Cloud Run Job.

Task type: ``structural_migration``

Payload::

    {
        "scope": "all" | "global" | "tenant",
        "dry_run": false
    }
"""

import logging
from typing import Any, Dict, Literal

from pydantic import BaseModel

from dynastore.modules.tasks.models import TaskPayload
from dynastore.tasks.protocols import TaskProtocol

logger = logging.getLogger(__name__)


class StructuralMigrationInputs(BaseModel):
    scope: Literal["all", "global", "tenant"] = "all"
    dry_run: bool = False


class StructuralMigrationTask(TaskProtocol):
    """
    Applies pending database migrations (global and/or tenant).

    Idempotent: re-running when up-to-date is a no-op.
    Reports progress via the task system.
    """

    priority: int = 5  # High priority — should run before data tasks
    task_type = "structural_migration"

    async def run(
        self, payload: TaskPayload[StructuralMigrationInputs]
    ) -> Dict[str, Any]:
        from dynastore.modules.db_config.migration_runner import run_migrations
        from dynastore.tools.protocol_helpers import get_engine

        inputs = payload.inputs
        scope = inputs.scope
        dry_run = inputs.dry_run

        engine = get_engine()
        if not engine:
            raise RuntimeError(
                "StructuralMigrationTask: No database engine available."
            )

        logger.info(
            f"StructuralMigrationTask: Running migrations "
            f"(scope={scope}, dry_run={dry_run})..."
        )

        result = await run_migrations(
            engine, dry_run=dry_run, scope=scope
        )

        action = "previewed" if dry_run else "applied"
        global_count = len(result.get("global", []))
        tenant_count = sum(
            len(v) for v in result.get("tenant", {}).values()
        )

        logger.info(
            f"StructuralMigrationTask: {action} {global_count} global + "
            f"{tenant_count} tenant migration(s)."
        )

        return {
            "action": action,
            "scope": scope,
            "global_count": global_count,
            "tenant_count": tenant_count,
            **result,
        }
