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

from dynastore.modules.processes.models import (
    JobControlOptions,
    ProcessScope,
    TransmissionMode,
)
from dynastore.tools.process_factory import create_process_definition

from .models import RequeueDeadLetterTasksRequest

REQUEUE_DEAD_LETTER_PROCESS_DEFINITION = create_process_definition(
    id="requeue_dead_letter_tasks",
    title="Requeue DEAD_LETTER tasks",
    description=(
        "Bulk-requeue DEAD_LETTER task rows back to PENDING after an "
        "operator fixes the underlying cause (e.g. a SCOPE drift that "
        "the reactive reaper #502 turned into DLQ rows). When invoked on "
        "a catalog- or collection-scoped endpoint, the replay is "
        "restricted to rows whose JSONB inputs match the path scope; "
        "the platform-scoped form replays every matching DEAD_LETTER "
        "row regardless of catalog. Returns the count of rows "
        "transitioned back to PENDING."
    ),
    version="1.0.0",
    input_model=RequeueDeadLetterTasksRequest,
    scopes=[
        ProcessScope.COLLECTION,
        ProcessScope.CATALOG,
        ProcessScope.PLATFORM,
    ],
    job_control_options=[
        JobControlOptions.SYNC_EXECUTE,
        JobControlOptions.ASYNC_EXECUTE,
    ],
    output_transmission=[TransmissionMode.VALUE],
)
