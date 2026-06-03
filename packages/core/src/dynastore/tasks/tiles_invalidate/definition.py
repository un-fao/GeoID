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
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

from dynastore.tools.process_factory import create_process_definition
from dynastore.modules.processes.models import JobControlOptions, ProcessScope, TransmissionMode
from .models import TileInvalidateRequest

TILES_INVALIDATE_PROCESS_DEFINITION = create_process_definition(
    id="tiles_invalidate",
    title="Tile Cache Invalidation",
    description=(
        "Light write-reactive tile-cache invalidation. Deletes cached tiles "
        "covering the supplied bounding boxes across the served TMS ids and "
        "zoom range — no MVT render, no save. Runs in-process on the catalog "
        "service (background runner) so spinning up a Cloud Run Job is avoided "
        "for this fast delete-only path."
    ),
    version="1.0.0",
    input_model=TileInvalidateRequest,
    scopes=[ProcessScope.COLLECTION],
    job_control_options=[JobControlOptions.ASYNC_EXECUTE],
    output_transmission=[TransmissionMode.REFERENCE],
)
