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

"""OGC Process definition for the geovolumes_tileset task."""

from dynastore.tools.process_factory import create_process_definition
from dynastore.modules.processes.models import (
    JobControlOptions,
    ProcessScope,
    TransmissionMode,
)
from .models import GeoVolumesTilesetRequest

GEOVOLUMES_TILESET_PROCESS_DEFINITION = create_process_definition(
    id="geovolumes_tileset",
    title="3D Tiles 1.1 Generation",
    description=(
        "Generates a 3D Tiles 1.1 tileset from CityJSON features stored in a "
        "collection, saves GLB tiles and tileset.json to object storage, and "
        "registers the tileset as a collection asset."
    ),
    version="1.0.0",
    input_model=GeoVolumesTilesetRequest,
    scopes=[ProcessScope.COLLECTION],
    job_control_options=[JobControlOptions.ASYNC_EXECUTE],
    output_transmission=[TransmissionMode.REFERENCE],
)
