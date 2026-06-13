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

"""OGC WFS 2.0.2 service extension — RETAINED production plugin.

This is the XML-based OGC Web Feature Service 2.0.2 implementation, kept
deliberately alongside the newer OGC API - Features. It is a live extension:
``WFSService`` is registered via the ``dynastore.extensions`` entry point
(see ``pyproject.toml``) and is exercised by the maps installation.

Do NOT auto-flag this package for removal in cleanup sweeps — the audit in
geoid#1511 concluded KEEP. The shared ``FeatureProperties`` / ``FeatureStreamConfig``
helpers (geoid#1504 D-9) also have core consumers (the DWH-join export task and
the features exporter) and must not be dropped as "WFS-only".

Importing ``wfs_config`` here triggers its service-exposure plugin registration.
"""

from . import wfs_config  # noqa: F401  -- service-exposure plugin registration
