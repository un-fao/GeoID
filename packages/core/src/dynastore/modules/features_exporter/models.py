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

"""Shared request model for the feature-exporter service."""

from typing import List, Optional

from pydantic import BaseModel, Field

from dynastore.models.shared_models import OutputFormatEnum


class ExportFeaturesRequest(BaseModel):
    """Request model for exporting features from a collection.

    Consumed by every execution tier:
      - sync REST (small payloads, immediate response)
      - async background task (medium; < ~60s)
      - Cloud Run Job (long-running; production only)
    """

    catalog: str = Field(..., description="Catalog ID")
    collection: str = Field(..., description="Collection ID")
    output_format: OutputFormatEnum = Field(..., description="Output file format")
    # NOTE: the output location is NOT a client input. Per OGC API - Processes
    # the server owns result storage; the task derives a per-job key in the
    # catalog's own bucket and returns the artifact as a signed URL. The
    # destination is passed to ``export_features`` by the caller, not carried
    # on this request.
    encoding: str = Field(default="utf-8", description="Character encoding for output")
    cql_filter: Optional[str] = Field(None, description="CQL2/ECQL filter expression")
    property_names: Optional[List[str]] = Field(
        None, description="List of properties to include (None = all)"
    )
    limit: Optional[int] = Field(
        None, description="Maximum number of features to export"
    )
    offset: Optional[int] = Field(None, description="Number of features to skip")
    target_srid: Optional[int] = Field(
        None, description="Target SRID for geometry transformation"
    )
    reporting: Optional[dict] = Field(None, description="Reporter configuration")
