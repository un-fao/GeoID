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

# dynastore/extensions/tools/formatters.py

import logging
import re
from enum import Enum
from typing import Iterable, Dict, Any, Optional

# File: src/dynastore/extensions/formatters.py

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Literal

from dynastore.modules.catalog.models import (
    Collection as CoreCollection,
    Catalog as CoreCatalog,
)
from dynastore.models.shared_models import Link
from fastapi import Request
from fastapi.responses import StreamingResponse
# Use the real file writers instead of placeholders
from dynastore.tools.file_io import write_csv, write_geopackage, write_parquet, write_shapefile, write_geojson


logger = logging.getLogger(__name__)

from dynastore.models.shared_models import OutputFormatEnum

logger = logging.getLogger(__name__)

# class OutputFormatEnum(str, Enum): ... REMOVED

def _parse_srid_from_srs_name(srs_name: Optional[str]) -> Optional[int]:
    """
    Parses an SRID from a URN-style srsName (e.g., 'urn:ogc:def:crs:EPSG::4326').
    """
    if not srs_name:
        return None
    match = re.search(r'::(\d+)$', srs_name)
    return int(match.group(1)) if match else None

# --- Format Dispatcher ---

format_map = {
    OutputFormatEnum.CSV: {
        "writer": write_csv,
        "media_type": "text/csv",
        "extension": "csv"
    },
    OutputFormatEnum.GEOPACKAGE: {
        "writer": write_geopackage,
        "media_type": "application/geopackage+sqlite3",
        "extension": "gpkg"
    },
    OutputFormatEnum.SHAPEFILE: {
        "writer": write_shapefile,
        "media_type": "application/zip",
        "extension": "zip"
    },
    OutputFormatEnum.PARQUET: {
        "writer": write_parquet,
        "media_type": "application/octet-stream",
        "extension": "parquet"
    },
    OutputFormatEnum.GEOJSON: {
        "writer": write_geojson,
        "media_type": "application/geo+json",
        "extension": "geojson"
    },
    OutputFormatEnum.JSON: {
        "writer": write_geojson,
        "media_type": "application/json",
        "extension": "json"
    }
}

def format_response(
    features: Iterable[Any],
    output_format: OutputFormatEnum,
    request: Optional[Request] = None,
    collection_id: str = "layer",
    target_srid: int = 4326,
    encoding: str = "utf-8"
) -> StreamingResponse:
    """
    Dynamically formats a stream of features into the specified output format.
    """
    formatter = format_map.get(output_format)
    if not formatter:
        raise ValueError(f"Unsupported format: {output_format}")

    # The `features` iterable can yield Pydantic models or dicts.
    # If they are Pydantic models, we dump them to dicts for the writers.
    # Otherwise, we assume they are already dicts.
    feature_dicts = (f.model_dump(by_alias=True) if hasattr(f, 'model_dump') else f for f in features)

    # The writers from file_io expect an iterator and the target SRID.
    # They now also support an optional 'encoding' parameter.
    content_stream = formatter["writer"](feature_dicts, target_srid, encoding=encoding)
    
    headers = {
        "Content-Disposition": f'attachment; filename="{collection_id}.{formatter["extension"]}"'
    }

    return StreamingResponse(content=content_stream, media_type=formatter["media_type"], headers=headers)
