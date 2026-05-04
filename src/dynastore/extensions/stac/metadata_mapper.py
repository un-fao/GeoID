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

import logging
from typing import Any, Dict
import pystac
from pystac.extensions.raster import RasterExtension, RasterBand, DataType
from pystac.extensions.eo import EOExtension, Band as EOBand

logger = logging.getLogger(__name__)

# --- Type Mappings (from user algorithm) ---

# Mapping GDAL Raster Data Types to STAC Raster Extension Data Types
GDAL_TO_STAC_DTYPE: dict[str, DataType] = {
    "Byte": DataType.UINT8,
    "UInt16": DataType.UINT16,
    "Int16": DataType.INT16,
    "UInt32": DataType.UINT32,
    "Int32": DataType.INT32,
    "Float32": DataType.FLOAT32,
    "Float64": DataType.FLOAT64,
    "CInt16": DataType.CINT16,
    "CInt32": DataType.CINT32,
    "CFloat32": DataType.CFLOAT32,
    "CFloat64": DataType.CFLOAT64,
}

# Mapping OGR Field Types to STAC Table Extension Types
OGR_TO_STAC_TABLE_TYPE = {
    "String": "string",
    "Integer": "integer",
    "Integer64": "integer",
    "Real": "number",
    "Date": "date",
    "Time": "time",
    "DateTime": "datetime",
    "Binary": "binary",
    "StringList": "array",
    "IntegerList": "array",
    "RealList": "array"
}

def enrich_collection_from_metadata(collection: pystac.Collection, metadata: Dict[str, Any]):
    """
    Enriches a STAC Collection with information from asset metadata, 
    specifically looking for GDAL/OGR information (gdalinfo).
    """
    try:
        if not metadata:
            return

        # 1. Process custom fields with 'asset:' prefix
        for key, value in metadata.items():
            if key == "gdalinfo" or key.startswith("stac"):
                continue
            # Use 'asset:' prefix for custom fields
            collection.extra_fields[f"asset:{key}"] = value

        # 2. Process GDAL/OGR info
        gdal_info = metadata.get("gdalinfo")
        if not gdal_info:
            return

        # Detect if it's Vector or Raster
        if "layers" in gdal_info:
            _enrich_from_vector(collection, gdal_info)
        elif "size" in gdal_info or "bands" in gdal_info:
            _enrich_from_raster(collection, gdal_info)
            
    except Exception as e:
        logger.exception(f"Error enriching STAC Collection from metadata: {e}")
        # We don't want to crash the whole service if metadata enrichment fails.
        raise

def _add_extension(collection: pystac.Collection, extension_url: str):
    """Safely adds an extension URL to the collection's stac_extensions list."""
    if extension_url not in collection.stac_extensions:
        collection.stac_extensions.append(extension_url)

def _enrich_from_vector(collection: pystac.Collection, gdal_info: Dict[str, Any]):
    """Enriches collection from Vector (OGR) metadata."""
    layers = gdal_info.get("layers", [])
    if not layers:
        return
    
    # Use the first layer for primary metadata
    layer = layers[0]
    
    # Projection Extension (using extra_fields as .ext() doesn't support Collection)
    _add_extension(collection, "https://stac-extensions.github.io/projection/v1.1.0/schema.json")
    if "coordinateSystem" in layer and "wkt" in layer["coordinateSystem"]:
        collection.extra_fields["proj:wkt2"] = layer["coordinateSystem"]["wkt"]
    
    # Vector Extension (standard prefix is 'vector:')
    collection.extra_fields["vector:geometry_type"] = layer.get("geometryType")
    collection.extra_fields["vector:count"] = layer.get("featureCount")
    
    # Table Extension (schema)
    if "fields" in layer:
        _add_extension(collection, "https://stac-extensions.github.io/table/v1.2.0/schema.json")
        columns = []
        for f in layer["fields"]:
            columns.append({
                "name": f["name"],
                "type": OGR_TO_STAC_TABLE_TYPE.get(f["type"], "string")
            })
        collection.extra_fields["table:columns"] = columns

    # Update extent if available
    if "extent" in layer:
        collection.extent.spatial = pystac.SpatialExtent([layer["extent"]])

def _enrich_from_raster(collection: pystac.Collection, gdal_info: Dict[str, Any]):
    """Enriches collection from Raster (GDAL) metadata."""
    
    # Projection Extension
    _add_extension(collection, "https://stac-extensions.github.io/projection/v1.1.0/schema.json")
    if "coordinateSystem" in gdal_info and "wkt" in gdal_info["coordinateSystem"]:
        collection.extra_fields["proj:wkt2"] = gdal_info["coordinateSystem"]["wkt"]
    
    if "size" in gdal_info:
        size = gdal_info["size"]
        if isinstance(size, list) and len(size) == 2:
            collection.extra_fields["proj:shape"] = [size[1], size[0]]

    if "geoTransform" in gdal_info:
        collection.extra_fields["proj:transform"] = gdal_info["geoTransform"]

    # Raster & EO Extensions (Summaries are standard for Collections)
    # Note: For virtual collections representing a single asset, 
    # we use summaries to describe the bands.
    raster_summaries = RasterExtension.summaries(collection, add_if_missing=True)
    eo_summaries = EOExtension.summaries(collection, add_if_missing=True)
    
    bands = gdal_info.get("bands", [])
    stac_bands = []
    eo_bands = []
    
    for b in bands:
        # Raster Band
        raw_dt = b.get("type")
        stac_dt = GDAL_TO_STAC_DTYPE.get(raw_dt)
        
        rb = RasterBand.create(
            nodata=b.get("noDataValue"),
            data_type=stac_dt
        )
        # Note: description is not in RasterBand.create but we can add it to extra_fields if needed.
        stac_bands.append(rb)
        
        # EO Band
        eb = EOBand.create(
            name=f"band_{b.get('band')}",
            description=b.get("description")
        )
        eo_bands.append(eb)
        
    if stac_bands:
        raster_summaries.bands = stac_bands
    if eo_bands:
        eo_summaries.bands = eo_bands

    # Update extent if WGS84 extent is available
    if "wgs84Extent" in gdal_info and gdal_info["wgs84Extent"]:
        coords = gdal_info["wgs84Extent"].get("coordinates")
        if coords and len(coords) > 0:
            ring = coords[0]
            lons = [c[0] for c in ring]
            lats = [c[1] for c in ring]
            bbox = [min(lons), min(lats), max(lons), max(lats)]
            collection.extent.spatial = pystac.SpatialExtent([bbox])
    elif "cornerCoordinates" in gdal_info:
        corners = gdal_info["cornerCoordinates"]
        ul = corners.get("upperLeft")
        lr = corners.get("lowerRight")
        if ul and lr:
            bbox = [min(ul[0], lr[0]), min(ul[1], lr[1]), max(ul[0], lr[0]), max(ul[1], lr[1])]
            collection.extent.spatial = pystac.SpatialExtent([bbox])