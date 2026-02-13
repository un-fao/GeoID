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

import io
import os
import json
import zipfile
import tempfile
from typing import Generator, Dict, Any, List, Union, Tuple, Optional, Iterable, Iterator, AsyncIterator
from dynastore.models.shared_models import OutputFormatEnum
from dynastore.tools.json import CustomJSONEncoder
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
import uuid
import fiona
from shapely import wkb
from shapely.geometry import mapping # To convert shapely geom to GeoJSON-like dict

from dynastore.tools.features import Feature
import logging
logger = logging.getLogger(__name__)

def _ensure_temp_dir() -> str:
    """
    Ensures the directory for temporary files exists and returns its path.
    It respects the TMPDIR environment variable, which is crucial for Cloud Run volume mounts.
    """
    temp_dir = tempfile.gettempdir()
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

# --- FILE READERS ---

def get_file_reader(file_path_or_buffer: Union[str, io.BytesIO], batch_size: int=1000, encoding: str ='utf-8') -> Generator[Dict[str, Any], None, None]:
    """
    Determines the appropriate file reader based on file extension
    and returns a generator yielding records.
    Handles both local file paths and GCS/HTTPS URLs.
    """
    # This function is now a simple dispatcher. It doesn't need column mapping info.
    file_source = file_path_or_buffer # type: ignore
    ext = None

    if isinstance(file_path_or_buffer, str):
        if file_path_or_buffer.startswith("gs://"):
            # GCP module
            try:
                from dynastore.modules.gcp.tools.bucket import get_gcs_file_as_buffer
                file_source = get_gcs_file_as_buffer(file_path_or_buffer)
                ext = os.path.splitext(file_path_or_buffer)[1].lower()
            except Exception as e:
                message = f"Unable to use gs:// file source:{e}"
                logger.error(message)
                raise ValueError(message)
        elif file_path_or_buffer.startswith("https://") or file_path_or_buffer.startswith("http://"):
            logger.warning("Direct HTTP/HTTPS streaming not fully implemented. Ensure file_path_or_buffer is a local path or GCS for now.")
            ext = os.path.splitext(file_path_or_buffer)[1].lower()
        else:
            ext = os.path.splitext(file_path_or_buffer)[1].lower()
    elif isinstance(file_path_or_buffer, io.BytesIO):
        logger.debug("Received BytesIO buffer. File type inference might be limited.")
        # A more robust inference might be needed here if BytesIO is a primary use case.
        # For now, we assume the caller knows what they are doing or the extension is hinted elsewhere.
        # A common pattern is to check the first few bytes for magic numbers.
        # Let's assume for now the caller will guide this for buffers.
        # A simple heuristic: if it smells like text, it might be CSV.
        try:
            file_path_or_buffer.seek(0)
            is_text = file_path_or_buffer.read(1024).decode('utf-8')
            file_path_or_buffer.seek(0)
            ext = '.csv' # Assume CSV for text-like buffers
        except (UnicodeDecodeError, AttributeError):
            file_path_or_buffer.seek(0)
            ext = '.zip' # Default to binary format like zip/parquet for non-text

    if ext == '.zip':
        return read_shapefile(file_source, encoding=encoding)
    elif ext == '.gpkg':
        return read_geopackage(file_source, layer_name=None, encoding=encoding)
    elif ext == '.parquet':
        return read_parquet(file_source, batch_size, encoding)
    elif ext == '.geojson':
        return read_geopackage(file_source, layer_name=None, encoding=encoding)
    elif ext == '.json':
        # Detect if it's GeoJSON or just JSON.
        # But 'read_geopackage' uses gpd.read_file which handles GeoJSON.
        return read_geopackage(file_source, layer_name=None, encoding=encoding)
    elif ext == '.gml':
        return read_geopackage(file_source, layer_name=None, encoding=encoding)
    elif ext == '.csv':
        # The CSV reader is specialized and needs column info, so it's not called via the generic dispatcher.
        # The caller (`main_ingestion`) will call `read_csv` directly if it's a CSV file.
        raise NotImplementedError("CSV reading requires specific column configuration and should be called directly via read_csv, not the generic get_file_reader.")
    
    raise ValueError(f"Unsupported file format or path: {file_path_or_buffer} (inferred extension: {ext})")

def read_shapefile(file_path_or_buffer: Union[str, io.BytesIO], encoding = 'utf-8') -> Generator[Dict[str, Any], None, None]:
    """Reads a zipped Shapefile and yields records."""
    logger.info(f"Reading Zipped Shapefile from: {file_path_or_buffer}")
    
    # Handle both file paths and in-memory buffers
    if isinstance(file_path_or_buffer, io.BytesIO):
        # If it's a BytesIO, geopandas can read it directly if fiona is properly configured.
        # However, unzipping still requires a temporary location.
        # Using a TemporaryDirectory ensures it writes to the GCS-backed volume via TMPDIR.
        # Fiona can read from a vsizip path, which avoids full extraction to disk.
        # The format is /vsizip/{path_to_zip}/{path_inside_zip}
        # We still need to write the buffer to a temporary file for fiona to access it.
        with tempfile.NamedTemporaryFile(suffix=".zip", dir=_ensure_temp_dir(), delete=True) as tmp_zip:
            file_path_or_buffer.seek(0)
            tmp_zip.write(file_path_or_buffer.read())
            tmp_zip.flush()
            zip_path = tmp_zip.name
            # Find the .shp file inside the zip
            with zipfile.ZipFile(zip_path, 'r') as zf:
                shp_in_zip = next((s for s in zf.namelist() if s.endswith('.shp')), None)
                if not shp_in_zip:
                    raise FileNotFoundError("No .shp file found in the zip archive.")
            # Use fiona's vsizip virtual filesystem to stream records
            with fiona.open(f'/vsizip/{zip_path}/{shp_in_zip}', encoding=encoding) as collection:
                for feature in collection:
                    yield feature
    else: # It's a file path string
        with fiona.open(f'zip://{file_path_or_buffer}', encoding=encoding) as collection:
            for feature in collection:
                yield feature

def read_geopackage(file_path_or_buffer: Union[str, io.BytesIO], layer_name: str = None, encoding: str ='utf-8') -> Generator[Dict[str, Any], None, None]:
    """Reads a GeoPackage file and yields records."""
    logger.info(f"Reading GeoPackage from: {file_path_or_buffer}")
    
    # Geopandas can often read directly from buffer-like objects for GPKG
    gdf = gpd.read_file(file_path_or_buffer, layer=layer_name, encoding=encoding)
    yield from gdf.iterfeatures()

def read_parquet(file_path_or_buffer: Union[str, io.BytesIO], batch_size: int = 1000, encoding: str ='utf-8') -> Generator[Dict[str, Any], None, None]:
    """Reads a Parquet file and yields records."""
    logger.info(f"Reading Parquet from: {file_path_or_buffer}")
    
    # Pyarrow can read directly from file paths or buffer-like objects
    parquet_file = pq.ParquetFile(file_path_or_buffer)
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        # Yielding raw dictionaries from each row in the batch
        for record in batch.to_pylist():
            yield record

def read_csv(file_path_or_buffer: Union[str, io.BytesIO], lat_col: Optional[str] = None, lon_col: Optional[str] = None, elevation_col: Optional[str] = None, elevation_unit: Optional[str] = None, wkt_col: Optional[str] = None, chunk_size: int = 1000, encoding: str = 'utf-8') -> Generator[Dict[str, Any], None, None]:
    """Reads a CSV file and yields records, creating Point geometry from lat/lon or parsing WKT."""
    logger.info(f"Reading CSV from: {file_path_or_buffer}")
    #TODO elevation and elevation unit handling can be added here as additional properties, but for now we focus on geometry creation.

    # Use chunksize to read large CSVs without loading everything into memory.
    chunk_iterator = pd.read_csv(file_path_or_buffer, chunksize=chunk_size, encoding=encoding)

    for df_chunk in chunk_iterator:
        yield from _process_csv_chunk(df_chunk, lat_col, lon_col, wkt_col)

def _process_csv_chunk(df: pd.DataFrame, lat_col: Optional[str], lon_col: Optional[str], wkt_col: Optional[str]) -> Generator[Dict[str, Any], None, None]:
    """Helper to process a DataFrame chunk from a CSV into GeoJSON-like features."""
    geometries = None
    if wkt_col:
        # Create GeoSeries from WKT column
        geometries = gpd.GeoSeries.from_wkt(df[wkt_col])
    elif lat_col and lon_col:
        # Create Point geometries from latitude and longitude columns
        # Ensure lat/lon columns are numeric and handle potential errors
        if pd.api.types.is_numeric_dtype(df[lon_col]) and pd.api.types.is_numeric_dtype(df[lat_col]):
            geometries = gpd.points_from_xy(df[lon_col], df[lat_col])
        else:
            raise ValueError(f"Latitude ({lat_col}) and/or Longitude ({lon_col}) columns are not numeric.")
    else:
        raise ValueError("CSV reader requires either 'wkt_col' or both 'lat_col' and 'lon_col'.")

    # Create a GeoDataFrame from the DataFrame and geometries
    gdf = gpd.GeoDataFrame(df, geometry=geometries)
    yield from gdf.iterfeatures()


# --- FILE WRITERS ---

def _process_records_for_writing(records: Iterable[Union[Feature, Dict[str, Any]]]) -> Generator[Dict[str, Any], None, None]:
    """ 
    Generator that processes an iterator of Feature objects or dicts, yielding dictionaries
    with cleaned, user-facing attributes and a shapely geometry object, suitable for
    direct use in GeoDataFrame creation.
    """
    # Define columns that are considered internal and should not be exposed to the end-user.
    INTERNAL_COLS = { 
        'id', 'geom', 'bbox_geom', 'attributes', 'transaction_time', 'valid_from', 
        'valid_to', 'geom_type', 'asset_code', 'was_geom_fixed', 'content_hash'
    }

    for r in records:
        # Handle both Pydantic models and plain dicts
        record_dict = r.model_dump() if hasattr(r, 'model_dump') else r

        # Start with the attributes from the JSONB field.
        user_facing_attrs = record_dict.get('attributes', {})
        if not isinstance(user_facing_attrs, dict):
             user_facing_attrs = {}

        # Add top-level fields that are useful for identification but not internal.
        for key in ['external_id', 'geoid']:
            if key in record_dict:
                user_facing_attrs[key] = record_dict[key]
        
        # Add any other top-level fields that are NOT in the internal list or attributes.
        for key, value in record_dict.items():
            if key not in INTERNAL_COLS and key not in user_facing_attrs and not key.startswith(('h3_lvl', 's2_lvl')):
                user_facing_attrs[key] = value
        
        # Explicitly add bbox if it exists, as it's useful but not a standard attribute.
        if 'bbox' in record_dict and record_dict['bbox']:
            user_facing_attrs['bbox'] = record_dict['bbox']
        
        # Decode geometry and add it to the dictionary for GeoDataFrame
        geom_bytes = record_dict.get('geom')
        geometry = wkb.loads(geom_bytes) if geom_bytes else None

        # Yield a full GeoJSON-like feature dictionary, as expected by from_features
        yield {
            "type": "Feature",
            "properties": user_facing_attrs,
            "geometry": mapping(geometry) if geometry else None
        }


def _prepare_gdf_chunk_for_writing(records_chunk: List[Dict[str, Any]], srid: int) -> gpd.GeoDataFrame:
    """Converts a chunk of processed records into a GeoDataFrame."""
    if not records_chunk:
        return gpd.GeoDataFrame(columns=['geometry'], crs=f"EPSG:{srid}")

    gdf = gpd.GeoDataFrame.from_features(records_chunk, crs=f"EPSG:{srid}")
    if 'geoid' in gdf.columns:
        if any(isinstance(val, uuid.UUID) for val in gdf['geoid'].dropna()):
            gdf['geoid'] = gdf['geoid'].astype(str)
    return gdf

def _iterate_chunks(generator: Generator, chunk_size: int) -> Generator[List, None, None]:
    """Yields chunks of a given size from a generator."""
    chunk = []
    for item in generator:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def write_csv(records: Generator[Feature, None, None], srid: int, chunk_size: int = 1000, encoding: str = 'utf-8') -> Generator[bytes, None, None]:
    """Streams records to a CSV byte string, converting geometry to WKT."""
    processed_records_gen = _process_records_for_writing(records)
    record_chunks = _iterate_chunks(processed_records_gen, chunk_size=chunk_size)

    header_written = False
    for chunk in record_chunks:
        gdf = _prepare_gdf_chunk_for_writing(chunk, srid)
        if gdf.empty:
            continue

        gdf['geometry_wkt'] = gdf.geometry.to_wkt()
        gdf = gdf.drop(columns=['geometry'])

        buffer = io.StringIO()
        gdf.to_csv(buffer, index=False, header=not header_written, encoding=encoding)
        header_written = True
        yield buffer.getvalue().encode(encoding)

def write_parquet(records: Generator[Feature, None, None], srid: int, chunk_size: int = 1000, encoding: str = 'utf-8') -> Generator[bytes, None, None]:
    """Streams records to a Parquet byte string by writing chunks to a temporary file."""
    processed_records_gen = _process_records_for_writing(records)
    record_chunks = _iterate_chunks(processed_records_gen, chunk_size=chunk_size)

    with tempfile.NamedTemporaryFile(suffix=".parquet", dir=_ensure_temp_dir(), delete=True) as tmpfile:
        writer = None
        for chunk in record_chunks:
            gdf = _prepare_gdf_chunk_for_writing(chunk, srid)
            if gdf.empty:
                continue

            # Explicitly convert the geometry column to WKB (Well-Known Binary) format.
            # This is necessary because PyArrow's from_pandas does not natively
            # understand Shapely geometry objects, but it does understand bytes.
            if not gdf.geometry.empty:
                gdf['geometry'] = gdf.geometry.to_wkb()

            table = pa.Table.from_pandas(gdf, preserve_index=False)

            if writer is None:
                # Initialize the writer with the schema from the first table.
                writer = pq.ParquetWriter(tmpfile.name, table.schema, version='2.6')
            writer.write_table(table=table)

        if writer:
            writer.close()
            tmpfile.seek(0)
            while chunk := tmpfile.read(8192):
                yield chunk

def write_geopackage(records: Generator[Feature, None, None], srid: int, chunk_size: int = 1000, encoding: str = 'utf-8') -> Generator[bytes, None, None]:
    """
    Streams records to a GeoPackage byte string by writing chunks to a temporary file.
    """
    processed_records_gen = _process_records_for_writing(records)
    record_chunks = _iterate_chunks(processed_records_gen, chunk_size=chunk_size)
    
    with tempfile.NamedTemporaryFile(suffix=".gpkg", dir=_ensure_temp_dir(), delete=True) as tmpfile:
        first_chunk = True
        for chunk in record_chunks:
            gdf = _prepare_gdf_chunk_for_writing(chunk, srid)
            if gdf.empty:
                continue
            gdf.to_file(tmpfile.name, driver='GPKG', layer='features', mode='w' if first_chunk else 'a')
            first_chunk = False

        tmpfile.seek(0)
        # Stream the file in chunks to handle very large outputs
        while chunk := tmpfile.read(8192):
            yield chunk

def _truncate_shapefile_columns(gdf: gpd.GeoDataFrame):
    """Helper to handle Shapefile's 10-character column name limit."""
    original_columns = list(gdf.columns)
    new_names = {}
    for col in original_columns:
        if len(col) > 10:
            new_col_name = col[:10]
            logger.warning(f"Shapefile column name '{col}' truncated to '{new_col_name}'.")
            # Handle potential name collisions after truncation
            if (new_col_name in gdf.columns and new_col_name != col) or new_col_name in new_names.values():
                # Simple collision handling: find a new unique name
                i = 1
                while f"{new_col_name[:9]}{i}" in gdf.columns or f"{new_col_name[:9]}{i}" in new_names.values():
                    i += 1
                new_col_name = f"{new_col_name[:9]}{i}"
            new_names[col] = new_col_name
    if new_names:
        gdf.rename(columns=new_names, inplace=True)
    return gdf

def write_shapefile(records: Generator[Feature, None, None], srid: int, encoding: str = 'utf-8') -> Generator[bytes, None, None]:
    """
    Writes records to a zipped Shapefile byte string by using a temporary directory.
    NOTE: This still buffers all records in memory due to shapefile driver limitations.
    """
    # Shapefile driver does not support append mode, so we must buffer.
    # This is a known limitation of the format/driver.
    processed_records = list(_process_records_for_writing(records))
    if not processed_records:
        yield b""
        return

    gdf = _prepare_gdf_chunk_for_writing(processed_records, srid)
    gdf = _truncate_shapefile_columns(gdf) # Truncation must happen on the full GDF

    with tempfile.TemporaryDirectory(dir=_ensure_temp_dir()) as tmpdir:
        # Use a random filename to avoid collisions and better hygiene
        random_name = f"features_{uuid.uuid4()}"
        shapefile_name = os.path.join(tmpdir, f"{random_name}.shp")
        
        # Shapefile encoding is handled by the driver, specifically via 'encoding' param if using fiona/gpd
        gdf.to_file(shapefile_name, driver='ESRI Shapefile', encoding=encoding)
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for f in os.listdir(tmpdir):
                # We want the files inside the zip to have the random name or a standard name?
                # Usually standard name 'features.*' is better for the consumer even if temp file was random.
                # Let's keep the user facing name simple: features.*
                # But to avoid collision in the temp dir we used random_name.
                arcname = f.replace(random_name, "features") 
                zf.write(os.path.join(tmpdir, f), arcname=arcname)

        zip_buffer.seek(0)
        # Stream the zip buffer in chunks
        while chunk := zip_buffer.read(8192):
            yield chunk

def write_geojson(records: Generator[Feature, None, None], srid: int, chunk_size: int = 1000, encoding: str = 'utf-8') -> Generator[bytes, None, None]:
    """
    Streams records as a GeoJSON FeatureCollection.
    """
    processed_records_gen = _process_records_for_writing(records)
    
    yield '{"type": "FeatureCollection", "features": ['.encode(encoding)
    
    first = True
    for chunk in _iterate_chunks(processed_records_gen, chunk_size):
        for record in chunk:
            if not first:
                yield ','.encode(encoding)
            else:
                first = False
            yield json.dumps(record, cls=CustomJSONEncoder).encode(encoding)
            
    yield ']}'.encode(encoding)


def get_features_as_byte_stream(
    features: Iterable[Any],
    output_format: OutputFormatEnum,
    target_srid: int = 4326,
    encoding: str = "utf-8"
) -> Iterator[bytes]:
    """
    Dynamically formats a stream of features into the specified output format as a byte stream.
    
    Args:
        features: Iterable of feature dictionaries or Pydantic models
        output_format: Desired output format
        target_srid: Target SRID for the output
        encoding: Character encoding for the output
        
    Returns:
        Generator yielding bytes
    """
    from dynastore.extensions.tools.formatters import format_map
    
    formatter = format_map.get(output_format)
    if not formatter:
        raise ValueError(f"Unsupported format: {output_format}")

    # The `features` iterable can yield Pydantic models or dicts.
    # If they are Pydantic models, we dump them to dicts for the writers.
    feature_dicts = (f.model_dump(by_alias=True) if hasattr(f, 'model_dump') else f for f in features)

    # The writers from file_io expect an iterator and the target SRID.
    return formatter["writer"](feature_dicts, target_srid, encoding=encoding)


class GeneratorStream(object):
    """
    Wraps a generator of bytes into a file-like object with a read() method.
    Useful for integrating generators with APIs that expect a file-like object (e.g. GCS upload).
    """
    def __init__(self, generator: Iterator[bytes]):
        self.generator = generator
        self.buffer = b""

    def read(self, size: Optional[int] = None) -> bytes:
        if size is None:  # Read all
            return self.buffer + b"".join(self.generator)
        
        while len(self.buffer) < size:
            try:
                chunk = next(self.generator)
                self.buffer += chunk
            except StopIteration:
                break
        
        result = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return result

async def write_geojson_async(features_async_iter: AsyncIterator[Dict], encoding: str = "utf-8") -> AsyncIterator[bytes]:
    """
    Async generator to stream GeoJSON features.
    """
    yield '{"type": "FeatureCollection", "features": ['.encode(encoding)
    
    first = True
    async for feature in features_async_iter:
        if not first:
            yield ','.encode(encoding)
        else:
            first = False
            
        # Process feature for GeoJSON
        # feature is a dict with 'geom' as WKB bytes, and 'attributes'
        
        properties = feature.get("attributes", {}) or {}
        # Merge top level keys if needed (like in file_io.py)
        for k, v in feature.items():
            if k not in ["geom", "attributes", "bbox_geom"]:
                 properties[k] = v
        
        geometry = None
        if "geom" in feature and feature["geom"]:
             try:
                geometry = mapping(wkb.loads(feature["geom"]))
             except Exception:
                 logger.warning("Failed to decode geometry WKB", exc_info=True)
                 pass
        
        geojson_feature = {
            "type": "Feature",
            "properties": properties,
            "geometry": geometry
        }
        
        yield json.dumps(geojson_feature, cls=CustomJSONEncoder).encode(encoding)
        
    yield ']}'.encode(encoding)
