"""
Shared feature streaming utilities for querying and streaming features from the database.

This module provides reusable async generators for streaming features with support for:
- CQL filtering (using pygeofilter for security)
- Property projection (selecting specific columns)
- Pagination (limit/offset)
- Geometry handling

Used by export tasks, WFS service, and other components that need to stream features.
"""

import logging
from typing import AsyncIterator, Dict, Any, Optional, List, Set
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.sql import column as sql_column

from dynastore.modules.db_config.query_executor import DbResource, managed_transaction
from dynastore.modules.db_config import shared_queries
from dynastore.tools.db import validate_sql_identifier
from dynastore.modules.tools.cql import parse_cql_filter, PYGEOFILTER_AVAILABLE

logger = logging.getLogger(__name__)


class FeatureStreamConfig(BaseModel):
    """Configuration for streaming features from a collection."""
    
    catalog: str = Field(..., description="Catalog ID (schema name)")
    collection: str = Field(..., description="Collection ID (table name)")
    cql_filter: Optional[str] = Field(None, description="CQL2/ECQL filter expression")
    property_names: Optional[List[str]] = Field(None, description="List of properties to include (None = all)")
    limit: Optional[int] = Field(None, description="Maximum number of features to return")
    offset: Optional[int] = Field(None, description="Number of features to skip")
    include_geometry: bool = Field(True, description="Whether to include geometry in results")
    target_srid: Optional[int] = Field(None, description="Target SRID for geometry transformation")


async def stream_features(
    config: FeatureStreamConfig,
    db_resource: DbResource
) -> AsyncIterator[Dict[str, Any]]:
    """
    Stream features from a collection with optional filtering and projection.
    
    Args:
        config: Configuration for the feature stream
        db_resource: Database engine or connection
        
    Yields:
        Feature dictionaries with selected properties
        
    Raises:
        ValueError: If CQL filter is invalid or contains unknown properties
        RuntimeError: If collection doesn't exist
    """
    validate_sql_identifier(config.catalog)
    validate_sql_identifier(config.collection)
    
    async with managed_transaction(db_resource) as conn:
        # Verify collection exists
        if not await shared_queries.table_exists_query.execute(
            conn, schema=config.catalog, table=config.collection
        ):
            raise RuntimeError(
                f"Collection '{config.catalog}.{config.collection}' does not exist"
            )
        
        # Get available columns for validation and mapping
        columns = await shared_queries.get_table_column_names(
            conn, config.catalog, config.collection
        )
        
        # Build SELECT clause
        select_expressions = _build_select_expressions(
            config, columns
        )
        
        # Build WHERE clause with CQL filtering
        where_clause, bind_params = _build_where_clause(
            config, columns
        )
        
        # Construct query
        query_parts = [
            f"SELECT {', '.join(select_expressions)}",
            f'FROM "{config.catalog}"."{config.collection}"'
        ]
        
        if where_clause:
            query_parts.append(f"WHERE {where_clause}")
        
        if config.limit is not None:
            query_parts.append("LIMIT :limit")
            bind_params["limit"] = config.limit
        
        if config.offset is not None:
            query_parts.append("OFFSET :offset")
            bind_params["offset"] = config.offset
        
        query_str = " ".join(query_parts)
        logger.debug(f"Streaming features with query: {query_str}")
        
        # Stream results
        stmt = text(query_str).execution_options(stream_results=True)
        result = await conn.stream(stmt, bind_params)
        
        async for row in result:
            yield dict(row._mapping)


def _build_select_expressions(
    config: FeatureStreamConfig,
    available_columns: List[str]
) -> List[str]:
    """
    Build SELECT expressions based on property projection and geometry settings.
    
    Args:
        config: Feature stream configuration
        available_columns: List of available column names in the table
        
    Returns:
        List of SQL select expressions
    """
    select_expressions = []
    
    # Handle geometry
    if config.include_geometry and "geom" in available_columns:
        if config.target_srid:
            select_expressions.append(
                f"ST_Transform(geom, {config.target_srid}) AS geom"
            )
        else:
            select_expressions.append("geom")
    
    # Handle property projection
    if config.property_names:
        # Specific properties requested
        for prop in config.property_names:
            if prop == "geom":
                continue  # Already handled above
            
            if prop in available_columns:
                # Direct column
                if prop == "bbox_geom" and "bbox_geom" in available_columns:
                    if config.target_srid:
                        select_expressions.append(
                            f"ST_Transform(bbox_geom, {config.target_srid}) AS bbox_geom"
                        )
                    else:
                        select_expressions.append("bbox_geom")
                else:
                    select_expressions.append(f'"{prop}"')
            else:
                # Assume it's in JSONB attributes column
                if "attributes" in available_columns:
                    select_expressions.append(
                        f"(attributes->>'{prop}') AS \"{prop}\""
                    )
    else:
        # No projection - select all non-geometry columns
        for col in available_columns:
            if col == "geom":
                continue  # Already handled
            if col == "bbox_geom":
                if config.target_srid:
                    select_expressions.append(
                        f"ST_Transform(bbox_geom, {config.target_srid}) AS bbox_geom"
                    )
                else:
                    select_expressions.append("bbox_geom")
            else:
                select_expressions.append(f'"{col}"')
    
    # Fallback to * if nothing selected
    if not select_expressions:
        select_expressions = ["*"]
    
    return select_expressions


def _build_where_clause(
    config: FeatureStreamConfig,
    available_columns: List[str]
) -> tuple[str, Dict[str, Any]]:
    """
    Build WHERE clause from CQL filter.
    
    Args:
        config: Feature stream configuration
        available_columns: List of available column names
        
    Returns:
        Tuple of (where_clause_string, bind_parameters)
        
    Raises:
        ValueError: If CQL filter is invalid
    """
    if not config.cql_filter:
        return "", {}
    
    if not PYGEOFILTER_AVAILABLE:
        raise ValueError(
            "CQL filtering requires pygeofilter to be installed. "
            "Install with: pip install pygeofilter"
        )
    
    # Build field mapping for CQL parser
    # Map property names to SQL expressions
    field_mapping = {}
    
    # Direct columns
    for col in available_columns:
        if col == "geom":
            # Geometry column - use PostGIS functions
            field_mapping[col] = sql_column("geom")
        elif col == "attributes":
            # JSONB column - handle separately for nested properties
            continue
        else:
            field_mapping[col] = sql_column(col)
    
    # For attributes in JSONB, we need to handle them dynamically
    # The CQL parser will validate against valid_props
    # For now, we'll add common JSONB attribute access patterns
    # This is a simplified approach - a full implementation would introspect the schema
    
    # Valid properties include all columns
    valid_props = set(available_columns)
    
    try:
        where_sql, bind_params = parse_cql_filter(
            cql_text=config.cql_filter,
            field_mapping=field_mapping,
            valid_props=valid_props,
            parser_type='cql2'
        )
        return where_sql, bind_params
    except Exception as e:
        logger.error(f"CQL filter parsing failed: {e}")
        raise ValueError(f"Invalid CQL filter: {e}") from e
