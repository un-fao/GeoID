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

from typing import Optional, List, Dict, Any, Tuple, Union, AsyncIterator
from datetime import datetime, timezone
import logging
import orjson
from fastapi import HTTPException, status, Request
from fastapi.responses import StreamingResponse
from dynastore.models.query_builder import (
    QueryRequest,
    FilterCondition,
    SortOrder,
    QueryResponse,
)
from dynastore.extensions.tools.formatters import (
    OutputFormatEnum,
    format_response,
    OGCResponseMetadata,
)
from dynastore.tools.json import orjson_default

logger = logging.getLogger(__name__)


async def resolve_items_read_policy(
    catalog_id: str,
    collection_id: str,
) -> Optional[Any]:
    """Resolve a collection's :class:`ItemsReadPolicy` for OGC read assembly.

    Shared by the OGC generators (Features / Records) so a raw-row fallback in
    ``map_row_to_feature`` honours the read-time wire-shape contract —
    ``feature_type.expose`` value-merge and ``external_id_as_feature_id``.
    Mirrors ``ItemQueryMixin._resolve_read_policy`` for callers that don't have
    a mixin instance. Returns ``None`` when the configs protocol is unavailable
    or the lookup fails; callers then fall back to the default wire shape.
    """
    try:
        from dynastore.models.protocols import ConfigsProtocol
        from dynastore.modules.storage.read_policy import ItemsReadPolicy
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return None
        return await configs.get_config(
            ItemsReadPolicy,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
    except Exception as exc:  # noqa: BLE001 - read assembly must not break on config miss
        logger.debug(
            "read policy resolution skipped for %s/%s: %s",
            catalog_id,
            collection_id,
            exc,
        )
        return None


def parse_ogc_query_request(
    bbox: Optional[str] = None,
    datetime_param: Optional[str] = None,
    sortby: Optional[str] = None,
    filter: Optional[str] = None,
    item_ids: Optional[Union[str, List[str]]] = None,
    limit: int = 10,
    offset: int = 0,
    bbox_crs_srid: Optional[int] = None,
    include_total_count: bool = True,
) -> QueryRequest:
    """
    Unifies OGC parameter parsing into a structured QueryRequest.
    """
    request_obj = QueryRequest(
        limit=limit,
        offset=offset,
        include_total_count=include_total_count,
        filters=[],
    )

    # 0. Item IDs
    if item_ids:
        if isinstance(item_ids, str):
            item_ids = [id.strip() for id in item_ids.split(",")]

        request_obj.filters.append(
            FilterCondition(
                field="geoid",  # Default to geoid; ItemService handles mapping to external_id if configured
                operator="IN",
                value=item_ids,
            )
        )

    if bbox:
        try:
            parsed_bbox = tuple(map(float, bbox.split(",")))
            if len(parsed_bbox) != 4:
                raise ValueError("BBOX must have 4 coordinates.")
            srid = bbox_crs_srid or 4326
            request_obj.filters.append(
                FilterCondition(
                    field="geom",
                    operator="&&",
                    value=f"SRID={srid};POLYGON(({parsed_bbox[0]} {parsed_bbox[1]}, {parsed_bbox[0]} {parsed_bbox[3]}, {parsed_bbox[2]} {parsed_bbox[3]}, {parsed_bbox[2]} {parsed_bbox[1]}, {parsed_bbox[0]} {parsed_bbox[1]}))",
                    spatial_op=True,
                )
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid BBOX: {e}")

    if datetime_param:
        try:
            if "/" in datetime_param:
                start_str, end_str = datetime_param.split("/")
                start_dt_str = start_str if start_str != ".." else None
                end_dt_str = end_str if end_str != ".." else None
                start_dt = (
                    datetime.fromisoformat(start_dt_str.replace("Z", "+00:00"))
                    if start_dt_str
                    else None
                )
                end_dt = (
                    datetime.fromisoformat(end_dt_str.replace("Z", "+00:00"))
                    if end_dt_str
                    else None
                )

                if start_dt and end_dt:
                    request_obj.filters.append(
                        FilterCondition(
                            field="validity",
                            operator="&&",
                            value=f"[{start_dt.isoformat()},{end_dt.isoformat()})",
                        )
                    )
                elif start_dt:
                    request_obj.filters.append(
                        FilterCondition(field="validity", operator="@>", value=start_dt)
                    )
                elif end_dt:
                    request_obj.filters.append(
                        FilterCondition(field="validity", operator="@>", value=end_dt)
                    )
            else:
                dt = datetime.fromisoformat(datetime_param.replace("Z", "+00:00"))
                request_obj.filters.append(
                    FilterCondition(field="validity", operator="@>", value=dt)
                )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid datetime format: {e}")
    else:
        request_obj.filters.append(
            FilterCondition(
                field="validity", operator="@>", value=datetime.now(timezone.utc)
            )
        )

    if sortby:
        request_obj.sort = []
        for v in sortby.split(","):
            v = v.strip()
            if not v:
                continue
            direction = "DESC" if v.startswith("-") else "ASC"
            request_obj.sort.append(
                SortOrder(field=v.lstrip("+-"), direction=direction)
            )

    if filter:
        request_obj.cql_filter = filter

    return request_obj


def stream_ogc_features(
    request: Request,
    query_response: QueryResponse,
    output_format: OutputFormatEnum,
    catalog_id: str,
    collection_id: str,
    target_srid: int = 4326,
    links: Optional[List[Any]] = None,
) -> StreamingResponse:
    """
    Unified streaming response for OGC Features/WFS/DWH.
    """
    ogc_metadata = OGCResponseMetadata(
        numberMatched=query_response.total_count,
        links=[l.model_dump() if hasattr(l, "model_dump") else l for l in links]
        if links
        else None,
    )

    return format_response(
        request=request,
        features=query_response.items,
        output_format=output_format,
        collection_id=collection_id,
        target_srid=target_srid,
        metadata=ogc_metadata,
    )
