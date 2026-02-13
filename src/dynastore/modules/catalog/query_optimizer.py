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

from typing import Dict, List, Any, Tuple, Set, Optional
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
from dynastore.modules.catalog.sidecars.base import SidecarProtocol, FieldDefinition, FieldCapability, SidecarConfig
from dynastore.models.query_builder import QueryRequest, FieldSelection, FilterCondition, SortOrder

class QueryOptimizer:
    """Optimizes queries based on sidecar capabilities and requested operations."""
    
    def __init__(self, col_config: CollectionPluginConfig):
        self.col_config = col_config
        self.field_index: Dict[str, Tuple[SidecarProtocol, FieldDefinition]] = {}
        self._build_capability_index()
    
    def _build_capability_index(self):
        """Build index of all available fields and their capabilities."""
        from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
        
        for sc_config in self.col_config.sidecars:
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            for field_name, field_def in sidecar.get_field_definitions().items():
                self.field_index[field_name] = (sidecar, field_def)
    
    def validate_query(self, query: QueryRequest) -> List[str]:
        """
        Validate query against available capabilities.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Validate SELECT fields
        for sel in query.select:
            if sel.field == "*":
                continue
            
            if sel.field not in self.field_index:
                errors.append(f"Unknown field: {sel.field}")
                continue
            
            sidecar, field_def = self.field_index[sel.field]
            
            if sel.aggregation and not sidecar.supports_aggregation(sel.field, sel.aggregation):
                errors.append(f"Field {sel.field} does not support aggregation {sel.aggregation}")
            
            if sel.transformation and not sidecar.supports_transformation(sel.field, sel.transformation):
                errors.append(f"Field {sel.field} does not support transformation {sel.transformation}")
        
        # Validate filters
        for filt in query.filters:
            if filt.field not in self.field_index:
                # Allow special Hub fields
                if filt.field in ["geoid", "deleted_at"]:
                    continue
                errors.append(f"Cannot filter by unknown field: {filt.field}")
                continue
            
            _, field_def = self.field_index[filt.field]
            if FieldCapability.FILTERABLE not in field_def.capabilities:
                errors.append(f"Field {filt.field} is not filterable")
        
        # Validate sort
        if query.sort:
            for sort in query.sort:
                if sort.field not in self.field_index:
                    # Allow special Hub fields
                    if sort.field in ["geoid", "deleted_at"]:
                        continue
                    errors.append(f"Cannot sort by unknown field: {sort.field}")
                    continue
                
                _, field_def = self.field_index[sort.field]
                if FieldCapability.SORTABLE not in field_def.capabilities:
                    errors.append(f"Field {sort.field} is not sortable")
        
        # Validate group by
        if query.group_by:
            for field in query.group_by:
                if field not in self.field_index:
                    # Allow special Hub fields
                    if field in ["geoid", "deleted_at"]:
                        continue
                    errors.append(f"Cannot group by unknown field: {field}")
                    continue
                
                _, field_def = self.field_index[field]
                if FieldCapability.GROUPABLE not in field_def.capabilities:
                    errors.append(f"Field {field} is not groupable")
        
        return errors
    
    def determine_required_sidecars(self, query: QueryRequest) -> List[SidecarConfig]:
        """
        Determine which sidecars are actually needed for this query.
        
        Returns:
            List of sidecar configs that must be joined
        """
        required_sidecars = set()
        
        # We assume if raw_where is used, it might require all sidecars just to be safe.
        # Alternatively, we just verify if select * is requested.
        
        # Check SELECT fields
        for sel in query.select:
            if sel.field == "*":
                # Need all sidecars if selecting *
                return list(self.col_config.sidecars)
            
            if sel.field in self.field_index:
                sidecar, _ = self.field_index[sel.field]
                required_sidecars.add(sidecar.config.sidecar_id)
        
        # Check filters
        for filt in query.filters:
            if filt.field in self.field_index:
                sidecar, _ = self.field_index[filt.field]
                required_sidecars.add(sidecar.config.sidecar_id)
        
        # Check sort
        if query.sort:
            for sort in query.sort:
                if sort.field in self.field_index:
                    sidecar, _ = self.field_index[sort.field]
                    required_sidecars.add(sidecar.config.sidecar_id)
        
        # Check group by
        if query.group_by:
            for field in query.group_by:
                if field in self.field_index:
                    sidecar, _ = self.field_index[field]
                    required_sidecars.add(sidecar.config.sidecar_id)
        
        # Return only required sidecar configs, preserving order
        return [sc for sc in self.col_config.sidecars if sc.sidecar_id in required_sidecars]
    
    def build_optimized_query(
        self,
        query: QueryRequest,
        schema: str,
        table: str
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Build optimized SQL query based on request.
        
        Returns:
            (sql_string, parameters)
        """
        from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
        
        # Validate first
        errors = self.validate_query(query)
        if errors:
            raise ValueError(f"Invalid query: {'; '.join(errors)}")
        
        # Determine required sidecars
        required_sidecars = self.determine_required_sidecars(query)
        
        # Build SELECT clause
        select_fields = []
        
        if getattr(query, 'include_total_count', False):
            select_fields.append("COUNT(*) OVER() AS _total_count")
            
        if any(sel.field == "*" for sel in query.select):
            select_fields.append("h.*")
            for sc_config in required_sidecars:
                sidecar = SidecarRegistry.get_sidecar(sc_config)
                select_fields.extend(sidecar.get_select_fields(hub_alias="h"))
        else:
            # Always check if geoid is explicitly requested or aggregation implies it
            has_aggregations = any(sel.aggregation for sel in query.select)
            if not has_aggregations and "geoid" not in [s.field for s in query.select]:
                # Only add geoid implicit selection if no aggregations (don't break GROUP BY)
                select_fields.append("h.geoid") 
            
            for sel in query.select:
                if sel.field == "geoid":
                    select_fields.append("h.geoid")
                    continue
                    
                sidecar, field_def = self.field_index[sel.field]
                expr = field_def.sql_expression
                
                # Apply transformation
                if sel.transformation:
                    # PERFORMANCE OPTIMIZATION: Skip ST_Transform if target SRID matches native SRID
                    if sel.transformation == "ST_Transform" and field_def.data_type and "geometry" in field_def.data_type:
                         target_srid = sel.transform_args.get("srid") or sel.transform_args.get("0")
                         if str(target_srid) in field_def.data_type:
                              # Skip transform
                              pass
                         else:
                              args_str = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in sel.transform_args.values())
                              expr = f"{sel.transformation}({expr}{', ' + args_str if args_str else ''})"
                    else:
                        args_str = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in sel.transform_args.values())
                        expr = f"{sel.transformation}({expr}{', ' + args_str if args_str else ''})"
                
                # Apply aggregation
                if sel.aggregation:
                    expr = f"{sel.aggregation}({expr})"
                
                # Apply alias
                alias = sel.alias or sel.field
                select_fields.append(f"{expr} as {alias}")
                
        if getattr(query, 'raw_selects', None):
            select_fields.extend(query.raw_selects)
        
        # Build JOINs (only for required sidecars)
        joins = []
        for sc_config in required_sidecars:
            sidecar = SidecarRegistry.get_sidecar(sc_config)
            joins.append(sidecar.get_join_clause(schema, table, hub_alias="h"))
        
        # Build WHERE clause
        where_conditions = ["h.deleted_at IS NULL"]
        params = {}
        
        for i, filt in enumerate(query.filters):
            # Special handling for Hub fields
            if filt.field in ["geoid", "deleted_at"]:
                expr = f"h.{filt.field}"
            else:
                _, field_def = self.field_index[filt.field]
                expr = field_def.sql_expression
                
            param_name = f"filter_{i}"
            where_conditions.append(f"{expr} {filt.operator} :{param_name}")
            params[param_name] = filt.value
            
        if getattr(query, 'raw_where', None):
            where_conditions.append(f"({query.raw_where})")
        if getattr(query, 'raw_params', None):
            params.update(query.raw_params)
        
        # Build GROUP BY
        group_by_clause = ""
        if query.group_by:
            group_fields = []
            for field in query.group_by:
                if field in ["geoid", "deleted_at"]:
                    group_fields.append(f"h.{field}")
                else:
                    _, field_def = self.field_index[field]
                    group_fields.append(field_def.sql_expression)
            group_by_clause = f"GROUP BY {', '.join(group_fields)}"
        elif any(sel.aggregation for sel in query.select):
            # Implicit GROUP BY for non-aggregated selected fields
            group_fields = []
            for sel in query.select:
                if not sel.aggregation and sel.field != "*":
                    if sel.field in ["geoid", "deleted_at"]:
                        group_fields.append(f"h.{sel.field}")
                    else:
                        _, field_def = self.field_index[sel.field]
                        group_fields.append(field_def.sql_expression)
            if group_fields:
                group_by_clause = f"GROUP BY {', '.join(group_fields)}"
        
        # Build ORDER BY
        order_by_clause = ""
        if query.sort:
            order_fields = []
            for sort in query.sort:
                if sort.field in ["geoid", "deleted_at"]:
                    order_fields.append(f"h.{sort.field} {sort.direction}")
                else:
                    _, field_def = self.field_index[sort.field]
                    order_fields.append(f"{field_def.sql_expression} {sort.direction}")
            order_by_clause = f"ORDER BY {', '.join(order_fields)}"
        elif not query.group_by and not any(sel.aggregation for sel in query.select):
            # Apply default sort from sidecars if no explicit sort and no aggregation
            for sc_config in required_sidecars:
                sidecar = SidecarRegistry.get_sidecar(sc_config)
                default_sort = sidecar.get_default_sort()
                if default_sort:
                    order_fields = [f"{field} {direction}" for field, direction in default_sort]
                    order_by_clause = f"ORDER BY {', '.join(order_fields)}"
                    break
        
        # Build LIMIT/OFFSET
        limit_clause = f"LIMIT {query.limit}" if query.limit else ""
        offset_clause = f"OFFSET {query.offset}" if query.offset else ""
        
        # Assemble final query
        sql = f"""
            SELECT {', '.join(select_fields)}
            FROM "{schema}"."{table}" h
            {' '.join(joins)}
            WHERE {' AND '.join(where_conditions)}
            {group_by_clause}
            {order_by_clause}
            {limit_clause}
            {offset_clause}
        """.strip()
        
        return sql, params