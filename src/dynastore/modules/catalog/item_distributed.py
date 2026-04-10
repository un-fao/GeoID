"""
Distributed insert/update mixin for ItemService.

Extracted from item_service.py to reduce file size.  All methods access
``self.*`` helpers defined on the main ``ItemService`` class, which
inherits from this mixin.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict

from sqlalchemy import text

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
)
from dynastore.modules.storage.driver_config import (
    PostgresCollectionDriverConfig,
    CollectionWritePolicy,
    WriteConflictPolicy,
    WRITE_POLICY_PLUGIN_ID,
)
from dynastore.models.protocols import ConfigsProtocol
from dynastore.modules.catalog.sidecars.base import SidecarProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.catalog.query_optimizer import QueryOptimizer

logger = logging.getLogger(__name__)


async def _run_query(conn, stmt, params=None):
    """Run a statement on either sync or async connection."""
    import inspect as _inspect

    result = conn.execute(stmt, params or {})
    if _inspect.isawaitable(result):
        result = await result
    return result


class ItemDistributedMixin:
    """Distributed insert/update operations for ItemService."""

    async def insert_or_update_distributed(
        self,
        conn: DbResource,
        catalog_id: str,
        collection_id: str,
        hub_payload: Dict[str, Any],
        sidecar_payloads: Dict[str, Dict[str, Any]],
        col_config: PostgresCollectionDriverConfig,
        sidecars: List[SidecarProtocol],
        processing_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Coordinates multi-table upsert for Hub and Sidecars."""
        phys_schema = await self._resolve_physical_schema(catalog_id, db_resource=conn)
        phys_table = await self._resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if not phys_table:
            phys_table = collection_id

        logger.info(
            f"DISTRIBUTED UPSERT: collection={catalog_id}.{collection_id}, phys={phys_schema}.{phys_table}, sidecars={[s.sidecar_id for s in sidecars]}"
        )

        # 1. Resolve write policy from the config waterfall (same as all drivers)
        write_policy = await get_protocol(ConfigsProtocol).get_config(
            WRITE_POLICY_PLUGIN_ID, catalog_id, collection_id, db_resource=conn
        )
        on_conflict = (
            write_policy.on_conflict if write_policy else WriteConflictPolicy.UPDATE
        )

        # 1.5 Acceptance Check
        for sidecar in sidecars:
            if not sidecar.is_acceptable(hub_payload, processing_context):
                logger.warning(f"Feature rejected by sidecar {sidecar.sidecar_id}")
                return None

        # Standardized Identity Resolution via Sidecar Protocol
        active_rec = None
        if on_conflict != WriteConflictPolicy.NEW_VERSION:
            for sidecar in sidecars:
                active_rec = await sidecar.resolve_existing_item(
                    conn, phys_schema, phys_table, processing_context
                )
                if active_rec:
                    logger.info(
                        f"DISTRIBUTED UPSERT: found active record geoid={active_rec.get('geoid')} (via {sidecar.sidecar_id})"
                    )
                    break

        # 1.6 Additional Checks: Identity/Asset/Unique Collision
        if on_conflict == WriteConflictPolicy.REFUSE_INGESTION:
            for sidecar in sidecars:
                if await sidecar.check_upsert_collision(
                    conn, phys_schema, phys_table, processing_context
                ):
                    logger.warning(
                        f"Feature rejected: Identity/Unique collision found (via {sidecar.sidecar_id})"
                    )
                    return None

        result = None
        # 2. Execution Path
        if not active_rec or on_conflict == WriteConflictPolicy.NEW_VERSION:
            if active_rec and on_conflict == WriteConflictPolicy.NEW_VERSION:
                # Archive the existing version before inserting
                expire_at = hub_payload.get("valid_from") or datetime.now(timezone.utc)
                for sidecar in sidecars:
                    await sidecar.expire_version(
                        conn,
                        phys_schema,
                        phys_table,
                        geoid=active_rec["geoid"],
                        expire_at=expire_at,
                    )

            # INSERT NEW
            result = await self._execute_distributed_insert(
                conn,
                phys_schema,
                phys_table,
                hub_payload,
                sidecar_payloads,
                col_config=col_config,
                sidecars=sidecars,
                processing_context=processing_context,
            )

        elif on_conflict == WriteConflictPolicy.REFUSE:
            logger.info(
                "DISTRIBUTED UPSERT: identity matched and REFUSE set. Skipping."
            )
            return None

        else:
            # UPDATE path (WriteConflictPolicy.UPDATE)
            processing_context["operation"] = "update"
            for sidecar in sidecars:
                val_result = sidecar.validate_update(
                    sidecar_payloads.get(sidecar.sidecar_id, {}),
                    active_rec,
                    processing_context,
                )
                if not val_result.valid:
                    raise ValueError(
                        f"Sidecar {sidecar.sidecar_id} rejected update: {val_result.error}"
                    )

            # Resolve Validity for Hub & Sidecars
            valid_from = processing_context.get("valid_from") or datetime.now(
                timezone.utc
            )
            valid_to = processing_context.get("valid_to")

            from asyncpg import Range

            validity = Range(valid_from, valid_to, lower_inc=True, upper_inc=False)

            if "validity" not in hub_payload:
                hub_payload["validity"] = validity

            # UPDATE EXISTING (HUB + Sidecars)
            if active_rec and "validity" in active_rec:
                validity = active_rec["validity"]
                hub_payload["validity"] = validity

            result = await self._execute_distributed_update(
                conn,
                phys_schema,
                phys_table,
                active_rec["geoid"],
                hub_payload,
                sidecar_payloads,
                col_config=col_config,
                sidecars=sidecars,
                processing_context=processing_context,
                active_rec=active_rec,
            )

        return result

    async def _execute_distributed_insert(
        self,
        conn,
        schema,
        hub_table,
        hub_payload,
        sc_data_map,
        col_config,
        sidecars=None,
        processing_context=None,
    ) -> Dict[str, Any]:
        """Performs inserts across Hub and all sidecars."""
        # A. Insert Hub
        logger.warning(f"DEBUG: Inserting into Hub {schema}.{hub_table}")
        hub_row = await self._insert_table_raw(conn, schema, hub_table, hub_payload)
        hub_data = hub_row._mapping if hasattr(hub_row, "_mapping") else hub_row
        geoid = hub_data["geoid"]

        # B. Insert Sidecars
        for sidecar in sidecars:
            sc_id = sidecar.sidecar_id
            sc_payload = sc_data_map.get(sc_id, {})
            sc_table = f"{hub_table}_{sc_id}"

            # 1. Identity Columns (Conflict Target)
            conflict_cols = sidecar.get_identity_columns()

            # 2. Add partitioning keys to conflict target if enabled
            if col_config.partitioning and col_config.partitioning.enabled:
                for key in col_config.partitioning.partition_keys:
                    if key not in conflict_cols:
                        conflict_cols.insert(0, key)

            # 3. Finalize Payload (Inject validity, geoid, etc.)
            if sc_id not in sc_data_map and not sidecar.is_mandatory():
                continue

            if "geoid" not in sc_payload:
                sc_payload["geoid"] = geoid

            full_payload = sidecar.finalize_upsert_payload(
                sc_payload, hub_data, processing_context or {}
            )

            logger.debug(f"Upserting sidecar {sc_table} for geoid {geoid}")
            await self._upsert_sidecar_table_raw(
                conn, schema, sc_table, full_payload, conflict_cols=conflict_cols
            )

            # JSON-FG Place Statistics: insert into <hub_table>_place if configured
            if hasattr(sidecar, "prepare_place_upsert_payload"):
                try:
                    place_payload = sidecar.prepare_place_upsert_payload(
                        processing_context.get("_raw_item", {}), processing_context
                    )
                    if place_payload:
                        place_table = f"{hub_table}_place"
                        if "geoid" not in place_payload:
                            place_payload["geoid"] = geoid
                        await self._upsert_sidecar_table_raw(
                            conn, schema, place_table, place_payload, conflict_cols=["geoid"]
                        )
                        logger.debug(f"Upserted place stats into {schema}.{place_table} for geoid {geoid}")
                except Exception as e:
                    logger.warning(f"Place stats upsert skipped for geoid {geoid}: {e}")

        optimizer = QueryOptimizer(col_config)
        fetch_req = QueryRequest(
            raw_where="h.geoid = :target_geoid",
            raw_params={"target_geoid": geoid},
            limit=1,
        )
        sql, params = optimizer.build_optimized_query(fetch_req, schema, hub_table)
        result = await _run_query(conn, text(sql), params)
        row = result.mappings().first()
        if row is None:
            return None
        res = self.map_row_to_feature(dict(row), col_config)
        logger.debug(f"FINAL RESULT FROM EXECUTOR: {res}")
        return res

    async def _execute_distributed_update(
        self,
        conn,
        schema,
        hub_table,
        geoid,
        hub_data,
        sc_data_map,
        col_config,
        sidecars=None,
        processing_context=None,
        active_rec=None,
    ) -> Dict[str, Any]:
        """Performs updates across Hub and all sidecars."""
        # A. Update Hub
        hub_row = await self._update_table_raw(conn, schema, hub_table, geoid, hub_data)
        if not hub_row:
            return None

        row_data = hub_row._mapping if hasattr(hub_row, "_mapping") else hub_row
        res_geoid = row_data["geoid"]

        # B. Resolve Identity and Finalize Payloads for Sidecars
        for sidecar in sidecars:
            sc_id = sidecar.sidecar_id
            sc_payload = sc_data_map.get(sc_id, {})
            sc_table = f"{hub_table}_{sc_id}"

            # 1. Identity Columns
            conflict_cols = sidecar.get_identity_columns()
            if col_config.partitioning and col_config.partitioning.enabled:
                for key in col_config.partitioning.partition_keys:
                    if key not in conflict_cols:
                        conflict_cols.insert(0, key)

            # 2. Finalize Payload
            if "geoid" not in sc_payload:
                sc_payload["geoid"] = geoid

            full_payload = sidecar.finalize_upsert_payload(
                sc_payload, hub_data, processing_context or {}
            )

            await self._upsert_sidecar_table_raw(
                conn, schema, sc_table, full_payload, conflict_cols=conflict_cols
            )
        optimizer = QueryOptimizer(col_config)
        fetch_req = QueryRequest(
            raw_where="h.geoid = :lookup_geoid",
            raw_params={"lookup_geoid": str(geoid)},
            limit=1,
        )
        sql, params = optimizer.build_optimized_query(fetch_req, schema, hub_table)
        result = await _run_query(conn, text(sql), params)
        row = result.mappings().first()
        if row is None:
            return None
        return self.map_row_to_feature(dict(row), col_config)

    async def _insert_table_raw(self, conn, schema, table, data) -> Dict[str, Any]:
        """Generic table insert (No special geometry handling here, already processed by sidecars)."""
        cols = []
        vals = []
        params = {}
        for k, v in data.items():
            cols.append(f'"{k}"')
            vals.append(f":{k}")
            params[k] = v

        sql = f'INSERT INTO "{schema}"."{table}" ({", ".join(cols)}) VALUES ({", ".join(vals)}) RETURNING *;'
        return await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(
            conn, **params
        )

    async def _update_table_raw(
        self, conn, schema, table, geoid, data
    ) -> Dict[str, Any]:
        """Generic table update by geoid."""
        clauses = []
        params = {"geoid": geoid}
        for k, v in data.items():
            if k == "geoid":
                continue
            clauses.append(f'"{k}" = :{k}')
            params[k] = v

        sql = f'UPDATE "{schema}"."{table}" SET {", ".join(clauses)} WHERE geoid = :geoid RETURNING *;'
        return await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(
            conn, **params
        )

    async def _upsert_sidecar_table_raw(
        self, conn, schema, table, data, conflict_cols: List[str] = ["geoid"]
    ):
        """Sidecar upsert with ON CONFLICT (conflict_cols)."""
        cols: list = []
        vals: list = []
        updates: list = []
        params = {}
        for k, v in data.items():
            cols.append(f'"{k}"')
            # Geometry columns: pass WKB hex through ST_GeomFromEWKB
            if k in ["geom", "bbox_geom", "centroid"] and isinstance(v, str):
                vals.append(f"ST_GeomFromEWKB(decode(:{k}, 'hex'))")
                params[k] = v
            # Range columns (e.g. validity TSTZRANGE): duck-type for any Range-like
            # object (asyncpg.Range, etc.) which psycopg2 cannot serialise directly.
            # Expand into lower/upper params and emit tstzrange() so both drivers work.
            elif hasattr(v, "lower") and hasattr(v, "upper") and hasattr(v, "lower_inc"):
                lk, uk = f"{k}_lower", f"{k}_upper"
                lb = "[" if v.lower_inc else "("
                ub = "]" if v.upper_inc else ")"
                vals.append(f"tstzrange(:{lk}, :{uk}, '{lb}{ub}')")
                params[lk] = v.lower
                params[uk] = v.upper
            else:
                vals.append(f":{k}")
                params[k] = v
            if k not in conflict_cols:
                updates.append(f'"{k}" = EXCLUDED."{k}"')

        sql = f"""
INSERT INTO "{schema}"."{table}" ({", ".join(cols)})
VALUES ({", ".join(vals)})
ON CONFLICT ({", ".join([f'"{c}"' for c in conflict_cols])}) DO UPDATE SET {", ".join(updates)};
"""
        await DDLQuery(sql).execute(conn, **params)
