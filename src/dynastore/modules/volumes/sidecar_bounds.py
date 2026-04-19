"""Sidecar-backed BoundsSourceProtocol implementation.

Queries the geometries sidecar table for per-feature 3D bboxes.
Uses ST_XMin/XMax/YMin/YMax/ZMin/ZMax on ST_Force3D(geom) so Z is
populated for both 2D and 3D features (2D collapses to z=0; the
height-attr fallback is applied by a separate extension when
configured).

Module contract:
    - ``build_bounds_query(...)`` — pure SQL string builder
    - ``row_to_feature_bounds(row)`` — pure row-parser
    - ``SidecarBoundsSource`` — protocol-compliant class that wires the
      two together against a live DB connection

The pure pieces are TDD'd; the class itself is exercised with fake
connection fixtures. End-to-end integration against a real tenant
database is deferred to a future test fixture setup.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Sequence

from dynastore.modules.volumes.bounds import FeatureBounds


@dataclass(frozen=True)
class BoundsQuerySpec:
    """Inputs needed to build the sidecar bounds query for one collection."""

    schema: str
    hub_table: str
    geometries_table: str
    feature_id_column: str = "geoid"
    geom_column: str = "geom"
    height_column: Optional[str] = None  # COALESCE fallback for flat features
    limit: Optional[int] = None


def build_bounds_query(spec: BoundsQuerySpec) -> str:
    """Emit the SQL text for one collection's 3D bbox extraction.

    Columns returned per row: ``feature_id, min_x, min_y, min_z, max_x,
    max_y, max_z`` — exactly matches ``row_to_feature_bounds`` expectations.
    """
    # Identifiers are caller-controlled (validated upstream via
    # validate_sql_identifier in the class wrapper); this builder assumes
    # they're safe. Both sides of the JOIN use validated schema-qualified
    # double-quoted identifiers.
    hub = f'"{spec.schema}"."{spec.hub_table}"'
    geoms = f'"{spec.schema}"."{spec.geometries_table}"'

    zmin_expr = f"ST_ZMin(ST_Force3D(g.{spec.geom_column}))"
    zmax_expr = f"ST_ZMax(ST_Force3D(g.{spec.geom_column}))"
    if spec.height_column:
        # 2D feature → ST_ZMin/ST_ZMax on ST_Force3D return 0. Widen the
        # z range to [0, height_col] when the hub carries a height column.
        zmin_expr = f"LEAST({zmin_expr}, 0)"
        zmax_expr = (
            f"GREATEST({zmax_expr}, "
            f'COALESCE(h."{spec.height_column}", 0))'
        )

    sql = (
        "SELECT "
        f'h."{spec.feature_id_column}" AS feature_id, '
        f"ST_XMin(g.{spec.geom_column}) AS min_x, "
        f"ST_YMin(g.{spec.geom_column}) AS min_y, "
        f"{zmin_expr} AS min_z, "
        f"ST_XMax(g.{spec.geom_column}) AS max_x, "
        f"ST_YMax(g.{spec.geom_column}) AS max_y, "
        f"{zmax_expr} AS max_z "
        f"FROM {hub} h "
        f"JOIN {geoms} g "
        f'ON h."{spec.feature_id_column}" = g."{spec.feature_id_column}" '
        f"WHERE g.{spec.geom_column} IS NOT NULL"
    )
    if spec.limit is not None:
        sql += f" LIMIT {int(spec.limit)}"
    return sql


def row_to_feature_bounds(row: Dict[str, Any]) -> FeatureBounds:
    """Translate a row dict from build_bounds_query into a FeatureBounds.

    Accepts either ``str`` or non-str feature_id — the FeatureBounds
    dataclass stringifies it for stable tile hashing downstream.
    """
    return FeatureBounds(
        feature_id=str(row["feature_id"]),
        min_x=float(row["min_x"]),
        min_y=float(row["min_y"]),
        min_z=float(row["min_z"]),
        max_x=float(row["max_x"]),
        max_y=float(row["max_y"]),
        max_z=float(row["max_z"]),
    )


def rows_to_bounds(rows: Iterable[Dict[str, Any]]) -> Sequence[FeatureBounds]:
    """Drain an iterable of rows into a list of FeatureBounds.

    Skips rows where any coordinate is NULL (defensive against partial
    sidecar data — the WHERE clause already filters null geoms, but
    Force3D on a degenerate geometry can still emit NULLs).
    """
    out = []
    for r in rows:
        if any(
            r.get(k) is None
            for k in (
                "feature_id",
                "min_x",
                "min_y",
                "min_z",
                "max_x",
                "max_y",
                "max_z",
            )
        ):
            continue
        out.append(row_to_feature_bounds(r))
    return out
