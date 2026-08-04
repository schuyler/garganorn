"""Tests for overture_place_import.sql."""

import inspect
import json
import os
import pathlib
import time

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    OV_BBOX, run_overture_import,
)

import garganorn.stages as _stages


# ---------------------------------------------------------------------------
# Tests: overture_import.sql
# ---------------------------------------------------------------------------

class TestOvertureImport:
    """Tests for garganorn/sql/overture_place_import.sql."""

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def test_places_table_created(self, overture_parquet, tmp_path):
        """After import, the `places` table must exist."""
        db_path = tmp_path / "test_ov_import.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_id_column(self, overture_parquet, tmp_path):
        """After import, `places` must have an `id` column."""
        db_path = tmp_path / "test_ov_id_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "id" in cols, f"id column missing; found columns: {cols}"

    def test_places_has_qk17_column(self, overture_parquet, tmp_path):
        """After import, `places` must have a `qk17` column."""
        db_path = tmp_path / "test_ov_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "qk17" in cols, f"qk17 column missing; found columns: {cols}"

    def test_places_expected_columns(self, overture_parquet, tmp_path):
        """After import, `places` must include the columns expected downstream."""
        required = {"id", "bbox", "qk17"}
        db_path = tmp_path / "test_ov_cols.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, overture_parquet, tmp_path):
        """Rows outside the bbox must be excluded."""
        db_path = tmp_path / "test_ov_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        assert "ov006" not in ids, "Out-of-bbox row (ov006) must be excluded"

    def test_null_geometry_excluded(self, overture_parquet, tmp_path):
        """Rows with geometry IS NULL must be excluded."""
        db_path = tmp_path / "test_ov_null_geom.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        assert "ov007" not in ids, "Null-geometry row (ov007) must be excluded"

    def test_good_rows_included(self, overture_parquet, tmp_path):
        """All in-bbox, non-null-geometry rows must survive the import filters."""
        expected = {"ov001", "ov002", "ov003", "ov004", "ov005"}
        db_path = tmp_path / "test_ov_good.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        missing = expected - ids
        assert not missing, f"Expected rows missing after import: {missing}"

    def test_qk17_is_17_chars(self, overture_parquet, tmp_path):
        """qk17 values must be 17-character strings for all surviving rows."""
        db_path = tmp_path / "test_ov_qk17_len.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        bad = conn.execute("""
            SELECT id, qk17
            FROM places
            WHERE qk17 IS NULL OR length(qk17) != 17
        """).fetchall()
        conn.close()
        assert not bad, f"Rows with invalid qk17: {bad}"

    def test_qk17_contains_only_valid_chars(self, overture_parquet, tmp_path):
        """qk17 values must consist only of digits 0-3 (quadkey alphabet)."""
        db_path = tmp_path / "test_ov_qk17_chars.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        bad = conn.execute("""
            SELECT id, qk17
            FROM places
            WHERE NOT regexp_matches(qk17, '^[0-3]{17}$')
        """).fetchall()
        conn.close()
        assert not bad, f"qk17 values with unexpected characters: {bad}"

    def test_geometry_not_carried_into_places(self, overture_parquet, tmp_path):
        """The `places` table must NOT carry a geometry column at all.

        geometry is only needed for the existence filter (WHERE geometry IS
        NOT NULL) and the bbox-derived qk17 computation, both resolved before
        `places` is materialized. Nothing downstream (density/idf/variants
        joins, tile assignment, export) reads geometry — export builds points
        from bbox midpoints instead — so carrying it through those joins and
        the final CTAS only inflates sort/join spill for no benefit.
        """
        db_path = tmp_path / "test_ov_no_geom.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "geometry" not in cols, (
            f"places must not carry a geometry column; found columns: {cols}"
        )

    def test_import_sql_does_not_recast_geometry(self):
        """overture_place_import.sql must not recast/carry geometry into ov_base.

        geometry should be dropped via EXCLUDE in ov_base's own SELECT, not
        recast to GEOMETRY and carried through downstream CTEs only to be
        excluded at the final COPY.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"
        sql_text = sql_path.read_text()
        assert "geometry::GEOMETRY AS geometry" not in sql_text, (
            "overture_place_import.sql recasts and carries geometry through "
            "ov_base again. geometry should be dropped via EXCLUDE in "
            "ov_base's own SELECT instead."
        )


# ---------------------------------------------------------------------------
# §7.2 Phase 2 import artifact tests (RED — overture_place)
# ---------------------------------------------------------------------------

class TestOvertureImportArtifactPhase2:
    """§7.2: stage_import must write places.parquet without 'geometry' column.

    All tests fail in Red phase because stage_import still takes 'con'
    as its first positional argument.
    """

    _BBOX = (-122.55, 37.60, -122.30, 37.85)

    def test_stage_import_no_con_parameter(self):
        """stage_import must not take 'con' as its first parameter."""
        params = list(inspect.signature(_stages.stage_import).parameters.keys())
        assert params[0] != "con", (
            f"stage_import must not have 'con' as first param; got {params[0]!r}"
        )

    def test_stage_import_writes_places_parquet(self, overture_parquet, tmp_path):
        """stage_import must write places.parquet for overture_place."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_place", overture_parquet, self._BBOX, output)
        assert pathlib.Path(output).exists(), f"places.parquet not written to {output}"

    def test_places_parquet_no_geometry_column(self, overture_parquet, tmp_path):
        """places.parquet must not contain the 'geometry' column (§3.2 EXCLUDE)."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_place", overture_parquet, self._BBOX, output)
        con = duckdb.connect()
        cols = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}')"
        ).fetchall()}
        con.close()
        assert "geometry" not in cols, (
            f"places.parquet must not contain 'geometry' column; found: {cols}"
        )

    def test_places_parquet_retains_bbox_struct(self, overture_parquet, tmp_path):
        """places.parquet must retain the 'bbox' struct (used by _coord_exprs)."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_place", overture_parquet, self._BBOX, output)
        con = duckdb.connect()
        cols = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}')"
        ).fetchall()}
        con.close()
        assert "bbox" in cols, (
            f"places.parquet must retain 'bbox' struct; found: {cols}"
        )

    def test_places_parquet_qk17_sorted_nulls_last(self, overture_parquet, tmp_path):
        """places.parquet must be sorted by qk17 NULLS LAST."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_place", overture_parquet, self._BBOX, output)
        con = duckdb.connect()
        rows = [r[0] for r in con.execute(
            f"SELECT qk17 FROM read_parquet('{output}')"
        ).fetchall()]
        con.close()
        non_null = [v for v in rows if v is not None]
        assert non_null == sorted(non_null), "qk17 values must be in sorted order"
