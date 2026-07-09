"""Tests for foursquare_import.sql."""

import inspect
import json
import os
import pathlib
import time

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    SF_BBOX, FSQ_ROWS, run_fsq_import,
)

import garganorn.stages as _stages


# ---------------------------------------------------------------------------
# Tests: foursquare_import.sql
# ---------------------------------------------------------------------------

class TestFsqImport:
    """Tests for garganorn/sql/foursquare_import.sql."""

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "foursquare_import.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def test_places_table_created(self, fsq_parquet, tmp_path):
        """After import, the `places` table must exist."""
        db_path = tmp_path / "test_import.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_qk17_column(self, fsq_parquet, tmp_path):
        """After import, `places` must have a qk17 column."""
        db_path = tmp_path / "test_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "qk17" in cols, f"qk17 column missing; found columns: {cols}"

    def test_places_expected_columns(self, fsq_parquet, tmp_path):
        """After import, `places` must include the columns expected downstream."""
        required = {"fsq_place_id", "name", "latitude", "longitude", "geom",
                    "date_refreshed", "date_closed", "bbox", "fsq_category_ids", "qk17"}
        db_path = tmp_path / "test_cols.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, fsq_parquet, tmp_path):
        """Rows outside the bbox must be excluded."""
        db_path = tmp_path / "test_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq003" not in ids, "Out-of-bbox row must be excluded"

    def test_closed_places_excluded(self, fsq_parquet, tmp_path):
        """Rows with date_closed IS NOT NULL must be excluded."""
        db_path = tmp_path / "test_closed.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq004" not in ids, "Closed place must be excluded"

    def test_zero_longitude_excluded(self, fsq_parquet, tmp_path):
        """Rows with longitude == 0 must be excluded."""
        db_path = tmp_path / "test_zero_lon.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq005" not in ids, "Zero-longitude row must be excluded"

    def test_null_geom_excluded(self, fsq_parquet, tmp_path):
        """Rows with geom IS NULL must be excluded."""
        db_path = tmp_path / "test_null_geom.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq006" not in ids, "Null-geom row must be excluded"

    def test_stale_date_refreshed_excluded(self, fsq_parquet, tmp_path):
        """Rows with date_refreshed <= 2020-03-15 must be excluded."""
        db_path = tmp_path / "test_stale.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq007" not in ids, "Stale place must be excluded"

    def test_good_rows_included(self, fsq_parquet, tmp_path):
        """All rows that pass filters must appear in the result."""
        expected = {row[0] for row in FSQ_ROWS if row[-1]}
        db_path = tmp_path / "test_good.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        missing = expected - ids
        assert not missing, f"Expected rows missing after import: {missing}"

    def test_qk17_is_17_chars(self, fsq_parquet, tmp_path):
        """qk17 values must be 17-character strings for all surviving rows."""
        db_path = tmp_path / "test_qk17_len.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        bad = conn.execute("""
            SELECT fsq_place_id, qk17
            FROM places
            WHERE qk17 IS NULL OR length(qk17) != 17
        """).fetchall()
        conn.close()
        assert not bad, f"Rows with invalid qk17: {bad}"

    def test_qk17_contains_only_valid_chars(self, fsq_parquet, tmp_path):
        """qk17 values must consist only of digits 0-3 (quadkey alphabet)."""
        db_path = tmp_path / "test_qk17_chars.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        # regexp_full_match: entire string must be [0-3]{17}
        bad = conn.execute("""
            SELECT fsq_place_id, qk17
            FROM places
            WHERE NOT regexp_matches(qk17, '^[0-3]{17}$')
        """).fetchall()
        conn.close()
        assert not bad, f"qk17 values with unexpected characters: {bad}"

    def test_geom_column_is_geometry_type(self, fsq_parquet, tmp_path):
        """The geom column must be of GEOMETRY type after import."""
        db_path = tmp_path / "test_geom_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "geom" in describe, "geom column missing from places"
        assert describe["geom"] == "GEOMETRY", (
            f"Expected geom to be GEOMETRY, got {describe['geom']}"
        )

    def test_qk17_computed_in_ctas_not_alter_update(self):
        """qk17 must be computed inline in the CTAS, not via ALTER TABLE + UPDATE.

        The ALTER TABLE / UPDATE two-pass approach causes a full second scan of
        the places table. Once qk17 is folded into the CTAS SELECT list those
        two statements must not appear in the SQL file.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "foursquare_import.sql"
        sql = sql_path.read_text()
        assert "ALTER TABLE places ADD COLUMN qk17" not in sql, (
            "foursquare_import.sql still uses ALTER TABLE to add qk17; "
            "fold qk17 into the CTAS SELECT list instead"
        )
        assert "UPDATE places SET qk17" not in sql, (
            "foursquare_import.sql still uses UPDATE to populate qk17; "
            "fold qk17 into the CTAS SELECT list instead"
        )


# ---------------------------------------------------------------------------
# §7.2 Phase 2 import artifact tests (RED — foursquare)
# ---------------------------------------------------------------------------

class TestFsqImportArtifactPhase2:
    """§7.2: stage_import must write places.parquet (no working DB, no geom, qk17 sorted).

    All tests here fail in Red phase because stage_import still takes 'con'
    as its first positional argument and does not accept 'output_path'.
    """

    _BBOX = (-122.55, 37.60, -122.30, 37.85)

    def test_stage_import_no_con_parameter(self):
        """stage_import must not take 'con' as its first parameter (Phase 2 signature)."""
        params = list(inspect.signature(_stages.stage_import).parameters.keys())
        assert params[0] != "con", (
            f"stage_import must not have 'con' as first parameter; "
            f"got first param: {params[0]!r}. Phase 2 drops the connection argument."
        )

    def test_stage_import_has_output_path_param(self):
        """stage_import must accept an output_path parameter."""
        params = list(inspect.signature(_stages.stage_import).parameters.keys())
        assert "output_path" in params, (
            f"stage_import missing output_path parameter; params: {params}"
        )

    def test_stage_import_writes_places_parquet(self, fsq_parquet, tmp_path):
        """stage_import must write places.parquet to the given output_path."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)
        assert pathlib.Path(output).exists(), f"places.parquet not written to {output}"

    def test_places_parquet_no_geom_column(self, fsq_parquet, tmp_path):
        """places.parquet must not contain the 'geom' geometry column (§3.2 EXCLUDE)."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)
        con = duckdb.connect()
        cols = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}')"
        ).fetchall()}
        con.close()
        assert "geom" not in cols, (
            f"places.parquet must not contain 'geom' column; found: {cols}"
        )

    def test_places_parquet_qk17_sorted_nulls_last(self, fsq_parquet, tmp_path):
        """places.parquet must be sorted by qk17 NULLS LAST (load-bearing invariant D2)."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)
        con = duckdb.connect()
        rows = [r[0] for r in con.execute(
            f"SELECT qk17 FROM read_parquet('{output}')"
        ).fetchall()]
        con.close()
        non_null = [v for v in rows if v is not None]
        assert non_null == sorted(non_null), "qk17 values must be in sorted order"
        # NULLs must come after non-null values
        if None in rows and non_null:
            last_non_null_idx = max(i for i, v in enumerate(rows) if v is not None)
            first_null_idx = next(i for i, v in enumerate(rows) if v is None)
            assert first_null_idx > last_non_null_idx, "NULL qk17 values must sort last"

    def test_places_parquet_has_meta_sidecar(self, fsq_parquet, tmp_path):
        """stage_import must write a .meta.json sidecar alongside places.parquet."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)
        meta_path = pathlib.Path(output + ".meta.json")
        assert meta_path.exists(), f".meta.json sidecar missing at {meta_path}"
        meta = json.loads(meta_path.read_text())
        assert "params" in meta and "bbox" in meta.get("params", {}), (
            "meta must record bbox in params"
        )

    def test_freshness_second_call_noop(self, fsq_parquet, tmp_path):
        """Second call to stage_import with same inputs must be a no-op (mtime unchanged)."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)
        mtime1 = os.path.getmtime(output)
        time.sleep(0.05)
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)
        mtime2 = os.path.getmtime(output)
        assert mtime1 == mtime2, "Second call must skip (artifact mtime must not change)"

    def test_force_true_rebuilds(self, fsq_parquet, tmp_path):
        """force=True must rebuild even when the artifact is fresh."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)
        mtime1 = os.path.getmtime(output)
        time.sleep(0.05)
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output, force=True)
        mtime2 = os.path.getmtime(output)
        assert mtime2 > mtime1, "force=True must rebuild (mtime must increase)"

    def test_no_working_duckdb_in_output_tree(self, fsq_parquet, tmp_path):
        """stage_import must not leave any .duckdb working database under output_path."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)
        duckdb_files = list(tmp_path.rglob("*.duckdb"))
        assert not duckdb_files, (
            f"stage_import must not create .duckdb files; found: {duckdb_files}"
        )


# ---------------------------------------------------------------------------
# §7.2.3 — per-source row-set parity vs old CTE (foursquare)
# ---------------------------------------------------------------------------

class TestFsqImportRowSetParity:
    """§7.2.3: stage_import output row set must equal the old CTE result on the same fixture.

    The places.parquet from the Phase 2 stage_import must have:
      - The same place_id values as the old CREATE TABLE places AS … query
      - No geometry/geom column (§3.2 EXCLUDE)
      - Rows excluded by the old DELETE WHERE geom IS NULL logic must be absent

    Fails RED because stage_import does not yet write to a parquet output_path
    (still writes to a working DB via the old signature).
    """

    _BBOX = dict(xmin=-122.55, ymin=37.60, xmax=-122.30, ymax=37.85)

    def test_fsq_places_parquet_row_ids_match_old_cte(self, fsq_parquet, tmp_path):
        """Row IDs in places.parquet must equal the old CTE's places table IDs.

        Fails RED because stage_import(source, parquet, bbox, output_path) signature
        does not yet exist; current signature takes 'con' as first arg.
        """
        from tests.quadtree_helpers import run_fsq_import
        # Reference: old CTE-based approach.  Spatial must be loaded so that the
        # foursquare SQL's GEOMETRY cast works (run_fsq_import strips INSTALL/LOAD
        # lines, so spatial must already be active on the connection).
        ref_conn = duckdb.connect()
        ref_conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(ref_conn, fsq_parquet, self._BBOX)
        ref_ids = {
            row[0]
            for row in ref_conn.execute("SELECT fsq_place_id FROM places").fetchall()
        }

        # New Phase 2 approach: stage_import writes places.parquet
        output = str(tmp_path / "fsq_parity_places.parquet")
        # Fails RED: current stage_import does not accept (source, glob, bbox, output_path)
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)

        new_ids = {
            row[0]
            for row in duckdb.connect().execute(
                f"SELECT fsq_place_id FROM read_parquet('{output}')"
            ).fetchall()
        }
        assert new_ids == ref_ids, (
            f"Phase 2 places.parquet row IDs differ from old CTE.\n"
            f"  In new but not old: {new_ids - ref_ids}\n"
            f"  In old but not new: {ref_ids - new_ids}"
        )

    def test_fsq_places_parquet_no_geom_column(self, fsq_parquet, tmp_path):
        """places.parquet must not contain a 'geom' column (§3.2 EXCLUDE).

        This is a parity-enforcement test: the old CTE had a geom column;
        Phase 2 must EXCLUDE it.
        Fails RED for the same signature reason as the row-ID test.
        """
        output = str(tmp_path / "fsq_nogeom_places.parquet")
        _stages.stage_import("foursquare", fsq_parquet, self._BBOX, output)

        cols = {
            row[0]
            for row in duckdb.connect().execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output}')"
            ).fetchall()
        }
        assert "geom" not in cols, (
            f"places.parquet must not contain 'geom' column (§3.2 EXCLUDE); "
            f"found columns: {cols}"
        )


# ---------------------------------------------------------------------------
# §7.2 / SCORE-1/2/3/4: Norm constant validation (ported from
#   TestNormConstantValidation which used the Phase 1 con-based interface)
# ---------------------------------------------------------------------------

class TestImportNormValidation:
    """Norm constant validation for stage_import (Phase 2 signature).

    SCORE-1/2/3/4: stage_import must raise ValueError before any DuckDB work
    when a norm constant (density_norm, idf_norm, pop_norm) is <= 0.

    Ported from tests/test_audit_scoring.py::TestNormConstantValidation which
    tested the same behavior via the Phase 1 con-based interface.  The
    validation check runs before the ephemeral connection is opened, so these
    tests work with any well-formed output path and any valid-schema input
    parquet (the file is never actually read for the error cases).
    """

    def test_density_norm_zero_raises_error(self, tmp_path):
        """density_norm=0 raises ValueError (SCORE-1) before SQL execution."""
        with pytest.raises(ValueError, match="density_norm.*must be positive"):
            _stages.stage_import(
                "foursquare", "/dev/null", None,
                str(tmp_path / "out.parquet"),
                density_norm=0.0,
            )

    def test_density_norm_negative_raises_error(self, tmp_path):
        """density_norm<0 raises ValueError (SCORE-1) before SQL execution."""
        with pytest.raises(ValueError, match="density_norm.*must be positive"):
            _stages.stage_import(
                "foursquare", "/dev/null", None,
                str(tmp_path / "out.parquet"),
                density_norm=-10.0,
            )

    def test_idf_norm_zero_raises_error(self, tmp_path):
        """idf_norm=0 raises ValueError (SCORE-2) before SQL execution."""
        with pytest.raises(ValueError, match="idf_norm.*must be positive"):
            _stages.stage_import(
                "foursquare", "/dev/null", None,
                str(tmp_path / "out.parquet"),
                idf_norm=0.0,
            )

    def test_idf_norm_negative_raises_error(self, tmp_path):
        """idf_norm<0 raises ValueError (SCORE-2) before SQL execution."""
        with pytest.raises(ValueError, match="idf_norm.*must be positive"):
            _stages.stage_import(
                "foursquare", "/dev/null", None,
                str(tmp_path / "out.parquet"),
                idf_norm=-18.0,
            )

    def test_pop_norm_zero_raises_error(self, tmp_path):
        """pop_norm=0 raises ValueError (SCORE-3) before SQL execution.

        pop_norm is validated for all sources even though it is only used
        by overture_division; the check runs before the unsupported-source
        guard, so passing 'foursquare' still exercises the validation path.
        """
        with pytest.raises(ValueError, match="pop_norm.*must be positive"):
            _stages.stage_import(
                "foursquare", "/dev/null", None,
                str(tmp_path / "out.parquet"),
                pop_norm=0.0,
            )

    def test_pop_norm_negative_raises_error(self, tmp_path):
        """pop_norm<0 raises ValueError (SCORE-3) before SQL execution."""
        with pytest.raises(ValueError, match="pop_norm.*must be positive"):
            _stages.stage_import(
                "foursquare", "/dev/null", None,
                str(tmp_path / "out.parquet"),
                pop_norm=-20.0,
            )

    def test_multiple_invalid_norms_raise_error(self, tmp_path):
        """Multiple invalid norms raise ValueError naming all bad constants (SCORE-4)."""
        with pytest.raises(ValueError, match="density_norm.*idf_norm"):
            _stages.stage_import(
                "foursquare", "/dev/null", None,
                str(tmp_path / "out.parquet"),
                density_norm=-10.0,
                idf_norm=0.0,
            )

    def test_valid_norms_pass_validation(self, fsq_parquet, tmp_path):
        """All-positive norms must not raise ValueError during validation.

        This is a smoke test that valid norms clear the validation gate.
        The actual import runs to completion; if it fails for a different
        reason (e.g. SQL error) that is a separate concern.
        """
        # Should not raise ValueError from norm validation
        _stages.stage_import(
            "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
            str(tmp_path / "places.parquet"),
            density_norm=10.0, idf_norm=18.0, pop_norm=20.0,
        )
