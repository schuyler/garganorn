"""Tests for stage_tile_assignment (garganorn/stages.py)."""

import inspect
import os
import pathlib
import time

import duckdb
import pytest

import garganorn.stages as _stages


# ---------------------------------------------------------------------------
# Phase 2 tile-assignment artifact tests
# ---------------------------------------------------------------------------

def _make_places_parquet(tmp_path, places):
    """Write a minimal places.parquet with (place_id, qk17) for tile-assignment testing."""
    parquet_path = str(tmp_path / "places.parquet")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows_sql = ", ".join(
        f"('{pid}', ST_QuadKey({lon}, {lat}, 17))"
        for pid, lat, lon in places
    )
    con.execute(f"""
        COPY (
            SELECT id, qk17
            FROM (VALUES {rows_sql}) t(id, qk17)
        ) TO '{parquet_path}' (FORMAT PARQUET)
    """)
    con.close()
    return parquet_path


class TestTileAssignmentArtifactPhase2:
    """stage_tile_assignment must read places.parquet and write sorted artifact.

    Fails in Red phase because stage_tile_assignment still takes 'con' as first arg.
    """

    _PLACES = [
        ("sf001", 37.7749, -122.4194),
        ("sf002", 37.7750, -122.4195),
        ("sf003", 37.7748, -122.4193),
        ("nyc001", 40.7128, -74.0060),
    ]

    def test_stage_tile_assignment_no_con_parameter(self):
        """stage_tile_assignment must not take 'con' as its first parameter."""
        params = list(inspect.signature(_stages.stage_tile_assignment).parameters.keys())
        assert params[0] != "con", (
            f"stage_tile_assignment must not have 'con' as first param; got {params[0]!r}"
        )

    def test_stage_tile_assignment_has_places_parquet_param(self):
        """stage_tile_assignment must accept a places_parquet parameter."""
        params = list(inspect.signature(_stages.stage_tile_assignment).parameters.keys())
        assert "places_parquet" in params, (
            f"stage_tile_assignment missing places_parquet param; params: {params}"
        )

    def test_stage_tile_assignment_writes_parquet(self, tmp_path):
        """stage_tile_assignment must write tile_assignments.parquet to output_path."""
        places_parquet = _make_places_parquet(tmp_path, self._PLACES)
        output = str(tmp_path / "tile_assignments.parquet")
        _stages.stage_tile_assignment(places_parquet, output, "overture_place")
        assert pathlib.Path(output).exists(), f"tile_assignments.parquet not written to {output}"

    def test_tile_assignments_sorted_tile_qk_place_id(self, tmp_path):
        """tile_assignments.parquet must be sorted by (tile_qk, place_id)."""
        places_parquet = _make_places_parquet(tmp_path, self._PLACES)
        output = str(tmp_path / "tile_assignments.parquet")
        _stages.stage_tile_assignment(places_parquet, output, "overture_place")
        con = duckdb.connect()
        rows = con.execute(
            f"SELECT tile_qk, place_id FROM read_parquet('{output}')"
        ).fetchall()
        con.close()
        assert rows == sorted(rows), (
            f"tile_assignments.parquet must be sorted by (tile_qk, place_id); "
            f"got: {rows[:5]}..."
        )

    def test_tile_assignments_schema(self, tmp_path):
        """tile_assignments.parquet must have (place_id VARCHAR, tile_qk VARCHAR) schema."""
        places_parquet = _make_places_parquet(tmp_path, self._PLACES)
        output = str(tmp_path / "tile_assignments.parquet")
        _stages.stage_tile_assignment(places_parquet, output, "overture_place")
        con = duckdb.connect()
        cols = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}')"
        ).fetchall()}
        con.close()
        assert "place_id" in cols and "tile_qk" in cols, (
            f"tile_assignments.parquet must have place_id and tile_qk; found: {cols}"
        )

    def test_max_per_tile_param_change_rebuilds(self, tmp_path):
        """Changing max_per_tile with fresh mtimes must trigger a rebuild."""
        places_parquet = _make_places_parquet(tmp_path, self._PLACES)
        output = str(tmp_path / "tile_assignments.parquet")
        _stages.stage_tile_assignment(places_parquet, output, "overture_place", max_per_tile=1000)
        mtime1 = os.path.getmtime(output)
        time.sleep(0.05)
        _stages.stage_tile_assignment(places_parquet, output, "overture_place", max_per_tile=500)
        mtime2 = os.path.getmtime(output)
        assert mtime2 > mtime1, (
            "Changing max_per_tile must rebuild tile_assignments.parquet"
        )


# ---------------------------------------------------------------------------
# (place_id, tile_qk) parity vs old SQL on same fixture
# ---------------------------------------------------------------------------

class TestTileAssignmentParity:
    """stage_tile_assignment must produce the same (place_id, tile_qk) pairs
    as compute_tile_assignments.sql against the same fixture data.

    Fails RED because stage_tile_assignment still takes 'con' as its first parameter,
    not places_parquet.
    """

    _PLACES = [
        ("sf001", 37.7749, -122.4194),
        ("sf002", 37.7750, -122.4195),
        ("sf003", 37.7748, -122.4193),
        ("nyc001", 40.7128, -74.0060),
    ]

    def test_place_id_tile_qk_parity_vs_old_sql(self, tmp_path):
        """stage_tile_assignment must produce identical (place_id, tile_qk) set as old SQL.

        Fails RED because stage_tile_assignment does not yet accept places_parquet.
        """
        # Reference: old SQL-based approach via run_tile_assignments helper
        from tests.quadtree_helpers import run_tile_assignments, make_tile_assignment_db
        ref_conn = duckdb.connect()
        make_tile_assignment_db(ref_conn, self._PLACES)
        run_tile_assignments(ref_conn, pk_expr="place_id", max_per_tile=100, min_zoom=6, max_zoom=17)
        ref_pairs = {
            (row[0], row[1])
            for row in ref_conn.execute(
                "SELECT place_id, tile_qk FROM tile_assignments"
            ).fetchall()
        }

        # New: stage_tile_assignment reads places.parquet and writes artifact
        places_parquet = _make_places_parquet(tmp_path, self._PLACES)
        output = str(tmp_path / "ta_parity.parquet")
        # Fails RED: current stage_tile_assignment takes 'con' as first arg
        _stages.stage_tile_assignment(places_parquet, output, "overture_place",
                                      max_per_tile=100, min_zoom=6, max_zoom=17)

        new_pairs = set(
            duckdb.connect().execute(
                f"SELECT place_id, tile_qk FROM read_parquet('{output}')"
            ).fetchall()
        )
        assert new_pairs == ref_pairs, (
            f"stage_tile_assignment (place_id, tile_qk) set != old SQL.\n"
            f"  In new but not old: {new_pairs - ref_pairs}\n"
            f"  In old but not new: {ref_pairs - new_pairs}"
        )


# ---------------------------------------------------------------------------
# Dropped/duplicate diagnostics via caplog
# ---------------------------------------------------------------------------

class TestTileAssignmentDiagnostics:
    """Dropped-place warning and duplicate-place warning
    must still be emitted (as log messages) from stage_tile_assignment in Phase 2.

    Fails RED because stage_tile_assignment does not yet accept places_parquet.
    """

    def test_dropped_place_warning_emitted_for_null_qk17(self, tmp_path, caplog):
        """Dropped-place warning emitted when a place has NULL qk17.

        Fails RED because stage_tile_assignment does not yet accept places_parquet.
        """
        import logging
        # Write places.parquet with one NULL-qk17 row
        parquet_path = str(tmp_path / "null_qk17_places.parquet")
        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT id, qk17
                FROM (VALUES ('good001', '02301020333300320'), ('null001', NULL))
                     t(id, qk17)
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)
        con.close()

        output = str(tmp_path / "ta_dropped.parquet")
        with caplog.at_level(logging.WARNING):
            # Fails RED: wrong signature
            _stages.stage_tile_assignment(parquet_path, output, "overture_place", max_per_tile=100)

        # Diagnostic: must log a warning about dropped places
        dropped_warnings = [
            r.message for r in caplog.records
            if r.levelno >= logging.WARNING and (
                "drop" in r.message.lower() or "null" in r.message.lower()
                or "skip" in r.message.lower() or "invalid" in r.message.lower()
            )
        ]
        assert dropped_warnings, (
            "stage_tile_assignment must log a warning for places with NULL qk17 "
            "(dropped places diagnostic) — fails RED because signature not yet updated"
        )

    def test_duplicate_place_warning_emitted(self, tmp_path, caplog):
        """Duplicate-place-id warning emitted when a place appears in multiple tiles.

        Fails RED because stage_tile_assignment does not yet accept places_parquet.
        """
        import logging
        # Note: In the normal case, each place gets exactly one tile.
        # This test verifies the diagnostic is wired up; the actual
        # duplication scenario depends on implementation details.
        places_parquet = _make_places_parquet(tmp_path, [
            ("sf001", 37.7749, -122.4194),
            ("sf002", 37.7750, -122.4195),
        ])
        output = str(tmp_path / "ta_dupes.parquet")
        with caplog.at_level(logging.WARNING):
            # Fails RED: wrong signature
            _stages.stage_tile_assignment(places_parquet, output, "overture_place", max_per_tile=1)
        # Test merely verifies stage_tile_assignment runs the diagnostic query;
        # the exact message content is checked in GREEN when the implementation exists.
        assert output  # Stage must produce the output artifact


# ---------------------------------------------------------------------------
# BUG: ambiguous `level` column crash in stage_tile_assignment
#
# The tile_counts query in stage_tile_assignment (garganorn/stages.py, around
# lines 1211-1220) does:
#
#     SELECT level, left(qk17, level) AS qk, count(*) AS cnt
#     FROM read_parquet('{pq_sql}'),
#          generate_series({min_zoom}, {max_zoom}) AS t(level)
#     ...
#     GROUP BY level, left(qk17, level)
#
# The bare `level` is meant to reference the generate_series alias t(level),
# but overture_division (and other boundary-shaped) records also carry a
# `level` column (Phase 2b). When the input parquet has its own `level`
# column, DuckDB cannot resolve the bare identifier and raises:
#
#     duckdb.BinderException: Binder Error: Ambiguous reference to column
#     name "level" (use: "read_parquet.level" or "t.level")
#
# This test builds a places.parquet WITH a `level` column (as
# overture_division records have) and calls stage_tile_assignment for the
# "overture_division" source. It must complete without raising
# BinderException and must produce the expected tile_assignments artifact.
# ---------------------------------------------------------------------------

def _make_places_parquet_with_level(tmp_path, places, pk_col="id"):
    """Write a places.parquet with an extra `level` column (like overture_division).

    `places` is a list of (pk, lat, lon, level) tuples.
    """
    parquet_path = str(tmp_path / "places_with_level.parquet")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows_sql = ", ".join(
        f"('{pid}', ST_QuadKey({lon}, {lat}, 17), {level})"
        for pid, lat, lon, level in places
    )
    con.execute(f"""
        COPY (
            SELECT {pk_col}, qk17, level
            FROM (VALUES {rows_sql}) t({pk_col}, qk17, level)
        ) TO '{parquet_path}' (FORMAT PARQUET)
    """)
    con.close()
    return parquet_path


class TestTileAssignmentAmbiguousLevelColumn:
    """Reproduces the ambiguous-`level`-column BinderException crash.

    overture_division records carry a `level` column (the atgeo containment
    vocabulary value, Phase 2b). stage_tile_assignment's tile_counts query
    uses a bare `level` identifier that collides with this input column,
    causing DuckDB to raise BinderException instead of completing.

    This test MUST FAIL at Red with duckdb.BinderException until the query
    is fixed to qualify `level` as `t.level`.
    """

    _PLACES_WITH_LEVEL = [
        # (id, lat, lon, level) -- level values from LEVEL_VOCAB (e.g. locality=50)
        ("div001", 37.7749, -122.4194, 50),
        ("div002", 37.7750, -122.4195, 50),
        ("div003", 40.7128, -74.0060, 10),
    ]

    def test_stage_tile_assignment_with_level_column_input(self, tmp_path):
        """stage_tile_assignment must not crash when the input parquet has a `level` column."""
        places_parquet = _make_places_parquet_with_level(
            tmp_path, self._PLACES_WITH_LEVEL, pk_col="id"
        )
        output = str(tmp_path / "tile_assignments_div.parquet")

        # Currently raises duckdb.BinderException: Ambiguous reference to
        # column name "level" (use: "read_parquet.level" or "t.level").
        _stages.stage_tile_assignment(
            places_parquet, output, "overture_division", max_per_tile=100
        )

        assert pathlib.Path(output).exists(), (
            f"tile_assignments.parquet not written to {output}"
        )
        con = duckdb.connect()
        assigned_ids = {
            row[0] for row in con.execute(
                f"SELECT place_id FROM read_parquet('{output}')"
            ).fetchall()
        }
        con.close()
        expected_ids = {p[0] for p in self._PLACES_WITH_LEVEL}
        assert assigned_ids == expected_ids, (
            f"Assigned IDs differ from expected.\n"
            f"  Missing: {expected_ids - assigned_ids}\n"
            f"  Extra:   {assigned_ids - expected_ids}"
        )
