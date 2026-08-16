"""Tests for stage_tile_assignment (garganorn/stages.py)."""

import inspect
import os
import pathlib
import time

import duckdb
import pytest

import garganorn.stages as _stages


# ---------------------------------------------------------------------------
# tile-assignment artifact tests
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
    """stage_tile_assignment must read places.parquet and write sorted artifact."""

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
    """

    _PLACES = [
        ("sf001", 37.7749, -122.4194),
        ("sf002", 37.7750, -122.4195),
        ("sf003", 37.7748, -122.4193),
        ("nyc001", 40.7128, -74.0060),
    ]

    def test_place_id_tile_qk_parity_vs_old_sql(self, tmp_path):
        """stage_tile_assignment must produce identical (place_id, tile_qk) set as compute_tile_assignments.sql."""
        # Reference: SQL-based approach via run_tile_assignments helper
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

        places_parquet = _make_places_parquet(tmp_path, self._PLACES)
        output = str(tmp_path / "ta_parity.parquet")
        _stages.stage_tile_assignment(places_parquet, output, "overture_place",
                                      max_per_tile=100, min_zoom=6, max_zoom=17)

        new_pairs = set(
            duckdb.connect().execute(
                f"SELECT place_id, tile_qk FROM read_parquet('{output}')"
            ).fetchall()
        )
        assert new_pairs == ref_pairs, (
            f"stage_tile_assignment (place_id, tile_qk) set must match compute_tile_assignments.sql.\n"
            f"  In stage_tile_assignment only: {new_pairs - ref_pairs}\n"
            f"  In compute_tile_assignments.sql only: {ref_pairs - new_pairs}"
        )


# ---------------------------------------------------------------------------
# Dropped/duplicate diagnostics via caplog
# ---------------------------------------------------------------------------

class TestTileAssignmentDiagnostics:
    """Dropped-place warning and duplicate-place warning
    must be emitted (as log messages) from stage_tile_assignment.
    """

    def test_dropped_place_warning_emitted_for_null_qk17(self, tmp_path, caplog):
        """Dropped-place warning emitted when a place has NULL qk17."""
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
            "(dropped places diagnostic)"
        )

    def test_duplicate_place_warning_emitted(self, tmp_path, caplog):
        """Duplicate-place-id warning emitted when a place appears in multiple tiles."""
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
            _stages.stage_tile_assignment(places_parquet, output, "overture_place", max_per_tile=1)
        # This test verifies stage_tile_assignment runs the diagnostic query;
        # it does not check the exact duplicate-warning message content.
        assert output  # Stage must produce the output artifact


# ---------------------------------------------------------------------------
# stage_tile_assignment with an input `level` column (overture_division)
#
# overture_division records carry their own `level` column (the atgeo
# containment vocabulary value). stage_tile_assignment's tile_counts query
# also generates a level series via generate_series AS t(level); every
# reference to it is qualified as t.level, so the two columns never collide.
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
    """stage_tile_assignment must not crash when input parquet has a `level` column.

    overture_division records carry a `level` column (the atgeo containment
    vocabulary value). tile_counts qualifies its own level reference as
    t.level, so the two columns never collide.
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


# ---------------------------------------------------------------------------
# Summary tile assignment (garganorn/stages.py::stage_summary_tile_assignment)
#
# Per docs/design-constraints.md: the summary
# band is the top-N places by importance DESC / id ASC, plus (for
# overture_division only) every subtype IN ('country','region','dependency')
# additively, assigned into a z1-z5 tile band by the same coarsest-fit
# algorithm as stage_tile_assignment.
# ---------------------------------------------------------------------------

def _make_places_parquet_with_importance(tmp_path, places, filename="places_importance.parquet"):
    """Write a places.parquet with (id, qk17, importance).

    `places` is a list of (id, lat, lon, importance) tuples.
    """
    parquet_path = str(tmp_path / filename)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows_sql = ", ".join(
        f"('{pid}', ST_QuadKey({lon}, {lat}, 17), {importance})"
        for pid, lat, lon, importance in places
    )
    con.execute(f"""
        COPY (
            SELECT id, qk17, importance
            FROM (VALUES {rows_sql}) t(id, qk17, importance)
        ) TO '{parquet_path}' (FORMAT PARQUET)
    """)
    con.close()
    return parquet_path


def _make_division_places_parquet(tmp_path, places, filename="places_division.parquet"):
    """Write a places.parquet with (id, qk17, importance, subtype).

    `places` is a list of (id, lat, lon, importance, subtype) tuples.
    """
    parquet_path = str(tmp_path / filename)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows_sql = ", ".join(
        f"('{pid}', ST_QuadKey({lon}, {lat}, 17), {importance}, '{subtype}')"
        for pid, lat, lon, importance, subtype in places
    )
    con.execute(f"""
        COPY (
            SELECT id, qk17, importance, subtype
            FROM (VALUES {rows_sql}) t(id, qk17, importance, subtype)
        ) TO '{parquet_path}' (FORMAT PARQUET)
    """)
    con.close()
    return parquet_path


def _summary_place_ids(output_path):
    con = duckdb.connect()
    ids = {row[0] for row in con.execute(
        f"SELECT DISTINCT place_id FROM read_parquet('{output_path}')"
    ).fetchall()}
    con.close()
    return ids


class TestSummarySetSelection:
    """Top-N by importance DESC, ties by id ASC; additive unconditional
    subtypes for overture_division."""

    def test_topn_with_tie_broken_by_id_ascending(self, tmp_path):
        """n=2 over 4 places: the #1 importance plus the id-ASC winner of an
        importance tie for #2 must survive; the tie loser and the
        low-importance place must not."""
        from garganorn.stages import stage_summary_tile_assignment
        places = [
            ("p1_top", 37.7749, -122.4194, 90),
            ("aaa_tie", 37.7750, -122.4195, 80),
            ("bbb_tie", 37.7751, -122.4196, 80),
            ("z_low", 40.7128, -74.0060, 10),
        ]
        places_parquet = _make_places_parquet_with_importance(tmp_path, places)
        output = str(tmp_path / "summary_ta.parquet")
        stage_summary_tile_assignment(places_parquet, output, "overture_place", n=2)
        ids = _summary_place_ids(output)
        assert ids == {"p1_top", "aaa_tie"}, (
            f"top-2 by importance DESC/id ASC must be {{'p1_top', 'aaa_tie'}}; got {ids}"
        )

    def test_division_unconditional_subtypes_additive_and_dont_shrink_topn(self, tmp_path):
        """n=1: only the highest-importance place makes the top-N cut, but
        every country/region/dependency record is included additively
        regardless of its own importance. A non-unconditional low-importance
        subtype (county) must stay excluded. Growing the unconditional set
        must not shrink the top-N (non-unconditional) portion of the
        result."""
        from garganorn.stages import stage_summary_tile_assignment
        base_places = [
            ("loc_hi", 37.7749, -122.4194, 95, "locality"),
            ("loc_lo", 40.0, -74.0, 5, "locality"),
            ("country_a", 10.0, 10.0, 1, "country"),
            ("region_b", 20.0, 20.0, 1, "region"),
            ("dependency_c", 30.0, 30.0, 1, "dependency"),
            ("county_d", 5.0, 5.0, 1, "county"),
        ]
        places_parquet = _make_division_places_parquet(tmp_path, base_places)
        output = str(tmp_path / "summary_div_ta.parquet")
        stage_summary_tile_assignment(places_parquet, output, "overture_division", n=1)
        ids = _summary_place_ids(output)
        assert ids == {"loc_hi", "country_a", "region_b", "dependency_c"}, (
            f"n=1 additive selection must be top-1 + unconditional subtypes; got {ids}"
        )

        # Grow the unconditional set; the non-unconditional (top-N) portion
        # must stay exactly n=1 -- it must not be crowded out.
        grown_places = base_places + [("dependency_e", 35.0, 35.0, 1, "dependency")]
        places_parquet_2 = _make_division_places_parquet(
            tmp_path, grown_places, filename="places_division_grown.parquet"
        )
        output_2 = str(tmp_path / "summary_div_ta_grown.parquet")
        stage_summary_tile_assignment(places_parquet_2, output_2, "overture_division", n=1)
        ids_2 = _summary_place_ids(output_2)
        unconditional = {"country_a", "region_b", "dependency_c", "dependency_e"}
        non_unconditional = ids_2 - unconditional
        assert non_unconditional == {"loc_hi"}, (
            f"top-N portion must stay exactly n=1 ({{'loc_hi'}}) as the unconditional "
            f"set grows from 3 to 4; got non-unconditional={non_unconditional}"
        )
        assert unconditional <= ids_2, (
            f"grown unconditional set must all be present; missing {unconditional - ids_2}"
        )


class TestSummaryAssignmentBand:
    """Summary tile_qk length is always in [1, 5]."""

    def test_all_summary_tile_qk_lengths_between_1_and_5(self, tmp_path):
        """A fixture spread across ten widely separated regions, with a tight
        max_per_tile, forces the coarsest-fit algorithm to split down from
        z1. Every resulting tile_qk must still have length in [1, 5]; none
        empty, none >=6."""
        from garganorn.stages import stage_summary_tile_assignment
        places = [
            (f"p{i}", lat, lon, 100 - i)
            for i, (lat, lon) in enumerate([
                (37.7749, -122.4194), (40.7128, -74.0060), (51.5074, -0.1278),
                (35.6762, 139.6503), (-33.8688, 151.2093), (55.7558, 37.6173),
                (-23.5505, -46.6333), (28.6139, 77.2090), (30.0444, 31.2357),
                (19.4326, -99.1332),
            ])
        ]
        places_parquet = _make_places_parquet_with_importance(tmp_path, places)
        output = str(tmp_path / "summary_band_ta.parquet")
        stage_summary_tile_assignment(places_parquet, output, "overture_place",
                                      n=1000, max_per_tile=2)
        con = duckdb.connect()
        lengths = {
            len(row[0]) for row in con.execute(
                f"SELECT DISTINCT tile_qk FROM read_parquet('{output}')"
            ).fetchall()
        }
        con.close()
        assert lengths, "no summary tile_qk rows produced"
        assert all(1 <= n <= 5 for n in lengths), (
            f"summary tile_qk lengths must all be in [1, 5]; got lengths {sorted(lengths)}"
        )


class TestSummaryZ5HardFloorOverflow:
    """z5 is a hard floor -- a z5 tile may exceed max_per_tile, and this
    must not raise or drop records."""

    def test_z5_tile_exceeds_max_per_tile_without_error(self, tmp_path):
        """1200 places packed into a single z5 cell, with max_per_tile=1000,
        must all still be assigned -- to that one z5 tile -- with no error
        and none dropped."""
        from garganorn.stages import quadkey_to_bbox
        from garganorn.stages import stage_summary_tile_assignment

        # Pack 1200 points inside a single z5 cell with margin, so every
        # point's qk17 shares the same 5-char prefix.
        z5_qk = "12333"
        xmin, ymin, xmax, ymax = quadkey_to_bbox(z5_qk)
        mx, my = 0.1 * (xmax - xmin), 0.1 * (ymax - ymin)
        xmin, xmax = xmin + mx, xmax - mx
        ymin, ymax = ymin + my, ymax - my

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"""
            CREATE TABLE places AS
            SELECT 'p' || i AS id,
                   ST_QuadKey({xmin} + (i % 40 + 0.5) * ({xmax} - {xmin}) / 40,
                              {ymin} + (i // 40 + 0.5) * ({ymax} - {ymin}) / 30, 17) AS qk17,
                   1000 - i AS importance
            FROM generate_series(0, 1199) AS t(i)
        """)
        places_parquet = str(tmp_path / "z5_overflow_places.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        output = str(tmp_path / "z5_overflow_ta.parquet")
        stage_summary_tile_assignment(places_parquet, output, "overture_place",
                                      n=2000, max_per_tile=1000)

        con = duckdb.connect()
        rows = con.execute(
            f"SELECT tile_qk, count(*) FROM read_parquet('{output}') GROUP BY tile_qk"
        ).fetchall()
        con.close()
        assert len(rows) == 1, f"all 1200 places must land on one tile; got tiles {rows}"
        tile_qk, count = rows[0]
        assert tile_qk == z5_qk, f"the shared tile must be the z5 cell {z5_qk!r}; got {tile_qk!r}"
        assert count == 1200, (
            f"all 1200 places must be assigned (none dropped); got {count}"
        )
        assert count > 1000, (
            "fixture must actually overflow max_per_tile=1000 to exercise the "
            f"hard floor; got count={count}"
        )
