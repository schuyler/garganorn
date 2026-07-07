"""Tests for pipeline checkpoint/resume feature."""
from datetime import datetime, timezone

import duckdb
import pytest

from garganorn.stages import compute_containment


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_db(tmp_path):
    """Create a fresh DuckDB connection with sentinel table."""
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    from garganorn.quadtree import _ensure_sentinel_table
    _ensure_sentinel_table(con)
    yield con
    con.close()


# ---------------------------------------------------------------------------
# Test class organization: grouped by functionality being tested
# ---------------------------------------------------------------------------

class TestSentinelTableHelpers:
    """Tests for _ensure_sentinel_table, _read_sentinel, _mark_complete."""

    def test_ensure_sentinel_table_creates_table(self, fresh_db):
        """_ensure_sentinel_table creates the _pipeline_progress table."""
        # Table should exist with correct schema
        schema = fresh_db.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '_pipeline_progress'
            ORDER BY ordinal_position
        """).fetchall()
        assert len(schema) == 2
        assert schema[0][0] == "stage"
        assert schema[0][1] in ("VARCHAR", "TEXT")
        assert schema[1][0] == "completed_at"
        assert schema[1][1] in ("VARCHAR", "TEXT")

    def test_ensure_sentinel_table_idempotent(self, fresh_db):
        """_ensure_sentinel_table is idempotent — calling twice is safe."""
        from garganorn.quadtree import _ensure_sentinel_table

        _ensure_sentinel_table(fresh_db)  # Should not error

        # Table should exist (not duplicated)
        result = fresh_db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = '_pipeline_progress'"
        ).fetchone()[0]
        assert result == 1

    def test_read_sentinel_empty_db(self, fresh_db):
        """_read_sentinel returns empty set for new database."""
        from garganorn.quadtree import _read_sentinel

        completed = _read_sentinel(fresh_db)
        assert completed == set()

    def test_read_sentinel_after_mark_complete(self, fresh_db):
        """_read_sentinel returns correct set after _mark_complete."""
        from garganorn.quadtree import _mark_complete, _read_sentinel

        _mark_complete(fresh_db, "import")
        _mark_complete(fresh_db, "importance")

        completed = _read_sentinel(fresh_db)
        assert completed == {"import", "importance"}

    def test_mark_complete_inserts_row(self, fresh_db):
        """_mark_complete inserts a row with stage and timestamp."""
        from garganorn.quadtree import _mark_complete

        before = datetime.now(timezone.utc)
        _mark_complete(fresh_db, "import")
        after = datetime.now(timezone.utc)

        rows = fresh_db.execute(
            "SELECT stage, completed_at FROM _pipeline_progress"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "import"

        # Timestamp should be ISO format and recent
        completed_at = datetime.fromisoformat(rows[0][1])
        assert before <= completed_at <= after

    def test_mark_complete_remarks_same_stage(self, fresh_db):
        """_mark_complete handles re-marking the same stage (updates timestamp)."""
        from garganorn.quadtree import _mark_complete

        # Mark complete once
        _mark_complete(fresh_db, "import")
        first_timestamp = fresh_db.execute(
            "SELECT completed_at FROM _pipeline_progress WHERE stage = 'import'"
        ).fetchone()[0]

        # Mark complete again (should update, not duplicate)
        _mark_complete(fresh_db, "import")
        second_timestamp = fresh_db.execute(
            "SELECT completed_at FROM _pipeline_progress WHERE stage = 'import'"
        ).fetchone()[0]

        # Should still be only one row with updated timestamp
        count = fresh_db.execute(
            "SELECT COUNT(*) FROM _pipeline_progress"
        ).fetchone()[0]
        assert count == 1
        assert first_timestamp != second_timestamp

    def test_mark_complete_calls_checkpoint(self, tmp_path):
        """_mark_complete calls CHECKPOINT after insert."""
        from garganorn.quadtree import _ensure_sentinel_table, _mark_complete

        db_path = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_path))
        _ensure_sentinel_table(con)
        _mark_complete(con, "import")
        con.close()

        # Reopen and verify persistence
        con = duckdb.connect(str(db_path))
        rows = con.execute(
            "SELECT stage FROM _pipeline_progress WHERE stage = 'import'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "import"
        con.close()


class TestStageOrder:
    """Tests for STAGE_ORDER constant (Phase 2: now a simple list)."""

    def test_stage_order_is_list(self):
        """STAGE_ORDER is a simple list after Phase 2 restructuring."""
        from garganorn.quadtree import STAGE_ORDER

        assert isinstance(STAGE_ORDER, list)

    def test_stage_order_has_expected_stages(self):
        """STAGE_ORDER contains the expected stage names in order."""
        from garganorn.quadtree import STAGE_ORDER

        assert STAGE_ORDER == [
            "import",
            "tile_assignment",
            "containment",
            "export",
            "manifest",
        ]

    def test_stage_order_no_old_stages(self):
        """STAGE_ORDER does not contain old stage names."""
        from garganorn.quadtree import STAGE_ORDER

        old_stage_names = ['importance', 'variants', 'division_importance_backfill']
        for old_name in old_stage_names:
            assert old_name not in STAGE_ORDER


class TestFindIncompleteRun:
    """Tests for _find_incomplete_run function."""

    def test_returns_most_recent_incomplete_run(self, tmp_path):
        """_find_incomplete_run returns the most recent incomplete run."""
        from garganorn.quadtree import _find_incomplete_run

        source_dir = tmp_path / "foursquare"
        source_dir.mkdir()

        # Create multiple timestamped directories
        old_ts = tmp_path / "foursquare" / "20260101T000000"
        old_ts.mkdir()
        old_db = old_ts / ".foursquare_work.duckdb"
        duckdb.connect(str(old_db)).close()

        recent_ts = tmp_path / "foursquare" / "20260102T120000"
        recent_ts.mkdir()
        recent_db = recent_ts / ".foursquare_work.duckdb"
        duckdb.connect(str(recent_db)).close()

        # Add manifest.json to old run (marking it complete)
        (old_ts / "manifest.json").write_text("{}")

        result = _find_incomplete_run(str(source_dir), "foursquare")
        assert result == str(recent_ts)

    @pytest.mark.parametrize("name,create_run,has_work_db,has_manifest", [
        ("all_complete", True, True, True),
        ("no_runs", False, False, False),
        ("no_work_db", True, False, False),
    ])
    def test_returns_none_when_no_incomplete(self, tmp_path, name, create_run, has_work_db, has_manifest):
        """_find_incomplete_run returns None when no incomplete run matches criteria."""
        from garganorn.quadtree import _find_incomplete_run

        source_dir = tmp_path / "foursquare"
        source_dir.mkdir()

        if create_run:
            ts_dir = tmp_path / "foursquare" / "20260101T000000"
            ts_dir.mkdir()
            if has_work_db:
                db_path = ts_dir / ".foursquare_work.duckdb"
                duckdb.connect(str(db_path)).close()
            if has_manifest:
                (ts_dir / "manifest.json").write_text("{}")

        result = _find_incomplete_run(str(source_dir), "foursquare")
        assert result is None

    def test_force_deletes_incomplete_run_db(self, tmp_path):
        """--force flag deletes incomplete run's working DB."""
        from garganorn.quadtree import _find_incomplete_run

        source_dir = tmp_path / "foursquare"
        source_dir.mkdir()

        # Create an incomplete run (has work DB, no manifest)
        incomplete_ts = tmp_path / "foursquare" / "20260101T120000"
        incomplete_ts.mkdir()
        work_db = incomplete_ts / ".foursquare_work.duckdb"
        duckdb.connect(str(work_db)).close()

        # Verify _find_incomplete_run finds it
        result = _find_incomplete_run(str(source_dir), "foursquare")
        assert result == str(incomplete_ts)

        # Verify the work DB exists and would be targeted for deletion
        assert work_db.exists()
        assert str(work_db).endswith(".foursquare_work.duckdb")
        assert str(incomplete_ts) == result

    def test_skips_symlinks(self, tmp_path):
        """_find_incomplete_run skips symlinked directories."""
        from garganorn.quadtree import _find_incomplete_run

        source_dir = tmp_path / "foursquare"
        source_dir.mkdir()

        # Create a regular incomplete run
        regular_ts = tmp_path / "foursquare" / "20260101T000000"
        regular_ts.mkdir()
        regular_db = regular_ts / ".foursquare_work.duckdb"
        duckdb.connect(str(regular_db)).close()

        # Create a symlink target that WOULD match incomplete criteria
        link_target = tmp_path / "foursquare" / "20260102T000000"
        link_target.mkdir()
        link_db = link_target / ".foursquare_work.duckdb"
        duckdb.connect(str(link_db)).close()

        # Create symlink to the target
        symlink_path = tmp_path / "foursquare" / "20260103T000000"
        symlink_path.symlink_to(link_target)

        result = _find_incomplete_run(str(source_dir), "foursquare")
        assert result == str(regular_ts)

    def test_skips_non_timestamped_dirs(self, tmp_path):
        """_find_incomplete_run skips directories that don't match timestamp format."""
        from garganorn.quadtree import _find_incomplete_run

        source_dir = tmp_path / "foursquare"
        source_dir.mkdir()

        # Create a directory that doesn't match timestamp format
        non_ts_dir = tmp_path / "foursquare" / "not_a_timestamp"
        non_ts_dir.mkdir()
        non_ts_db = non_ts_dir / ".foursquare_work.duckdb"
        duckdb.connect(str(non_ts_db)).close()

        # Create a valid incomplete run
        valid_ts = tmp_path / "foursquare" / "20260101T000000"
        valid_ts.mkdir()
        valid_db = valid_ts / ".foursquare_work.duckdb"
        duckdb.connect(str(valid_db)).close()

        result = _find_incomplete_run(str(source_dir), "foursquare")
        assert result == str(valid_ts)

    def test_handles_corrupted_dbs(self, tmp_path, caplog):
        """_find_incomplete_run skips corrupted databases."""
        from garganorn.quadtree import _find_incomplete_run

        source_dir = tmp_path / "foursquare"
        source_dir.mkdir()

        # Create a corrupted DB (write garbage bytes)
        corrupted_ts = tmp_path / "foursquare" / "20260101T000000"
        corrupted_ts.mkdir()
        corrupted_db = corrupted_ts / ".foursquare_work.duckdb"
        corrupted_db.write_bytes(b"not a valid duckdb file")

        # Create a valid incomplete run
        valid_ts = tmp_path / "foursquare" / "20260102T000000"
        valid_ts.mkdir()
        valid_db = valid_ts / ".foursquare_work.duckdb"
        duckdb.connect(str(valid_db)).close()

        result = _find_incomplete_run(str(source_dir), "foursquare")
        assert result == str(valid_ts)

        # Should log a warning about the corrupted DB
        assert any("Corrupted working DB" in record.message
                   for record in caplog.records)


class TestPhase2Restructuring:
    """Tests for Phase 2 pipeline restructuring.

    Phase 2 merges importance and variants computation into the import stage.
    These tests FAIL against the current code and PASS after Phase 2 implementation.
    """

    def test_stage_order_is_simple_list(self):
        """STAGE_ORDER must be a simple list, not a dict."""
        from garganorn.quadtree import STAGE_ORDER

        # Must be a list, not a dict
        assert isinstance(STAGE_ORDER, list), (
            f"STAGE_ORDER should be a list, got {type(STAGE_ORDER).__name__}"
        )

        # Must have the expected stage names in order
        expected = ['import', 'tile_assignment', 'containment', 'export', 'manifest']
        assert STAGE_ORDER == expected, (
            f"STAGE_ORDER should be {expected}, got {STAGE_ORDER}"
        )

        # Old stage names must NOT be present
        old_stage_names = ['importance', 'variants', 'division_importance_backfill']
        for old_name in old_stage_names:
            assert old_name not in STAGE_ORDER, (
                f"Old stage name '{old_name}' should not be in STAGE_ORDER"
            )

    def test_old_stage_functions_removed(self):
        """Old stage functions must be removed from garganorn.stages."""
        import garganorn.stages as stages

        # These functions should NOT exist after Phase 2
        old_functions = [
            'stage_importance',
            'stage_variants',
            'stage_division_importance_backfill',
        ]

        for func_name in old_functions:
            assert not hasattr(stages, func_name), (
                f"Old function '{func_name}' should be removed from garganorn.stages"
            )

    def test_old_sql_files_deleted(self):
        """Old SQL files must be deleted after Phase 2."""
        from pathlib import Path

        sql_dir = Path(__file__).parent.parent / "garganorn" / "sql"

        # These files should NOT exist after Phase 2
        old_files = [
            'foursquare_importance.sql',
            'overture_place_importance.sql',
            'osm_importance.sql',
            'foursquare_variants.sql',
            'overture_place_variants.sql',
            'osm_variants.sql',
            'division_importance_backfill.sql',
        ]

        for filename in old_files:
            file_path = sql_dir / filename
            assert not file_path.exists(), (
                f"Old SQL file '{filename}' should be deleted"
            )

    def test_osm_category_snippet_exists(self):
        """OSM category CASE expression must be extracted to a shared snippet."""
        from pathlib import Path

        snippet_path = Path(__file__).parent.parent / "garganorn" / "sql" / "_osm_category_case.sql"

        assert snippet_path.exists(), (
            "OSM category snippet '_osm_category_case.sql' should exist"
        )

        # Verify it contains a CASE expression with expected tags
        content = snippet_path.read_text()

        # Should have CASE expression
        assert 'CASE' in content.upper(), (
            "Snippet should contain a CASE expression"
        )

        # Should have expected OSM tags
        expected_tags = ['amenity', 'shop', 'tourism', 'leisure', 'office']
        for tag in expected_tags:
            assert tag in content, (
                f"Snippet should reference '{tag}' tag"
            )

    def test_stage_import_signature_has_new_params(self):
        """stage_import must accept density_parquet and idf_parquet parameters."""
        import inspect
        from garganorn.stages import stage_import

        sig = inspect.signature(stage_import)
        params = list(sig.parameters.keys())

        # New parameters that must be present after Phase 2
        required_params = ['density_parquet', 'idf_parquet']

        for param in required_params:
            assert param in params, (
                f"stage_import should have parameter '{param}', "
                f"got: {params}"
            )


class TestComputeContainmentIdempotency:
    """Tests for compute_containment idempotency fix."""

    def test_compute_containment_idempotent(self, tmp_path):
        """Calling compute_containment twice should not error."""
        # Create a minimal working DB with places table
        work_db = tmp_path / "work.duckdb"
        con = duckdb.connect(str(work_db))

        # Create minimal places table required by compute_containment
        con.execute("""
            CREATE TABLE places (
                id VARCHAR PRIMARY KEY,
                longitude DOUBLE,
                latitude DOUBLE,
                qk17 VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO places VALUES
                ('place1', -122.4, 37.7, '12345678901234567'),
                ('place2', -122.5, 37.8, '12345678901234568')
        """)

        # First call should succeed
        compute_containment(
            con,
            boundaries_db=None,
            pk_expr="id",
            lon_expr="longitude",
            lat_expr="latitude"
        )

        # Verify place_containment was created
        result = con.execute(
            "SELECT COUNT(*) FROM place_containment"
        ).fetchone()[0]
        assert result == 0  # Empty because boundaries_db is None

        # Second call should also succeed (idempotency)
        compute_containment(
            con,
            boundaries_db=None,
            pk_expr="id",
            lon_expr="longitude",
            lat_expr="latitude"
        )

        # Should still have a valid place_containment table
        result = con.execute(
            "SELECT COUNT(*) FROM place_containment"
        ).fetchone()[0]
        assert result == 0

        con.close()

    def test_compute_containment_with_boundaries_idempotent(self, tmp_path):
        """compute_containment is idempotent even with boundaries_db."""
        # Create a minimal boundaries DB
        boundaries_db = tmp_path / "boundaries.duckdb"
        bnd_con = duckdb.connect(str(boundaries_db))
        bnd_con.execute("INSTALL spatial; LOAD spatial")
        bnd_con.execute("""
            CREATE TABLE places (
                id VARCHAR PRIMARY KEY,
                geometry GEOMETRY,
                admin_level INTEGER,
                min_latitude DOUBLE,
                max_latitude DOUBLE,
                min_longitude DOUBLE,
                max_longitude DOUBLE
            )
        """)
        # Insert a simple boundary (San Francisco bbox)
        bnd_con.execute("""
            INSERT INTO places VALUES (
                'sf',
                ST_MakeEnvelope(-122.5, 37.7, -122.3, 37.85),
                1,
                37.7, 37.85, -122.5, -122.3
            )
        """)
        bnd_con.close()

        # Build covering explicitly (§4: orchestrator responsibility, not compute_containment's).
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering")
        stage_covering(str(boundaries_db), covering_dir)

        # Create working DB with places
        work_db = tmp_path / "work.duckdb"
        con = duckdb.connect(str(work_db))
        con.execute("INSTALL spatial; LOAD spatial")

        con.execute("""
            CREATE TABLE places (
                id VARCHAR PRIMARY KEY,
                longitude DOUBLE,
                latitude DOUBLE,
                qk17 VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO places VALUES
                ('place1', -122.4, 37.77, ST_QuadKey(-122.4::DOUBLE, 37.77::DOUBLE, 17))
        """)

        # tile_assignments is a §3.2 precondition (no fallback in fixed code).
        con.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        con.execute("INSERT INTO tile_assignments SELECT id, left(qk17, 6) FROM places")

        containment_dir = str(tmp_path / "containment")

        # First call should succeed
        compute_containment(
            con,
            boundaries_db=str(boundaries_db),
            pk_expr="id",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=containment_dir,
        )

        # Verify place_containment was created
        result = con.execute(
            "SELECT COUNT(*) FROM place_containment"
        ).fetchone()[0]
        assert result >= 0

        # Second call should also succeed (idempotency)
        compute_containment(
            con,
            boundaries_db=str(boundaries_db),
            pk_expr="id",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=containment_dir,
        )

        # Should still have a valid place_containment table
        result = con.execute(
            "SELECT COUNT(*) FROM place_containment"
        ).fetchone()[0]
        assert result >= 0

        con.close()
