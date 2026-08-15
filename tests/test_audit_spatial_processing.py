"""Tests for stage_tile_assignment diagnostics: dropped-place warnings and
duplicate-assignment error logging.
"""

import duckdb
import logging

from garganorn.stages import stage_tile_assignment


class TestNullQK17Logging:
    """stage_tile_assignment logs a warning when places are dropped because
    their qk17 is NULL, comparing count(places) against count(tile_assignments)
    after tile assignment.
    """

    def test_null_qk17_logs_warning(self, tmp_path, caplog):
        """Places with NULL qk17 should trigger a warning log."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        # Create places table with some NULL qk17 values
        con.execute("""
            CREATE TABLE places (
                id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert valid places
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320'),
            ('valid002', 'Valid Place 2', 37.7750, -122.4200, '02301020333300321')
        """)

        # Insert a place with NULL qk17
        con.execute("""
            INSERT INTO places VALUES
            ('null001', 'Null QK17 Place', 37.7751, -122.4201, NULL)
        """)

        # Export places to parquet, the stage_tile_assignment input format
        places_parquet = str(tmp_path / "places_null_qk17.parquet")
        ta_parquet = str(tmp_path / "ta_null_qk17.parquet")
        total_places = con.execute("SELECT count(*) FROM places").fetchone()[0]
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        # Capture logging output
        with caplog.at_level(logging.WARNING):
            stage_tile_assignment(
                places_parquet, ta_parquet, "overture_place",
                max_per_tile=1000, force=True
            )

        # Check that a warning was logged about dropped places
        warning_messages = [record.message for record in caplog.records
                           if record.levelno >= logging.WARNING
                           and ('dropped' in record.message.lower()
                                or 'null' in record.message.lower()
                                or 'qk17' in record.message.lower())]

        assert len(warning_messages) > 0, \
            "Expected warning log about places with NULL qk17 being dropped"

        # Verify the place count difference
        check_con = duckdb.connect()
        assigned_places = check_con.execute(
            f"SELECT count(*) FROM read_parquet('{ta_parquet}')"
        ).fetchone()[0]
        check_con.close()

        assert total_places == 3, f"Expected 3 total places, got {total_places}"
        assert assigned_places == 2, f"Expected 2 assigned places, got {assigned_places}"

    def test_all_valid_qk17_no_warning(self, tmp_path, caplog):
        """When all places have valid qk17, no warning should be logged."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert only valid places
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320'),
            ('valid002', 'Valid Place 2', 37.7750, -122.4200, '02301020333300321')
        """)

        # Export places to parquet, the stage_tile_assignment input format
        places_parquet = str(tmp_path / "places_all_valid.parquet")
        ta_parquet = str(tmp_path / "ta_all_valid.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        # Capture logging output
        with caplog.at_level(logging.WARNING):
            stage_tile_assignment(
                places_parquet, ta_parquet, "overture_place",
                max_per_tile=1000, force=True
            )

        # Check that NO warning was logged about dropped places
        warning_messages = [record.message for record in caplog.records
                           if record.levelno >= logging.WARNING
                           and ('dropped' in record.message.lower()
                                or 'null' in record.message.lower()
                                or 'qk17' in record.message.lower())]

        assert len(warning_messages) == 0, \
            f"Expected no warning when all qk17 are valid, but got: {warning_messages}"


class TestTileAssignmentUniqueness:
    """stage_tile_assignment checks the written artifact for duplicate
    place_ids and logs an error if any place was assigned to multiple tiles.
    """

    def test_normal_assignment_no_duplicates(self, tmp_path, caplog):
        """Normal tile assignment should produce no duplicate place_ids."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert valid places
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320'),
            ('valid002', 'Valid Place 2', 37.7750, -122.4200, '02301020333300321'),
            ('valid003', 'Valid Place 3', 37.7751, -122.4201, '02301020333300322')
        """)

        places_parquet = str(tmp_path / "places_no_dup.parquet")
        ta_parquet = str(tmp_path / "ta_no_dup.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        # Capture logging output
        with caplog.at_level(logging.ERROR):
            stage_tile_assignment(
                places_parquet, ta_parquet, "overture_place",
                max_per_tile=1000, force=True
            )

        # Check that NO error was logged about duplicate assignments
        error_messages = [record.message for record in caplog.records
                         if record.levelno >= logging.ERROR
                         and ('multiple' in record.message.lower()
                              or 'duplicate' in record.message.lower())]

        assert len(error_messages) == 0, \
            f"Expected no duplicate/multiple-tile error for normal assignment, but got: {error_messages}"

        # Verify uniqueness in parquet output
        check_con = duckdb.connect()
        duplicate_count = check_con.execute(f"""
            SELECT place_id, COUNT(*) as cnt
            FROM read_parquet('{ta_parquet}')
            GROUP BY place_id
            HAVING cnt > 1
        """).fetchone()
        check_con.close()

        assert duplicate_count is None, "Expected no duplicate place_ids"

    def test_manually_inserted_duplicates_detected(self, tmp_path, caplog):
        """If input places parquet has duplicate pk values, an error should be logged."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert the same place_id twice to simulate duplicate pk in input
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320'),
            ('valid001', 'Valid Place 1 Dup', 37.7749, -122.4194, '02301020333300320')
        """)

        places_parquet = str(tmp_path / "places_dup.parquet")
        ta_parquet = str(tmp_path / "ta_dup.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        # Run stage_tile_assignment with duplicate input and capture error logs
        with caplog.at_level(logging.ERROR):
            stage_tile_assignment(
                places_parquet, ta_parquet, "overture_place",
                max_per_tile=1000, force=True
            )

        # Check that an error was logged (message contains "multiple" or "duplicate")
        error_messages = [record.message for record in caplog.records
                         if record.levelno >= logging.ERROR
                         and ('multiple' in record.message.lower()
                              or 'duplicate' in record.message.lower())]

        assert len(error_messages) > 0, (
            "Expected error log about places assigned to multiple tiles when input "
            "places parquet has duplicate id values; "
            f"captured log records: {[(r.levelno, r.message) for r in caplog.records]}"
        )

    def test_tile_assignments_has_primary_key_constraint(self, tmp_path):
        """tile_assignments parquet output should have place_id as first column."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320')
        """)

        places_parquet = str(tmp_path / "places_pk.parquet")
        ta_parquet = str(tmp_path / "ta_pk.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        stage_tile_assignment(places_parquet, ta_parquet, "overture_place", max_per_tile=1000, force=True)

        # Check parquet schema - place_id should be the first column
        check_con = duckdb.connect()
        table_info = check_con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{ta_parquet}')"
        ).fetchall()
        check_con.close()

        # place_id should be the first column
        assert len(table_info) >= 2, "tile_assignments should have at least 2 columns"
        assert table_info[0][0] == "place_id", "First column should be place_id"
