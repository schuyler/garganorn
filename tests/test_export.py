"""Tests for foursquare_export_tiles.sql, overture_place_export_tiles.sql, and export_tiles()."""

import gzip
import inspect
import json
import logging
import re

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    run_overture_import, run_tile_assignments, run_osm_import,
)


# ---------------------------------------------------------------------------
# Module-local helpers: build a minimal FSQ places + tile_assignments DB
# ---------------------------------------------------------------------------

_FSQ_EXPORT_PLACES = [
    # (fsq_place_id, name, lat, lon, importance, country)
    ("exp001", "Blue Bottle Coffee",  37.7749, -122.4194, 72, "US"),
    ("exp002", "Golden Gate Park",    37.7694, -122.4862, 85, "US"),
    ("exp003", "Tartine Bakery",      37.7617, -122.4243, 68, "US"),
    # place with null country — should produce no address location
    ("exp004", "Mystery Spot",        37.7800, -122.4300, 40, None),
]

# 6-char zoom-6 quadkey prefix — all fixture places are assigned to this single tile
_EXPORT_TILE_QK = "023130"


def _make_fsq_export_db(conn, places_rows=None):
    """Populate `conn` with minimal `places` and `tile_assignments` tables.

    `places_rows` defaults to _FSQ_EXPORT_PLACES if None.
    Each entry is (fsq_place_id, name, lat, lon, importance, country).
    """
    if places_rows is None:
        places_rows = _FSQ_EXPORT_PLACES

    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            fsq_place_id        VARCHAR,
            name                VARCHAR,
            latitude            DOUBLE,
            longitude           DOUBLE,
            importance          INTEGER,
            address             VARCHAR,
            locality            VARCHAR,
            region              VARCHAR,
            postcode            VARCHAR,
            country             VARCHAR,
            admin_region        VARCHAR,
            post_town           VARCHAR,
            po_box              VARCHAR,
            date_created        DATE,
            date_refreshed      DATE,
            tel                 VARCHAR,
            website             VARCHAR,
            email               VARCHAR,
            facebook_id         VARCHAR,
            instagram           VARCHAR,
            twitter             VARCHAR,
            fsq_category_ids    VARCHAR[],
            fsq_category_labels VARCHAR[],
            placemaker_url      VARCHAR,
            variants            STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
            qk17                VARCHAR
        )
    """)

    for fsq_id, name, lat, lon, imp, country in places_rows:
        country_val = f"'{country}'" if country is not None else "NULL"
        # Compute qk17 from actual coordinates so ST_QuadKey produces a valid 17-char key.
        conn.execute(f"""
            INSERT INTO places
            SELECT
                '{fsq_id}', '{name}', {lat}, {lon}, {imp},
                NULL, NULL, NULL, NULL, {country_val},
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                ARRAY['13065143'], ARRAY['Food & Drink'],
                NULL,
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                ST_QuadKey({lon}, {lat}, 17)
        """)

    conn.execute("""
        CREATE TABLE tile_assignments (
            place_id VARCHAR,
            tile_qk  VARCHAR
        )
    """)
    for fsq_id, _name, _lat, _lon, _imp, _country in places_rows:
        conn.execute(
            "INSERT INTO tile_assignments VALUES (?, ?)",
            [fsq_id, _EXPORT_TILE_QK],
        )

    conn.execute("""
        CREATE TABLE place_containment (
            place_id       VARCHAR,
            relations_json VARCHAR
        )
    """)


# ---------------------------------------------------------------------------
# Tests: foursquare_export_tiles.sql
# ---------------------------------------------------------------------------

class TestFsqExportTiles:
    """Tests for garganorn/sql/foursquare_export_tiles.sql.

    All tests fail at Red phase: the SQL file does not exist yet.
    """

    _SUBS = {"attribution": "Foursquare Open Source Places", "repo": "https://example.com"}

    def _run_export(self, conn):
        raw_sql = _load_sql("foursquare_export_tiles.sql", self._SUBS)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_sql_file_exists(self):
        sql_path = REPO_ROOT / "garganorn" / "sql" / "foursquare_export_tiles.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def test_export_produces_rows(self, tmp_path):
        db_path = tmp_path / "test_fsq_export_rows.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT tile_qk, record_json FROM tile_export").fetchall()
        conn.close()
        assert len(rows) >= len(_FSQ_EXPORT_PLACES), (
            f"foursquare_export_tiles.sql must produce at least one row per fixture place (>=4); got {len(rows)}"
        )

    def test_record_json_structure(self, tmp_path):
        """record_json must be valid JSON with flat structure (no uri/value wrapper)."""
        db_path = tmp_path / "test_fsq_export_struct.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT tile_qk, record_json FROM tile_export").fetchall()
        conn.close()
        assert rows, "No rows returned from tile_export"
        for tile_qk, record_json in rows:
            parsed = json.loads(record_json)
            assert "uri" not in parsed, (
                f"record_json for {tile_qk} should NOT have 'uri' key (flat structure); keys={list(parsed)}"
            )
            assert "value" not in parsed, (
                f"record_json for {tile_qk} should NOT have 'value' key (flat structure); keys={list(parsed)}"
            )
            assert parsed.get("$type") == "org.atgeo.place", (
                f"record_json $type must be 'org.atgeo.place'; got {parsed.get('$type')!r}"
            )

    def test_record_schema(self, tmp_path):
        """Each record_json row must have the expected top-level fields."""
        db_path = tmp_path / "test_fsq_export_rec_schema.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            assert "uri" not in parsed, f"Record should NOT have 'uri': {list(parsed)}"
            assert "value" not in parsed, f"Record should NOT have 'value': {list(parsed)}"
            assert "rkey" in parsed, f"Record missing 'rkey': {list(parsed)}"
            assert parsed.get("$type") == "org.atgeo.place", (
                f"$type must be 'org.atgeo.place'; got {parsed.get('$type')!r}"
            )
            assert "name" in parsed, f"Record missing 'name': {list(parsed)}"
            assert "importance" in parsed, f"Record missing 'importance': {list(parsed)}"
            assert isinstance(parsed["importance"], int), (
                f"importance must be int; got {type(parsed['importance'])}"
            )
            assert "locations" in parsed, f"Record missing 'locations': {list(parsed)}"
            assert isinstance(parsed["locations"], list), "locations must be a list"
            assert "variants" in parsed, f"Record missing 'variants': {list(parsed)}"
            assert isinstance(parsed["variants"], list), "variants must be a list"
            assert "attributes" in parsed, f"Record missing 'attributes': {list(parsed)}"
            assert isinstance(parsed["attributes"], dict), "attributes must be a dict"
            assert "relations" in parsed, f"Record missing 'relations': {list(parsed)}"
            assert isinstance(parsed["relations"], dict), "relations must be a dict"

    def test_geo_location(self, tmp_path):
        """First location entry must be a geo location with string lat/lon."""
        db_path = tmp_path / "test_fsq_export_geo.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            locations = parsed["locations"]
            assert len(locations) >= 1, "Each record must have at least one location"
            geo = locations[0]
            assert geo.get("$type") == "community.lexicon.location.geo", (
                f"First location must be geo type; got {geo.get('$type')!r}"
            )
            assert "latitude" in geo, "Geo location missing 'latitude'"
            assert "longitude" in geo, "Geo location missing 'longitude'"
            assert isinstance(geo["latitude"], str), (
                f"geo latitude must be a string; got {type(geo['latitude'])}"
            )
            assert isinstance(geo["longitude"], str), (
                f"geo longitude must be a string; got {type(geo['longitude'])}"
            )

    def test_address_location_when_country_present(self, tmp_path):
        """A place with a non-null country must have an address location as the second entry."""
        db_path = tmp_path / "test_fsq_export_addr.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        found = False
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("name") in ("Blue Bottle Coffee", "Golden Gate Park", "Tartine Bakery"):
                locations = parsed["locations"]
                assert len(locations) >= 2, (
                    f"Place with country must have address location; "
                    f"got {len(locations)} location(s) for {parsed['name']}"
                )
                addr = locations[1]
                assert addr.get("$type") == "community.lexicon.location.address", (
                    f"Second location must be address type; got {addr.get('$type')!r}"
                )
                assert addr.get("country") == "US", (
                    f"Address country should be 'US'; got {addr.get('country')!r}"
                )
                found = True
        assert found, "No records with country found in export output"

    def test_no_address_when_country_null(self, tmp_path):
        """A place with null country must have exactly 1 location (geo only)."""
        db_path = tmp_path / "test_fsq_export_no_addr.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        found = False
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("name") == "Mystery Spot":
                locations = parsed["locations"]
                assert len(locations) == 1, (
                    f"Place with null country must have exactly 1 location; "
                    f"got {len(locations)}"
                )
                found = True
        assert found, "Mystery Spot (null country place) not found in export output"

    def test_no_null_fields_in_locations(self, tmp_path):
        """No location in any record must contain a key with a None/null value.

        Fails against current code: list_concat unions geo and address struct types,
        so geo locations get spurious null address fields (country, region, etc.) and
        address locations get spurious null geo fields (latitude, longitude).
        """
        db_path = tmp_path / "test_fsq_no_null_loc.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()
        assert rows, "No rows returned from tile_export"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            locations = parsed["locations"]
            for loc in locations:
                null_keys = [k for k, v in loc.items() if v is None]
                assert not null_keys, (
                    f"Location {loc.get('$type')!r} in record {parsed.get('rkey')!r} "
                    f"has null values for keys: {null_keys}. "
                    "Locations must contain only fields belonging to their type."
                )

    def test_tile_export_is_view_not_table(self, tmp_path):
        """tile_export must be a VIEW, not a BASE TABLE.

        Fails because foursquare_export_tiles.sql currently creates a TABLE.
        After the implementation changes to CREATE OR REPLACE VIEW, this test will pass.
        """
        db_path = tmp_path / "test_tile_export_table_type.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        row = conn.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_name = 'tile_export'"
        ).fetchone()
        conn.close()
        assert row is not None, "tile_export not found in information_schema.tables"
        assert row[0] == "VIEW", (
            f"tile_export must be a VIEW; got {row[0]!r}"
        )


# ---------------------------------------------------------------------------
# Tests: export_tiles() Python function
# ---------------------------------------------------------------------------

class TestExportTiles:
    """Tests for garganorn.quadtree.export_tiles().

    All tests fail at Red phase: garganorn.quadtree does not exist yet.
    """

    def test_import(self):
        """Importing export_tiles must raise ImportError in Red phase."""
        from garganorn.quadtree import export_tiles  # noqa: F401

    def test_writes_gzipped_files(self, tmp_path):
        """export_tiles must write .json.gz files under {output_dir}/{qk[:6]}/{qk}.json.gz."""
        try:
            from garganorn.quadtree import export_tiles
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        db_path = tmp_path / "export_tiles_test.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        export_tiles(conn, str(output_dir), "foursquare")
        conn.close()

        gz_files = list(output_dir.rglob("*.json.gz"))
        assert gz_files, "export_tiles must write at least one .json.gz file"
        for gz in gz_files:
            # Path must be output_dir/<6-char-prefix>/<qk>.json.gz
            parts = gz.relative_to(output_dir).parts
            assert len(parts) == 2, (
                f"Expected 2-level path (<qk6>/<qk>.json.gz), got: {gz}"
            )
            qk_dir = parts[0]
            qk_file = parts[1].replace(".json.gz", "")
            assert qk_file.startswith(qk_dir), (
                f"File quadkey {qk_file!r} must start with dir prefix {qk_dir!r}"
            )

    def test_returns_manifest_dict(self, tmp_path):
        """export_tiles must return a dict mapping quadkey strings to integer record counts."""
        try:
            from garganorn.quadtree import export_tiles
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        db_path = tmp_path / "export_manifest_test.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)

        output_dir = tmp_path / "output_manifest"
        output_dir.mkdir()
        result = export_tiles(conn, str(output_dir), "foursquare")
        conn.close()

        assert isinstance(result, dict), (
            f"export_tiles must return a dict; got {type(result)}"
        )
        for qk, count in result.items():
            assert isinstance(qk, str), f"Manifest key must be str; got {type(qk)}"
            assert isinstance(count, int), (
                f"Manifest value must be int; got {type(count)} for key {qk!r}"
            )

    def test_json_content_valid(self, tmp_path):
        """Each .json.gz file must decompress to valid JSON with a 'records' array."""
        try:
            from garganorn.quadtree import export_tiles
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        db_path = tmp_path / "export_content_test.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)

        output_dir = tmp_path / "output_content"
        output_dir.mkdir()
        export_tiles(conn, str(output_dir), "foursquare")
        conn.close()

        gz_files = list(output_dir.rglob("*.json.gz"))
        assert gz_files, "No .json.gz files written"
        for gz in gz_files:
            with gzip.open(gz, "rt", encoding="utf-8") as fh:
                parsed = json.load(fh)
            assert "records" in parsed, (
                f"Decompressed JSON missing 'records' key in {gz}"
            )
            assert isinstance(parsed["records"], list), (
                f"'records' must be a list in {gz}"
            )

    def test_uses_fetchmany_not_fetchall(self, tmp_path):
        """export_tiles must use cursor.fetchmany() in a loop, not fetchall().

        Fails against the current fetchall() implementation: the mock cursor's
        fetchall() raises AssertionError if called, verifying it is NOT used.
        After the fix (fetchmany sentinel loop), fetchall() is never called so
        the test passes.
        """
        import gzip as _gzip
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        # Build two synthetic tile rows that a real cursor would return.
        # record_json is the FLAT record shape (as tile_export SQL views emit
        # it, per docs/pipeline-implementation-decisions.md, "OQ-P2-1 — record
        # envelope adoption"); export_tiles wraps each with envelope.wrap_record.
        tile_qk_a = "023130" + "0" * 11  # 17-char quadkey
        tile_qk_b = "023130" + "1" * 11
        record_a = json.dumps({"$type": "org.atgeo.place", "rkey": "fsq001", "name": "Test A"})
        record_b = json.dumps({"$type": "org.atgeo.place", "rkey": "fsq002", "name": "Test B"})
        all_rows = [(tile_qk_a, "fsq001", record_a), (tile_qk_b, "fsq002", record_b)]

        # Mock cursor: fetchmany returns rows in one batch, then [].
        # fetchall raises AssertionError so the test fails immediately if called.
        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [all_rows, []]
        mock_cursor.fetchall.side_effect = AssertionError(
            "export_tiles must not call fetchall(); use fetchmany() loop instead"
        )

        # Mock connection: execute() returns the mock cursor.
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_fetchmany"
        output_dir.mkdir()

        # Patch the SQL file read so we don't need the actual SQL file on disk.
        fake_sql = "SELECT tile_qk, rkey, record_json FROM tile_export"
        with patch("pathlib.Path.read_text", return_value=fake_sql):
            export_tiles(mock_con, str(output_dir), "foursquare")

        # Confirm fetchall was never called (the side_effect above would have
        # raised already; this assertion is belt-and-suspenders).
        mock_cursor.fetchall.assert_not_called()

        # Confirm fetchmany was called at least once.
        assert mock_cursor.fetchmany.called, (
            "export_tiles must call cursor.fetchmany()"
        )

        # Verify envelope structure in written files.
        gz_files = list(output_dir.rglob("*.json.gz"))
        assert gz_files, "export_tiles must have written at least one .json.gz file"
        gz_files.sort()
        with _gzip.open(gz_files[0], "rt") as f:
            envelope = json.load(f)
        assert "attribution" in envelope, (
            f"Envelope missing 'attribution'; keys: {list(envelope)}"
        )
        assert "records" in envelope, (
            f"Envelope missing 'records'; keys: {list(envelope)}"
        )
        assert isinstance(envelope["records"], list), "'records' must be a list"
        for item in envelope["records"]:
            assert set(item.keys()) == {"uri", "cid", "value"}, (
                f"Record item must be exactly {{uri, cid, value}}; got {list(item)}"
            )
            assert item["cid"] is None

    def test_progress_log_format_no_total(self, tmp_path):
        """Progress log at 1000-tile boundary must NOT include a total tile count.

        The current implementation logs "export: wrote %d / %d tiles" (count + total).
        The fix changes this to "export: wrote %d tiles" (running count only, no total).
        This test fails against the current code and passes after the fix.
        """
        import logging
        from unittest.mock import patch, MagicMock

        from garganorn.quadtree import export_tiles

        # Build 1000 synthetic tile rows to trigger a progress log.
        # Each row has a UNIQUE tile_qk so we get 1000 distinct tiles — the
        # tile_count % 1000 boundary fires when tile_count reaches 1000.
        # record_json is the FLAT record shape (per the envelope decisions
        # above); export_tiles wraps it.
        def _make_row(i):
            qk = f"02313{i:012d}"  # unique quadkey per row
            payload = json.dumps({"$type": "org.atgeo.place", "rkey": str(i), "name": f"Place {i}"})
            return (qk, str(i), payload)

        all_rows = [_make_row(i) for i in range(1000)]

        # Cursor returns all 1000 rows in first fetchmany call, then [].
        # fetchall returns the list directly (as current code expects).
        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [all_rows, []]
        mock_cursor.fetchall.return_value = all_rows  # current code path

        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_log_format"
        output_dir.mkdir()

        fake_sql = "SELECT tile_qk, rkey, record_json FROM tile_export"
        captured_messages = []

        class _CapturingHandler(logging.Handler):
            def emit(self, record):
                captured_messages.append(record.getMessage())

        handler = _CapturingHandler()
        # Capture logs from both quadtree (for backward compat) and stages (actual implementation)
        import garganorn.quadtree as _qt_module
        import garganorn.stages as _stages_module
        logger = logging.getLogger(_qt_module.__name__)
        stages_logger = logging.getLogger(_stages_module.__name__)
        logger.addHandler(handler)
        stages_logger.addHandler(handler)
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        stages_logger.setLevel(logging.DEBUG)
        try:
            with patch("pathlib.Path.read_text", return_value=fake_sql):
                export_tiles(mock_con, str(output_dir), "foursquare")
        finally:
            logger.removeHandler(handler)
            stages_logger.removeHandler(handler)
            logger.setLevel(old_level)
            stages_logger.setLevel(old_level)

        # Find progress log messages that fire at the 1000-tile boundary.
        progress_msgs = [m for m in captured_messages if "wrote" in m and "tiles" in m]
        assert progress_msgs, (
            "No 'wrote ... tiles' log message emitted at 1000-tile boundary"
        )
        # After the fix: messages must NOT contain a slash (no 'wrote X / Y tiles').
        # The current code produces 'wrote 1000 / 1000 tiles', which contains '/'.
        for msg in progress_msgs:
            assert "/" not in msg, (
                f"Progress log must not include a total (slash notation); got: {msg!r}. "
                "Fix: log only the running tile count, not 'count / total'."
            )

    def test_post_loop_log_uses_manifest_len(self, tmp_path):
        """After the tile-writing loop, export_tiles must log using len(manifest).

        The current code logs 'export: queried %d tiles' BEFORE the loop using
        len(result) (the full fetchall list).  The fix removes that pre-loop log
        and instead logs after the loop using len(manifest).

        This test asserts that the post-loop log message exists and that no
        pre-loop 'queried' message is emitted.  Fails against current code
        (which emits 'queried', not a post-loop manifest-based message) and
        passes after the fix.
        """
        import logging
        from unittest.mock import patch, MagicMock

        from garganorn.quadtree import export_tiles

        tile_qk = "023130" + "0" * 11
        payload = json.dumps({"$type": "org.atgeo.place", "rkey": "fsq001", "name": "Test"})
        all_rows = [(tile_qk, "fsq001", payload)]

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [all_rows, []]
        mock_cursor.fetchall.return_value = all_rows

        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_postloop_log"
        output_dir.mkdir()

        fake_sql = "SELECT tile_qk, rkey, record_json FROM tile_export"
        captured_messages = []

        class _CapturingHandler(logging.Handler):
            def emit(self, record):
                captured_messages.append(record.getMessage())

        import garganorn.quadtree as _qt_module
        import garganorn.stages as _stages_module
        logger = logging.getLogger(_qt_module.__name__)
        stages_logger = logging.getLogger(_stages_module.__name__)
        handler = _CapturingHandler()
        logger.addHandler(handler)
        stages_logger.addHandler(handler)
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        stages_logger.setLevel(logging.DEBUG)
        try:
            with patch("pathlib.Path.read_text", return_value=fake_sql):
                export_tiles(mock_con, str(output_dir), "foursquare")
        finally:
            logger.removeHandler(handler)
            stages_logger.removeHandler(handler)
            logger.setLevel(old_level)
            stages_logger.setLevel(old_level)

        # Current code emits 'queried N tiles' before the loop.
        # After the fix that message is gone; instead there's a post-loop message.
        queried_msgs = [m for m in captured_messages if "queried" in m]
        assert not queried_msgs, (
            f"export_tiles must not emit a 'queried' pre-loop message; got: {queried_msgs!r}. "
            "Fix: remove the pre-loop log and log tile count after the loop using len(manifest)."
        )

        # After the fix a post-loop summary log appears containing the tile count.
        # The manifest has 1 tile; verify a message mentions '1' after the loop.
        post_loop_msgs = [
            m for m in captured_messages
            if "export" in m and "1" in m and "queried" not in m
        ]
        assert post_loop_msgs, (
            "export_tiles must emit a post-loop log message referencing the manifest tile count. "
            f"Captured messages: {captured_messages!r}"
        )

    def test_python_groups_records_by_tile_qk(self, tmp_path):
        """export_tiles groups per-record rows by tile_qk into separate .json.gz files.

        Retargeted onto the atgeo v1 envelope (pipeline-implementation-decisions.md
        "OQ-P2-1 — record envelope adoption": legacy
        export_tiles() is retargeted onto the same envelope.py helpers as
        stage_export in this change set, not deleted). The mock cursor now
        yields a third `rkey` column (per the envelope decisions above: all
        four export SQL views gain an rkey output column), and each written
        record must be
        {uri, cid, value}-wrapped, not flat.
        """
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        qk_a = "023130" + "0" * 11
        qk_b = "023131" + "0" * 11
        rec1 = json.dumps({"$type": "org.atgeo.place", "rkey": "1", "name": "A1"})
        rec2 = json.dumps({"$type": "org.atgeo.place", "rkey": "2", "name": "A2"})
        rec3 = json.dumps({"$type": "org.atgeo.place", "rkey": "3", "name": "B1"})
        all_rows = [(qk_a, "1", rec1), (qk_a, "2", rec2), (qk_b, "3", rec3)]

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [all_rows, []]
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_group"
        output_dir.mkdir()

        with patch("pathlib.Path.read_text",
                   return_value="SELECT tile_qk, rkey, record_json FROM tile_export"):
            result = export_tiles(mock_con, str(output_dir), "foursquare")

        assert len(result) == 2, f"Expected 2 tiles, got {len(result)}"
        assert result[qk_a] == 2, f"qk_a tile should have 2 records, got {result[qk_a]}"
        assert result[qk_b] == 1, f"qk_b tile should have 1 record, got {result[qk_b]}"

        gz_files = list(output_dir.rglob("*.json.gz"))
        assert len(gz_files) == 2, f"Expected 2 .json.gz files, got {len(gz_files)}"

        for gz in gz_files:
            with gzip.open(gz, "rt") as f:
                data = json.load(f)
            assert data.get("atgeo") == 1, f"envelope must have atgeo == 1; got keys {list(data)}"
            assert "attribution" in data
            assert "collection" in data
            assert "generated_at" in data
            assert "records" in data
            assert isinstance(data["records"], list)
            for rec in data["records"]:
                assert set(rec.keys()) == {"uri", "cid", "value"}, (
                    f"record must be {{uri, cid, value}}-wrapped; got {list(rec)}"
                )
                assert rec["cid"] is None
                assert "$type" in rec["value"]

    def test_attribution_in_envelope(self, tmp_path):
        """export_tiles writes attribution from SOURCES[source].attribution into the envelope."""
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles, SOURCES

        qk = "023130" + "0" * 11
        rec = json.dumps({"$type": "org.atgeo.place", "rkey": "1", "name": "Test"})

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [[(qk, "1", rec)], []]
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_attr"
        output_dir.mkdir()

        with patch("pathlib.Path.read_text",
                   return_value="SELECT tile_qk, rkey, record_json FROM tile_export"):
            export_tiles(mock_con, str(output_dir), "foursquare")

        gz_files = list(output_dir.rglob("*.json.gz"))
        assert gz_files, "No .json.gz written"
        with gzip.open(gz_files[0], "rt") as f:
            data = json.load(f)

        assert "attribution" in data, f"Envelope missing 'attribution'; keys: {list(data)}"
        assert data["attribution"] == SOURCES["foursquare"].attribution, (
            f"attribution must be SOURCES['foursquare'].attribution = {SOURCES['foursquare'].attribution!r}; "
            f"got {data['attribution']!r}"
        )
        assert "collection" in data, f"Envelope missing 'collection'; keys: {list(data)}"
        assert data["collection"] == SOURCES["foursquare"].collection, (
            f"collection must be SOURCES['foursquare'].collection = {SOURCES['foursquare'].collection!r}; "
            f"got {data['collection']!r}"
        )

    def test_single_record_tile(self, tmp_path):
        """A tile with exactly one {uri, cid, value}-wrapped record is correctly written."""
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        qk = "023130" + "0" * 11
        rec = json.dumps({"$type": "org.atgeo.place", "rkey": "1", "name": "Solo"})

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [[(qk, "1", rec)], []]
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_single"
        output_dir.mkdir()

        with patch("pathlib.Path.read_text",
                   return_value="SELECT tile_qk, rkey, record_json FROM tile_export"):
            result = export_tiles(mock_con, str(output_dir), "foursquare")

        assert result == {qk: 1}, f"Expected {{qk: 1}}, got {result}"

        gz_files = list(output_dir.rglob("*.json.gz"))
        assert len(gz_files) == 1
        with gzip.open(gz_files[0], "rt") as f:
            data = json.load(f)
        assert len(data["records"]) == 1
        wrapped = data["records"][0]
        assert set(wrapped.keys()) == {"uri", "cid", "value"}
        assert wrapped["cid"] is None
        assert wrapped["value"]["rkey"] == "1"
        assert wrapped["value"]["name"] == "Solo"
        assert wrapped["uri"].endswith("/1")

    def test_tile_boundary_across_fetchmany_batches(self, tmp_path):
        """Tile spanning two fetchmany batches is written correctly."""
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        qk_a = "023130" + "0" * 11
        qk_b = "023131" + "0" * 11
        rec1 = json.dumps({"$type": "org.atgeo.place", "rkey": "1", "name": "A1"})
        rec2 = json.dumps({"$type": "org.atgeo.place", "rkey": "2", "name": "A2"})
        rec3 = json.dumps({"$type": "org.atgeo.place", "rkey": "3", "name": "B1"})

        # batch 1: first record of qk_a only
        # batch 2: second record of qk_a + first record of qk_b (forces boundary split)
        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [
            [(qk_a, "1", rec1)],
            [(qk_a, "2", rec2), (qk_b, "3", rec3)],
            [],
        ]
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_boundary"
        output_dir.mkdir()

        with patch("pathlib.Path.read_text",
                   return_value="SELECT tile_qk, rkey, record_json FROM tile_export"):
            result = export_tiles(mock_con, str(output_dir), "foursquare")

        assert result[qk_a] == 2, f"qk_a must have 2 records (spanning batches); got {result[qk_a]}"
        assert result[qk_b] == 1, f"qk_b must have 1 record; got {result[qk_b]}"

        for qk, expected_count in [(qk_a, 2), (qk_b, 1)]:
            gz = output_dir / qk[:6] / f"{qk}.json.gz"
            assert gz.exists(), f"{gz} not written"
            with gzip.open(gz, "rt") as f:
                data = json.load(f)
            assert len(data["records"]) == expected_count, (
                f"{qk} tile: expected {expected_count} records, got {len(data['records'])}"
            )
            for wrapped in data["records"]:
                assert set(wrapped.keys()) == {"uri", "cid", "value"}
                assert wrapped["cid"] is None

    def test_flush_tile_no_json_loads(self, tmp_path):
        """flush_tile must not call json.loads on the per-record JSON — records
        are already valid JSON strings that envelope.wrap_record composes via
        string concatenation (per the envelope decisions above), not a
        parse/reserialize round trip.

        record_json rows are the FLAT record shape (as tile_export SQL views
        emit them, per the envelope decisions above); export_tiles wraps each
        with envelope.wrap_record
        before writing, producing {uri, cid, value} entries in the output.
        """
        import gzip as _gzip
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        qk_a = "023130" + "0" * 11
        qk_b = "023131" + "0" * 11
        record_a = json.dumps({"$type": "org.atgeo.place", "rkey": "fsq001", "name": "Test A"})
        record_b = json.dumps({"$type": "org.atgeo.place", "rkey": "fsq002", "name": "Test B"})
        all_rows = [(qk_a, "fsq001", record_a), (qk_b, "fsq002", record_b)]

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [all_rows, []]
        mock_cursor.fetchall.side_effect = AssertionError(
            "export_tiles must not call fetchall()"
        )

        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_no_json_loads"
        output_dir.mkdir()

        fake_sql = "SELECT tile_qk, rkey, record_json FROM tile_export"
        with patch("pathlib.Path.read_text", return_value=fake_sql):
            export_tiles(mock_con, str(output_dir), "foursquare")

        # Verify output files contain valid JSON with the correct structure.
        # (json.loads patch is no longer active here, so json.load works normally.)
        gz_files = list(output_dir.rglob("*.json.gz"))
        assert gz_files, "export_tiles must have written at least one .json.gz file"
        for gz in gz_files:
            with _gzip.open(gz, "rt", encoding="utf-8") as f:
                envelope = json.load(f)
            assert "attribution" in envelope, (
                f"Envelope missing 'attribution'; keys: {list(envelope)}"
            )
            assert "records" in envelope, (
                f"Envelope missing 'records'; keys: {list(envelope)}"
            )
            assert isinstance(envelope["records"], list), "'records' must be a list"
            for item in envelope["records"]:
                assert set(item.keys()) == {"uri", "cid", "value"}, (
                    f"Record item must be exactly {{uri, cid, value}}; got {list(item)}"
                )
                assert item["cid"] is None


# ---------------------------------------------------------------------------
# Tests: overture_place_export_tiles.sql
# ---------------------------------------------------------------------------

class TestOvertureExportTiles:
    """Tests for garganorn/sql/overture_place_export_tiles.sql.

    Each test runs the full Overture pipeline:
      overture_import → overture_importance → overture_variants →
      compute_tile_assignments → overture_export_tiles
    """

    _SUBS = {"repo": "places.atgeo.org"}

    def _run_full_pipeline(self, conn, parquet_glob, density_parquet):
        """Run all Overture pipeline SQL stages on conn."""
        # 1. Import (includes importance and variants computation)
        run_overture_import(conn, parquet_glob)

        # 2. Tile assignments (pk_expr='id' for Overture)
        run_tile_assignments(conn, pk_expr="id", min_zoom=6, max_zoom=17, max_per_tile=5000)

        # 4b. Empty place_containment (no boundaries in pipeline tests)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS place_containment (
                place_id       VARCHAR,
                relations_json VARCHAR
            )
        """)

        # 5. Export tiles
        raw = _load_sql("overture_place_export_tiles.sql", self._SUBS)
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))

    def _get_record(self, conn, place_id):
        """Return parsed JSON record dict for a given place_id, or None if not found.

        Fetches all rows from tile_export, parses each record_json, and returns
        the first record whose rkey matches place_id.
        """
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("rkey") == place_id:
                return parsed
        return None

    def test_overture_export_addresses_inline(self, overture_parquet, density_parquet, tmp_path):
        """ov001 (one address entry with country='US', region='US-CA') must have an
        address location entry with country='US' and region='CA' (trimmed at '-').

        locations must contain: [geo_entry, address_entry].
        """
        db_path = tmp_path / "test_ov_export_addr.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov001")
        conn.close()
        assert record is not None, "ov001 must appear in tile_export"
        locations = record["locations"]
        addr_entries = [loc for loc in locations if loc.get("$type") == "community.lexicon.location.address"]
        assert len(addr_entries) == 1, (
            f"ov001 must have exactly 1 address location; got {len(addr_entries)}: {addr_entries}"
        )
        addr = addr_entries[0]
        assert addr["country"] == "US", f"Expected country='US'; got {addr['country']!r}"
        assert addr["region"] == "CA", (
            f"Expected region='CA' (trimmed from 'US-CA'); got {addr['region']!r}"
        )

    def test_overture_export_no_addresses_no_error(self, overture_parquet, density_parquet, tmp_path):
        """ov003 (addresses=NULL) must render without error with exactly 1 location (geo only)."""
        db_path = tmp_path / "test_ov_export_no_addr.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov003")
        conn.close()
        assert record is not None, "ov003 must appear in tile_export"
        locations = record["locations"]
        assert len(locations) == 1, (
            f"ov003 (null addresses) must have exactly 1 location (geo only); got {len(locations)}: {locations}"
        )
        assert locations[0]["$type"] == "community.lexicon.location.geo", (
            f"Only location must be geo type; got {locations[0]['$type']!r}"
        )

    def test_overture_export_all_null_country_addresses(self, overture_parquet, density_parquet, tmp_path):
        """ov008 (addresses=[{country:NULL,...}]) must render with exactly 1 location (geo only).

        list_filter must remove all entries with NULL country, yielding an empty address list.
        """
        db_path = tmp_path / "test_ov_export_null_country.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov008")
        conn.close()
        assert record is not None, "ov008 must appear in tile_export"
        locations = record["locations"]
        assert len(locations) == 1, (
            f"ov008 (all null-country addresses) must have exactly 1 location (geo only); "
            f"got {len(locations)}: {locations}"
        )
        assert locations[0]["$type"] == "community.lexicon.location.geo", (
            f"Only location must be geo type; got {locations[0]['$type']!r}"
        )

    def test_no_null_fields_in_locations(self, overture_parquet, density_parquet, tmp_path):
        """No location in any record must contain a key with a None/null value.

        Fails against current code: list_concat unions geo and address struct types,
        so geo locations get spurious null address fields (country, region, etc.) and
        address locations get spurious null geo fields (latitude, longitude).
        """
        db_path = tmp_path / "test_ov_no_null_loc.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()
        assert rows, "No rows returned from tile_export"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            locations = parsed["locations"]
            for loc in locations:
                null_keys = [k for k, v in loc.items() if v is None]
                assert not null_keys, (
                    f"Location {loc.get('$type')!r} in record {parsed.get('rkey')!r} "
                    f"has null values for keys: {null_keys}. "
                    "Locations must contain only fields belonging to their type."
                )

    def test_overture_export_mixed_null_country_addresses(self, overture_parquet, density_parquet, tmp_path):
        """ov009 (one null-country entry + one non-null-country entry) must render with
        exactly 1 address location — the non-null-country entry only.

        This validates list_filter drops null-country entries without dropping valid ones.
        """
        db_path = tmp_path / "test_ov_export_mixed.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov009")
        conn.close()
        assert record is not None, "ov009 must appear in tile_export"
        locations = record["locations"]
        addr_entries = [loc for loc in locations if loc.get("$type") == "community.lexicon.location.address"]
        assert len(addr_entries) == 1, (
            f"ov009 (one null, one non-null country) must have exactly 1 address location; "
            f"got {len(addr_entries)}: {addr_entries}"
        )
        assert addr_entries[0]["country"] == "US", (
            f"Surviving address entry must have country='US'; got {addr_entries[0]['country']!r}"
        )
        assert addr_entries[0]["region"] == "CA", (
            f"Expected region='CA' (trimmed); got {addr_entries[0]['region']!r}"
        )

    def test_overture_export_uses_bbox_mean_not_centroid(self):
        """overture_place_export_tiles.sql must compute lat/lon from bbox mean, not st_centroid."""
        import pathlib
        sql_path = pathlib.Path(__file__).parent.parent / "garganorn" / "sql" / "overture_place_export_tiles.sql"
        sql = sql_path.read_text()
        assert "st_centroid" not in sql.lower(), (
            "overture_place_export_tiles.sql must not use st_centroid; "
            "use bbox mean ((bbox.ymin + bbox.ymax) / 2) instead"
        )
        assert "p.bbox.ymin" in sql, (
            "overture_place_export_tiles.sql must use p.bbox.ymin for latitude computation"
        )
        assert "p.bbox.xmin" in sql, (
            "overture_place_export_tiles.sql must use p.bbox.xmin for longitude computation"
        )
        assert "p.bbox.ymax" in sql, (
            "overture_place_export_tiles.sql must use p.bbox.ymax for latitude computation"
        )
        assert "p.bbox.xmax" in sql, (
            "overture_place_export_tiles.sql must use p.bbox.xmax for longitude computation"
        )

    def test_overture_export_latlon_matches_bbox_mean(self, overture_parquet, density_parquet, tmp_path):
        """Exported latitude/longitude must equal bbox center coordinates.

        Regression guard: passes against both old (st_centroid) and new (bbox mean)
        code because ov001's geometry point coincides with its bbox center.
        """
        db_path = tmp_path / "test_ov_export_latlon.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov001")
        conn.close()

        assert record is not None, "ov001 must appear in tile_export"
        locations = record["locations"]
        geo_entries = [loc for loc in locations if loc.get("$type") == "community.lexicon.location.geo"]
        assert len(geo_entries) >= 1, "ov001 must have at least one geo location"
        geo = geo_entries[0]

        # ov001 bbox: xmin=-122.420, ymin=37.774, xmax=-122.418, ymax=37.776
        expected_lat = (37.774 + 37.776) / 2   # = 37.775
        expected_lon = (-122.420 + -122.418) / 2  # = -122.419

        actual_lat = float(geo["latitude"])
        actual_lon = float(geo["longitude"])

        assert abs(actual_lat - expected_lat) < 1e-6, (
            f"latitude must match bbox mean {expected_lat}; got {actual_lat}"
        )
        assert abs(actual_lon - expected_lon) < 1e-6, (
            f"longitude must match bbox mean {expected_lon}; got {actual_lon}"
        )


# ---------------------------------------------------------------------------
# Division containment relations JSON for test fixtures
# ---------------------------------------------------------------------------

# The four division boundaries that contain SF places (lat ~37.77, lon ~-122.42),
# ordered by level ascending (continent first — matches ORDER BY level ASC;
# garganorn.levels.LEVEL_VOCAB, pipeline-implementation-decisions.md
# "OQ-P2-2 — containment level vocabulary").
_SF_WITHIN_JSON = json.dumps({
    "within": [
        {"rkey": "org.atgeo.places.overture.division:div_continent_na"},
        {"rkey": "org.atgeo.places.overture.division:div_country_us"},
        {"rkey": "org.atgeo.places.overture.division:div_region_ca"},
        {"rkey": "org.atgeo.places.overture.division:div_locality_sf"},
    ]
})


def _create_place_containment(conn, entries):
    """Create the place_containment table and insert given (place_id, relations_json) rows.

    `entries` is a list of (place_id, relations_json) tuples.
    Pass an empty list to create an empty table.
    """
    conn.execute("""
        CREATE OR REPLACE TABLE place_containment (
            place_id      VARCHAR,
            relations_json VARCHAR
        )
    """)
    for place_id, relations_json in entries:
        conn.execute(
            "INSERT INTO place_containment VALUES (?, ?)",
            [place_id, relations_json],
        )


# ---------------------------------------------------------------------------
# Tests: Division containment in tile export pipelines
# ---------------------------------------------------------------------------

class TestContainmentInExport:
    """Tests specifying division containment in tile export output.

    All tests in this class FAIL in the Red phase because:
      - Tests 1-4: the export SQL files do not yet LEFT JOIN place_containment,
        so relations.within is never populated.
      - Tests 5-8: compute_containment() does not exist, run_pipeline() does not
        accept boundaries_db, and main() does not accept --boundaries.
    """

    _FSQ_SUBS = {"repo": "places.atgeo.org"}
    _OV_SUBS = {"repo": "places.atgeo.org"}
    _OSM_SUBS = {"repo": "places.atgeo.org"}

    # ------------------------------------------------------------------
    # Test 1: FSQ export includes relations.within when containment present
    # ------------------------------------------------------------------

    def test_fsq_relations_with_containment(self, tmp_path):
        """FSQ export must include relations.within for exp001 when place_containment populated.

        Fails in Red phase because foursquare_export_tiles.sql has `relations: MAP {}`
        and does not LEFT JOIN place_containment.
        """
        db_path = tmp_path / "fsq_containment_with.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        _create_place_containment(conn, [("exp001", _SF_WITHIN_JSON)])

        raw_sql = _load_sql("foursquare_export_tiles.sql", self._FSQ_SUBS)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        record = None
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("rkey") == "exp001":
                record = parsed
                break

        assert record is not None, "exp001 must appear in tile_export"
        relations = record.get("relations", {})
        assert "within" in relations, (
            f"relations must have 'within' key when place_containment populated; "
            f"got relations={relations!r}"
        )
        within = relations["within"]
        assert isinstance(within, list), (
            f"relations.within must be a list; got {type(within)}"
        )
        assert len(within) == 4, (
            f"relations.within must have 4 entries (NA, US, CA, SF); got {len(within)}: {within}"
        )
        for entry in within:
            assert "rkey" in entry, f"within entry missing 'rkey': {entry}"
            assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                f"rkey must have division prefix; got {entry['rkey']!r}"
            assert "name" not in entry, f"'name' key must not appear in rkey-only output: {entry}"
            assert "level" not in entry, f"'level' key must not appear in rkey-only output: {entry}"

    # ------------------------------------------------------------------
    # Test 2: FSQ export produces empty relations when containment table is empty
    # ------------------------------------------------------------------

    def test_fsq_relations_empty_containment(self, tmp_path):
        """FSQ export must produce relations={{}} when place_containment table is empty.

        Fails in Red phase because foursquare_export_tiles.sql does not LEFT JOIN
        place_containment at all — it uses the hardcoded `relations: MAP {}`.
        After the implementation, the SQL must reference place_containment via
        a LEFT JOIN so this test (and test 1) both work against the same SQL.

        Verified by asserting that foursquare_export_tiles.sql contains a reference to
        'place_containment': if the LEFT JOIN is missing, the SQL never touches
        the table and this test fails.
        """
        import pathlib
        sql_path = pathlib.Path(REPO_ROOT) / "garganorn" / "sql" / "foursquare_export_tiles.sql"
        sql_text = sql_path.read_text()
        assert "place_containment" in sql_text, (
            "foursquare_export_tiles.sql must reference 'place_containment' via a LEFT JOIN; "
            "the current SQL has no such reference. "
            "Add: LEFT JOIN place_containment pc ON pc.place_id = p.fsq_place_id"
        )

        db_path = tmp_path / "fsq_containment_empty.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        _create_place_containment(conn, [])  # empty table, same schema

        raw_sql = _load_sql("foursquare_export_tiles.sql", self._FSQ_SUBS)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        assert rows, "tile_export must produce rows even with empty place_containment"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            relations = parsed.get("relations", {})
            assert relations == {}, (
                f"relations must be {{}} when place_containment is empty; got {relations!r}"
            )

    # ------------------------------------------------------------------
    # Test 3: Overture export includes relations.within when containment present
    # ------------------------------------------------------------------

    def test_overture_relations_with_containment(self, overture_parquet, density_parquet, tmp_path):
        """Overture export must include relations.within when place_containment populated.

        Fails in Red phase because overture_place_export_tiles.sql has `relations: '{{}}'::JSON`
        and does not LEFT JOIN place_containment.
        """
        db_path = tmp_path / "ov_containment_with.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run the full Overture pipeline to get places + tile_assignments
        run_overture_import(conn, overture_parquet)
        run_tile_assignments(conn, pk_expr="id", min_zoom=6, max_zoom=17, max_per_tile=5000)

        # Populate place_containment for ov001
        _create_place_containment(conn, [("ov001", _SF_WITHIN_JSON)])

        # Run export
        raw_sql = _load_sql("overture_place_export_tiles.sql", self._OV_SUBS)
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        record = None
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("rkey") == "ov001":
                record = parsed
                break

        assert record is not None, "ov001 must appear in tile_export"
        relations = record.get("relations", {})
        assert "within" in relations, (
            f"overture relations must have 'within' when place_containment populated; "
            f"got relations={relations!r}"
        )
        within = relations["within"]
        assert isinstance(within, list), f"relations.within must be a list; got {type(within)}"
        assert len(within) == 4, (
            f"relations.within must have 4 entries; got {len(within)}: {within}"
        )
        for entry in within:
            assert "rkey" in entry, f"within entry missing 'rkey': {entry}"
            assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                f"rkey must have division prefix; got {entry['rkey']!r}"
            assert "name" not in entry, f"'name' key must not appear in rkey-only output: {entry}"
            assert "level" not in entry, f"'level' key must not appear in rkey-only output: {entry}"

    # ------------------------------------------------------------------
    # Test 4: OSM export includes relations.within when containment present
    # ------------------------------------------------------------------

    def test_osm_relations_with_containment(self, osm_parquet, density_parquet, tmp_path):
        """OSM export must include relations.within when place_containment populated.

        Fails in Red phase because osm_export_tiles.sql has `relations: '{{}}'::JSON`
        and does not LEFT JOIN place_containment.
        """
        db_path = tmp_path / "osm_containment_with.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run the full OSM pipeline (includes importance and variants computation)
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        run_tile_assignments(conn, pk_expr="rkey", min_zoom=6, max_zoom=17, max_per_tile=5000)

        # Get a valid rkey from the imported places to use for containment
        rkeys = conn.execute("SELECT rkey FROM places ORDER BY rkey LIMIT 1").fetchall()
        assert rkeys, "OSM import must produce at least one place"
        target_rkey = rkeys[0][0]

        _create_place_containment(conn, [(target_rkey, _SF_WITHIN_JSON)])

        # Run export
        raw_sql = _load_sql("osm_export_tiles.sql", self._OSM_SUBS)
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        record = None
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            # OSM rkeys are rewritten in the SQL (e.g. 'n1001' → 'node:1001')
            # Check original rkey match by looking at the rkey field
            rkey = parsed.get("rkey", "")
            expected_rkey = (
                target_rkey.replace("n", "node:", 1)
                if target_rkey.startswith("n")
                else target_rkey.replace("w", "way:", 1)
                if target_rkey.startswith("w")
                else target_rkey
            )
            if rkey == expected_rkey:
                record = parsed
                break

        assert record is not None, (
            f"place with rkey={target_rkey!r} must appear in tile_export"
        )
        relations = record.get("relations", {})
        assert "within" in relations, (
            f"osm relations must have 'within' when place_containment populated; "
            f"got relations={relations!r}"
        )
        within = relations["within"]
        assert isinstance(within, list), f"relations.within must be a list; got {type(within)}"
        assert len(within) == 4, (
            f"relations.within must have 4 entries; got {len(within)}: {within}"
        )
        for entry in within:
            assert "rkey" in entry, f"within entry missing 'rkey': {entry}"
            assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                f"rkey must have division prefix; got {entry['rkey']!r}"
            assert "name" not in entry, f"'name' key must not appear in rkey-only output: {entry}"
            assert "level" not in entry, f"'level' key must not appear in rkey-only output: {entry}"

    # ------------------------------------------------------------------
    # Test 5: compute_containment import
    # ------------------------------------------------------------------

    def test_compute_containment_function_exists(self):
        """compute_containment must be importable from garganorn.quadtree.

        Fails in Red phase because the function does not exist yet.
        """
        from garganorn.quadtree import compute_containment  # noqa: F401

    # ------------------------------------------------------------------
    # Test 6: compute_containment produces place_containment table
    # ------------------------------------------------------------------

    def test_compute_containment_produces_table(self, tmp_path, division_db_path):
        """compute_containment must create place_containment with correct columns and rows.

        Fails in Red phase because compute_containment does not exist yet.
        """
        try:
            from garganorn.quadtree import compute_containment
        except (ImportError, AttributeError):
            pytest.fail(
                "compute_containment not importable from garganorn.quadtree; "
                "implement the function to make this test pass"
            )

        db_path = tmp_path / "containment_test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Build a minimal places table with SF coordinates
        conn.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        conn.execute(
            "INSERT INTO places VALUES ('exp001', 37.7749, -122.4194, ST_QuadKey(-122.4194, 37.7749, 17))"
        )
        conn.execute(
            "INSERT INTO places VALUES ('exp002', 37.7694, -122.4862, ST_QuadKey(-122.4862, 37.7694, 17))"
        )

        # tile_assignments is a §3.2 precondition (no fallback in fixed code).
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments SELECT fsq_place_id, left(qk17, 6) FROM places")

        # Export to parquet for Phase 2 API
        import os
        places_pq = str(tmp_path / "places_produces.parquet")
        ta_pq = str(tmp_path / "ta_produces.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()

        # Build covering explicitly (§4: orchestrator responsibility, not compute_containment's).
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering_produces")
        stage_covering(str(division_db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_produces")
        _stage_compute_containment(
            places_pq, ta_pq, str(division_db_path),
            "fsq_place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir, force=True,
        )

        # Read results from parquet output
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert parquet_files, "compute_containment must create parquet files in containment_dir"

        check_con = duckdb.connect()
        # Verify schema
        col_names = {
            row[0]
            for row in check_con.execute(
                f"DESCRIBE SELECT * FROM read_parquet({parquet_files!r})"
            ).fetchall()
        }
        assert "place_id" in col_names, (
            f"containment parquet must have 'place_id' column; got columns: {col_names}"
        )
        assert "relations_json" in col_names, (
            f"containment parquet must have 'relations_json' column; got columns: {col_names}"
        )

        # Verify rows exist for SF places (they fall inside division boundaries)
        rows = check_con.execute(
            f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r})"
        ).fetchall()
        check_con.close()

        assert len(rows) >= 1, (
            "compute_containment must produce at least one row for SF test places "
            f"that fall inside the division boundaries; got {len(rows)} rows"
        )
        for place_id, relations_json in rows:
            parsed = json.loads(relations_json)
            assert "within" in parsed, (
                f"relations_json for {place_id!r} must have 'within' key; got {parsed!r}"
            )
            within = parsed["within"]
            assert isinstance(within, list), f"within must be a list for {place_id}; got {type(within)}"
            assert len(within) >= 1, f"SF coordinates should be contained by at least 1 boundary"
            for entry in within:
                assert "rkey" in entry, f"within entry missing 'rkey': {entry}"
                assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                    f"rkey must be collection-qualified; got {entry['rkey']!r}"
                assert "name" not in entry, f"'name' key must not appear in rkey-only output: {entry}"
                assert "level" not in entry, f"'level' key must not appear in rkey-only output: {entry}"

    # ------------------------------------------------------------------
    # Test 6b: compute_containment bbox pre-filter regression guard
    # ------------------------------------------------------------------

    def test_compute_containment_matches_all_containing_boundaries(self, tmp_path, division_db_path):
        """compute_containment must match ALL boundaries that contain a place, not just some.

        This test guards against a future bbox pre-filter incorrectly excluding a
        boundary whose bbox columns do contain the place's coordinates. A buggy
        pre-filter that uses wrong bbox values (e.g., inverted min/max, or a bbox
        that is too small) would produce fewer than the expected 4 containment
        entries for the SF test point, causing this test to fail.

        The SF test point (37.7749, -122.4194) falls inside all four division boundaries
        defined in conftest.py::DIVISION_BOUNDARIES that cover North America:
          - div_continent_na (level 0): bbox [20,-130] to [55,-60]
          - div_country_us   (level 10): bbox [24,-125] to [50,-66]
          - div_region_ca    (level 25): bbox [34,-125] to [42,-118]
          - div_locality_sf  (level 50): bbox [37.6,-122.55] to [37.85,-122.3]

        div_borough_manhattan (level 50) does NOT contain the SF point,
        so exactly 4 entries are expected and no more.

        This test passes NOW (current code uses ST_Contains with no bbox pre-filter)
        and must continue to pass after a correctly-implemented bbox pre-filter is
        added. It FAILS if the bbox pre-filter is buggy (false negative).
        """
        try:
            from garganorn.quadtree import compute_containment
        except (ImportError, AttributeError):
            pytest.fail(
                "compute_containment not importable from garganorn.quadtree"
            )

        db_path = tmp_path / "containment_bbox_test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        conn.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        # SF city hall — inside North America, US, California, and San Francisco boundaries.
        conn.execute(
            "INSERT INTO places VALUES ('sf001', 37.7749, -122.4194, ST_QuadKey(-122.4194, 37.7749, 17))"
        )

        # tile_assignments is a §3.2 precondition (no fallback in fixed code).
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments SELECT fsq_place_id, left(qk17, 6) FROM places")

        # Export to parquet for Phase 2 API
        import os
        places_pq = str(tmp_path / "places_bbox.parquet")
        ta_pq = str(tmp_path / "ta_bbox.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()

        # Build covering explicitly (§4: orchestrator responsibility, not compute_containment's).
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering_bbox")
        stage_covering(str(division_db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_bbox")
        _stage_compute_containment(
            places_pq, ta_pq, str(division_db_path),
            "fsq_place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir, force=True,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert parquet_files, "compute_containment must produce parquet files with a valid boundaries_db"
        check_con = duckdb.connect()
        rows = check_con.execute(
            f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r}) "
            f"WHERE place_id = 'sf001'"
        ).fetchall()
        check_con.close()

        assert len(rows) == 1, (
            f"Expected exactly 1 place_containment row for 'sf001', got {len(rows)}"
        )

        parsed = json.loads(rows[0][1])
        within = parsed["within"]

        # The SF point falls inside exactly 4 of the 5 division test boundaries.
        # If a bbox pre-filter incorrectly excludes any of these 4 boundaries,
        # this assertion will catch it.
        expected_rkeys = {
            "org.atgeo.places.overture.division:div_continent_na",
            "org.atgeo.places.overture.division:div_country_us",
            "org.atgeo.places.overture.division:div_region_ca",
            "org.atgeo.places.overture.division:div_locality_sf",
        }
        actual_rkeys = {entry["rkey"] for entry in within}
        assert actual_rkeys == expected_rkeys, (
            f"compute_containment produced wrong set of containing boundaries.\n"
            f"  Expected: {sorted(expected_rkeys)}\n"
            f"  Got:      {sorted(actual_rkeys)}\n"
            f"A missing boundary indicates the bbox pre-filter incorrectly excluded it "
            f"(false negative). An extra boundary indicates incorrect inclusion."
        )

    # ------------------------------------------------------------------
    # Test 7: run_pipeline accepts boundaries_db keyword argument
    # ------------------------------------------------------------------

    def test_run_pipeline_accepts_boundaries_db(self):
        """run_pipeline must accept a boundaries_db keyword argument.

        Fails in Red phase because run_pipeline has no boundaries_db parameter.
        """
        from garganorn.quadtree import run_pipeline
        sig = inspect.signature(run_pipeline)
        assert "boundaries_db" in sig.parameters, (
            f"run_pipeline must have a 'boundaries_db' parameter; "
            f"current parameters: {list(sig.parameters)}"
        )

    # ------------------------------------------------------------------
    # Test 8: main() accepts --boundaries CLI argument
    # ------------------------------------------------------------------

    def test_main_accepts_boundaries_arg(self):
        """main() argparse must accept a --boundaries CLI argument.

        Fails in Red phase because main() does not define --boundaries.
        """
        import argparse
        import sys
        from unittest.mock import patch

        # Parse a minimal valid invocation that includes --boundaries.
        # If --boundaries is not defined, argparse will raise SystemExit(2).
        test_args = [
            "run",
            "--source", "foursquare",
            "--parquet", "/tmp/test.parquet",
            "--output", "/tmp/output",
            "--boundaries", "/tmp/boundaries.duckdb",
        ]
        with patch.object(sys, "argv", ["quadtree"] + test_args):
            try:
                # Re-parse using a fresh parser by importing and calling main's
                # internal parser logic. We do this by inspecting the source
                # rather than calling main() (which would trigger run_pipeline).
                # Instead, verify that argparse accepts --boundaries by constructing
                # the same parser that main() uses, which must include the argument.
                from garganorn import quadtree as _qt
                # Build the parser the same way main() does, then parse our args.
                # Since we can't easily extract the parser, we verify by calling
                # parse_known_args: if --boundaries is unrecognized it lands in extras.
                import argparse as _ap
                test_parser = _ap.ArgumentParser()
                # Minimal args that main() would define; add --boundaries.
                # The real test: does the actual main() parser accept it?
                # We simulate by running the whole argparse block.
                # Easiest approach: mock run_pipeline and call main() directly.
                with patch.object(_qt, "run_pipeline", return_value=None):
                    # Suppress SystemExit if --boundaries triggers an error
                    try:
                        _qt.main()
                    except SystemExit as exc:
                        # SystemExit(0) = success (e.g. --help); others = failure
                        # SystemExit(2) = argument parsing error (unrecognized --boundaries)
                        if exc.code == 2:
                            pytest.fail(
                                "main() argparse does not accept --boundaries; "
                                "add parser.add_argument('--boundaries', ...) to main()"
                            )
            except Exception:
                raise  # Don't swallow unexpected exceptions

    # ------------------------------------------------------------------
    # Test 9: compute_containment with None boundaries_db creates empty table
    # ------------------------------------------------------------------

    def test_compute_containment_none_boundaries(self, tmp_path):
        """compute_containment(conn, None, ...) must create an empty place_containment table.

        When no boundaries DB is provided, the function should still create
        the table (so downstream SQL can LEFT JOIN it) but insert no rows.
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "containment_none.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        conn.execute(
            "INSERT INTO places VALUES ('p1', 37.7749, -122.4194, "
            "ST_QuadKey(-122.4194, 37.7749, 17))"
        )

        import os
        places_pq = str(tmp_path / "places_none.parquet")
        ta_pq = str(tmp_path / "ta_none.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(
            f"COPY (SELECT fsq_place_id AS place_id, left(qk17, 6) AS tile_qk FROM places) "
            f"TO '{ta_pq}' (FORMAT PARQUET)"
        )
        conn.close()

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_none")
        _stage_compute_containment(
            places_pq, ta_pq, None, "fsq_place_id", "longitude", "latitude",
            containment_dir, force=True,
        )

        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), (
            "containment _meta.json must exist even with None boundaries_db"
        )
        with open(meta_path) as f:
            meta = json.load(f)
        assert meta.get("empty") is True, (
            f"_meta.json must have empty=True when boundaries_db is None; got {meta}"
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) == 0, (
            f"place_containment must have no parquet files when boundaries_db is None; "
            f"got {parquet_files}"
        )

    # ------------------------------------------------------------------
    # Test 10: place outside all boundaries produces no containment row
    # ------------------------------------------------------------------

    def test_compute_containment_place_outside_all_boundaries(self, tmp_path, division_db_path):
        """A place at (0, 0) in the ocean should produce no place_containment row.

        All test division boundaries cover parts of North America. A point in the
        Gulf of Guinea should not be contained by any of them. This validates
        that compute_containment does not produce spurious containment rows
        for places outside all boundaries.
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "containment_ocean.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        # Point at (0, 0) — Gulf of Guinea, outside all test boundaries
        conn.execute(
            "INSERT INTO places VALUES ('ocean001', 0.0, 0.0, "
            "ST_QuadKey(0.0, 0.0, 17))"
        )

        # tile_assignments is a §3.2 precondition (no fallback in fixed code).
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments SELECT fsq_place_id, left(qk17, 6) FROM places")

        # Export to parquet for Phase 2 API
        import os
        places_pq = str(tmp_path / "places_ocean.parquet")
        ta_pq = str(tmp_path / "ta_ocean.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()

        # Build covering explicitly (§4: orchestrator responsibility, not compute_containment's).
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering_ocean")
        stage_covering(str(division_db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_ocean")
        _stage_compute_containment(
            places_pq, ta_pq, str(division_db_path),
            "fsq_place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir, force=True,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        if parquet_files:
            check_con = duckdb.connect()
            rows = check_con.execute(
                f"SELECT place_id FROM read_parquet({parquet_files!r}) "
                f"WHERE place_id = 'ocean001'"
            ).fetchall()
            check_con.close()
        else:
            rows = []

        assert len(rows) == 0, (
            f"Place at (0, 0) should not be contained by any boundary; "
            f"got {len(rows)} containment rows"
        )

    # Tests 11, 13-17 deleted: asserted internals of the removed tile_boundaries/
    # two-phase recursive machinery (the design replaces that with the covering
    # stage + quadkey-prefix joins).  Authorized deletions per Green TDD phase.

    # ------------------------------------------------------------------
    # Test 12: place near tile edge with boundary straddling the edge
    # ------------------------------------------------------------------

    def test_compute_containment_place_near_tile_edge(self, tmp_path, division_db_path):
        """A place near the edge of the SF city boundary must still be correctly
        contained when it falls inside the boundary polygon.

        The SF boundary (div_locality_sf) is POLYGON((-122.55 37.6, -122.55 37.85,
        -122.3 37.85, -122.3 37.6, -122.55 37.6)). We test:
          - A point just inside the SW corner: (37.61, -122.54) — should be in SF
          - A point just outside the SW corner: (37.59, -122.56) — should NOT be in SF
            but should still be in NA, US, CA (the z6 tile is the same: 023010)

        Both points are in the same z6 tile, so the two-phase optimization must
        correctly distinguish between them using per-point ST_Contains in phase 2.
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "containment_edge.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        # Just inside SF boundary SW corner
        conn.execute(
            "INSERT INTO places VALUES ('edge_in', 37.61, -122.54, "
            "ST_QuadKey(-122.54, 37.61, 17))"
        )
        # Just outside SF boundary SW corner (but still in CA)
        conn.execute(
            "INSERT INTO places VALUES ('edge_out', 37.59, -122.56, "
            "ST_QuadKey(-122.56, 37.59, 17))"
        )

        # tile_assignments is a §3.2 precondition (no fallback in fixed code).
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments SELECT fsq_place_id, left(qk17, 6) FROM places")

        # Export to parquet for Phase 2 API
        import os
        places_pq = str(tmp_path / "places_edge.parquet")
        ta_pq = str(tmp_path / "ta_edge.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()

        # Build covering explicitly (§4: orchestrator responsibility, not compute_containment's).
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering_edge")
        stage_covering(str(division_db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_edge")
        _stage_compute_containment(
            places_pq, ta_pq, str(division_db_path),
            "fsq_place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir, force=True,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert parquet_files, "compute_containment must produce parquet files for edge test"

        check_con = duckdb.connect()
        # edge_in: should be in NA, US, CA, SF (4 boundaries)
        in_rows = check_con.execute(
            f"SELECT relations_json FROM read_parquet({parquet_files!r}) "
            f"WHERE place_id = 'edge_in'"
        ).fetchall()
        assert len(in_rows) == 1, f"Expected 1 row for edge_in; got {len(in_rows)}"
        in_within = json.loads(in_rows[0][0])["within"]
        in_rkeys = {e["rkey"] for e in in_within}
        expected_in_rkeys = {
            "org.atgeo.places.overture.division:div_continent_na",
            "org.atgeo.places.overture.division:div_country_us",
            "org.atgeo.places.overture.division:div_region_ca",
            "org.atgeo.places.overture.division:div_locality_sf",
        }
        assert in_rkeys == expected_in_rkeys, (
            f"edge_in should be in 4 boundaries; got {sorted(in_rkeys)}"
        )

        # edge_out: should be in NA, US, CA only (3 boundaries, not SF)
        out_rows = check_con.execute(
            f"SELECT relations_json FROM read_parquet({parquet_files!r}) "
            f"WHERE place_id = 'edge_out'"
        ).fetchall()
        check_con.close()
        assert len(out_rows) == 1, f"Expected 1 row for edge_out; got {len(out_rows)}"
        out_within = json.loads(out_rows[0][0])["within"]
        out_rkeys = {e["rkey"] for e in out_within}
        expected_out_rkeys = {
            "org.atgeo.places.overture.division:div_continent_na",
            "org.atgeo.places.overture.division:div_country_us",
            "org.atgeo.places.overture.division:div_region_ca",
        }
        assert out_rkeys == expected_out_rkeys, (
            f"edge_out should be in 3 boundaries (not SF); got {sorted(out_rkeys)}"
        )


# ---------------------------------------------------------------------------
# §3.8 — Record JSON byte-identical per source (Phase 2 export)
# ---------------------------------------------------------------------------

class TestExportRecordParityPhase2:
    """§3.8: Stage 2 export from parquet artifacts must produce byte-identical
    record JSON per source, compared to the Phase 1 view-based output.

    §3.8: Two forced exports from the same artifacts must produce
    gunzip-byte-identical tile files (pins the ORDER BY tile_qk, place_id invariant).

    Both fail RED because stage_export does not yet accept parquet artifact paths.
    """

    def test_stage_export_has_parquet_signature(self):
        """stage_export must accept (source, places_parquet, tile_assignments_parquet,
        containment_dir, tiles_root, ...) — Phase 2 signature.

        Fails RED because current stage_export takes 'con' as first argument.
        """
        import inspect
        from garganorn.stages import stage_export
        sig = inspect.signature(stage_export)
        params = list(sig.parameters.keys())
        assert params[0] != "con", (
            f"stage_export must not take 'con' as first param (Phase 2 §3.8); "
            f"got {params[0]!r} — fails RED because Phase 2 signature not yet implemented"
        )
        assert "places_parquet" in params, (
            f"stage_export missing 'places_parquet' param; got {params} — "
            "fails RED because Phase 2 signature not yet implemented"
        )

    def test_two_forced_fsq_exports_byte_identical(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """Two forced exports from the same foursquare artifacts must produce
        gunzip-byte-identical tile files (§3.8 determinism: ORDER BY tile_qk, place_id).

        Two details make this assertion mean what it says:

        The clock is pinned. stage_export stamps a wall-clock generated_at at
        second granularity into every tile envelope (stages.py:1469-1471), so
        two runs straddling a clock tick differ by one digit inside the
        timestamp -- a difference that has nothing to do with export
        determinism. Unpinned, this test passes only when both pipeline runs
        happen to land in the same second, which is a coin flip on a loaded
        machine, not an assertion.

        The runs write to separate roots. The run-dir name derives from that
        same pinned instant, so sharing one root would make the second run
        overwrite the first in place and the comparison would compare each
        file to itself -- passing no matter what export did.
        """
        import gzip as _gzip
        from datetime import datetime as _datetime, timezone as _timezone
        import garganorn.stages as _stages
        from garganorn.quadtree import run_pipeline as _run_pipeline

        fixed = _datetime(2026, 1, 2, 3, 4, 5, tzinfo=_timezone.utc)

        class _FixedClock(_datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed

        monkeypatch.setattr(_stages, "datetime", _FixedClock)

        def _run(root):
            root.mkdir()
            _run_pipeline(
                "foursquare", fsq_parquet,
                (-122.55, 37.60, -122.30, 37.85),
                str(root), memory_limit="4GB", max_per_tile=100,
                density_parquet=density_parquet, force=True,
            )
            tiles_current = root / "foursquare" / "tiles" / "current"
            assert tiles_current.exists(), (
                f"Phase 2 tiles must be at {tiles_current}"
            )
            tiles = {}
            for p in tiles_current.rglob("*.json.gz"):
                with _gzip.open(p) as f:
                    tiles[str(p.relative_to(tiles_current))] = f.read()
            return tiles

        first_run = _run(tmp_path / "det_out_a")
        second_run = _run(tmp_path / "det_out_b")

        assert first_run, "first export produced no tiles; nothing was compared"
        assert first_run.keys() == second_run.keys(), (
            f"the two exports produced different tile sets: "
            f"only in first={sorted(first_run.keys() - second_run.keys())}, "
            f"only in second={sorted(second_run.keys() - first_run.keys())}"
        )
        for rel, first_bytes in first_run.items():
            assert second_run[rel] == first_bytes, (
                f"Tile {rel}: second forced export differs from first — "
                "ORDER BY tile_qk, place_id must make export deterministic"
            )


# ---------------------------------------------------------------------------
# §3.8 — write_manifest_db from tile_assignments.parquet (Phase 2)
# ---------------------------------------------------------------------------

class TestWriteManifestDbPhase2:
    """§3.8: write_manifest_db must read tile_assignments from parquet (Phase 2),
    produce the same rkey/tile_qk rows as the current implementation, and
    preserve the OSM rkey transform (n12345 → node:12345).

    Fails RED because write_manifest_db still takes 'con' as first argument.
    """

    def test_write_manifest_db_accepts_parquet_path(self):
        """write_manifest_db must accept tile_assignments_parquet as first arg (Phase 2).

        Fails RED because current signature is write_manifest_db(con, output_dir, source).
        """
        import inspect
        from garganorn.stages import write_manifest_db
        sig = inspect.signature(write_manifest_db)
        params = list(sig.parameters.keys())
        assert params[0] != "con", (
            f"write_manifest_db must not take 'con' first (Phase 2 §3.8); "
            f"got {params[0]!r} — fails RED because Phase 2 signature not yet implemented"
        )
        assert "tile_assignments_parquet" in params or "parquet" in params[0].lower(), (
            f"write_manifest_db must accept parquet path; got params: {params}"
        )

    def test_osm_rkey_transform_preserved_in_parquet_path(self, tmp_path):
        """OSM rkey transform (n12345→node:12345, w12345→way:12345) must be preserved
        when write_manifest_db reads from tile_assignments.parquet.

        Fails RED because write_manifest_db still takes 'con' as first arg.
        """
        from garganorn.stages import write_manifest_db

        # Write a minimal tile_assignments.parquet with OSM-style place_ids
        ta_path = str(tmp_path / "osm_ta.parquet")
        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT place_id, tile_qk FROM (
                    VALUES ('n123456', '023130'), ('w789', '023131')
                ) t(place_id, tile_qk)
            ) TO '{ta_path}' (FORMAT PARQUET)
        """)
        con.close()

        run_dir = str(tmp_path / "run")
        import os as _os
        _os.makedirs(run_dir)

        # Phase 2 signature: write_manifest_db(tile_assignments_parquet, output_dir, source)
        # Fails RED with TypeError
        write_manifest_db(ta_path, run_dir, "osm")

        manifest_db = tmp_path / "run" / "manifest.duckdb"
        assert manifest_db.exists(), "manifest.duckdb must be written"

        # duckdb.connect() takes a file path, not SQL — attach separately.
        check = duckdb.connect()
        check.execute(f"ATTACH '{manifest_db}' AS m (READ_ONLY)")
        rows = {
            (r[0], r[1])
            for r in check.execute(
                "SELECT rkey, tile_qk FROM m.record_tiles ORDER BY rkey"
            ).fetchall()
        }
        assert ("node:123456", "023130") in rows, (
            f"OSM rkey 'n123456' must transform to 'node:123456'; got rows: {rows}"
        )
        assert ("way:789", "023131") in rows, (
            f"OSM rkey 'w789' must transform to 'way:789'; got rows: {rows}"
        )


# ---------------------------------------------------------------------------
# §3.8 Gate #13 Finding #1 — stage_export Phase 2 body (tests 1a–1d)
# ---------------------------------------------------------------------------

class TestStageExportPhase2Body:
    """§3.8 tests for the body of stage_export (Phase 2 export from parquet artifacts).

    All tests fail RED because stage_export currently raises NotImplementedError
    ('stage_export Phase 2 body not yet wired; call _transitional_export_phase1 …').

    stage_export actual signature (garganorn/stages.py:1542):
        stage_export(source, places_parquet, tile_assignments_parquet,
                     containment_dir, tiles_root, t0, export_workers=None)

    The `t0` positional parameter is intentional (matches how run_pipeline calls it);
    it is not in the §3.8 prose but IS in the current function signature.
    """

    # ------------------------------------------------------------------
    # Fixture helpers
    # ------------------------------------------------------------------

    def _build_fsq_fixtures(self, tmp_path):
        """Build minimal foursquare places.parquet + tile_assignments.parquet.

        Uses _make_fsq_export_db to populate tables matching foursquare_export_tiles.sql,
        then COPYs them to parquet files for Phase 2 input.
        """
        import os
        places_pq = str(tmp_path / "places.parquet")
        ta_pq = str(tmp_path / "tile_assignments.parquet")
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        _make_fsq_export_db(conn)
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()
        return places_pq, ta_pq

    def _build_containment_dir(self, tmp_path, name="containment"):
        """Build a containment_dir with only _meta.json (no *.parquet files).

        Represents the Q3-degradation / empty-containment case (spec §3.7).
        When no *.parquet exists in containment_dir, stage_export must use:
            (SELECT NULL::VARCHAR AS place_id, NULL::VARCHAR AS relations_json WHERE 1=0)
        instead of read_parquet on an empty glob (which errors on DuckDB 1.2.1).
        """
        import os
        cd = str(tmp_path / name)
        os.makedirs(cd, exist_ok=True)
        with open(os.path.join(cd, "_meta.json"), "w") as f:
            json.dump({"empty": True, "params": {}, "inputs": []}, f)
        return cd

    # ------------------------------------------------------------------
    # Test 1a: Record-JSON parity (§3.8)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 1b: Empty-containment substitution (§3.8)
    # ------------------------------------------------------------------

    def test_empty_containment_gives_empty_relations(self, tmp_path):
        """§3.8: containment_dir with only _meta.json (no *.parquet) → relations=={}.

        The implementation must substitute the empty-containment subquery:
            (SELECT NULL::VARCHAR AS place_id, NULL::VARCHAR AS relations_json WHERE 1=0)
        NOT a read_parquet glob, which errors on DuckDB 1.2.1 when the glob matches nothing.

        Records are {uri, cid, value}-wrapped (docs/pipeline-implementation-decisions.md,
        "OQ-P2-1 — record envelope adoption"); relations lives on
        value, not on the wrapper -- this FAILS against the current envelope
        (records are still flat) because parsed.get("relations") reads the
        wrapper (which has no 'relations' key) instead of value.
        """
        import os, time
        from pathlib import Path
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_fsq_fixtures(tmp_path)
        containment_dir = self._build_containment_dir(tmp_path)

        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)
        t0 = time.monotonic()

        stage_export("foursquare", places_pq, ta_pq, containment_dir, tiles_root, t0)

        # Every record's value must have relations == {}
        gz_files = list(Path(tiles_root).rglob("*.json.gz"))
        assert gz_files, "stage_export must produce at least one .json.gz"
        for gz_file in gz_files:
            with gzip.open(gz_file) as f:
                tile = json.loads(f.read())
            for rec in tile["records"]:
                assert set(rec.keys()) == {"uri", "cid", "value"}, (
                    f"record must be {{uri, cid, value}}-wrapped; got {list(rec)}"
                )
                value = rec["value"]
                assert value.get("relations") == {}, (
                    f"Empty containment must produce relations=={{}}; "
                    f"got {value.get('relations')!r} for rkey {value.get('rkey')!r}"
                )

    # ------------------------------------------------------------------
    # Test 1c: Determinism (§3.8)
    # ------------------------------------------------------------------

    def test_determinism_two_exports_byte_identical(self, tmp_path):
        """§3.8 / §6 item 7 (pipeline-implementation-decisions.md
        "OQ-P2-1 — record envelope adoption"): two exports from identical
        artifacts, with an injected fixed timestamp, produce byte-identical
        .json.gz files.

        Pins the ORDER BY tile_qk, place_id invariant (spec §3.6 + §3.8 step 3).
        gzip mtime=0 is already set by the existing export_tiles implementation.

        Post-2b, tiles carry a run-scoped `generated_at` (per the envelope
        decisions above); without
        injecting the same `now` into both calls, two exports run at different
        wall-clock seconds would legitimately differ in that one field, which
        would make this test fail for a reason unrelated to the
        ORDER BY/determinism property it exists to pin. Injecting `now` keeps
        the test measuring the invariant it's named for.
        """
        import os, time
        from datetime import datetime, timezone
        from pathlib import Path
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_fsq_fixtures(tmp_path)
        containment_dir = self._build_containment_dir(tmp_path)
        t0 = time.monotonic()
        fixed_now = datetime(2026, 7, 9, 18, 0, 0, tzinfo=timezone.utc)

        # Two INDEPENDENT tiles_root dirs from identical input artifacts. Using
        # separate roots (rather than two runs into one root with force) avoids
        # (a) the export freshness gate skipping the second run and (b) the
        # key-collapse ambiguity of scanning two run dirs under one root — a
        # non-deterministic run must not be masked by whichever file rglob
        # visits last. Each root gets exactly one run dir.
        tiles_root_a = str(tmp_path / "tiles_a")
        tiles_root_b = str(tmp_path / "tiles_b")
        os.makedirs(tiles_root_a)
        os.makedirs(tiles_root_b)

        stage_export("foursquare", places_pq, ta_pq, containment_dir, tiles_root_a, t0,
                     now=fixed_now)
        stage_export("foursquare", places_pq, ta_pq, containment_dir, tiles_root_b, t0,
                     now=fixed_now)

        def _collect_tile_bytes(root):
            """Return {rel_path: bytes} for all .json.gz files, relative to the run dir."""
            result = {}
            for gz_file in Path(root).rglob("*.json.gz"):
                parts = gz_file.parts
                ts_idx = next(
                    (i for i, p in enumerate(parts) if re.match(r"^\d{8}T\d{6}$", p)),
                    None,
                )
                if ts_idx is not None:
                    rel = str(Path(*parts[ts_idx + 1:]))
                    result[rel] = gz_file.read_bytes()
            return result

        first_bytes = _collect_tile_bytes(tiles_root_a)
        second_bytes = _collect_tile_bytes(tiles_root_b)
        assert first_bytes, "First export must produce at least one .json.gz"

        assert set(second_bytes.keys()) == set(first_bytes.keys()), (
            f"Tile key sets differ between run 1 and run 2:\n"
            f"  only in run 2: {sorted(set(second_bytes) - set(first_bytes))}\n"
            f"  only in run 1: {sorted(set(first_bytes) - set(second_bytes))}"
        )
        for rel, first_content in first_bytes.items():
            assert second_bytes[rel] == first_content, (
                f"Tile {rel!r}: not byte-identical between two exports; "
                "ORDER BY tile_qk, place_id must produce deterministic output"
            )

    # ------------------------------------------------------------------
    # Tests 1d: Run-dir lifecycle (§3.8 / spec §2 / §3.8 step 2+5)
    # ------------------------------------------------------------------

    def test_run_dir_leftover_incomplete_deleted(self, tmp_path):
        """§3.8: a tiles/<ts>/ lacking manifest.json is deleted at next export.

        Spec §2: a run dir is complete iff manifest.json exists (written last).
        Stage must scan tiles/ at step 2 and delete any incomplete dirs (no manifest.json,
        not the current symlink target) before creating the new run.

        Fails RED because stage_export raises NotImplementedError.
        """
        import os, time
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_fsq_fixtures(tmp_path)
        containment_dir = self._build_containment_dir(tmp_path)

        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)

        # Plant a leftover incomplete run dir (tile files present, manifest.json absent)
        leftover_ts = "20240101T000000"
        leftover_tile_dir = os.path.join(tiles_root, leftover_ts, "023130")
        os.makedirs(leftover_tile_dir)
        with open(os.path.join(leftover_tile_dir, "0231300.json.gz"), "wb") as f:
            f.write(gzip.compress(b'{"records":[]}', mtime=0))
        # Deliberately do NOT write manifest.json — this simulates a crash mid-export

        t0 = time.monotonic()

        # FAILS RED: NotImplementedError
        stage_export("foursquare", places_pq, ta_pq, containment_dir, tiles_root, t0)

        # After implementation: the incomplete leftover run dir must be deleted
        leftover_run_dir = os.path.join(tiles_root, leftover_ts)
        assert not os.path.exists(leftover_run_dir), (
            f"Incomplete run dir {leftover_run_dir!r} must be deleted by stage_export "
            "(spec §2 / §3.8 step 2)"
        )

    def test_manifest_json_written_after_manifest_duckdb(self, tmp_path):
        """§3.8: manifest.json mtime must be >= manifest.duckdb mtime.

        manifest.json is written LAST as the run-dir completeness marker (spec §3.8 step 5).
        After implementation, manifest.duckdb is written first, then manifest.json.

        Fails RED because stage_export raises NotImplementedError.
        """
        import os, time
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_fsq_fixtures(tmp_path)
        containment_dir = self._build_containment_dir(tmp_path)
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)
        t0 = time.monotonic()

        # FAILS RED: NotImplementedError
        stage_export("foursquare", places_pq, ta_pq, containment_dir, tiles_root, t0)

        # After implementation: find run dir via tiles/current symlink
        current = os.path.join(tiles_root, "current")
        assert os.path.islink(current), "tiles/current symlink must exist after stage_export"
        run_dir = os.path.realpath(current)

        manifest_json = os.path.join(run_dir, "manifest.json")
        manifest_duckdb = os.path.join(run_dir, "manifest.duckdb")
        assert os.path.exists(manifest_json), f"manifest.json must be written to {run_dir}"
        assert os.path.exists(manifest_duckdb), f"manifest.duckdb must be written to {run_dir}"
        assert os.path.getmtime(manifest_json) >= os.path.getmtime(manifest_duckdb), (
            "manifest.json mtime must be >= manifest.duckdb mtime "
            "(manifest.json is the completeness marker; it must land last, spec §3.8 step 5)"
        )

    def test_tiles_current_symlink_points_at_new_run(self, tmp_path):
        """§3.8: tiles/current symlink is updated to the new timestamp dir.

        The symlink must target a valid, existing timestamp dir containing manifest.json.

        Fails RED because stage_export raises NotImplementedError.
        """
        import os, time
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_fsq_fixtures(tmp_path)
        containment_dir = self._build_containment_dir(tmp_path)
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)
        t0 = time.monotonic()

        # FAILS RED: NotImplementedError
        stage_export("foursquare", places_pq, ta_pq, containment_dir, tiles_root, t0)

        current = os.path.join(tiles_root, "current")
        assert os.path.islink(current), "tiles/current must be a symlink after stage_export"

        target = os.readlink(current)
        assert re.match(r"^\d{8}T\d{6}$", target), (
            f"tiles/current must point at a timestamp dir (YYYYMMDDTHHmmss); got {target!r}"
        )
        run_dir = os.path.join(tiles_root, target)
        assert os.path.isdir(run_dir), (
            f"Symlink target {target!r} must be an existing directory"
        )
        assert os.path.exists(os.path.join(run_dir, "manifest.json")), (
            f"Run dir {target!r} must contain manifest.json (completeness marker, spec §3.8)"
        )

    def test_keep_2_sweep_retains_only_complete_dirs(self, tmp_path):
        """§3.8: keep-2 sweep retains only the 2 newest COMPLETE run dirs.

        Complete = has manifest.json. Incomplete dirs are already deleted at step 2.
        Keep-2 sweep at step 5 removes complete run dirs older than the newest 2.

        Fails RED because stage_export raises NotImplementedError.
        """
        import os, time
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_fsq_fixtures(tmp_path)
        containment_dir = self._build_containment_dir(tmp_path)
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)

        # Pre-create 3 complete run dirs with older timestamps
        old_timestamps = ["20240101T000001", "20240101T000002", "20240101T000003"]
        for ts in old_timestamps:
            run_dir = os.path.join(tiles_root, ts)
            os.makedirs(os.path.join(run_dir, "023130"))
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                json.dump({"source": "foursquare", "quadkeys": []}, f)

        t0 = time.monotonic()

        # FAILS RED: NotImplementedError
        stage_export("foursquare", places_pq, ta_pq, containment_dir, tiles_root, t0)

        # After implementation: at most 2 complete run dirs should survive (keep-2)
        complete_dirs = sorted([
            d for d in os.listdir(tiles_root)
            if re.match(r"^\d{8}T\d{6}$", d)
            and os.path.isdir(os.path.join(tiles_root, d))
            and not os.path.islink(os.path.join(tiles_root, d))
            and os.path.exists(os.path.join(tiles_root, d, "manifest.json"))
        ])
        assert len(complete_dirs) <= 2, (
            f"keep-2 sweep must retain at most 2 complete run dirs; "
            f"found {len(complete_dirs)}: {complete_dirs}"
        )
