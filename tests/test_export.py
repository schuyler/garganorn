"""Tests for fsq_export_tiles.sql, overture_export_tiles.sql, and export_tiles()."""

import gzip
import json

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    run_overture_import, run_tile_assignments,
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


# ---------------------------------------------------------------------------
# Tests: fsq_export_tiles.sql
# ---------------------------------------------------------------------------

class TestFsqExportTiles:
    """Tests for garganorn/sql/fsq_export_tiles.sql.

    All tests fail at Red phase: the SQL file does not exist yet.
    """

    _SUBS = {"attribution": "Foursquare Open Source Places", "repo": "https://example.com"}

    def _run_export(self, conn):
        raw_sql = _load_sql("fsq_export_tiles.sql", self._SUBS)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_sql_file_exists(self):
        sql_path = REPO_ROOT / "garganorn" / "sql" / "fsq_export_tiles.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def test_export_produces_rows(self, tmp_path):
        db_path = tmp_path / "test_fsq_export_rows.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT tile_qk, record_json FROM tile_export").fetchall()
        conn.close()
        assert len(rows) >= len(_FSQ_EXPORT_PLACES), (
            f"fsq_export_tiles.sql must produce at least one row per fixture place (>=4); got {len(rows)}"
        )

    def test_record_json_structure(self, tmp_path):
        """record_json must be valid JSON with top-level 'uri' and 'value' keys."""
        db_path = tmp_path / "test_fsq_export_struct.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        self._run_export(conn)
        rows = conn.execute("SELECT tile_qk, record_json FROM tile_export").fetchall()
        conn.close()
        assert rows, "No rows returned from tile_export"
        for tile_qk, record_json in rows:
            parsed = json.loads(record_json)
            assert "uri" in parsed, (
                f"record_json for {tile_qk} missing 'uri' key; keys={list(parsed)}"
            )
            assert parsed["uri"].startswith("https://"), (
                f"record_json uri must start with 'https://'; got {parsed['uri']!r}"
            )
            assert "value" in parsed, (
                f"record_json for {tile_qk} missing 'value' key; keys={list(parsed)}"
            )
            assert parsed["value"].get("$type") == "org.atgeo.place", (
                f"record_json value.$type must be 'org.atgeo.place'; got {parsed['value'].get('$type')!r}"
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
            assert "uri" in parsed, f"Record missing 'uri': {list(parsed)}"
            assert isinstance(parsed["uri"], str), "uri must be a string"
            assert parsed["uri"].startswith("https://"), (
                f"uri must start with 'https://': {parsed['uri']!r}"
            )
            val = parsed.get("value", {})
            assert "rkey" in val, f"value missing 'rkey': {list(val)}"
            assert val.get("$type") == "org.atgeo.place", (
                f"value.$type must be 'org.atgeo.place'; got {val.get('$type')!r}"
            )
            assert "name" in val, f"value missing 'name': {list(val)}"
            assert "importance" in val, f"value missing 'importance': {list(val)}"
            assert isinstance(val["importance"], int), (
                f"importance must be int; got {type(val['importance'])}"
            )
            assert "locations" in val, f"value missing 'locations': {list(val)}"
            assert isinstance(val["locations"], list), "locations must be a list"
            assert "variants" in val, f"value missing 'variants': {list(val)}"
            assert isinstance(val["variants"], list), "variants must be a list"
            assert "attributes" in val, f"value missing 'attributes': {list(val)}"
            assert isinstance(val["attributes"], dict), "attributes must be a dict"
            assert "relations" in val, f"value missing 'relations': {list(val)}"
            assert isinstance(val["relations"], dict), "relations must be a dict"

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
            locations = parsed["value"]["locations"]
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
            if parsed["value"].get("name") in ("Blue Bottle Coffee", "Golden Gate Park", "Tartine Bakery"):
                locations = parsed["value"]["locations"]
                assert len(locations) >= 2, (
                    f"Place with country must have address location; "
                    f"got {len(locations)} location(s) for {parsed['value']['name']}"
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
            if parsed["value"].get("name") == "Mystery Spot":
                locations = parsed["value"]["locations"]
                assert len(locations) == 1, (
                    f"Place with null country must have exactly 1 location; "
                    f"got {len(locations)}"
                )
                found = True
        assert found, "Mystery Spot (null country place) not found in export output"

    def test_tile_export_is_view_not_table(self, tmp_path):
        """tile_export must be a VIEW, not a BASE TABLE.

        Fails because fsq_export_tiles.sql currently creates a TABLE.
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
        export_tiles(conn, str(output_dir), "fsq")
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
        result = export_tiles(conn, str(output_dir), "fsq")
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
        export_tiles(conn, str(output_dir), "fsq")
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
        tile_qk_a = "023130" + "0" * 11  # 17-char quadkey
        tile_qk_b = "023130" + "1" * 11
        record_a = json.dumps({"uri": "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001",
                                "value": {"$type": "org.atgeo.place", "rkey": "fsq001", "name": "Test A"}})
        record_b = json.dumps({"uri": "https://places.atgeo.org/org.atgeo.places.foursquare/fsq002",
                                "value": {"$type": "org.atgeo.place", "rkey": "fsq002", "name": "Test B"}})
        all_rows = [(tile_qk_a, record_a), (tile_qk_b, record_b)]

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
        fake_sql = "SELECT tile_qk, record_json FROM tile_export"
        with patch("pathlib.Path.read_text", return_value=fake_sql):
            export_tiles(mock_con, str(output_dir), "fsq")

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
            assert "uri" in item, f"Record item missing 'uri': {list(item)}"
            assert "value" in item, f"Record item missing 'value': {list(item)}"

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
        def _make_row(i):
            qk = f"02313{i:012d}"  # unique quadkey per row
            payload = json.dumps({"uri": f"https://example.com/{i}",
                                   "value": {"$type": "org.atgeo.place", "rkey": str(i), "name": f"Place {i}"}})
            return (qk, payload)

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

        fake_sql = "SELECT tile_qk, record_json FROM tile_export"
        captured_messages = []

        class _CapturingHandler(logging.Handler):
            def emit(self, record):
                captured_messages.append(record.getMessage())

        handler = _CapturingHandler()
        import garganorn.quadtree as _qt_module
        logger = logging.getLogger(_qt_module.__name__)
        logger.addHandler(handler)
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        try:
            with patch("pathlib.Path.read_text", return_value=fake_sql):
                export_tiles(mock_con, str(output_dir), "fsq")
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)

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
        payload = json.dumps({"uri": "https://example.com/fsq001",
                               "value": {"$type": "org.atgeo.place", "rkey": "fsq001", "name": "Test"}})
        all_rows = [(tile_qk, payload)]

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [all_rows, []]
        mock_cursor.fetchall.return_value = all_rows

        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_postloop_log"
        output_dir.mkdir()

        fake_sql = "SELECT tile_qk, record_json FROM tile_export"
        captured_messages = []

        class _CapturingHandler(logging.Handler):
            def emit(self, record):
                captured_messages.append(record.getMessage())

        import garganorn.quadtree as _qt_module
        logger = logging.getLogger(_qt_module.__name__)
        handler = _CapturingHandler()
        logger.addHandler(handler)
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        try:
            with patch("pathlib.Path.read_text", return_value=fake_sql):
                export_tiles(mock_con, str(output_dir), "fsq")
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)

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
        """export_tiles groups per-record rows by tile_qk into separate .json.gz files."""
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        qk_a = "023130" + "0" * 11
        qk_b = "023131" + "0" * 11
        rec1 = json.dumps({"uri": "https://x/1", "value": {"$type": "org.atgeo.place", "rkey": "1", "name": "A1"}})
        rec2 = json.dumps({"uri": "https://x/2", "value": {"$type": "org.atgeo.place", "rkey": "2", "name": "A2"}})
        rec3 = json.dumps({"uri": "https://x/3", "value": {"$type": "org.atgeo.place", "rkey": "3", "name": "B1"}})
        all_rows = [(qk_a, rec1), (qk_a, rec2), (qk_b, rec3)]

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [all_rows, []]
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_group"
        output_dir.mkdir()

        with patch("pathlib.Path.read_text", return_value="SELECT tile_qk, record_json FROM tile_export"):
            result = export_tiles(mock_con, str(output_dir), "fsq")

        assert len(result) == 2, f"Expected 2 tiles, got {len(result)}"
        assert result[qk_a] == 2, f"qk_a tile should have 2 records, got {result[qk_a]}"
        assert result[qk_b] == 1, f"qk_b tile should have 1 record, got {result[qk_b]}"

        gz_files = list(output_dir.rglob("*.json.gz"))
        assert len(gz_files) == 2, f"Expected 2 .json.gz files, got {len(gz_files)}"

        for gz in gz_files:
            with gzip.open(gz, "rt") as f:
                data = json.load(f)
            assert "attribution" in data
            assert "records" in data
            assert isinstance(data["records"], list)
            for rec in data["records"]:
                assert "uri" in rec
                assert "value" in rec

    def test_attribution_in_envelope(self, tmp_path):
        """export_tiles writes attribution from ATTRIBUTION[source] into the envelope."""
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles, ATTRIBUTION

        qk = "023130" + "0" * 11
        rec = json.dumps({"uri": "https://x/1", "value": {"$type": "org.atgeo.place", "rkey": "1", "name": "Test"}})

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [[(qk, rec)], []]
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_attr"
        output_dir.mkdir()

        with patch("pathlib.Path.read_text", return_value="SELECT tile_qk, record_json FROM tile_export"):
            export_tiles(mock_con, str(output_dir), "fsq")

        gz_files = list(output_dir.rglob("*.json.gz"))
        assert gz_files, "No .json.gz written"
        with gzip.open(gz_files[0], "rt") as f:
            data = json.load(f)

        assert "attribution" in data, f"Envelope missing 'attribution'; keys: {list(data)}"
        assert data["attribution"] == ATTRIBUTION["fsq"], (
            f"attribution must be ATTRIBUTION['fsq'] = {ATTRIBUTION['fsq']!r}; "
            f"got {data['attribution']!r}"
        )

    def test_single_record_tile(self, tmp_path):
        """A tile with exactly one record is correctly written."""
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        qk = "023130" + "0" * 11
        rec = json.dumps({"uri": "https://x/1", "value": {"$type": "org.atgeo.place", "rkey": "1", "name": "Solo"}})

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [[(qk, rec)], []]
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_single"
        output_dir.mkdir()

        with patch("pathlib.Path.read_text", return_value="SELECT tile_qk, record_json FROM tile_export"):
            result = export_tiles(mock_con, str(output_dir), "fsq")

        assert result == {qk: 1}, f"Expected {{qk: 1}}, got {result}"

        gz_files = list(output_dir.rglob("*.json.gz"))
        assert len(gz_files) == 1
        with gzip.open(gz_files[0], "rt") as f:
            data = json.load(f)
        assert len(data["records"]) == 1
        assert data["records"][0]["uri"] == "https://x/1"

    def test_tile_boundary_across_fetchmany_batches(self, tmp_path):
        """Tile spanning two fetchmany batches is written correctly."""
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        qk_a = "023130" + "0" * 11
        qk_b = "023131" + "0" * 11
        rec1 = json.dumps({"uri": "https://x/1", "value": {"$type": "org.atgeo.place", "rkey": "1", "name": "A1"}})
        rec2 = json.dumps({"uri": "https://x/2", "value": {"$type": "org.atgeo.place", "rkey": "2", "name": "A2"}})
        rec3 = json.dumps({"uri": "https://x/3", "value": {"$type": "org.atgeo.place", "rkey": "3", "name": "B1"}})

        # batch 1: first record of qk_a only
        # batch 2: second record of qk_a + first record of qk_b (forces boundary split)
        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [
            [(qk_a, rec1)],
            [(qk_a, rec2), (qk_b, rec3)],
            [],
        ]
        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_boundary"
        output_dir.mkdir()

        with patch("pathlib.Path.read_text", return_value="SELECT tile_qk, record_json FROM tile_export"):
            result = export_tiles(mock_con, str(output_dir), "fsq")

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


# ---------------------------------------------------------------------------
# Tests: overture_export_tiles.sql
# ---------------------------------------------------------------------------

class TestOvertureExportTiles:
    """Tests for garganorn/sql/overture_export_tiles.sql.

    Each test runs the full Overture pipeline:
      overture_import → overture_importance → overture_variants →
      compute_tile_assignments → overture_export_tiles
    """

    _SUBS = {"repo": "places.atgeo.org"}

    def _run_full_pipeline(self, conn, parquet_glob):
        """Run all Overture pipeline SQL stages on conn."""
        # 1. Import
        run_overture_import(conn, parquet_glob)

        # 2. Importance
        raw = _load_sql("overture_importance.sql", {"density_norm": "10.0", "idf_norm": "18.0"})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))

        # 3. Variants
        raw = _load_sql("overture_variants.sql", {})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))

        # 4. Tile assignments (pk_expr='id' for Overture)
        run_tile_assignments(conn, pk_expr="id", min_zoom=6, max_zoom=17, max_per_tile=5000)

        # 5. Export tiles
        raw = _load_sql("overture_export_tiles.sql", self._SUBS)
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))

    def _get_record(self, conn, place_id):
        """Return parsed JSON record dict for a given place_id, or None if not found.

        Fetches all rows from tile_export, parses each record_json, and returns
        the first record whose value.rkey matches place_id.
        """
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("value", {}).get("rkey") == place_id:
                return parsed
        return None

    def test_overture_export_addresses_inline(self, overture_parquet, tmp_path):
        """ov001 (one address entry with country='US', region='US-CA') must have an
        address location entry with country='US' and region='CA' (trimmed at '-').

        locations must contain: [geo_entry, address_entry].
        """
        db_path = tmp_path / "test_ov_export_addr.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet)
        record = self._get_record(conn, "ov001")
        conn.close()
        assert record is not None, "ov001 must appear in tile_export"
        locations = record["value"]["locations"]
        addr_entries = [loc for loc in locations if loc.get("$type") == "community.lexicon.location.address"]
        assert len(addr_entries) == 1, (
            f"ov001 must have exactly 1 address location; got {len(addr_entries)}: {addr_entries}"
        )
        addr = addr_entries[0]
        assert addr["country"] == "US", f"Expected country='US'; got {addr['country']!r}"
        assert addr["region"] == "CA", (
            f"Expected region='CA' (trimmed from 'US-CA'); got {addr['region']!r}"
        )

    def test_overture_export_no_addresses_no_error(self, overture_parquet, tmp_path):
        """ov003 (addresses=NULL) must render without error with exactly 1 location (geo only)."""
        db_path = tmp_path / "test_ov_export_no_addr.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet)
        record = self._get_record(conn, "ov003")
        conn.close()
        assert record is not None, "ov003 must appear in tile_export"
        locations = record["value"]["locations"]
        assert len(locations) == 1, (
            f"ov003 (null addresses) must have exactly 1 location (geo only); got {len(locations)}: {locations}"
        )
        assert locations[0]["$type"] == "community.lexicon.location.geo", (
            f"Only location must be geo type; got {locations[0]['$type']!r}"
        )

    def test_overture_export_all_null_country_addresses(self, overture_parquet, tmp_path):
        """ov008 (addresses=[{country:NULL,...}]) must render with exactly 1 location (geo only).

        list_filter must remove all entries with NULL country, yielding an empty address list.
        """
        db_path = tmp_path / "test_ov_export_null_country.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet)
        record = self._get_record(conn, "ov008")
        conn.close()
        assert record is not None, "ov008 must appear in tile_export"
        locations = record["value"]["locations"]
        assert len(locations) == 1, (
            f"ov008 (all null-country addresses) must have exactly 1 location (geo only); "
            f"got {len(locations)}: {locations}"
        )
        assert locations[0]["$type"] == "community.lexicon.location.geo", (
            f"Only location must be geo type; got {locations[0]['$type']!r}"
        )

    def test_overture_export_mixed_null_country_addresses(self, overture_parquet, tmp_path):
        """ov009 (one null-country entry + one non-null-country entry) must render with
        exactly 1 address location — the non-null-country entry only.

        This validates list_filter drops null-country entries without dropping valid ones.
        """
        db_path = tmp_path / "test_ov_export_mixed.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet)
        record = self._get_record(conn, "ov009")
        conn.close()
        assert record is not None, "ov009 must appear in tile_export"
        locations = record["value"]["locations"]
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
