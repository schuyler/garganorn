"""Tests for fsq_export_tiles.sql, overture_export_tiles.sql, and export_tiles()."""

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

    def test_flush_tile_no_json_loads(self, tmp_path):
        """flush_tile must not call json.loads — records are already valid JSON strings."""
        import gzip as _gzip
        from unittest.mock import MagicMock, patch
        from garganorn.quadtree import export_tiles

        qk_a = "023130" + "0" * 11
        qk_b = "023131" + "0" * 11
        record_a = json.dumps({"uri": "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001",
                                "value": {"$type": "org.atgeo.place", "rkey": "fsq001", "name": "Test A"}})
        record_b = json.dumps({"uri": "https://places.atgeo.org/org.atgeo.places.foursquare/fsq002",
                                "value": {"$type": "org.atgeo.place", "rkey": "fsq002", "name": "Test B"}})
        all_rows = [(qk_a, record_a), (qk_b, record_b)]

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [all_rows, []]
        mock_cursor.fetchall.side_effect = AssertionError(
            "export_tiles must not call fetchall()"
        )

        mock_con = MagicMock()
        mock_con.execute.return_value = mock_cursor

        output_dir = tmp_path / "output_no_json_loads"
        output_dir.mkdir()

        fake_sql = "SELECT tile_qk, record_json FROM tile_export"
        with patch("pathlib.Path.read_text", return_value=fake_sql):
            with patch("garganorn.quadtree.json.loads",
                       side_effect=AssertionError(
                           "flush_tile must not call json.loads; "
                           "records are already valid JSON strings"
                       )) as mock_loads:
                export_tiles(mock_con, str(output_dir), "fsq")

        mock_loads.assert_not_called()

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
                assert "uri" in item, f"Record item missing 'uri': {list(item)}"
                assert "value" in item, f"Record item missing 'value': {list(item)}"


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

        # 4b. Empty place_containment (no boundaries in pipeline tests)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS place_containment (
                place_id       VARCHAR,
                relations_json VARCHAR
            )
        """)

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

    def test_overture_export_uses_bbox_mean_not_centroid(self):
        """overture_export_tiles.sql must compute lat/lon from bbox mean, not st_centroid."""
        import pathlib
        sql_path = pathlib.Path(__file__).parent.parent / "garganorn" / "sql" / "overture_export_tiles.sql"
        sql = sql_path.read_text()
        assert "st_centroid" not in sql.lower(), (
            "overture_export_tiles.sql must not use st_centroid; "
            "use bbox mean ((bbox.ymin + bbox.ymax) / 2) instead"
        )
        assert "p.bbox.ymin" in sql, (
            "overture_export_tiles.sql must use p.bbox.ymin for latitude computation"
        )
        assert "p.bbox.xmin" in sql, (
            "overture_export_tiles.sql must use p.bbox.xmin for longitude computation"
        )
        assert "p.bbox.ymax" in sql, (
            "overture_export_tiles.sql must use p.bbox.ymax for latitude computation"
        )
        assert "p.bbox.xmax" in sql, (
            "overture_export_tiles.sql must use p.bbox.xmax for longitude computation"
        )

    def test_overture_export_latlon_matches_bbox_mean(self, overture_parquet, tmp_path):
        """Exported latitude/longitude must equal bbox center coordinates.

        Regression guard: passes against both old (st_centroid) and new (bbox mean)
        code because ov001's geometry point coincides with its bbox center.
        """
        db_path = tmp_path / "test_ov_export_latlon.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet)
        record = self._get_record(conn, "ov001")
        conn.close()

        assert record is not None, "ov001 must appear in tile_export"
        locations = record["value"]["locations"]
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
# WoF containment relations JSON for test fixtures
# ---------------------------------------------------------------------------

# The four WoF boundaries that contain SF places (lat ~37.77, lon ~-122.42),
# ordered by level ascending (continent first — matches ORDER BY level ASC).
_SF_WITHIN_JSON = json.dumps({
    "within": [
        {"rkey": "org.atgeo.places.wof:102191575", "name": "North America", "level": 0},
        {"rkey": "org.atgeo.places.wof:85633793", "name": "United States", "level": 10},
        {"rkey": "org.atgeo.places.wof:85688637", "name": "California", "level": 25},
        {"rkey": "org.atgeo.places.wof:85922583", "name": "San Francisco", "level": 50},
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
# Tests: WoF containment in tile export pipelines (Red phase)
# ---------------------------------------------------------------------------

class TestContainmentInExport:
    """Tests specifying WoF containment in tile export output.

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

        Fails in Red phase because fsq_export_tiles.sql has `relations: MAP {}`
        and does not LEFT JOIN place_containment.
        """
        db_path = tmp_path / "fsq_containment_with.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        _create_place_containment(conn, [("exp001", _SF_WITHIN_JSON)])

        raw_sql = _load_sql("fsq_export_tiles.sql", self._FSQ_SUBS)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        record = None
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("value", {}).get("rkey") == "exp001":
                record = parsed
                break

        assert record is not None, "exp001 must appear in tile_export"
        relations = record["value"].get("relations", {})
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
            assert "name" in entry, f"within entry missing 'name': {entry}"
            assert "level" in entry, f"within entry missing 'level': {entry}"
        levels = [entry["level"] for entry in within]
        assert levels == sorted(levels), (
            f"within entries must be ordered by level ascending; got levels={levels}"
        )

    # ------------------------------------------------------------------
    # Test 2: FSQ export produces empty relations when containment table is empty
    # ------------------------------------------------------------------

    def test_fsq_relations_empty_containment(self, tmp_path):
        """FSQ export must produce relations={{}} when place_containment table is empty.

        Fails in Red phase because fsq_export_tiles.sql does not LEFT JOIN
        place_containment at all — it uses the hardcoded `relations: MAP {}`.
        After the implementation, the SQL must reference place_containment via
        a LEFT JOIN so this test (and test 1) both work against the same SQL.

        Verified by asserting that fsq_export_tiles.sql contains a reference to
        'place_containment': if the LEFT JOIN is missing, the SQL never touches
        the table and this test fails.
        """
        import pathlib
        sql_path = pathlib.Path(REPO_ROOT) / "garganorn" / "sql" / "fsq_export_tiles.sql"
        sql_text = sql_path.read_text()
        assert "place_containment" in sql_text, (
            "fsq_export_tiles.sql must reference 'place_containment' via a LEFT JOIN; "
            "the current SQL has no such reference. "
            "Add: LEFT JOIN place_containment pc ON pc.place_id = p.fsq_place_id"
        )

        db_path = tmp_path / "fsq_containment_empty.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)
        _create_place_containment(conn, [])  # empty table, same schema

        raw_sql = _load_sql("fsq_export_tiles.sql", self._FSQ_SUBS)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        assert rows, "tile_export must produce rows even with empty place_containment"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            relations = parsed.get("value", {}).get("relations", {})
            assert relations == {}, (
                f"relations must be {{}} when place_containment is empty; got {relations!r}"
            )

    # ------------------------------------------------------------------
    # Test 3: Overture export includes relations.within when containment present
    # ------------------------------------------------------------------

    def test_overture_relations_with_containment(self, overture_parquet, tmp_path):
        """Overture export must include relations.within when place_containment populated.

        Fails in Red phase because overture_export_tiles.sql has `relations: '{{}}'::JSON`
        and does not LEFT JOIN place_containment.
        """
        db_path = tmp_path / "ov_containment_with.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run the full Overture pipeline to get places + tile_assignments
        run_overture_import(conn, overture_parquet)
        raw = _load_sql("overture_importance.sql", {"density_norm": "10.0", "idf_norm": "18.0"})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))
        raw = _load_sql("overture_variants.sql", {})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))
        run_tile_assignments(conn, pk_expr="id", min_zoom=6, max_zoom=17, max_per_tile=5000)

        # Populate place_containment for ov001
        _create_place_containment(conn, [("ov001", _SF_WITHIN_JSON)])

        # Run export
        raw_sql = _load_sql("overture_export_tiles.sql", self._OV_SUBS)
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        record = None
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("value", {}).get("rkey") == "ov001":
                record = parsed
                break

        assert record is not None, "ov001 must appear in tile_export"
        relations = record["value"].get("relations", {})
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
            assert "name" in entry, f"within entry missing 'name': {entry}"
            assert "level" in entry, f"within entry missing 'level': {entry}"
        levels = [entry["level"] for entry in within]
        assert levels == sorted(levels), (
            f"within entries must be ordered by level ascending; got levels={levels}"
        )

    # ------------------------------------------------------------------
    # Test 4: OSM export includes relations.within when containment present
    # ------------------------------------------------------------------

    def test_osm_relations_with_containment(self, osm_parquet, tmp_path):
        """OSM export must include relations.within when place_containment populated.

        Fails in Red phase because osm_export_tiles.sql has `relations: '{{}}'::JSON`
        and does not LEFT JOIN place_containment.
        """
        db_path = tmp_path / "osm_containment_with.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run the full OSM pipeline
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        raw = _load_sql("osm_importance.sql", {"density_norm": "10.0", "idf_norm": "18.0"})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))
        raw = _load_sql("osm_variants.sql", {})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))
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
            # Check original rkey match by looking at the URI tail
            uri = parsed.get("uri", "")
            if uri.endswith(
                target_rkey.replace("n", "node:", 1)
                if target_rkey.startswith("n")
                else target_rkey.replace("w", "way:", 1)
                if target_rkey.startswith("w")
                else target_rkey
            ):
                record = parsed
                break

        assert record is not None, (
            f"place with rkey={target_rkey!r} must appear in tile_export"
        )
        relations = record["value"].get("relations", {})
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
            assert "name" in entry, f"within entry missing 'name': {entry}"
            assert "level" in entry, f"within entry missing 'level': {entry}"
        levels = [entry["level"] for entry in within]
        assert levels == sorted(levels), (
            f"within entries must be ordered by level ascending; got levels={levels}"
        )

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

    def test_compute_containment_produces_table(self, tmp_path, wof_db_path):
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

        compute_containment(conn, str(wof_db_path), "fsq_place_id", "longitude", "latitude")

        # Verify the table was created
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'place_containment'"
        ).fetchall()
        assert tables, "compute_containment must create a place_containment table"

        # Verify schema
        cols = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'place_containment'"
            ).fetchall()
        }
        assert "place_id" in cols, (
            f"place_containment must have 'place_id' column; got columns: {list(cols)}"
        )
        assert "relations_json" in cols, (
            f"place_containment must have 'relations_json' column; got columns: {list(cols)}"
        )

        # Verify rows exist for SF places (they fall inside NA/US/CA/SF boundaries)
        rows = conn.execute("SELECT place_id, relations_json FROM place_containment").fetchall()
        conn.close()

        assert len(rows) >= 1, (
            "compute_containment must produce at least one row for SF test places "
            f"that fall inside the WoF boundaries; got {len(rows)} rows"
        )
        for place_id, relations_json in rows:
            parsed = json.loads(relations_json)
            assert "within" in parsed, (
                f"relations_json for {place_id!r} must have 'within' key; got {parsed!r}"
            )
            within = parsed["within"]
            assert isinstance(within, list), f"within must be a list for {place_id}; got {type(within)}"
            assert len(within) >= 1, f"SF coordinates should be contained by at least 1 WoF boundary"
            for entry in within:
                assert "rkey" in entry, f"within entry missing 'rkey': {entry}"
                assert entry["rkey"].startswith("org.atgeo.places.wof:"), \
                    f"rkey must be collection-qualified; got {entry['rkey']!r}"
                assert "name" in entry, f"within entry missing 'name': {entry}"
                assert "level" in entry, f"within entry missing 'level': {entry}"
            levels = [e["level"] for e in within]
            assert levels == sorted(levels), f"within must be ordered by level ASC; got {levels}"

    # ------------------------------------------------------------------
    # Test 6b: compute_containment bbox pre-filter regression guard
    # ------------------------------------------------------------------

    def test_compute_containment_matches_all_containing_boundaries(self, tmp_path, wof_db_path):
        """compute_containment must match ALL boundaries that contain a place, not just some.

        This test guards against a future bbox pre-filter incorrectly excluding a
        boundary whose bbox columns do contain the place's coordinates. A buggy
        pre-filter that uses wrong bbox values (e.g., inverted min/max, or a bbox
        that is too small) would produce fewer than the expected 4 containment
        entries for the SF test point, causing this test to fail.

        The SF test point (37.7749, -122.4194) falls inside all four WoF boundaries
        defined in conftest.py::WOF_BOUNDARIES that cover North America:
          - North America  (level  0): bbox [20,-130] to [55,-60]
          - United States  (level 10): bbox [24,-125] to [50,-66]
          - California     (level 25): bbox [34,-125] to [42,-118]
          - San Francisco  (level 50): bbox [37.6,-122.55] to [37.85,-122.3]

        Manhattan (level 55) does NOT contain the SF point, so exactly 4 entries
        are expected and no more.

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

        compute_containment(conn, str(wof_db_path), "fsq_place_id", "longitude", "latitude")

        rows = conn.execute(
            "SELECT place_id, relations_json FROM place_containment WHERE place_id = 'sf001'"
        ).fetchall()
        conn.close()

        assert len(rows) == 1, (
            f"Expected exactly 1 place_containment row for 'sf001', got {len(rows)}"
        )

        parsed = json.loads(rows[0][1])
        within = parsed["within"]

        # The SF point falls inside exactly 4 of the 5 WoF test boundaries.
        # If a bbox pre-filter incorrectly excludes any of these 4 boundaries,
        # this assertion will catch it.
        expected_names = {"North America", "United States", "California", "San Francisco"}
        actual_names = {entry["name"] for entry in within}
        assert actual_names == expected_names, (
            f"compute_containment produced wrong set of containing boundaries.\n"
            f"  Expected: {sorted(expected_names)}\n"
            f"  Got:      {sorted(actual_names)}\n"
            f"A missing boundary indicates the bbox pre-filter incorrectly excluded it "
            f"(false negative). An extra boundary indicates incorrect inclusion."
        )

        # Verify levels are ordered ascending
        levels = [e["level"] for e in within]
        assert levels == sorted(levels), (
            f"within entries must be ordered by level ASC; got {levels}"
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
            "--source", "fsq",
            "--parquet", "/tmp/test.parquet",
            "--output", "/tmp/output",
            "--boundaries", "/tmp/wof.duckdb",
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

        compute_containment(conn, None, "fsq_place_id", "longitude", "latitude")

        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'place_containment'"
        ).fetchall()
        assert tables, "place_containment table must exist even with None boundaries_db"

        rows = conn.execute("SELECT * FROM place_containment").fetchall()
        assert len(rows) == 0, (
            f"place_containment must be empty when boundaries_db is None; got {len(rows)} rows"
        )
        conn.close()

    # ------------------------------------------------------------------
    # Test 10: place outside all boundaries produces no containment row
    # ------------------------------------------------------------------

    def test_compute_containment_place_outside_all_boundaries(self, tmp_path, wof_db_path):
        """A place at (0, 0) in the ocean should produce no place_containment row.

        All test WoF boundaries cover parts of North America. A point in the
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

        compute_containment(conn, str(wof_db_path), "fsq_place_id", "longitude", "latitude")

        rows = conn.execute(
            "SELECT place_id FROM place_containment WHERE place_id = 'ocean001'"
        ).fetchall()
        assert len(rows) == 0, (
            f"Place at (0, 0) should not be contained by any boundary; "
            f"got {len(rows)} containment rows"
        )
        conn.close()

    # ------------------------------------------------------------------
    # Test 11: two-phase split — tile-containing vs tile-straddling boundaries
    # ------------------------------------------------------------------

    def test_compute_containment_two_phase_split(self, tmp_path, wof_db_path):
        """Verify correct containment when some boundaries fully contain the z6
        tile and others only partially overlap it.

        The SF test point (37.7749, -122.4194) maps to z6 tile 023010, whose
        bbox is approximately (-123.75, 36.6) to (-118.125, 41.0).

        Phase 1 boundaries (fully contain the z6 tile):
          - North America: (-130, 20) to (-60, 55)  — fully contains tile
          - United States:  (-125, 24) to (-66, 50)  — fully contains tile
          - California:     (-125, 34) to (-118, 42)  — fully contains tile

        Phase 2 boundary (overlaps but does NOT fully contain the z6 tile):
          - San Francisco:  (-122.55, 37.6) to (-122.3, 37.85)  — small polygon
            inside the tile, must be evaluated per-point via ST_Contains

        Non-overlapping boundary:
          - Manhattan:      (-74.05, 40.68) to (-73.90, 40.88)  — different tile

        The SF point should match exactly 4 boundaries (NA, US, CA, SF).
        A Manhattan point should match exactly 3 (NA, US only — not CA or SF).
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "containment_two_phase.duckdb"
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
        # SF point — should match NA, US, CA, SF (4 boundaries)
        conn.execute(
            "INSERT INTO places VALUES ('sf_center', 37.7749, -122.4194, "
            "ST_QuadKey(-122.4194, 37.7749, 17))"
        )
        # Manhattan point — should match NA, US, Manhattan (3 boundaries)
        conn.execute(
            "INSERT INTO places VALUES ('nyc_center', 40.7831, -73.9712, "
            "ST_QuadKey(-73.9712, 40.7831, 17))"
        )

        compute_containment(conn, str(wof_db_path), "fsq_place_id", "longitude", "latitude")

        # Check SF point: 4 boundaries
        sf_rows = conn.execute(
            "SELECT relations_json FROM place_containment WHERE place_id = 'sf_center'"
        ).fetchall()
        assert len(sf_rows) == 1, f"Expected 1 row for sf_center; got {len(sf_rows)}"
        sf_within = json.loads(sf_rows[0][0])["within"]
        sf_names = {e["name"] for e in sf_within}
        assert sf_names == {"North America", "United States", "California", "San Francisco"}, (
            f"SF point should be in 4 boundaries; got {sorted(sf_names)}"
        )

        # Check Manhattan point: 2 boundaries (NA, US) + Manhattan
        nyc_rows = conn.execute(
            "SELECT relations_json FROM place_containment WHERE place_id = 'nyc_center'"
        ).fetchall()
        assert len(nyc_rows) == 1, f"Expected 1 row for nyc_center; got {len(nyc_rows)}"
        nyc_within = json.loads(nyc_rows[0][0])["within"]
        nyc_names = {e["name"] for e in nyc_within}
        assert nyc_names == {"North America", "United States", "Manhattan"}, (
            f"Manhattan point should be in NA, US, Manhattan; got {sorted(nyc_names)}"
        )

        # Verify level ordering for both
        for label, within in [("sf", sf_within), ("nyc", nyc_within)]:
            levels = [e["level"] for e in within]
            assert levels == sorted(levels), (
                f"{label} within entries must be ordered by level ASC; got {levels}"
            )
        conn.close()

    # ------------------------------------------------------------------
    # Test 12: place near tile edge with boundary straddling the edge
    # ------------------------------------------------------------------

    def test_compute_containment_place_near_tile_edge(self, tmp_path, wof_db_path):
        """A place near the edge of the SF city boundary must still be correctly
        contained when it falls inside the boundary polygon.

        The SF boundary is POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85,
        -122.3 37.6, -122.55 37.6)). We test:
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

        compute_containment(conn, str(wof_db_path), "fsq_place_id", "longitude", "latitude")

        # edge_in: should be in NA, US, CA, SF (4 boundaries)
        in_rows = conn.execute(
            "SELECT relations_json FROM place_containment WHERE place_id = 'edge_in'"
        ).fetchall()
        assert len(in_rows) == 1, f"Expected 1 row for edge_in; got {len(in_rows)}"
        in_within = json.loads(in_rows[0][0])["within"]
        in_names = {e["name"] for e in in_within}
        assert in_names == {"North America", "United States", "California", "San Francisco"}, (
            f"edge_in should be in 4 boundaries; got {sorted(in_names)}"
        )

        # edge_out: should be in NA, US, CA only (3 boundaries, not SF)
        out_rows = conn.execute(
            "SELECT relations_json FROM place_containment WHERE place_id = 'edge_out'"
        ).fetchall()
        assert len(out_rows) == 1, f"Expected 1 row for edge_out; got {len(out_rows)}"
        out_within = json.loads(out_rows[0][0])["within"]
        out_names = {e["name"] for e in out_within}
        assert out_names == {"North America", "United States", "California"}, (
            f"edge_out should be in 3 boundaries (not SF); got {sorted(out_names)}"
        )

    # ------------------------------------------------------------------
    # Test 13: ST_Intersects pre-filter creates tile_boundaries table
    # ------------------------------------------------------------------

    def test_prefilter_creates_tile_boundaries(self, tmp_path):
        """compute_containment must create a tile_boundaries temp table via
        ST_Intersects pre-filter (Step 0) that excludes non-intersecting
        boundaries.

        Creates a WoF DB with one boundary that intersects the tile and one
        that is far away. Uses a DuckDB connection wrapper to intercept SQL
        and verify that tile_boundaries is created and queried.

        FAILS on current code because there is no tile_boundaries temp table
        and no ST_Intersects pre-filter. Phase 1 and Phase 2 query
        wof.boundaries directly.
        """
        from garganorn.quadtree import compute_containment

        # Create a WoF DB with two boundaries:
        # 1. "Local Box" — small box around the test point
        # 2. "Distant Box" — box in the southern hemisphere, nowhere near the tile
        wof_path = tmp_path / "wof_prefilter.duckdb"
        wof_conn = duckdb.connect(str(wof_path))
        wof_conn.execute("INSTALL spatial; LOAD spatial;")
        wof_conn.execute("""
            CREATE TABLE boundaries (
                wof_id BIGINT,
                rkey VARCHAR,
                name VARCHAR,
                placetype VARCHAR,
                level INTEGER,
                latitude DOUBLE,
                longitude DOUBLE,
                geom GEOMETRY,
                country VARCHAR,
                min_latitude DOUBLE,
                min_longitude DOUBLE,
                max_latitude DOUBLE,
                max_longitude DOUBLE,
                names_json VARCHAR,
                concordances VARCHAR
            )
        """)
        # Local boundary containing the test point (SF area)
        wof_conn.execute("""
            INSERT INTO boundaries VALUES (
                1, '1', 'Local Box', 'region', 25, 37.77, -122.42,
                ST_GeomFromText('POLYGON((-123 37, -123 38, -122 38, -122 37, -123 37))'),
                'US', 37.0, -123.0, 38.0, -122.0, NULL, NULL
            )
        """)
        # Distant boundary — southern hemisphere, does not intersect the SF z6 tile
        wof_conn.execute("""
            INSERT INTO boundaries VALUES (
                2, '2', 'Distant Box', 'region', 25, -40.0, 170.0,
                ST_GeomFromText('POLYGON((169 -41, 169 -39, 171 -39, 171 -41, 169 -41))'),
                'NZ', -41.0, 169.0, -39.0, 171.0, NULL, NULL
            )
        """)
        wof_conn.execute("CREATE INDEX boundaries_rtree ON boundaries USING RTREE (geom)")
        wof_conn.execute("CREATE INDEX idx_rkey ON boundaries(rkey)")
        wof_conn.close()

        # Create places DB with one point in SF
        db_path = tmp_path / "prefilter_test.duckdb"
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
            "INSERT INTO places VALUES ('p1', 37.77, -122.42, "
            "ST_QuadKey(-122.42, 37.77, 17))"
        )

        # Use a wrapper to intercept SQL and detect tile_boundaries creation
        sql_log = []

        class _ConnWrapper:
            """Thin wrapper around DuckDBPyConnection that logs SQL strings."""
            def __init__(self, real):
                self._real = real

            def execute(self, sql, *args, **kwargs):
                if isinstance(sql, str):
                    sql_log.append(sql)
                return self._real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._real, name)

        wrapped = _ConnWrapper(conn)
        compute_containment(wrapped, str(wof_path), "fsq_place_id", "longitude", "latitude")

        # Verify tile_boundaries was created
        tb_creates = [s for s in sql_log if re.search(r"CREATE\b.*\bTABLE\b.*\btile_boundaries\b", s, re.IGNORECASE)]
        assert len(tb_creates) > 0, (
            "compute_containment must create a tile_boundaries temp table in Step 0 "
            "using ST_Intersects pre-filter. No such CREATE statement was found in "
            "the executed SQL."
        )

        # Verify the containment result is still correct
        rows = conn.execute("SELECT relations_json FROM place_containment").fetchall()
        assert len(rows) == 1, f"Expected 1 containment row; got {len(rows)}"
        within = json.loads(rows[0][0])["within"]
        names = {e["name"] for e in within}
        assert names == {"Local Box"}, (
            f"Only 'Local Box' should match; got {sorted(names)}"
        )
        conn.close()

    # ------------------------------------------------------------------
    # Test 14: geometry clipping reduces vertex count
    # ------------------------------------------------------------------

    def test_geometry_clipping_reduces_vertices(self, tmp_path):
        """After Step 0, tile_boundaries must contain clipped geometries with
        fewer vertices than the original boundary.

        Creates a WoF DB with one boundary that spans far beyond the tile.
        Uses a connection wrapper to intercept the CREATE of tile_boundaries
        and immediately query ST_NPoints on the clipped geometry before
        the table is dropped.

        FAILS on current code because tile_boundaries temp table does not
        exist -- the code joins directly against wof.boundaries without
        creating any intermediate table.
        """
        from garganorn.quadtree import compute_containment

        # Create a WoF DB with one large boundary (many vertices, spans wide)
        wof_path = tmp_path / "wof_clip.duckdb"
        wof_conn = duckdb.connect(str(wof_path))
        wof_conn.execute("INSTALL spatial; LOAD spatial;")
        wof_conn.execute("""
            CREATE TABLE boundaries (
                wof_id BIGINT,
                rkey VARCHAR,
                name VARCHAR,
                placetype VARCHAR,
                level INTEGER,
                latitude DOUBLE,
                longitude DOUBLE,
                geom GEOMETRY,
                country VARCHAR,
                min_latitude DOUBLE,
                min_longitude DOUBLE,
                max_latitude DOUBLE,
                max_longitude DOUBLE,
                names_json VARCHAR,
                concordances VARCHAR
            )
        """)
        # A large polygon spanning most of the Western Hemisphere.
        # The SF z6 tile bbox is approx (-123.75, 36.60, -118.125, 40.98).
        # This polygon spans far beyond that, with many vertices.
        # After clipping to the tile, the geometry should have fewer vertices.
        large_wkt = (
            "POLYGON(("
            "-170 10, -160 15, -150 20, -140 25, -135 30, "
            "-130 35, -125 37, -124 38, -123 39, -122 40, "
            "-121 41, -120 42, -118 43, -115 44, -110 45, "
            "-100 46, -90 47, -80 48, -70 49, -60 50, "
            "-60 10, -170 10"
            "))"
        )
        wof_conn.execute(f"""
            INSERT INTO boundaries VALUES (
                1, '1', 'Big Region', 'continent', 0, 30.0, -100.0,
                ST_GeomFromText('{large_wkt}'),
                'XX', 10.0, -170.0, 50.0, -60.0, NULL, NULL
            )
        """)
        wof_conn.execute("CREATE INDEX boundaries_rtree ON boundaries USING RTREE (geom)")
        wof_conn.execute("CREATE INDEX idx_rkey ON boundaries(rkey)")

        # Get original vertex count before closing
        orig_npoints = wof_conn.execute(
            "SELECT ST_NPoints(geom) FROM boundaries WHERE rkey = '1'"
        ).fetchone()[0]
        wof_conn.close()

        # Create places DB
        db_path = tmp_path / "clip_test.duckdb"
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
            "INSERT INTO places VALUES ('p1', 37.77, -122.42, "
            "ST_QuadKey(-122.42, 37.77, 17))"
        )

        # Wrapper that captures tile_boundaries npoints after it's created
        clipped_npoints = []

        class _ClipInspector:
            """Intercepts execute calls to inspect tile_boundaries after creation."""
            def __init__(self, real):
                self._real = real

            def execute(self, sql, *args, **kwargs):
                result = self._real.execute(sql, *args, **kwargs)
                if isinstance(sql, str) and re.search(r"CREATE\b.*\bTABLE\b.*\btile_boundaries\b", sql, re.IGNORECASE):
                    try:
                        row = self._real.execute(
                            "SELECT ST_NPoints(geom) FROM tile_boundaries WHERE rkey = '1'"
                        ).fetchone()
                        if row:
                            clipped_npoints.append(row[0])
                    except Exception:
                        pass
                return result

            def __getattr__(self, name):
                return getattr(self._real, name)

        wrapped = _ClipInspector(conn)
        compute_containment(wrapped, str(wof_path), "fsq_place_id", "longitude", "latitude")

        assert len(clipped_npoints) > 0, (
            "tile_boundaries temp table was never created. "
            "Step 0 (ST_Intersects pre-filter with ST_Intersection clipping) is missing. "
            "compute_containment must create tile_boundaries before phase 1."
        )
        assert clipped_npoints[0] < orig_npoints, (
            f"Clipped geometry should have fewer vertices than original. "
            f"Original: {orig_npoints}, clipped: {clipped_npoints[0]}. "
            f"ST_Intersection clipping in Step 0 is not reducing vertex count."
        )
        conn.close()

    # ------------------------------------------------------------------
    # Test 15: tile_boundaries temp table is cleaned up after execution
    # ------------------------------------------------------------------

    def test_tile_boundaries_cleanup(self, tmp_path, wof_db_path):
        """tile_boundaries temp table must be dropped in the finally block
        after compute_containment completes.

        FAILS on current code because tile_boundaries is never created,
        so there's nothing to clean up. This test verifies that:
        1. The table WAS created during execution (via SQL interception)
        2. tile_boundaries does NOT exist after compute_containment returns
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "cleanup_test.duckdb"
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

        # Track whether tile_boundaries was created during execution
        tile_boundaries_created = []

        class _TrackingConn:
            """Wrapper that tracks tile_boundaries CREATE statements."""
            def __init__(self, real):
                self._real = real

            def execute(self, sql, *args, **kwargs):
                result = self._real.execute(sql, *args, **kwargs)
                if isinstance(sql, str) and re.search(r"CREATE\b.*\bTABLE\b.*\btile_boundaries\b", sql, re.IGNORECASE):
                    tile_boundaries_created.append(True)
                return result

            def __getattr__(self, name):
                return getattr(self._real, name)

        wrapped = _TrackingConn(conn)
        compute_containment(wrapped, str(wof_db_path), "fsq_place_id", "longitude", "latitude")

        # tile_boundaries must have been created during execution
        assert len(tile_boundaries_created) > 0, (
            "tile_boundaries temp table was never created during compute_containment. "
            "Step 0 must CREATE TEMP TABLE tile_boundaries with ST_Intersects pre-filter."
        )

        # tile_boundaries must NOT exist after compute_containment returns
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name = 'tile_boundaries'
        """).fetchall()
        assert len(tables) == 0, (
            "tile_boundaries temp table still exists after compute_containment returned. "
            "It must be dropped in the finally block."
        )
        conn.close()

    # ------------------------------------------------------------------
    # Test 16: log output includes boundaries= field
    # ------------------------------------------------------------------

    def test_log_includes_boundaries_count(self, tmp_path, wof_db_path, caplog):
        """Per-tile log lines must include 'boundaries=N' showing the count
        of tile-intersecting boundaries from Step 0.

        FAILS on current code because the log format is:
            compute_containment: tile %d/%d z6=%s phase1=%d (%.1fs)
        and does not include a boundaries= field.
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "log_test.duckdb"
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

        with caplog.at_level(logging.INFO, logger="garganorn.quadtree"):
            compute_containment(conn, str(wof_db_path), "fsq_place_id", "longitude", "latitude")

        # Find per-tile log lines
        tile_lines = [
            r.message for r in caplog.records
            if "compute_containment: tile" in r.message
        ]
        assert len(tile_lines) > 0, (
            "No per-tile log lines found from compute_containment"
        )
        for line in tile_lines:
            assert "boundaries=" in line, (
                f"Per-tile log line must include 'boundaries=N' field. "
                f"Got: {line!r}"
            )
        conn.close()

    # ------------------------------------------------------------------
    # Test 17: Phase 2 queries tile_boundaries, not wof.boundaries
    # ------------------------------------------------------------------

    def test_phase2_queries_tile_boundaries_not_wof(self, tmp_path, wof_db_path):
        """Phase 2 (edge_matches CTE) must JOIN tile_boundaries instead of
        wof.boundaries.  After Step 0 creates tile_boundaries, all subsequent
        SQL should reference tile_boundaries — not wof.boundaries — for
        spatial joins.

        FAILS on current code because Phase 2 uses ``JOIN wof.boundaries b``.
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "phase2_retarget.duckdb"
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

        # Capture all executed SQL
        sql_log = []

        class _SQLLogger:
            """Wrapper that logs all executed SQL statements."""
            def __init__(self, real):
                self._real = real

            def execute(self, sql, *args, **kwargs):
                if isinstance(sql, str):
                    sql_log.append(sql)
                return self._real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._real, name)

        wrapped = _SQLLogger(conn)
        compute_containment(wrapped, str(wof_db_path), "fsq_place_id", "longitude", "latitude")

        # Find the index of the Step 0 CREATE tile_boundaries statement
        step0_idx = None
        for i, sql in enumerate(sql_log):
            if re.search(r"CREATE\b.*\bTABLE\b.*\btile_boundaries\b", sql, re.IGNORECASE):
                step0_idx = i
                break

        assert step0_idx is not None, (
            "Step 0 (CREATE tile_boundaries) was never executed. "
            "Cannot verify Phase 2 retargeting without Step 0."
        )

        # All SQL after Step 0 should NOT reference wof.boundaries in a
        # JOIN or FROM context — they should use tile_boundaries instead.
        post_step0 = sql_log[step0_idx + 1:]
        wof_boundary_refs = [
            sql for sql in post_step0
            if re.search(r"\b(JOIN|FROM)\s+wof\.boundaries\b", sql, re.IGNORECASE)
        ]
        assert len(wof_boundary_refs) == 0, (
            "After Step 0, Phase 2 must JOIN tile_boundaries instead of "
            "wof.boundaries. Found wof.boundaries references in post-Step-0 SQL:\n"
            + "\n---\n".join(wof_boundary_refs[:3])
        )

        # At least one post-Step-0 SQL should reference tile_boundaries
        tb_refs = [
            sql for sql in post_step0
            if "tile_boundaries" in sql
        ]
        assert len(tb_refs) > 0, (
            "No post-Step-0 SQL references tile_boundaries. "
            "Phase 2 must use tile_boundaries for spatial joins."
        )
        conn.close()
