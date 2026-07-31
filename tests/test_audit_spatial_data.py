"""Failing tests for spatial and data import bug fixes.

This test file exercises the bugs identified in the spatial/data import audit.
All tests should FAIL until the corresponding fixes are implemented.
"""
import pathlib
import string
import tempfile

import pytest
import duckdb

from garganorn.levels import level_case_sql


REPO_ROOT = pathlib.Path(__file__).parent.parent


def _load_sql(filename: str, substitutions: dict) -> str:
    """Load SQL file and apply substitutions."""
    sql_path = REPO_ROOT / "garganorn" / "sql" / filename
    raw = sql_path.read_text()
    return string.Template(raw).safe_substitute(substitutions)


def _strip_spatial_install(sql: str) -> str:
    """Remove INSTALL/LOAD spatial lines for in-memory tests."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("INSTALL spatial") or stripped.startswith("LOAD spatial"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _strip_memory_limit(sql: str) -> str:
    """Remove memory_limit SET commands for in-memory tests."""
    lines = [
        line for line in sql.splitlines()
        if not line.strip().startswith("SET memory_limit")
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Test SPATIAL-1: Coordinate range validation before ST_QuadKey()
# ---------------------------------------------------------------------------

class TestSpatial1CoordinateRangeValidation:
    """ST_QuadKey() must validate coordinates are in [-180,180]×[-90,90]."""

    def test_overture_place_out_of_range_longitude(self):
        """Overture place with longitude=200 should get NULL qk17."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create a test parquet with invalid coordinates
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'ov001' AS id,
                       'Test Place' AS name,
                       ST_Point(200.0, 37.7749) AS geometry,
                       {'primary': 'amenity=cafe'} AS categories,
                       {xmin: 200.0, xmax: 200.1, ymin: 37.7, ymax: 37.9} AS bbox,
                       {'primary': 'Test Place', 'common': MAP([]::VARCHAR[], []::VARCHAR[]), 'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]} AS names
            """)
            con.execute("COPY test_places TO '" + str(pathlib.Path(tmpdir) / "test.parquet") + "' (FORMAT PARQUET);")

            # Run import SQL - use filter bbox that overlaps the place bbox
            # so the place passes the overlap filter but fails coordinate validation
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "parquet_glob": str(pathlib.Path(tmpdir) / "test.parquet"),
                "xmin": 199.0, "xmax": 201.0,
                "ymin": 37.5, "ymax": 38.0,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
            }
            raw_sql = _load_sql("overture_place_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            # This should not raise an error, but qk17 should be NULL
            con.execute(sql)

            # Check that the place was imported with NULL qk17
            result = con.execute("SELECT qk17 FROM places WHERE id = 'ov001'").fetchone()
            assert result is not None, "Place should be imported even with invalid coordinates"
            qk17 = result[0]
            assert qk17 is None, f"qk17 should be NULL for out-of-range longitude, got {qk17}"

            con.close()

    def test_foursquare_out_of_range_latitude(self):
        """Foursquare place with latitude=100 should get NULL qk17."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create a test parquet with invalid coordinates
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'fsq001' AS fsq_place_id,
                       'Test Place' AS name,
                       100.0 AS latitude,
                       -122.4194 AS longitude,
                       ST_Point(-122.4194, 100.0) AS geom,
                       {xmin: -122.5, xmax: -122.3, ymin: 100.0, ymax: 100.2} AS bbox,
                       ['13065143'] AS fsq_category_ids,
                       '2023-01-01'::DATE AS date_refreshed,
                       NULL::DATE AS date_closed
            """)
            con.execute("COPY test_places TO '" + str(pathlib.Path(tmpdir) / "test.parquet") + "' (FORMAT PARQUET);")

            # Run import SQL - use filter bbox that overlaps the place bbox
            # so the place passes the overlap filter but fails coordinate validation
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "parquet_glob": str(pathlib.Path(tmpdir) / "test.parquet"),
                "xmin": -122.55, "xmax": -122.30,
                "ymin": 99.0, "ymax": 101.0,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
            }
            raw_sql = _load_sql("foursquare_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            # This should not raise an error, but qk17 should be NULL
            con.execute(sql)

            # Check that the place was imported with NULL qk17
            result = con.execute("SELECT qk17 FROM places WHERE fsq_place_id = 'fsq001'").fetchone()
            assert result is not None, "Place should be imported even with invalid coordinates"
            qk17 = result[0]
            assert qk17 is None, f"qk17 should be NULL for out-of-range latitude, got {qk17}"

            con.close()

    def test_density_extract_out_of_range_coordinates(self):
        """density_extract.sql should handle out-of-range coordinates gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")
            # density_extract.sql computes tile bounds via the qk_env() scalar
            # macro (garganorn/sql/qk_env_macro.sql); load it into this
            # standalone connection, mirroring stages._load_qk_env_macros.
            for stmt in (REPO_ROOT / "garganorn" / "sql" / "qk_env_macro.sql").read_text().split(";"):
                stmt = stmt.strip()
                if stmt:
                    con.execute(stmt)

            # Create a test parquet with some out-of-range coordinates
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'ov001' AS id,
                       ST_Point(200.0, 37.7749) AS geometry,
                       {xmin: 200.0, xmax: 200.1, ymin: 37.7, ymax: 37.9} AS bbox
                UNION ALL
                SELECT 'ov002' AS id,
                       ST_Point(-122.4194, 37.7749) AS geometry,
                       {xmin: -122.5, xmax: -122.3, ymin: 37.7, ymax: 37.9} AS bbox
            """)
            con.execute("COPY test_places TO '" + str(pathlib.Path(tmpdir) / "test.parquet") + "' (FORMAT PARQUET);")

            # Run density extract SQL
            substitutions = {
                "parquet_glob": str(pathlib.Path(tmpdir) / "test.parquet"),
            }
            raw_sql = _load_sql("density_extract.sql", substitutions)
            sql = _strip_spatial_install(raw_sql)

            # This should not raise an error
            con.execute(sql)

            # Verify we got at least the valid tile
            result = con.execute("SELECT COUNT(*) FROM density_tiles").fetchone()
            assert result[0] > 0, "Should have at least one density tile from valid coordinates"

            con.close()


# ---------------------------------------------------------------------------
# Test SPATIAL-4/DATA-4: Bbox filter should use overlap, not containment
# ---------------------------------------------------------------------------

class TestSpatial4BboxOverlapFilter:
    """Bbox filters should use overlap logic, not containment."""

    def test_overture_division_partial_overlap(self):
        """Division partially overlapping bbox should be included."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create division parquet with bbox that overlaps the filter boundary
            # Division bbox: xmin=-122.6 (outside), xmax=-122.4 (inside filter)
            # Filter bbox: xmin=-122.55, xmax=-122.30
            con.execute("""
                CREATE TABLE test_division AS
                SELECT 'div001' AS id,
                       {'primary': 'San Francisco', 'common': MAP([]::VARCHAR[], []::VARCHAR[]), 'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]} AS names,
                       'locality' AS subtype,
                       'US' AS country,
                       NULL::VARCHAR AS region,
                       NULL::VARCHAR AS wikidata,
                       1000000 AS population,
                       NULL::VARCHAR AS parent_division_id
            """)
            con.execute("COPY test_division TO '" + str(pathlib.Path(tmpdir) / "division.parquet") + "' (FORMAT PARQUET);")

            # Create division_area parquet with geometry that straddles the boundary
            con.execute("""
                CREATE TABLE test_division_area AS
                SELECT 'div001' AS division_id,
                       3 AS admin_level,
                       ST_GeomFromText('POLYGON((-122.6 37.75, -122.6 37.80, -122.4 37.80, -122.4 37.75, -122.6 37.75))') AS geometry,
                       true AS is_land,
                       {xmin: -122.6, xmax: -122.4, ymin: 37.75, ymax: 37.80} AS bbox
            """)
            con.execute("COPY test_division_area TO '" + str(pathlib.Path(tmpdir) / "division_area.parquet") + "' (FORMAT PARQUET);")

            # Run division import SQL
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "division_parquet": str(pathlib.Path(tmpdir) / "division.parquet"),
                "division_area_parquet": str(pathlib.Path(tmpdir) / "division_area.parquet"),
                "xmin": -122.55, "xmax": -122.30,
                "ymin": 37.60, "ymax": 37.85,
                "density_cte": density_cte,
                "density_norm": 10.0,
                "pop_norm": 15.0,
                "level_case": level_case_sql(),
            }
            raw_sql = _load_sql("overture_division_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            con.execute(sql)

            # Check that the division was imported (it overlaps the filter)
            result = con.execute("SELECT COUNT(*) FROM places WHERE id = 'div001'").fetchone()
            assert result[0] == 1, f"Division with overlapping bbox should be imported, got count {result[0]}"

            con.close()

    def test_overture_place_partial_overlap(self):
        """Place partially overlapping bbox should be included."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create a test parquet with bbox that partially overlaps filter
            # Place bbox: xmin=-122.6 (outside), xmax=-122.4 (inside)
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'ov001' AS id,
                       'Test Place' AS name,
                       ST_Point(-122.5, 37.7749) AS geometry,
                       {'primary': 'amenity=cafe'} AS categories,
                       {xmin: -122.6, xmax: -122.4, ymin: 37.7, ymax: 37.9} AS bbox,
                       {'primary': 'Test Place', 'common': MAP([]::VARCHAR[], []::VARCHAR[]), 'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]} AS names
            """)
            con.execute("COPY test_places TO '" + str(pathlib.Path(tmpdir) / "test.parquet") + "' (FORMAT PARQUET);")

            # Run import SQL
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "parquet_glob": str(pathlib.Path(tmpdir) / "test.parquet"),
                "xmin": -122.55, "xmax": -122.30,
                "ymin": 37.60, "ymax": 37.85,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
            }
            raw_sql = _load_sql("overture_place_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            con.execute(sql)

            # Check that the place was imported (it overlaps the filter)
            result = con.execute("SELECT COUNT(*) FROM places WHERE id = 'ov001'").fetchone()
            assert result[0] == 1, f"Place with overlapping bbox should be imported, got count {result[0]}"

            con.close()

    def test_foursquare_partial_overlap(self):
        """Foursquare place partially overlapping bbox should be included."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create a test parquet with bbox that partially overlaps filter
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'fsq001' AS fsq_place_id,
                       'Test Place' AS name,
                       37.7749 AS latitude,
                       -122.5 AS longitude,
                       ST_Point(-122.5, 37.7749) AS geom,
                       {xmin: -122.6, xmax: -122.4, ymin: 37.7, ymax: 37.9} AS bbox,
                       ['13065143'] AS fsq_category_ids,
                       '2023-01-01'::DATE AS date_refreshed,
                       NULL::DATE AS date_closed
            """)
            con.execute("COPY test_places TO '" + str(pathlib.Path(tmpdir) / "test.parquet") + "' (FORMAT PARQUET);")

            # Run import SQL
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "parquet_glob": str(pathlib.Path(tmpdir) / "test.parquet"),
                "xmin": -122.55, "xmax": -122.30,
                "ymin": 37.60, "ymax": 37.85,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
            }
            raw_sql = _load_sql("foursquare_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            con.execute(sql)

            # Check that the place was imported (it overlaps the filter)
            result = con.execute("SELECT COUNT(*) FROM places WHERE fsq_place_id = 'fsq001'").fetchone()
            assert result[0] == 1, f"Foursquare place with overlapping bbox should be imported, got count {result[0]}"

            con.close()


# ---------------------------------------------------------------------------
# Test DATA-1: OSM way centroid NaN from empty node sets
# ---------------------------------------------------------------------------

class TestData1OsmWayEmptyNodeGuard:
    """OSM ways with zero valid nodes should be filtered out."""

    def test_osm_way_with_no_valid_nodes(self):
        """Way referencing no valid nodes should not appear in places table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create a way parquet with a way that has no matching nodes
            # Use explicit schema to ensure BIGINT type is preserved
            con.execute("CREATE TABLE test_ways (id BIGINT, tags MAP(VARCHAR, VARCHAR), nds STRUCT(ref BIGINT)[]);")
            con.execute("INSERT INTO test_ways VALUES (9999999999::BIGINT, map(['name', 'amenity'], ['Empty Way', 'cafe']), []);")
            con.execute("COPY test_ways TO '" + str(pathlib.Path(tmpdir) / "ways.parquet") + "' (FORMAT PARQUET);")

            # Create empty nodes parquet
            con.execute("CREATE TABLE test_nodes (id BIGINT, lat DOUBLE, lon DOUBLE, tags MAP(VARCHAR, VARCHAR));")
            con.execute("COPY test_nodes TO '" + str(pathlib.Path(tmpdir) / "nodes.parquet") + "' (FORMAT PARQUET);")

            # Run OSM import SQL
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            osm_category_case = "CASE WHEN tags['amenity'] = 'cafe' THEN 'amenity=cafe' END"
            substitutions = {
                "memory_limit": "4GB",
                "node_parquet": str(pathlib.Path(tmpdir) / "nodes.parquet"),
                "way_parquet": str(pathlib.Path(tmpdir) / "ways.parquet"),
                "xmin": -122.55, "xmax": -122.30,
                "ymin": 37.60, "ymax": 37.85,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
                "osm_category_case": osm_category_case,
            }
            raw_sql = _load_sql("osm_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            con.execute(sql)

            # Check that the way was NOT imported
            result = con.execute("SELECT COUNT(*) FROM places WHERE osm_id = 9999999999 AND osm_type = 'w'").fetchone()
            assert result[0] == 0, f"Way with no valid nodes should not be imported, got count {result[0]}"

            con.close()


# ---------------------------------------------------------------------------
# Test DATA-3: Bbox min/max ordering never validated
# ---------------------------------------------------------------------------

class TestData3BboxNormalization:
    """Inverted bboxes should be normalized using LEAST/GREATEST."""

    def test_overture_place_inverted_bbox(self):
        """Place with inverted bbox should produce valid qk17 after normalization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create a test parquet with inverted bbox (xmin > xmax, ymin > ymax)
            # After normalization: bbox should be [-122.5, -122.3, 37.7, 37.9]
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'ov001' AS id,
                       'Test Place' AS name,
                       ST_Point(-122.4194, 37.7749) AS geometry,
                       {'primary': 'amenity=cafe'} AS categories,
                       {xmin: -122.3, xmax: -122.5, ymin: 37.9, ymax: 37.7} AS bbox,
                       {'primary': 'Test Place', 'common': MAP([]::VARCHAR[], []::VARCHAR[]), 'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]} AS names
            """)
            con.execute("COPY test_places TO '" + str(pathlib.Path(tmpdir) / "test.parquet") + "' (FORMAT PARQUET);")

            # Run import SQL - filter bbox overlaps the normalized place bbox
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "parquet_glob": str(pathlib.Path(tmpdir) / "test.parquet"),
                "xmin": -122.55, "xmax": -122.25,
                "ymin": 37.65, "ymax": 37.95,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
            }
            raw_sql = _load_sql("overture_place_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            con.execute(sql)

            # Check that qk17 was computed correctly after normalization
            result = con.execute("SELECT qk17 FROM places WHERE id = 'ov001'").fetchone()
            assert result is not None, "Place should be imported"
            qk17 = result[0]
            assert qk17 is not None, f"qk17 should not be NULL for normalized bbox, got {qk17}"
            # The qk17 should be for SF (roughly 023...)
            assert qk17.startswith("023"), f"qk17 should start with 023 for SF, got {qk17}"

            con.close()

    def test_foursquare_inverted_bbox(self):
        """Foursquare place with inverted bbox should still be importable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create a test parquet with inverted bbox (xmin > xmax, ymin > ymax)
            # After normalization: bbox should be [-122.5, -122.3, 37.7, 37.9]
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'fsq001' AS fsq_place_id,
                       'Test Place' AS name,
                       37.7749 AS latitude,
                       -122.4194 AS longitude,
                       ST_Point(-122.4194, 37.7749) AS geom,
                       {xmin: -122.3, xmax: -122.5, ymin: 37.9, ymax: 37.7} AS bbox,
                       ['13065143'] AS fsq_category_ids,
                       '2023-01-01'::DATE AS date_refreshed,
                       NULL::DATE AS date_closed
            """)
            con.execute("COPY test_places TO '" + str(pathlib.Path(tmpdir) / "test.parquet") + "' (FORMAT PARQUET);")

            # Run import SQL - filter bbox overlaps the normalized place bbox
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "parquet_glob": str(pathlib.Path(tmpdir) / "test.parquet"),
                "xmin": -122.55, "xmax": -122.25,
                "ymin": 37.65, "ymax": 37.95,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
            }
            raw_sql = _load_sql("foursquare_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            con.execute(sql)

            # Check that the place was imported with valid qk17
            result = con.execute("SELECT qk17 FROM places WHERE fsq_place_id = 'fsq001'").fetchone()
            assert result is not None, "Place should be imported"
            qk17 = result[0]
            assert qk17 is not None, f"qk17 should not be NULL after bbox normalization, got {qk17}"

            con.close()


# ---------------------------------------------------------------------------
# Test DATA-8: FSQ empty category array guard
# ---------------------------------------------------------------------------

class TestData8FsqEmptyCategoryArrayGuard:
    """Foursquare places with empty category arrays should be handled gracefully."""

    def test_fsq_empty_category_array(self):
        """Place with empty category array should import with idf_score=0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = pathlib.Path(tmpdir) / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("INSTALL spatial; LOAD spatial;")

            # Create a test parquet with empty category array
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'fsq001' AS fsq_place_id,
                       'Test Place' AS name,
                       37.7749 AS latitude,
                       -122.4194 AS longitude,
                       ST_Point(-122.4194, 37.7749) AS geom,
                       {xmin: -122.42, xmax: -122.41, ymin: 37.77, ymax: 37.78} AS bbox,
                       [] AS fsq_category_ids,
                       '2023-01-01'::DATE AS date_refreshed,
                       NULL::DATE AS date_closed
            """)
            con.execute("COPY test_places TO '" + str(pathlib.Path(tmpdir) / "test.parquet") + "' (FORMAT PARQUET);")

            # Run import SQL
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "parquet_glob": str(pathlib.Path(tmpdir) / "test.parquet"),
                "xmin": -122.55, "xmax": -122.30,
                "ymin": 37.60, "ymax": 37.85,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
            }
            raw_sql = _load_sql("foursquare_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))

            # This should not raise an error
            con.execute(sql)

            # Check that the place was imported with importance=0 (no IDF score)
            result = con.execute("SELECT importance FROM places WHERE fsq_place_id = 'fsq001'").fetchone()
            assert result is not None, "Place with empty category array should be imported"
            importance = result[0]
            assert importance == 0, f"importance should be 0 for empty categories, got {importance}"

            con.close()
