"""Polar coordinate->tile clamping.

qk17(lon, lat) clamps latitude to +/-85.05101030905541 and NULLs impossible
coordinates.  The 7 production call sites use it instead of calling
ST_QuadKey directly, and quadkey_to_bbox / qk_env extend the outermost tile
rows to +/-90.
"""
import pathlib
import string
import tempfile

import duckdb
import pytest

from garganorn.quadtree import TileManifest, quadkey_to_bbox

REPO_ROOT = pathlib.Path(__file__).parent.parent
_BASE_URL = "https://tiles.example.com"

_QK_ENV_SQL_PATH = REPO_ROOT / "garganorn" / "sql" / "qk_env_macro.sql"


def _load_qk_env_macro(con):
    for stmt in _QK_ENV_SQL_PATH.read_text().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def _load_sql(filename: str, substitutions: dict) -> str:
    sql_path = REPO_ROOT / "garganorn" / "sql" / filename
    raw = sql_path.read_text()
    return string.Template(raw).safe_substitute(substitutions)


def _strip_spatial_install(sql: str) -> str:
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("INSTALL spatial") or stripped.startswith("LOAD spatial"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _strip_memory_limit(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.strip().startswith("SET memory_limit")
    )


# ---------------------------------------------------------------------------
# a valid southern-polar coordinate gets a southern tile
# ---------------------------------------------------------------------------

class TestSouthernPolarCoordinateGetsSouthernTile:
    """overture_place_import.sql's bbox-center qk17 must clamp to the southern outermost z17 row."""

    def test_amundsen_scott_gets_southern_z17_quadkey(self):
        """Amundsen-Scott's qk17 must be the southern outermost z17 row.

        Unclamped, ST_QuadKey wraps any out-of-Mercator-range latitude to
        the north row regardless of sign, yielding '10000000000000000'.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = pathlib.Path(tmpdir)
            con = duckdb.connect(str(tmpdir / "test.duckdb"))
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'ov001' AS id,
                       'Amundsen-Scott' AS name,
                       ST_Point(0.0, -89.99) AS geometry,
                       {'primary': 'amenity=cafe'} AS categories,
                       {xmin: -0.1, xmax: 0.1, ymin: -89.99, ymax: -89.99} AS bbox,
                       {'primary': 'Amundsen-Scott', 'common': MAP([]::VARCHAR[], []::VARCHAR[]),
                        'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]} AS names
            """)
            con.execute(
                "COPY test_places TO '" + str(tmpdir / "test.parquet") + "' (FORMAT PARQUET);"
            )

            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            substitutions = {
                "memory_limit": "4GB",
                "parquet_glob": str(tmpdir / "test.parquet"),
                "xmin": -1.0, "xmax": 1.0,
                "ymin": -90.0, "ymax": -89.0,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
            }
            raw_sql = _load_sql("overture_place_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
            _load_qk_env_macro(con)
            con.execute(sql)

            qk17 = con.execute("SELECT qk17 FROM places WHERE id = 'ov001'").fetchone()[0]
            assert qk17 == "32222222222222222", (
                f"southern point got {qk17!r}, expected the southern outermost "
                "z17 row '32222222222222222'"
            )
            con.close()


# ---------------------------------------------------------------------------
# qk17 column and the density join key must agree
# ---------------------------------------------------------------------------

class TestQk17AndDensityJoinKeyAgree:
    """osm_import.sql's qk17 column and its density join key must agree there is no tile for an impossible coordinate."""

    def test_invalid_latitude_gets_no_density_boost(self):
        """lat=95 is out of range, so qk17 is NULL and the density join key
        must be NULL with it.  An unguarded join key would resolve to the
        wrap tile, match the seeded density row there, and return
        importance 60 instead of 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = pathlib.Path(tmpdir)
            con = duckdb.connect(str(tmpdir / "test.duckdb"))
            con.execute("INSTALL spatial; LOAD spatial;")

            con.execute(
                "CREATE TABLE test_nodes (id BIGINT, lat DOUBLE, lon DOUBLE, tags MAP(VARCHAR, VARCHAR));"
            )
            con.execute(
                "INSERT INTO test_nodes VALUES "
                "(42::BIGINT, 95.0, 0.0, map(['name', 'amenity'], ['Pole Cafe', 'cafe']));"
            )
            con.execute(
                "COPY test_nodes TO '" + str(tmpdir / "nodes.parquet") + "' (FORMAT PARQUET);"
            )
            con.execute(
                "CREATE TABLE test_ways (id BIGINT, tags MAP(VARCHAR, VARCHAR), nds STRUCT(ref BIGINT)[]);"
            )
            con.execute(
                "COPY test_ways TO '" + str(tmpdir / "ways.parquet") + "' (FORMAT PARQUET);"
            )

            # Seeded at left(ST_QuadKey(0, 95, 17), 15) -- the wrap tile an
            # unguarded join key would compute for lat=95.
            density_cte = (
                "CREATE TEMP TABLE density_tiles AS "
                "SELECT '100000000000000' AS tile_qk15, 10.0 AS density_score, "
                "NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, "
                "NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax;"
            )
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
            osm_category_case = "CASE WHEN tags['amenity'] = 'cafe' THEN 'amenity=cafe' END"
            substitutions = {
                "memory_limit": "4GB",
                "node_parquet": str(tmpdir / "nodes.parquet"),
                "way_parquet": str(tmpdir / "ways.parquet"),
                "xmin": -1, "xmax": 1,
                "ymin": 90, "ymax": 100,
                "density_cte": density_cte,
                "idf_cte": idf_cte,
                "density_norm": 10.0,
                "idf_norm": 18.0,
                "osm_category_case": osm_category_case,
            }
            raw_sql = _load_sql("osm_import.sql", substitutions)
            sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
            _load_qk_env_macro(con)
            con.execute(sql)

            qk17, importance = con.execute(
                "SELECT qk17, importance FROM places WHERE osm_id = 42"
            ).fetchone()
            assert qk17 is None, f"expected NULL qk17 for lat=95, got {qk17!r}"
            assert importance == 0, (
                f"expected importance=0 (no tile, no density match) for a place "
                f"with NULL qk17, got {importance}"
            )
            con.close()


# ---------------------------------------------------------------------------
# impossible coordinates get no tile, everywhere -- including sites
# with no existing BETWEEN guard
# ---------------------------------------------------------------------------

class TestDensityExtractRejectsImpossibleCoordinates:
    """density_extract.sql must NULL the tile bucket for an impossible bbox center."""

    def test_impossible_bbox_center_gets_null_tile_qk15(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = pathlib.Path(tmpdir)
            con = duckdb.connect(str(tmpdir / "test.duckdb"))
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute("""
                CREATE TABLE test_places AS
                SELECT 'ov001' AS id,
                       {xmin: 199.9, xmax: 200.1, ymin: 94.9, ymax: 95.1} AS bbox
            """)
            con.execute(
                "COPY test_places TO '" + str(tmpdir / "test.parquet") + "' (FORMAT PARQUET);"
            )

            substitutions = {"parquet_glob": str(tmpdir / "test.parquet")}
            raw_sql = _load_sql("density_extract.sql", substitutions)
            sql = _strip_spatial_install(raw_sql)
            _load_qk_env_macro(con)
            con.execute(sql)

            rows = con.execute("SELECT tile_qk15 FROM density_tiles").fetchall()
            assert len(rows) == 1, f"expected exactly one density group, got {rows}"
            assert rows[0][0] is None, (
                f"impossible bbox center (lon=200, lat=95) got tile_qk15="
                f"{rows[0][0]!r}, expected NULL"
            )
            con.close()


# ---------------------------------------------------------------------------
# a bbox entirely inside a polar cap returns the tiles there
# ---------------------------------------------------------------------------

class TestPolarCapBboxQuery:
    """TileManifest.get_tiles_for_bbox must return a tile whose top row extends to the pole."""

    def test_bbox_above_merc_limit_matches_outermost_row_tile(self, tmp_path):
        path = tmp_path / "manifest.duckdb"
        con = duckdb.connect(str(path))
        con.execute("CREATE TABLE record_tiles (rkey VARCHAR, tile_qk VARCHAR)")
        con.execute("INSERT INTO record_tiles VALUES ('place0', '100000')")
        con.execute("CREATE TABLE metadata (source VARCHAR, generated_at VARCHAR)")
        con.execute("INSERT INTO metadata VALUES ('test', '2026-01-01T00:00:00+00:00')")
        con.close()

        tm = TileManifest(str(path), _BASE_URL)
        urls = tm.get_tiles_for_bbox(5.0, 86.0, 15.0, 89.0)
        assert urls == [f"{_BASE_URL}/100000/100000.json.gz"], (
            f"bbox (5,86,15,89) lies entirely above tile '100000's reported "
            f"ymax; got {urls}, expected a match"
        )


class TestQuadkeyToBboxOutermostRowReachesPole:
    """Reverse direction: quadkey_to_bbox's outermost rows must reach +/-90, and qk_env must agree."""

    def test_northern_outermost_row_ymax_is_90(self):
        _, _, _, ymax = quadkey_to_bbox("1000")
        assert ymax == pytest.approx(90.0, abs=1e-6), (
            f"northern outermost row ymax={ymax}, expected 90.0"
        )

    def test_southern_outermost_row_ymin_is_negative_90(self):
        _, ymin, _, _ = quadkey_to_bbox("3222")
        assert ymin == pytest.approx(-90.0, abs=1e-6), (
            f"southern outermost row ymin={ymin}, expected -90.0"
        )

    def test_qk_env_agrees_with_python_at_outermost_rows(self):
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        _load_qk_env_macro(con)

        py_ymax = quadkey_to_bbox("1000")[3]
        py_ymin = quadkey_to_bbox("3222")[1]
        sql_ymax = con.execute("SELECT ST_YMax(qk_env('1000'))").fetchone()[0]
        sql_ymin = con.execute("SELECT ST_YMin(qk_env('3222'))").fetchone()[0]

        assert sql_ymax == pytest.approx(py_ymax, abs=1e-9)
        assert sql_ymin == pytest.approx(py_ymin, abs=1e-9)


# ---------------------------------------------------------------------------
# structural guard: ST_QuadKey lives only inside qk17's own definition
# ---------------------------------------------------------------------------

class TestStQuadKeyOnlyInsideQk17Macro:
    """Guard: every production ST_QuadKey call lives inside qk17's own definition."""

    def test_no_direct_st_quadkey_call_outside_qk17_definition(self):
        offending = []
        for path in sorted((REPO_ROOT / "garganorn" / "sql").glob("*.sql")):
            text = path.read_text()
            count = text.count("ST_QuadKey(")
            defines_qk17 = "MACRO qk17(" in text
            if defines_qk17:
                if count > 1:
                    offending.append((path.name, count))
            elif count > 0:
                offending.append((path.name, count))
        assert offending == [], (
            f"ST_QuadKey called outside the qk17 macro definition: {offending}"
        )
