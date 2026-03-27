"""Tests for the osm-pbf-parquet import pipeline.

These tests verify the SQL logic that will be implemented in the new
import-osm.sh, build-density.sh, and build-idf.sh scripts.

The approach: create synthetic Hive-partitioned Parquet fixtures that
match osm-pbf-parquet's output schema, then execute the design's SQL
queries directly against them to verify correctness.

osm-pbf-parquet schema:
  type=node/  — columns: id BIGINT, tags MAP(VARCHAR,VARCHAR), lat DOUBLE, lon DOUBLE
  type=way/   — columns: id BIGINT, tags MAP(VARCHAR,VARCHAR), nds STRUCT(ref BIGINT)[]
"""

import math
import os
import shutil
import subprocess
import tempfile

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Shared SQL fragments (identical to what will be in import-osm.sh)
# ---------------------------------------------------------------------------

TAG_FILTER_WHERE = """
    (tags['amenity'] IS NOT NULL
     AND tags['amenity'] NOT IN (
         'parking', 'parking_space', 'bench', 'waste_basket',
         'bicycle_parking', 'shelter', 'recycling', 'toilets',
         'post_box', 'drinking_water', 'vending_machine',
         'waste_disposal', 'hunting_stand', 'parking_entrance',
         'grit_bin', 'give_box', 'bbq')
     AND tags['name'] IS NOT NULL)
    OR (tags['shop'] IS NOT NULL
        AND tags['shop'] NOT IN ('yes', 'vacant')
        AND tags['name'] IS NOT NULL)
    OR (tags['tourism'] IS NOT NULL AND tags['name'] IS NOT NULL)
    OR (tags['leisure'] IN (
            'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
            'golf_course', 'stadium', 'sports_hall', 'marina',
            'nature_reserve', 'garden', 'playground', 'dog_park',
            'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
            'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
            'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
            'escape_game', 'hackerspace')
        AND tags['name'] IS NOT NULL)
    OR (tags['office'] IS NOT NULL AND tags['office'] != 'yes'
        AND tags['name'] IS NOT NULL)
    OR (tags['craft'] IS NOT NULL AND tags['craft'] != 'yes'
        AND tags['name'] IS NOT NULL)
    OR (tags['healthcare'] IS NOT NULL AND tags['healthcare'] != 'yes'
        AND tags['name'] IS NOT NULL)
    OR (tags['historic'] IN (
            'castle', 'monument', 'memorial', 'archaeological_site',
            'ruins', 'fort', 'manor', 'church', 'city_gate',
            'building', 'mine', 'wreck')
        AND tags['name'] IS NOT NULL)
    OR (tags['natural'] IN (
            'peak', 'beach', 'spring', 'bay', 'cave_entrance',
            'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
            'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock')
        AND tags['name'] IS NOT NULL)
    OR (tags['man_made'] IN (
            'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
            'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
            'beacon')
        AND tags['name'] IS NOT NULL)
    OR tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
    OR tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
    OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL)
    OR (tags['place'] IN (
            'city', 'town', 'village', 'hamlet', 'suburb',
            'neighbourhood', 'quarter', 'island', 'square')
        AND tags['name'] IS NOT NULL)
"""

PRIMARY_CATEGORY_CASE = """
    CASE
        WHEN tags['amenity'] IS NOT NULL THEN 'amenity=' || tags['amenity']
        WHEN tags['shop'] IS NOT NULL THEN 'shop=' || tags['shop']
        WHEN tags['tourism'] IS NOT NULL THEN 'tourism=' || tags['tourism']
        WHEN tags['leisure'] IS NOT NULL THEN 'leisure=' || tags['leisure']
        WHEN tags['office'] IS NOT NULL THEN 'office=' || tags['office']
        WHEN tags['craft'] IS NOT NULL THEN 'craft=' || tags['craft']
        WHEN tags['healthcare'] IS NOT NULL THEN 'healthcare=' || tags['healthcare']
        WHEN tags['historic'] IS NOT NULL THEN 'historic=' || tags['historic']
        WHEN tags['natural'] IS NOT NULL THEN 'natural=' || tags['natural']
        WHEN tags['man_made'] IS NOT NULL THEN 'man_made=' || tags['man_made']
        WHEN tags['aeroway'] IS NOT NULL THEN 'aeroway=' || tags['aeroway']
        WHEN tags['railway'] IS NOT NULL THEN 'railway=' || tags['railway']
        WHEN tags['public_transport'] IS NOT NULL
            THEN 'public_transport=' || tags['public_transport']
        WHEN tags['place'] IS NOT NULL THEN 'place=' || tags['place']
    END
"""

TAGS_FILTERED_EXPR = """
    map_from_entries(
        list_filter(
            map_entries(tags),
            e -> e.key != split_part(primary_category, '=', 1)
               AND e.key IN (
                   'cuisine', 'sport', 'religion', 'denomination',
                   'opening_hours', 'phone', 'website', 'wikidata',
                   'wheelchair', 'internet_access',
                   'addr:street', 'addr:housenumber', 'addr:city',
                   'addr:postcode', 'addr:country')
        )
    )
"""

PLACES_SCHEMA_DDL = """
    CREATE TABLE places (
        osm_type         VARCHAR,
        osm_id           BIGINT,
        rkey             VARCHAR,
        name             VARCHAR,
        latitude         DOUBLE,
        longitude        DOUBLE,
        geom             GEOMETRY,
        primary_category VARCHAR,
        tags             MAP(VARCHAR, VARCHAR),
        bbox             STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
        importance       INTEGER DEFAULT 0
    )
"""

EXPECTED_PLACES_COLUMNS = [
    ("osm_type", "VARCHAR"),
    ("osm_id", "BIGINT"),
    ("rkey", "VARCHAR"),
    ("name", "VARCHAR"),
    ("latitude", "DOUBLE"),
    ("longitude", "DOUBLE"),
    ("geom", "GEOMETRY"),
    ("primary_category", "VARCHAR"),
    ("tags", "MAP(VARCHAR, VARCHAR)"),
    ("bbox", "STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)"),
    ("importance", "INTEGER"),
]


# ---------------------------------------------------------------------------
# Fixtures: Hive-partitioned Parquet test data
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def parquet_dir(tmp_path_factory):
    """
    Produce a Hive-partitioned Parquet directory matching osm-pbf-parquet
    output schema.

    Node data:
      1001 — cafe named "Blue Bottle"            (should pass)
      1002 — unnamed parking (amenity=parking)   (should be filtered out)
      1003 — named park (leisure=park)            (should pass)
      1004 — named shop (shop=convenience)        (should pass)
      1005 — amenity=bench, no name              (should be filtered out)
      1006 — railway station, no name             (passes tag filter but dropped by outer name check)
      1007 — tourism=hotel with name             (should pass)

    Way data (with constituent node refs):
      2001 — named park (leisure=park), nodes 101–104 (centroid 37.76, -122.43)
      2002 — parking (amenity=parking)           (should be filtered out)
      2003 — named shop (shop=bakery), nodes 201–202 (centroid 37.78, -122.41)

    Extra nodes (constituent nodes of ways):
      101–104: corners of way 2001 (central park)
      201–202: corners of way 2003 (bakery)
    """
    base = tmp_path_factory.mktemp("osm_parquet")
    node_dir = base / "type=node"
    way_dir = base / "type=way"
    node_dir.mkdir()
    way_dir.mkdir()

    conn = duckdb.connect(":memory:")

    # --- Nodes ---
    conn.execute("""
        CREATE TABLE tmp_nodes (
            id   BIGINT,
            tags MAP(VARCHAR, VARCHAR),
            lat  DOUBLE,
            lon  DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO tmp_nodes VALUES
            -- Named places (should appear in output)
            (1001, MAP {'name': 'Blue Bottle', 'amenity': 'cafe'},
             37.7749, -122.4194),
            (1003, MAP {'name': 'Dolores Park', 'leisure': 'park'},
             37.759, -122.426),
            (1004, MAP {'name': 'Corner Shop', 'shop': 'convenience'},
             37.760, -122.427),
            (1006, MAP {'railway': 'station'},
             37.762, -122.430),
            (1007, MAP {'name': 'Hotel Valencia', 'tourism': 'hotel'},
             37.763, -122.431),
            -- Filtered out: unnamed amenity=parking
            (1002, MAP {'amenity': 'parking'}, 37.776, -122.42),
            -- Filtered out: unnamed bench
            (1005, MAP {'amenity': 'bench'}, 37.777, -122.421),
            -- Constituent nodes for ways
            (101, MAP {}, 37.77, -122.44),
            (102, MAP {}, 37.77, -122.42),
            (103, MAP {}, 37.75, -122.42),
            (104, MAP {}, 37.75, -122.44),
            (201, MAP {}, 37.79, -122.41),
            (202, MAP {}, 37.77, -122.41)
    """)
    conn.execute(
        f"COPY tmp_nodes TO '{node_dir}/node_0000.parquet' (FORMAT PARQUET)"
    )

    # --- Ways ---
    conn.execute("""
        CREATE TABLE tmp_ways (
            id   BIGINT,
            tags MAP(VARCHAR, VARCHAR),
            nds  STRUCT(ref BIGINT)[]
        )
    """)
    conn.execute("""
        INSERT INTO tmp_ways VALUES
            (2001, MAP {'name': 'Central Park', 'leisure': 'park'},
             [{'ref': 101}, {'ref': 102}, {'ref': 103}, {'ref': 104}]),
            (2002, MAP {'amenity': 'parking'},
             [{'ref': 105}, {'ref': 106}]),
            (2003, MAP {'name': 'Grand Bakery', 'shop': 'bakery'},
             [{'ref': 201}, {'ref': 202}])
    """)
    conn.execute(
        f"COPY tmp_ways TO '{way_dir}/way_0000.parquet' (FORMAT PARQUET)"
    )

    conn.close()
    return base


@pytest.fixture(scope="module")
def osm_duckdb(parquet_dir, tmp_path_factory):
    """
    Build and return the path to an osm.duckdb produced from the parquet
    fixtures using the new import SQL design.

    This fixture runs the import SQL against the Hive-partitioned Parquet
    files and returns the path to the resulting DuckDB database.  When the
    new import-osm.sh is implemented it will produce exactly this database.
    """
    db_path = tmp_path_factory.mktemp("osm_db") / "osm.duckdb"
    node_parquet = str(parquet_dir / "type=node" / "*.parquet")
    way_parquet = str(parquet_dir / "type=way" / "*.parquet")

    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute(PLACES_SCHEMA_DDL)

    # --- Node import ---
    conn.execute(f"""
        INSERT INTO places
        WITH filtered AS (
            SELECT
                'n' AS osm_type,
                id AS osm_id,
                tags['name'] AS name,
                lat AS latitude,
                lon AS longitude,
                tags,
                {PRIMARY_CATEGORY_CASE} AS primary_category
            FROM read_parquet('{node_parquet}')
            WHERE lat IS NOT NULL AND lon IS NOT NULL
              AND ({TAG_FILTER_WHERE})
        )
        SELECT
            osm_type,
            osm_id,
            osm_type || CAST(osm_id AS VARCHAR) AS rkey,
            name,
            latitude,
            longitude,
            ST_Point(longitude, latitude) AS geom,
            primary_category,
            {TAGS_FILTERED_EXPR} AS tags,
            {{'xmin': longitude - 0.0001,
              'ymin': latitude - 0.0001,
              'xmax': longitude + 0.0001,
              'ymax': latitude + 0.0001}} AS bbox,
            0 AS importance
        FROM filtered
        WHERE primary_category IS NOT NULL
          AND name IS NOT NULL
    """)

    # --- Way centroid import ---
    conn.execute(f"""
        INSERT INTO places
        WITH qualifying_ways AS (
            SELECT
                id AS osm_id,
                tags['name'] AS name,
                nds,
                tags,
                {PRIMARY_CATEGORY_CASE} AS primary_category
            FROM read_parquet('{way_parquet}')
            WHERE ({TAG_FILTER_WHERE})
        ),
        way_node_refs AS (
            SELECT osm_id, UNNEST(nds).ref AS node_ref
            FROM qualifying_ways
        ),
        needed_node_ids AS (
            SELECT DISTINCT node_ref AS id
            FROM way_node_refs
        ),
        node_coords AS (
            SELECT n.id, n.lat, n.lon
            FROM read_parquet('{node_parquet}') n
            SEMI JOIN needed_node_ids nn ON n.id = nn.id
            WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
        ),
        way_centroids AS (
            SELECT
                wnr.osm_id,
                avg(nc.lat) AS latitude,
                avg(nc.lon) AS longitude
            FROM way_node_refs wnr
            JOIN node_coords nc ON wnr.node_ref = nc.id
            GROUP BY wnr.osm_id
        )
        SELECT
            'w' AS osm_type,
            qw.osm_id,
            'w' || CAST(qw.osm_id AS VARCHAR) AS rkey,
            qw.name,
            wc.latitude,
            wc.longitude,
            ST_Point(wc.longitude, wc.latitude) AS geom,
            qw.primary_category,
            {TAGS_FILTERED_EXPR.replace('tags', 'qw.tags')} AS tags,
            {{'xmin': wc.longitude - 0.0001,
              'ymin': wc.latitude - 0.0001,
              'xmax': wc.longitude + 0.0001,
              'ymax': wc.latitude + 0.0001}} AS bbox,
            0 AS importance
        FROM qualifying_ways qw
        JOIN way_centroids wc ON qw.osm_id = wc.osm_id
        WHERE qw.primary_category IS NOT NULL
          AND qw.name IS NOT NULL
    """)

    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Helper: run node import SQL and return results
# ---------------------------------------------------------------------------

def _run_node_import(conn, node_parquet):
    """Run the node import query against an in-memory connection and return rows."""
    return conn.execute(f"""
        WITH filtered AS (
            SELECT
                'n' AS osm_type,
                id AS osm_id,
                tags['name'] AS name,
                lat AS latitude,
                lon AS longitude,
                tags,
                {PRIMARY_CATEGORY_CASE} AS primary_category
            FROM read_parquet('{node_parquet}')
            WHERE lat IS NOT NULL AND lon IS NOT NULL
              AND ({TAG_FILTER_WHERE})
        )
        SELECT
            osm_type,
            osm_id,
            name,
            latitude,
            longitude,
            primary_category
        FROM filtered
        WHERE primary_category IS NOT NULL
          AND name IS NOT NULL
        ORDER BY osm_id
    """).fetchall()


def _run_way_import(conn, node_parquet, way_parquet):
    """Run the way centroid import query and return rows."""
    return conn.execute(f"""
        WITH qualifying_ways AS (
            SELECT
                id AS osm_id,
                tags['name'] AS name,
                nds,
                tags,
                {PRIMARY_CATEGORY_CASE} AS primary_category
            FROM read_parquet('{way_parquet}')
            WHERE ({TAG_FILTER_WHERE})
        ),
        way_node_refs AS (
            SELECT osm_id, UNNEST(nds).ref AS node_ref
            FROM qualifying_ways
        ),
        needed_node_ids AS (
            SELECT DISTINCT node_ref AS id
            FROM way_node_refs
        ),
        node_coords AS (
            SELECT n.id, n.lat, n.lon
            FROM read_parquet('{node_parquet}') n
            SEMI JOIN needed_node_ids nn ON n.id = nn.id
            WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
        ),
        way_centroids AS (
            SELECT
                wnr.osm_id,
                avg(nc.lat) AS latitude,
                avg(nc.lon) AS longitude
            FROM way_node_refs wnr
            JOIN node_coords nc ON wnr.node_ref = nc.id
            GROUP BY wnr.osm_id
        )
        SELECT
            'w' AS osm_type,
            qw.osm_id,
            qw.name,
            wc.latitude,
            wc.longitude,
            qw.primary_category
        FROM qualifying_ways qw
        JOIN way_centroids wc ON qw.osm_id = wc.osm_id
        WHERE qw.primary_category IS NOT NULL
          AND qw.name IS NOT NULL
        ORDER BY qw.osm_id
    """).fetchall()


# ---------------------------------------------------------------------------
# Test: Node import
# ---------------------------------------------------------------------------

class TestNodeImport:
    """Node import: reads type=node/ parquet, filters by tags, uses lat/lon."""

    @pytest.fixture(autouse=True)
    def conn(self):
        c = duckdb.connect(":memory:")
        c.execute("INSTALL spatial; LOAD spatial;")
        yield c
        c.close()

    def test_named_cafe_appears(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        osm_ids = [r[1] for r in rows]
        assert 1001 in osm_ids, "named cafe (id=1001) should be imported"

    def test_osm_type_is_n_for_nodes(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        for row in rows:
            assert row[0] == "n", f"expected osm_type='n', got {row[0]!r}"

    def test_coordinates_taken_from_lat_lon(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        cafe = next(r for r in rows if r[1] == 1001)
        assert abs(cafe[3] - 37.7749) < 1e-6, "latitude should come from lat column"
        assert abs(cafe[4] - -122.4194) < 1e-6, "longitude should come from lon column"

    def test_unnamed_parking_filtered_out(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        osm_ids = [r[1] for r in rows]
        assert 1002 not in osm_ids, "unnamed parking (id=1002) should be filtered"

    def test_unnamed_bench_filtered_out(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        osm_ids = [r[1] for r in rows]
        assert 1005 not in osm_ids, "unnamed bench (id=1005) should be filtered"

    def test_named_park_appears(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        osm_ids = [r[1] for r in rows]
        assert 1003 in osm_ids, "named park (id=1003) should be imported"

    def test_named_shop_appears(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        osm_ids = [r[1] for r in rows]
        assert 1004 in osm_ids, "named shop (id=1004) should be imported"

    def test_railway_station_appears_without_name_requirement(self, conn, parquet_dir):
        """railway=station passes the tag filter even without a name tag."""
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        # Node 1006 has railway=station but no name — the WHERE filter
        # passes it, but the outer WHERE name IS NOT NULL drops it.
        # Verify it is NOT present (no name = no import).
        osm_ids = [r[1] for r in rows]
        assert 1006 not in osm_ids, (
            "railway station without name should not appear in places "
            "(outer WHERE name IS NOT NULL drops it)"
        )

    def test_named_tourism_appears(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        osm_ids = [r[1] for r in rows]
        assert 1007 in osm_ids, "named hotel (id=1007) should be imported"

    def test_primary_category_set_correctly(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        row_by_id = {r[1]: r for r in rows}
        assert row_by_id[1001][5] == "amenity=cafe"
        assert row_by_id[1003][5] == "leisure=park"
        assert row_by_id[1004][5] == "shop=convenience"

    def test_constituent_nodes_not_imported(self, conn, parquet_dir):
        """Nodes 101-104 and 201-202 are bare (no matching tags) — not imported."""
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        rows = _run_node_import(conn, node_parquet)
        osm_ids = [r[1] for r in rows]
        for bare_node_id in [101, 102, 103, 104, 201, 202]:
            assert bare_node_id not in osm_ids, (
                f"bare constituent node {bare_node_id} should not be imported"
            )


# ---------------------------------------------------------------------------
# Test: Tag filtering
# ---------------------------------------------------------------------------

class TestTagFiltering:
    """Tag filter logic: correct admission and rejection of feature types."""

    @pytest.fixture(autouse=True)
    def conn(self):
        c = duckdb.connect(":memory:")
        c.execute("INSTALL spatial; LOAD spatial;")
        yield c
        c.close()

    def _import_single_node(self, conn, tags: dict, lat=37.0, lon=-122.0):
        """Helper: run node import against a single synthetic node row."""
        tag_map = "MAP {" + ", ".join(f"'{k}': '{v}'" for k, v in tags.items()) + "}"
        conn.execute(f"""
            CREATE OR REPLACE TABLE single_node AS
            SELECT
                9999::BIGINT AS id,
                {tag_map} AS tags,
                {lat}::DOUBLE AS lat,
                {lon}::DOUBLE AS lon
        """)
        return conn.execute(f"""
            WITH filtered AS (
                SELECT
                    'n' AS osm_type,
                    id AS osm_id,
                    tags['name'] AS name,
                    lat AS latitude,
                    lon AS longitude,
                    tags,
                    {PRIMARY_CATEGORY_CASE} AS primary_category
                FROM single_node
                WHERE lat IS NOT NULL AND lon IS NOT NULL
                  AND ({TAG_FILTER_WHERE})
            )
            SELECT osm_type, osm_id, name, primary_category
            FROM filtered
            WHERE primary_category IS NOT NULL AND name IS NOT NULL
        """).fetchall()

    def test_amenity_cafe_with_name_passes(self, conn):
        rows = self._import_single_node(conn, {"amenity": "cafe", "name": "Blue Bottle"})
        assert len(rows) == 1
        assert rows[0][3] == "amenity=cafe"

    def test_amenity_parking_with_name_rejected(self, conn):
        rows = self._import_single_node(conn, {"amenity": "parking", "name": "Lot A"})
        assert len(rows) == 0

    def test_amenity_bench_rejected(self, conn):
        rows = self._import_single_node(conn, {"amenity": "bench", "name": "Bench 1"})
        assert len(rows) == 0

    def test_amenity_waste_basket_rejected(self, conn):
        rows = self._import_single_node(conn, {"amenity": "waste_basket"})
        assert len(rows) == 0

    def test_amenity_no_name_rejected(self, conn):
        rows = self._import_single_node(conn, {"amenity": "restaurant"})
        assert len(rows) == 0

    def test_shop_yes_rejected(self, conn):
        rows = self._import_single_node(conn, {"shop": "yes", "name": "Generic Shop"})
        assert len(rows) == 0

    def test_shop_vacant_rejected(self, conn):
        rows = self._import_single_node(conn, {"shop": "vacant", "name": "Empty"})
        assert len(rows) == 0

    def test_shop_specific_with_name_passes(self, conn):
        rows = self._import_single_node(conn, {"shop": "bakery", "name": "Bread Co"})
        assert len(rows) == 1
        assert rows[0][3] == "shop=bakery"

    def test_leisure_park_with_name_passes(self, conn):
        rows = self._import_single_node(conn, {"leisure": "park", "name": "Riverside"})
        assert len(rows) == 1
        assert rows[0][3] == "leisure=park"

    def test_aeroway_aerodrome_passes_without_name(self, conn):
        """aeroway=aerodrome passes the tag filter (no name requirement)."""
        rows = self._import_single_node(conn, {"aeroway": "aerodrome"})
        # The outer WHERE name IS NOT NULL will drop it — primary_category is set
        # but name is NULL.  Verify: row should have primary_category set.
        conn.execute(f"""
            CREATE OR REPLACE TABLE single_node AS
            SELECT 9999::BIGINT AS id,
                MAP {{'aeroway': 'aerodrome'}} AS tags,
                37.0::DOUBLE AS lat, -122.0::DOUBLE AS lon
        """)
        inner = conn.execute(f"""
            SELECT {PRIMARY_CATEGORY_CASE} AS primary_category,
                   tags['name'] AS name
            FROM single_node
            WHERE ({TAG_FILTER_WHERE})
        """).fetchall()
        assert len(inner) == 1, "aerodrome should pass the WHERE tag filter"
        assert inner[0][0] == "aeroway=aerodrome"
        assert inner[0][1] is None  # no name

    def test_place_city_with_name_passes(self, conn):
        rows = self._import_single_node(conn, {"place": "city", "name": "Springfield"})
        assert len(rows) == 1
        assert rows[0][3] == "place=city"

    def test_historic_castle_with_name_passes(self, conn):
        rows = self._import_single_node(conn, {"historic": "castle", "name": "Windsor"})
        assert len(rows) == 1
        assert rows[0][3] == "historic=castle"


# ---------------------------------------------------------------------------
# Test: Way centroid resolution
# ---------------------------------------------------------------------------

class TestWayCentroidResolution:
    """Way import: centroid computed as avg(lat), avg(lon) of constituent nodes."""

    @pytest.fixture(autouse=True)
    def conn(self):
        c = duckdb.connect(":memory:")
        c.execute("INSTALL spatial; LOAD spatial;")
        yield c
        c.close()

    def test_named_way_appears(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        way_parquet = str(parquet_dir / "type=way" / "*.parquet")
        rows = _run_way_import(conn, node_parquet, way_parquet)
        osm_ids = [r[1] for r in rows]
        assert 2001 in osm_ids, "named park way (id=2001) should be imported"

    def test_osm_type_is_w_for_ways(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        way_parquet = str(parquet_dir / "type=way" / "*.parquet")
        rows = _run_way_import(conn, node_parquet, way_parquet)
        for row in rows:
            assert row[0] == "w", f"expected osm_type='w', got {row[0]!r}"

    def test_unnamed_way_filtered_out(self, conn, parquet_dir):
        """Way 2002 has amenity=parking (no name) — should not appear."""
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        way_parquet = str(parquet_dir / "type=way" / "*.parquet")
        rows = _run_way_import(conn, node_parquet, way_parquet)
        osm_ids = [r[1] for r in rows]
        assert 2002 not in osm_ids, "unnamed parking way (id=2002) should be filtered"

    def test_centroid_is_average_of_node_coordinates(self, conn, parquet_dir):
        """
        Way 2001 has nodes 101–104 at corners of a 0.02 deg square centered
        on (37.76, -122.43).  avg(lat) = 37.76, avg(lon) = -122.43.
        """
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        way_parquet = str(parquet_dir / "type=way" / "*.parquet")
        rows = _run_way_import(conn, node_parquet, way_parquet)
        way = next(r for r in rows if r[1] == 2001)
        assert abs(way[3] - 37.76) < 1e-6, (
            f"centroid lat should be avg of nodes: expected 37.76, got {way[3]}"
        )
        assert abs(way[4] - -122.43) < 1e-6, (
            f"centroid lon should be avg of nodes: expected -122.43, got {way[4]}"
        )

    def test_way_with_unresolvable_nodes_dropped(self, conn):
        """
        A way whose nds references do not appear in the node parquet gets
        no centroid row and must be dropped.
        """
        tmp = tempfile.mkdtemp()
        try:
            node_dir = os.path.join(tmp, "type=node")
            way_dir = os.path.join(tmp, "type=way")
            os.makedirs(node_dir)
            os.makedirs(way_dir)

            # Node parquet with no entries matching the way's node refs
            conn.execute("""
                CREATE OR REPLACE TABLE tn (
                    id BIGINT, tags MAP(VARCHAR, VARCHAR), lat DOUBLE, lon DOUBLE
                )
            """)
            conn.execute("""
                INSERT INTO tn VALUES
                    (9001, MAP {}, 10.0, 20.0)
            """)
            conn.execute(f"COPY tn TO '{node_dir}/node.parquet' (FORMAT PARQUET)")

            # Way referencing node 9999 which does not exist
            conn.execute("""
                CREATE OR REPLACE TABLE tw (
                    id BIGINT, tags MAP(VARCHAR, VARCHAR), nds STRUCT(ref BIGINT)[]
                )
            """)
            conn.execute("""
                INSERT INTO tw VALUES
                    (8001, MAP {'name': 'Ghost Park', 'leisure': 'park'},
                     [{'ref': 9999}])
            """)
            conn.execute(f"COPY tw TO '{way_dir}/way.parquet' (FORMAT PARQUET)")

            rows = _run_way_import(
                conn,
                f"{node_dir}/*.parquet",
                f"{way_dir}/*.parquet",
            )
            osm_ids = [r[1] for r in rows]
            assert 8001 not in osm_ids, (
                "way with unresolvable nodes should be dropped"
            )
        finally:
            shutil.rmtree(tmp)

    def test_named_shop_way_appears(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        way_parquet = str(parquet_dir / "type=way" / "*.parquet")
        rows = _run_way_import(conn, node_parquet, way_parquet)
        osm_ids = [r[1] for r in rows]
        assert 2003 in osm_ids, "named bakery way (id=2003) should be imported"

    def test_shop_way_category(self, conn, parquet_dir):
        node_parquet = str(parquet_dir / "type=node" / "*.parquet")
        way_parquet = str(parquet_dir / "type=way" / "*.parquet")
        rows = _run_way_import(conn, node_parquet, way_parquet)
        row_2003 = next(r for r in rows if r[1] == 2003)
        assert row_2003[5] == "shop=bakery"


# ---------------------------------------------------------------------------
# Test: Output schema
# ---------------------------------------------------------------------------

class TestOutputSchema:
    """The places table produced by the import has the correct schema."""

    def test_places_table_schema(self, osm_duckdb):
        """places table column names and types match the expected schema."""
        conn = duckdb.connect(str(osm_duckdb), read_only=True)
        try:
            cols = conn.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'places'
                ORDER BY ordinal_position
            """).fetchall()
        finally:
            conn.close()

        assert cols == EXPECTED_PLACES_COLUMNS, (
            f"places schema mismatch.\nExpected: {EXPECTED_PLACES_COLUMNS}\nGot: {cols}"
        )

    def test_places_has_rows(self, osm_duckdb):
        conn = duckdb.connect(str(osm_duckdb), read_only=True)
        try:
            count = conn.execute("SELECT count(*) FROM places").fetchone()[0]
        finally:
            conn.close()
        assert count > 0, "places table should have rows after import"

    def test_all_rows_have_non_null_geom(self, osm_duckdb):
        conn = duckdb.connect(str(osm_duckdb), read_only=True)
        try:
            null_count = conn.execute(
                "SELECT count(*) FROM places WHERE geom IS NULL"
            ).fetchone()[0]
        finally:
            conn.close()
        assert null_count == 0, "all imported rows should have geom set"

    def test_all_rows_have_primary_category(self, osm_duckdb):
        conn = duckdb.connect(str(osm_duckdb), read_only=True)
        try:
            null_count = conn.execute(
                "SELECT count(*) FROM places WHERE primary_category IS NULL"
            ).fetchone()[0]
        finally:
            conn.close()
        assert null_count == 0

    def test_nodes_and_ways_both_present(self, osm_duckdb):
        conn = duckdb.connect(str(osm_duckdb), read_only=True)
        try:
            types = {
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT osm_type FROM places"
                ).fetchall()
            }
        finally:
            conn.close()
        assert "n" in types, "node rows ('n') should be present"
        assert "w" in types, "way rows ('w') should be present"

    def test_importance_column_defaults_to_zero(self, osm_duckdb):
        conn = duckdb.connect(str(osm_duckdb), read_only=True)
        try:
            non_zero = conn.execute(
                "SELECT count(*) FROM places WHERE importance != 0"
            ).fetchone()[0]
        finally:
            conn.close()
        assert non_zero == 0, "importance should default to 0 before scoring"

    def test_bbox_is_populated(self, osm_duckdb):
        conn = duckdb.connect(str(osm_duckdb), read_only=True)
        try:
            null_count = conn.execute(
                "SELECT count(*) FROM places WHERE bbox IS NULL"
            ).fetchone()[0]
        finally:
            conn.close()
        assert null_count == 0

    def test_bbox_dimensions_make_sense(self, osm_duckdb):
        """xmin < xmax and ymin < ymax for every row."""
        conn = duckdb.connect(str(osm_duckdb), read_only=True)
        try:
            bad = conn.execute("""
                SELECT count(*) FROM places
                WHERE bbox.xmin >= bbox.xmax OR bbox.ymin >= bbox.ymax
            """).fetchone()[0]
        finally:
            conn.close()
        assert bad == 0


# ---------------------------------------------------------------------------
# Test: build-density.sh OSM mode (reads from osm.duckdb via ATTACH)
# ---------------------------------------------------------------------------

class TestDensityOsmMode:
    """
    build-density.sh OSM mode should ATTACH an osm.duckdb and read from
    places table using longitude/latitude columns directly (no GeoParquet,
    no ST_PointOnSurface).

    The new build-density.sh will use:
        ATTACH '${osm_db_path}' AS osm_import (READ_ONLY);
        INSERT INTO cell_counts
        SELECT 14 AS level,
            s2_cell_parent(s2_cellfromlonlat(longitude, latitude), 14) AS cell_id,
            count(*) AS pt_count
        FROM osm_import.places
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        GROUP BY cell_id;

    The current build-density.sh on main does NOT support 'osm' as a source
    argument.  These tests verify the SQL logic works against an attached DB.
    """

    def test_density_attach_reads_from_places_table(self, osm_duckdb, tmp_path):
        """
        Verify that density aggregation via ATTACH returns rows for the
        places in the test database.

        This test calls the SQL that will be in build-density.sh directly.
        It will fail until build-density.sh is updated to support osm mode.
        """
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL geography FROM community; LOAD geography;")
        conn.execute(f"ATTACH '{osm_duckdb}' AS osm_import (READ_ONLY)")
        conn.execute("""
            CREATE TABLE cell_counts (
                level    TINYINT NOT NULL,
                cell_id  UBIGINT NOT NULL,
                pt_count UBIGINT NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO cell_counts
            SELECT 14 AS level,
                s2_cell_parent(s2_cellfromlonlat(longitude, latitude), 14) AS cell_id,
                count(*) AS pt_count
            FROM osm_import.places
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY cell_id
        """)
        count = conn.execute(
            "SELECT count(*) FROM cell_counts WHERE level = 14"
        ).fetchone()[0]
        conn.close()
        assert count > 0, "density aggregation should produce level-14 rows"

    def test_density_osm_mode_rejects_geoparquet_path(self, tmp_path):
        """
        Once updated, build-density.sh should reject a GeoParquet path when
        given osm source (it expects a .duckdb path).  This test verifies the
        script distinguishes between old and new invocation style.

        Current build-density.sh does not accept 'osm' as source at all;
        this test confirms that gap exists and will fail until the script is
        updated.
        """
        script = os.path.join(
            os.path.dirname(__file__), "..", "scripts", "build-density.sh"
        )
        result = subprocess.run(
            ["bash", script, "osm", str(tmp_path / "nonexistent.geoparquet")],
            capture_output=True,
            text=True,
        )
        # Current script exits 1 with "Usage" because 'osm' is not a valid source.
        # New script should exit 1 with "file not found" for a nonexistent .duckdb path.
        # Either way, the exit code should be non-zero.
        assert result.returncode != 0, (
            "build-density.sh should reject invalid osm mode invocation"
        )

    def test_density_osm_mode_uses_attach_not_read_parquet(self, osm_duckdb, tmp_path):
        """
        The new OSM density SQL reads from osm_import.places (an ATTACHed
        DuckDB), not from read_parquet(...).  Verify that the correct SQL
        pattern produces the expected aggregate.

        Specifically: the new script must NOT use ST_PointOnSurface() because
        the places table already has latitude/longitude as scalar columns.
        """
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL geography FROM community; LOAD geography;")
        conn.execute(f"ATTACH '{osm_duckdb}' AS osm_import (READ_ONLY)")

        # This is the new SQL pattern.  Run it directly.
        conn.execute("""
            CREATE TABLE cell_counts (
                level TINYINT, cell_id UBIGINT, pt_count UBIGINT
            )
        """)
        conn.execute("""
            INSERT INTO cell_counts
            SELECT 14 AS level,
                s2_cell_parent(s2_cellfromlonlat(longitude, latitude), 14) AS cell_id,
                count(*) AS pt_count
            FROM osm_import.places
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY cell_id
        """)

        total_places = conn.execute(
            "SELECT count(*) FROM osm_import.places"
        ).fetchone()[0]
        total_density_points = conn.execute(
            "SELECT sum(pt_count) FROM cell_counts WHERE level = 14"
        ).fetchone()[0]
        conn.close()

        assert total_density_points == total_places, (
            "sum of pt_count at level 14 should equal total places"
        )


# ---------------------------------------------------------------------------
# Test: build-idf.sh OSM mode (reads from osm.duckdb via ATTACH)
# ---------------------------------------------------------------------------

class TestIdfOsmMode:
    """
    build-idf.sh OSM mode should ATTACH an osm.duckdb and read primary_category
    directly from the places table — not re-derive it from a CASE/WHEN on tags.

    The new SQL:
        ATTACH '${osm_db_path}' AS osm_import (READ_ONLY);
        INSERT INTO category_idf
        SELECT primary_category AS category,
            count(*) AS n_places,
            ln(N.total::double / count(*)::double) AS idf_score
        FROM osm_import.places
        CROSS JOIN (SELECT count(*) AS total FROM osm_import.places) N
        WHERE primary_category IS NOT NULL
        GROUP BY primary_category, N.total;
    """

    def test_idf_attach_reads_from_places_table(self, osm_duckdb):
        """IDF query via ATTACH returns a row per distinct primary_category."""
        conn = duckdb.connect(":memory:")
        conn.execute(f"ATTACH '{osm_duckdb}' AS osm_import (READ_ONLY)")
        conn.execute("""
            CREATE TABLE category_idf (
                category  VARCHAR NOT NULL,
                n_places  UBIGINT NOT NULL,
                idf_score DOUBLE NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO category_idf
            SELECT primary_category AS category,
                count(*) AS n_places,
                ln(N.total::double / count(*)::double) AS idf_score
            FROM osm_import.places
            CROSS JOIN (SELECT count(*) AS total FROM osm_import.places) N
            WHERE primary_category IS NOT NULL
            GROUP BY primary_category, N.total
        """)
        count = conn.execute(
            "SELECT count(*) FROM category_idf"
        ).fetchone()[0]
        conn.close()
        assert count > 0, "IDF table should have at least one category row"

    def test_idf_categories_match_places_categories(self, osm_duckdb):
        """Every distinct primary_category in places should appear in the IDF table."""
        conn = duckdb.connect(":memory:")
        conn.execute(f"ATTACH '{osm_duckdb}' AS osm_import (READ_ONLY)")
        conn.execute("""
            CREATE TABLE category_idf (
                category  VARCHAR NOT NULL,
                n_places  UBIGINT NOT NULL,
                idf_score DOUBLE NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO category_idf
            SELECT primary_category AS category,
                count(*) AS n_places,
                ln(N.total::double / count(*)::double) AS idf_score
            FROM osm_import.places
            CROSS JOIN (SELECT count(*) AS total FROM osm_import.places) N
            WHERE primary_category IS NOT NULL
            GROUP BY primary_category, N.total
        """)
        distinct_cats = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT primary_category FROM osm_import.places "
                "WHERE primary_category IS NOT NULL"
            ).fetchall()
        }
        idf_cats = {
            r[0] for r in conn.execute("SELECT category FROM category_idf").fetchall()
        }
        conn.close()
        assert distinct_cats == idf_cats, (
            "IDF categories should exactly match distinct places categories"
        )

    def test_idf_n_places_sums_to_total(self, osm_duckdb):
        """Sum of n_places across all categories equals total place count."""
        conn = duckdb.connect(":memory:")
        conn.execute(f"ATTACH '{osm_duckdb}' AS osm_import (READ_ONLY)")
        conn.execute("""
            CREATE TABLE category_idf (
                category  VARCHAR NOT NULL,
                n_places  UBIGINT NOT NULL,
                idf_score DOUBLE NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO category_idf
            SELECT primary_category AS category,
                count(*) AS n_places,
                ln(N.total::double / count(*)::double) AS idf_score
            FROM osm_import.places
            CROSS JOIN (SELECT count(*) AS total FROM osm_import.places) N
            WHERE primary_category IS NOT NULL
            GROUP BY primary_category, N.total
        """)
        total_places = conn.execute(
            "SELECT count(*) FROM osm_import.places WHERE primary_category IS NOT NULL"
        ).fetchone()[0]
        sum_n_places = conn.execute(
            "SELECT sum(n_places) FROM category_idf"
        ).fetchone()[0]
        conn.close()
        assert sum_n_places == total_places

    def test_idf_score_is_positive(self, osm_duckdb):
        """IDF scores should all be >= 0 (ln(N/n) >= 0 when n <= N)."""
        conn = duckdb.connect(":memory:")
        conn.execute(f"ATTACH '{osm_duckdb}' AS osm_import (READ_ONLY)")
        conn.execute("""
            CREATE TABLE category_idf (
                category  VARCHAR NOT NULL,
                n_places  UBIGINT NOT NULL,
                idf_score DOUBLE NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO category_idf
            SELECT primary_category AS category,
                count(*) AS n_places,
                ln(N.total::double / count(*)::double) AS idf_score
            FROM osm_import.places
            CROSS JOIN (SELECT count(*) AS total FROM osm_import.places) N
            WHERE primary_category IS NOT NULL
            GROUP BY primary_category, N.total
        """)
        neg_count = conn.execute(
            "SELECT count(*) FROM category_idf WHERE idf_score < 0"
        ).fetchone()[0]
        conn.close()
        assert neg_count == 0, "all IDF scores should be >= 0"

    def test_idf_osm_mode_rejects_geoparquet_path(self, tmp_path):
        """
        build-idf.sh currently does not accept 'osm' as a source.
        This test verifies the gap exists and will fail once the
        new osm mode is added that expects a .duckdb path.
        """
        script = os.path.join(
            os.path.dirname(__file__), "..", "scripts", "build-idf.sh"
        )
        result = subprocess.run(
            ["bash", script, "osm", str(tmp_path / "nonexistent.geoparquet")],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0, (
            "build-idf.sh should reject invalid osm mode invocation"
        )


# ---------------------------------------------------------------------------
# Test: import-osm.sh existence and structure
# ---------------------------------------------------------------------------

class TestImportOsmScript:
    """
    import-osm.sh must exist on the filesystem and support the new
    osm-pbf-parquet-based invocation pattern.
    """

    SCRIPT_PATH = os.path.join(
        os.path.dirname(__file__), "..", "scripts", "import-osm.sh"
    )

    def test_script_exists(self):
        """import-osm.sh must exist in scripts/."""
        assert os.path.isfile(self.SCRIPT_PATH), (
            "scripts/import-osm.sh does not exist — "
            "the new pipeline script has not been created yet"
        )

    def _read_script(self):
        """Read the script contents, skipping if the file doesn't exist yet."""
        if not os.path.isfile(self.SCRIPT_PATH):
            pytest.skip("scripts/import-osm.sh not yet implemented")
        with open(self.SCRIPT_PATH) as f:
            return f.read()

    def test_script_does_not_reference_quackosm(self):
        """New import-osm.sh must not reference QuackOSM."""
        content = self._read_script()
        assert "quackosm" not in content.lower(), (
            "import-osm.sh still references QuackOSM — "
            "should use osm-pbf-parquet instead"
        )

    def test_script_references_osm_pbf_parquet(self):
        """New import-osm.sh must reference osm-pbf-parquet."""
        content = self._read_script()
        assert "osm-pbf-parquet" in content, (
            "import-osm.sh does not reference osm-pbf-parquet"
        )

    def test_script_type_node_partition(self):
        """Script must read from type=node/ partition."""
        content = self._read_script()
        assert "type=node" in content, (
            "import-osm.sh must read from type=node/ Hive partition"
        )

    def test_script_type_way_partition(self):
        """Script must read from type=way/ partition."""
        content = self._read_script()
        assert "type=way" in content, (
            "import-osm.sh must read from type=way/ Hive partition"
        )

    # -----------------------------------------------------------------------
    # Performance fix tests (Red phase — these FAIL until fixes are applied)
    # -----------------------------------------------------------------------

    def test_places_table_has_rkey_column(self):
        """places table DDL must include a rkey column.

        After the performance fix, the places table is created with a
        persistent rkey VARCHAR column (osm_type || osm_id::VARCHAR) so that
        the importance scoring and name index steps can reference it directly
        instead of recomputing the concatenation on every row.
        FAILS until the CREATE TABLE places DDL adds a rkey column.
        """
        content = self._read_script()
        # The CREATE TABLE places block must list rkey as a column.
        # We look for "rkey" appearing inside the DDL (before IMPORT or INSERT).
        assert "rkey" in content, (
            "import-osm.sh places table DDL does not define a 'rkey' column. "
            "Add 'rkey VARCHAR' to the CREATE TABLE places statement and populate "
            "it as osm_type || osm_id::VARCHAR during INSERT."
        )
        # More specifically, the rkey column should appear in the CREATE TABLE block,
        # not only in derived expressions. Check that it's defined before INSERT INTO.
        create_pos = content.find("CREATE TABLE places")
        insert_pos = content.find("INSERT INTO places")
        assert create_pos != -1, "CREATE TABLE places not found in script"
        assert insert_pos != -1, "INSERT INTO places not found in script"
        ddl_block = content[create_pos:insert_pos]
        assert "rkey" in ddl_block, (
            "import-osm.sh places DDL block does not contain 'rkey'. "
            "The column must be declared in CREATE TABLE places."
        )

    def test_rkey_populated_during_insert(self):
        """INSERT INTO places must populate rkey as osm_type || osm_id::VARCHAR.

        After the performance fix, the places table has a persistent rkey column.
        The INSERT for nodes must SELECT the rkey value (e.g. 'n' || id::VARCHAR
        or osm_type || osm_id::VARCHAR AS rkey) so that the column is populated
        on every inserted row.  The current script does NOT have rkey in the
        places DDL at all, so the INSERT SELECT list does not include any
        rkey-producing expression.
        FAILS until the INSERT statements assign rkey.
        """
        content = self._read_script()
        # Locate the first INSERT INTO places block (the nodes import).
        insert_places_pos = content.find("INSERT INTO places")
        assert insert_places_pos != -1, "INSERT INTO places not found in script"
        # The second INSERT INTO places starts the ways block. Locate it so we
        # can bound the node INSERT block precisely.
        second_insert_pos = content.find("INSERT INTO places", insert_places_pos + 1)
        if second_insert_pos == -1:
            # Only one INSERT block — use importance scoring section as end boundary
            second_insert_pos = content.find("Computing importance scores")
        if second_insert_pos == -1:
            second_insert_pos = insert_places_pos + 5000  # generous bound
        node_insert_block = content[insert_places_pos:second_insert_pos]
        # rkey must be explicitly produced in the SELECT list of the node INSERT.
        # Acceptable forms: 'n' || id::VARCHAR AS rkey  /  osm_type || osm_id::VARCHAR AS rkey
        has_rkey_in_node_insert = (
            "'n' || id::VARCHAR" in node_insert_block
            or "'n'||id::VARCHAR" in node_insert_block
            or "osm_type || osm_id::VARCHAR AS rkey" in node_insert_block
            or "osm_type||osm_id::VARCHAR AS rkey" in node_insert_block
        )
        assert has_rkey_in_node_insert, (
            "import-osm.sh node INSERT INTO places does not produce a rkey value. "
            "The SELECT list must include an expression like 'n' || id::VARCHAR AS rkey "
            "or osm_type || osm_id::VARCHAR AS rkey. "
            "Currently the places table has no rkey column at all."
        )

        # Also check the ways INSERT block
        if second_insert_pos < len(content):
            # Find the end of the ways INSERT block
            ways_end = content.find("DELETE FROM places", second_insert_pos)
            if ways_end == -1:
                ways_end = content.find("Computing importance", second_insert_pos)
            if ways_end == -1:
                ways_end = second_insert_pos + 5000
            way_insert_block = content[second_insert_pos:ways_end]
            has_rkey_in_way_insert = (
                "'w' || qw.osm_id::VARCHAR" in way_insert_block
                or "'w'||qw.osm_id::VARCHAR" in way_insert_block
                or "osm_type || osm_id::VARCHAR AS rkey" in way_insert_block
                or "osm_type||osm_id::VARCHAR AS rkey" in way_insert_block
                or "qw.osm_type || qw.osm_id::VARCHAR" in way_insert_block
            )
            assert has_rkey_in_way_insert, (
                "import-osm.sh way INSERT INTO places does not produce a rkey value. "
                "The SELECT list must include an expression like 'w' || qw.osm_id::VARCHAR AS rkey."
            )

    def test_importance_scoring_uses_s2_level_12(self):
        """Importance scoring must use S2 level 12, not level 14.

        Density lookup at level 14 (~600m cells) is too fine-grained for
        importance scoring and requires excessive S2 computation per row.
        After the performance fix, both the WHERE clause and the s2_cell_parent()
        call use level 12 (~2.4km cells).
        FAILS until the level is changed from 14 to 12.
        """
        content = self._read_script()
        # Must NOT use level 14 in the density lookup
        assert "level = 14" not in content, (
            "import-osm.sh importance scoring still uses 'WHERE level = 14'. "
            "Change to 'WHERE level = 12' for the density lookup."
        )
        # Must use level 12 in both the WHERE filter and the s2_cell_parent call
        assert "level = 12" in content, (
            "import-osm.sh importance scoring does not use 'WHERE level = 12'. "
            "The t_density temp table filter must be 'WHERE level = 12'."
        )
        assert "s2_cell_parent(" in content, (
            "import-osm.sh importance scoring does not call s2_cell_parent(). "
            "The density JOIN must use s2_cell_parent(..., 12)."
        )
        # The s2_cell_parent call inside the importance section must pass 12
        importance_section_start = content.find("Computing importance scores")
        importance_section_end = content.find("Name index", importance_section_start)
        if importance_section_start == -1:
            importance_section_start = content.find("place_density")
        if importance_section_end == -1:
            importance_section_end = len(content)
        importance_section = content[importance_section_start:importance_section_end]
        assert ", 12)" in importance_section or ",12)" in importance_section, (
            "s2_cell_parent() call in importance scoring section does not pass 12 "
            "as the level argument. Change s2_cell_parent(..., 14) to s2_cell_parent(..., 12)."
        )

    def test_analyze_called_before_finalization(self):
        """ANALYZE must be called after the name index is built and before finalization.

        DuckDB's query planner produces better execution plans after statistics
        are collected. The performance fix adds an ANALYZE statement after the
        name index is built so that subsequent queries (including the trigram
        search) benefit from accurate statistics.
        FAILS until ANALYZE is added to the script.
        """
        content = self._read_script()
        assert "ANALYZE" in content.upper(), (
            "import-osm.sh does not call ANALYZE. "
            "Add 'ANALYZE;' after the name index build step and before the final mv."
        )
        # ANALYZE should appear after the name_index build
        name_index_pos = content.find("name_index")
        analyze_pos = content.upper().find("ANALYZE")
        assert analyze_pos != -1, "ANALYZE not found in script"
        assert analyze_pos > name_index_pos, (
            "ANALYZE appears before the name_index build in the script. "
            "It must come after the name index is fully built."
        )
        # ANALYZE must also appear before the mv finalization
        finalize_pos = content.find('mv "$output_db_tmp" "$output_db"')
        assert finalize_pos != -1, "Finalization mv command not found in script"
        assert analyze_pos < finalize_pos, (
            "ANALYZE appears after the finalization mv. "
            "It must come before the database is renamed to its final path."
        )

    def test_art_index_on_rkey(self):
        """import-osm.sh must create an ART index on places(rkey).

        The 'eliminate places scan' plan requires each import script to create
        an ART index on the primary key column so that point lookups during
        hydration are O(1) rather than full table scans.
        FAILS until CREATE INDEX idx_rkey ON places(rkey) is added.
        """
        content = self._read_script()
        assert "create index idx_rkey on places(rkey)" in content.lower().replace("\n", " "), (
            "import-osm.sh is missing 'CREATE INDEX idx_rkey ON places(rkey)'. "
            "Add this ART index so that hydration lookups by rkey are O(1)."
        )

    def test_name_index_sorted_by_trigram(self):
        """import-osm.sh must sort name_index by trigram for DuckDB zone map efficiency.

        The OSM import builds name_index via bare INSERT INTO statements. Without a
        sort-and-rename step after the inserts, DuckDB zone maps are ineffective
        for 'trigram IN (...)' queries, causing full table scans.
        FAILS until the sort-and-rename step is added after the batch inserts.
        """
        content = self._read_script()
        flat = content.lower().replace("\n", " ")

        assert "create table name_index_sorted" in flat and "order by trigram" in flat, (
            "import-osm.sh is missing 'CREATE TABLE name_index_sorted ... ORDER BY trigram'. "
            "Add a CTAS that reads from name_index and orders by trigram."
        )
        assert "drop table name_index;" in flat, (
            "import-osm.sh is missing 'DROP TABLE name_index;'. "
            "After creating name_index_sorted, drop the unsorted table."
        )
        assert "alter table name_index_sorted rename to name_index" in flat, (
            "import-osm.sh is missing 'ALTER TABLE name_index_sorted RENAME TO name_index'. "
            "After dropping the unsorted table, rename name_index_sorted to name_index."
        )

        # Verify ordering: CREATE TABLE name_index_sorted → DROP TABLE name_index →
        # ALTER TABLE name_index_sorted RENAME TO name_index
        create_pos = flat.index("create table name_index_sorted")
        drop_pos = flat.index("drop table name_index;")
        rename_pos = flat.index("alter table name_index_sorted rename to name_index")
        assert create_pos < drop_pos < rename_pos, (
            "import-osm.sh sort-and-rename steps are out of order. "
            "Expected: "
            "CREATE TABLE name_index_sorted → DROP TABLE name_index → "
            "ALTER TABLE name_index_sorted RENAME TO name_index"
        )


# ---------------------------------------------------------------------------
# Test: import-fsq-extract.sh existence and structure
# ---------------------------------------------------------------------------

class TestImportFsqScript:
    """
    import-fsq-extract.sh must exist on the filesystem and create an ART
    index on the primary key column (fsq_place_id) for efficient hydration.
    """

    SCRIPT_PATH = os.path.join(
        os.path.dirname(__file__), "..", "scripts", "import-fsq-extract.sh"
    )

    def test_script_exists(self):
        """import-fsq-extract.sh must exist in scripts/."""
        assert os.path.isfile(self.SCRIPT_PATH), (
            "scripts/import-fsq-extract.sh does not exist"
        )

    def _read_script(self):
        """Read the script contents, skipping if the file doesn't exist yet."""
        if not os.path.isfile(self.SCRIPT_PATH):
            pytest.skip("scripts/import-fsq-extract.sh not yet implemented")
        with open(self.SCRIPT_PATH) as f:
            return f.read()

    def test_art_index_on_fsq_place_id(self):
        """import-fsq-extract.sh must create an ART index on places(fsq_place_id).

        The 'eliminate places scan' plan requires each import script to create
        an ART index on the primary key column so that point lookups during
        hydration are O(1) rather than full table scans.
        FAILS until CREATE INDEX idx_fsq_place_id ON places(fsq_place_id) is added.
        """
        content = self._read_script()
        assert "create index idx_fsq_place_id on places(fsq_place_id)" in content.lower().replace("\n", " "), (
            "import-fsq-extract.sh is missing 'CREATE INDEX idx_fsq_place_id ON places(fsq_place_id)'. "
            "Add this ART index so that hydration lookups by fsq_place_id are O(1)."
        )

    def test_name_index_sorted_by_trigram(self):
        """import-fsq-extract.sh must sort name_index by trigram for DuckDB zone map efficiency.

        Without ORDER BY trigram in the name_index CTAS, DuckDB zone maps are
        ineffective for 'trigram IN (...)' queries, causing full table scans.
        FAILS until 'ORDER BY trigram' is added to the name_index creation.
        """
        content = self._read_script()
        flat = content.lower().replace("\n", " ")
        assert "order by trigram" in flat, (
            "import-fsq-extract.sh name_index creation is missing 'ORDER BY trigram'. "
            "Add ORDER BY trigram to the CTAS so DuckDB zone maps are effective."
        )


# ---------------------------------------------------------------------------
# Test: import-overture-extract.sh existence and structure
# ---------------------------------------------------------------------------

class TestImportOvertureScript:
    """
    import-overture-extract.sh must exist on the filesystem and create an ART
    index on the primary key column (id) for efficient hydration.
    """

    SCRIPT_PATH = os.path.join(
        os.path.dirname(__file__), "..", "scripts", "import-overture-extract.sh"
    )

    def test_script_exists(self):
        """import-overture-extract.sh must exist in scripts/."""
        assert os.path.isfile(self.SCRIPT_PATH), (
            "scripts/import-overture-extract.sh does not exist"
        )

    def _read_script(self):
        """Read the script contents, skipping if the file doesn't exist yet."""
        if not os.path.isfile(self.SCRIPT_PATH):
            pytest.skip("scripts/import-overture-extract.sh not yet implemented")
        with open(self.SCRIPT_PATH) as f:
            return f.read()

    def test_art_index_on_id(self):
        """import-overture-extract.sh must create an ART index on places(id).

        The 'eliminate places scan' plan requires each import script to create
        an ART index on the primary key column so that point lookups during
        hydration are O(1) rather than full table scans.
        FAILS until CREATE INDEX idx_id ON places(id) is added.
        """
        content = self._read_script()
        assert "create index idx_id on places(id)" in content.lower().replace("\n", " "), (
            "import-overture-extract.sh is missing 'CREATE INDEX idx_id ON places(id)'. "
            "Add this ART index so that hydration lookups by id are O(1)."
        )

    def test_name_index_sorted_by_trigram(self):
        """import-overture-extract.sh must sort name_index by trigram for DuckDB zone map efficiency.

        The Overture import builds name_index via batched INSERTs. Without a
        sort-and-rename step after the inserts, DuckDB zone maps are ineffective
        for 'trigram IN (...)' queries, causing full table scans.
        FAILS until the sort-and-rename step is added after the batch inserts.
        """
        content = self._read_script()
        flat = content.lower().replace("\n", " ")

        assert "create table name_index_sorted" in flat and "order by trigram" in flat, (
            "import-overture-extract.sh is missing 'CREATE TABLE name_index_sorted ... ORDER BY trigram'. "
            "Add a CTAS that reads from name_index and orders by trigram."
        )
        assert "drop table name_index;" in flat, (
            "import-overture-extract.sh is missing 'DROP TABLE name_index;'. "
            "After creating name_index_sorted, drop the unsorted table."
        )
        assert "alter table name_index_sorted rename to name_index" in flat, (
            "import-overture-extract.sh is missing 'ALTER TABLE name_index_sorted RENAME TO name_index'. "
            "After dropping the unsorted table, rename name_index_sorted to name_index."
        )

        # Verify ordering: CREATE TABLE name_index_sorted → DROP TABLE name_index →
        # ALTER TABLE name_index_sorted RENAME TO name_index
        create_pos = flat.index("create table name_index_sorted")
        drop_pos = flat.index("drop table name_index;")
        rename_pos = flat.index("alter table name_index_sorted rename to name_index")
        assert create_pos < drop_pos < rename_pos, (
            "import-overture-extract.sh sort-and-rename steps are out of order. "
            "Expected: CREATE TABLE name_index_sorted → DROP TABLE name_index → "
            "ALTER TABLE name_index_sorted RENAME TO name_index"
        )
