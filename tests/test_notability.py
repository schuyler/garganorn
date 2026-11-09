"""Importance with a notability modifier (design.md, test plan).

Expected integers come from the formula below, evaluated here with the
design's constants; production constants are not imported.

    x   = clamp((page_rank - 0.20) / 0.80, 0, 1)
    f   = ln(1 + 50x) / ln(51)
    g   = 1 - clamp(ln(nc / FLOOR) / ln(CEILING / FLOOR), 0, 1)
    b   = 0.8 * base
    imp = round(b + (100 - b) * 0.95 * f * g)
"""
import math

import duckdb

from garganorn.stages import stage_division_import, stage_import
from tests.page_rank_file import write_page_rank
from tests.quadtree_helpers import write_minimal_overture_parquet

BBOX = (-180, -90, 180, 90)
FLOOR, CEILING = 1000, 10000


def _round(v):
    return math.floor(v + 0.5)


def expected(base, page_rank=0.0, nc=1, floor=FLOOR, ceiling=CEILING):
    x = min(max((page_rank - 0.20) / 0.80, 0), 1)
    f = math.log(1 + 50 * x) / math.log(51)
    g = 1 - min(max(math.log(nc / floor) / math.log(ceiling / floor), 0), 1)
    b = 0.8 * base
    return _round(b + (100 - b) * 0.95 * f * g)


def _small_name_counts(monkeypatch, floor, ceiling):
    monkeypatch.setattr("garganorn.stages.NAME_COUNT_FLOOR", floor, raising=False)
    monkeypatch.setattr("garganorn.stages.NAME_COUNT_CEILING", ceiling, raising=False)


# ---------------------------------------------------------------------------
# OSM
# ---------------------------------------------------------------------------

def _quadkey15(lon, lat):
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        return con.execute("SELECT left(ST_QuadKey(?, ?, 17), 15)", [lon, lat]).fetchone()[0]
    finally:
        con.close()


def _osm_parquets(tmp_path, nodes):
    """nodes: (id, name, lon, lat, extra tags). Every node is an amenity=cafe."""
    node, way, rel = (str(tmp_path / f"{k}.parquet") for k in ("node", "way", "rel"))
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE n (id BIGINT, tags MAP(VARCHAR, VARCHAR), lat DOUBLE, lon DOUBLE)")
        for id, name, lon, lat, extra in nodes:
            tags = {"name": name, "amenity": "cafe", **extra}
            con.execute("INSERT INTO n VALUES (?, ?, ?, ?)", [id, tags, lat, lon])
        con.execute("CREATE TABLE w (id BIGINT, tags MAP(VARCHAR, VARCHAR), nds STRUCT(ref BIGINT)[])")
        con.execute("CREATE TABLE r (id BIGINT, tags MAP(VARCHAR, VARCHAR), "
                    "members STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[])")
        for table, path in (("n", node), ("w", way), ("r", rel)):
            con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()
    return node, way, rel


def _osm_importance(tmp_path, nodes, page_rank, idf=9.0, density=()):
    """Import nodes (all category amenity=cafe, idf_score `idf`, so base =
    round(40 * idf / 18) with the default idf_norm) and return {name: importance}.
    density: (lon, lat, score) rows, each a density tile at that point."""
    parquets = _osm_parquets(tmp_path, nodes)
    idf_path = str(tmp_path / "idf.parquet")
    dens_path = str(tmp_path / "density.parquet")
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE i (category VARCHAR, idf_score DOUBLE)")
        con.execute("INSERT INTO i VALUES ('amenity=cafe', ?)", [idf])
        con.execute(f"COPY i TO '{idf_path}' (FORMAT PARQUET)")
        con.execute("CREATE TABLE d (tile_qk15 VARCHAR, density_score DOUBLE, tile_xmin DOUBLE, "
                    "tile_ymin DOUBLE, tile_xmax DOUBLE, tile_ymax DOUBLE)")
        for lon, lat, score in density:
            con.execute("INSERT INTO d VALUES (?, ?, 0, 0, 0, 0)", [_quadkey15(lon, lat), score])
        con.execute(f"COPY d TO '{dens_path}' (FORMAT PARQUET)")
    finally:
        con.close()
    output = str(tmp_path / "places.parquet")
    stage_import("osm", parquets, BBOX, output, memory_limit="4GB",
                 density_parquet=dens_path, idf_parquet=idf_path,
                 page_rank_parquet=write_page_rank(tmp_path / "pr.parquet", page_rank),
                 force=True)
    con = duckdb.connect()
    try:
        return dict(con.execute(
            f"SELECT name, importance FROM read_parquet('{output}')").fetchall())
    finally:
        con.close()


def test_osm_record_with_page_rank_gets_the_formula_value(tmp_path):
    """N1"""
    nodes = [(1, "Notable Cafe", -122.40, 37.70, {"wikidata": "Q1"})]
    got = _osm_importance(tmp_path, nodes, {"Q1": 0.6})
    assert got == {"Notable Cafe": expected(20, 0.6)}
    assert got["Notable Cafe"] == 82


def test_osm_record_without_a_qid_or_page_rank_scales_the_base(tmp_path):
    """N1: no QID, and a QID absent from the page-rank file, get round(0.8 * base)."""
    nodes = [(1, "No Qid Cafe", -122.40, 37.70, {}),
             (2, "Unranked Cafe", -122.30, 37.70, {"wikidata": "Q404"})]
    got = _osm_importance(tmp_path, nodes, {"Q1": 0.6})
    assert got == {"No Qid Cafe": 16, "Unranked Cafe": 16}


def test_weakly_notable_record_on_a_modest_base_ranks_below_a_busy_non_notable_one(tmp_path):
    """N2: pr 0.25 on base 10 (idf only) vs base 100 (idf + density 10, norm 10)."""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    modest = _osm_importance(
        tmp_path / "a", [(1, "Weak Cafe", -122.40, 37.70, {"wikidata": "Q1"})],
        {"Q1": 0.25}, idf=4.5)
    busy = _osm_importance(
        tmp_path / "b", [(2, "Busy Cafe", -122.30, 37.70, {})], {}, idf=18.0,
        density=[(-122.30, 37.70, 10.0)])
    assert modest == {"Weak Cafe": expected(10, 0.25)}
    assert busy == {"Busy Cafe": expected(100)}
    assert modest["Weak Cafe"] < busy["Busy Cafe"]


# ---------------------------------------------------------------------------
# Damping
# ---------------------------------------------------------------------------

def test_name_shared_at_the_ceiling_gets_no_lift_for_any_name_on_the_qid(tmp_path, monkeypatch):
    """N3: 8 records named alike carry Q1, one localized name carries Q1 too."""
    _small_name_counts(monkeypatch, 2, 8)
    nodes = [(i, "Chain Cafe", -122.40 + i * 0.01, 37.70, {"wikidata": "Q1"})
             for i in range(1, 9)]
    nodes.append((9, "Cafe en Cadena", -122.20, 37.70, {"wikidata": "Q1"}))
    got = _osm_importance(tmp_path, nodes, {"Q1": 0.6})
    assert got == {"Chain Cafe": 16, "Cafe en Cadena": 16}


def test_name_count_between_floor_and_ceiling_gets_the_intermediate_damping(tmp_path, monkeypatch):
    """N3 ramp: floor 2, ceiling 8, nc 4 gives g = 0.5."""
    _small_name_counts(monkeypatch, 2, 8)
    nodes = [(i, "Shared Cafe", -122.40 + i * 0.01, 37.70, {"wikidata": "Q1"})
             for i in range(1, 5)]
    got = _osm_importance(tmp_path, nodes, {"Q1": 0.6})
    assert got == {"Shared Cafe": expected(20, 0.6, nc=4, floor=2, ceiling=8)}


def test_names_count_together_after_lowercasing_trimming_and_stripping_accents(tmp_path, monkeypatch):
    """N3 via D3: "Chain Cafe" and " chain café" are one name for counting."""
    _small_name_counts(monkeypatch, 2, 8)
    nodes = [(i, "Chain Cafe", -122.40 + i * 0.01, 37.70, {"wikidata": "Q1"})
             for i in range(1, 5)]
    nodes += [(i, " chain café", -122.40 + i * 0.01, 37.80, {"wikidata": "Q1"})
              for i in range(5, 9)]
    got = _osm_importance(tmp_path, nodes, {"Q1": 0.6})
    assert got == {"Chain Cafe": 16, " chain café": 16}


# ---------------------------------------------------------------------------
# Divisions
# ---------------------------------------------------------------------------

def _division_importance(tmp_path, rows, page_rank):
    """rows: (id, subtype, population, wikidata, west, south, east, north).
    Returns {id: importance} from stage_division_import."""
    division, area = str(tmp_path / "division.parquet"), str(tmp_path / "area.parquet")
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE d (id VARCHAR,
                names STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR),
                             rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]),
                subtype VARCHAR, country VARCHAR, region VARCHAR, wikidata VARCHAR,
                population BIGINT, parent_division_id VARCHAR,
                hierarchies STRUCT(division_id VARCHAR, subtype VARCHAR, name VARCHAR)[][])
        """)
        con.execute("""
            CREATE TABLE a (division_id VARCHAR, admin_level INTEGER, is_land BOOLEAN,
                geometry VARCHAR,
                bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE))
        """)
        for id, subtype, pop, qid, w, s, e, n in rows:
            con.execute("""
                INSERT INTO d VALUES (?, {'primary': ?, 'common': map([]::VARCHAR[], []::VARCHAR[]),
                    'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
                    ?, 'US', NULL, ?, ?, NULL,
                    [[{'division_id': ?, 'subtype': ?, 'name': ?}]]
                        ::STRUCT(division_id VARCHAR, subtype VARCHAR, name VARCHAR)[][])
            """, [id, id, subtype, qid, pop, id, subtype, id])
            con.execute(
                "INSERT INTO a VALUES (?, 1, true, ?, {'xmin': ?, 'ymin': ?, 'xmax': ?, 'ymax': ?})",
                [id, f"POLYGON(({w} {s}, {w} {n}, {e} {n}, {e} {s}, {w} {s}))", w, s, e, n])
        con.execute(f"COPY d TO '{division}' (FORMAT PARQUET)")
        con.execute(f"COPY a TO '{area}' (FORMAT PARQUET)")
    finally:
        con.close()

    output = str(tmp_path / "places.parquet")
    stage_division_import((division, area), None, output, memory_limit="4GB", force=True,
                          page_rank_parquet=write_page_rank(tmp_path / "pr.parquet", page_rank))
    con = duckdb.connect()
    try:
        return dict(con.execute(
            f"SELECT id, importance FROM read_parquet('{output}')").fetchall())
    finally:
        con.close()


def _division_base(population, west, south, east, north):
    mid = math.radians((south + north) / 2)
    area = ((east - west) * 111.32 * math.cos(mid)) * ((north - south) * 110.57)
    return _round(100 * max(min(math.log(1 + population) / 20, 1),
                            min(math.log(1 + area) / 16, 1)))


def test_divisions_of_different_subtypes_with_equal_inputs_score_the_same(tmp_path):
    """N6"""
    rows = [("r1", "region", 1_000_000, "Q10", -120.0, 35.0, -119.0, 36.0),
            ("l1", "locality", 1_000_000, "Q11", -110.0, 35.0, -109.0, 36.0)]
    got = _division_importance(tmp_path, rows, {"Q10": 0.6, "Q11": 0.6})
    want = expected(_division_base(1_000_000, -120.0, 35.0, -119.0, 36.0), 0.6)
    assert got == {"r1": want, "l1": want}


def test_division_without_population_scores_from_its_area(tmp_path):
    """N6"""
    rows = [("big", "region", None, None, -5.0, -5.0, 5.0, 5.0)]
    got = _division_importance(tmp_path, rows, {})
    base = _division_base(0, -5.0, -5.0, 5.0, 5.0)
    assert base > 0
    assert got == {"big": _round(0.8 * base)}


def test_division_without_page_rank_scores_at_most_80(tmp_path):
    """N6: a saturating base (population 1.4 billion) without page rank stays at 80."""
    rows = [("huge", "country", 1_400_000_000, "Q9", -10.0, -10.0, 10.0, 10.0)]
    got = _division_importance(tmp_path, rows, {})
    assert got == {"huge": 80}


# ---------------------------------------------------------------------------
# Overture places
# ---------------------------------------------------------------------------

def test_overture_place_importance_is_the_old_value_scaled_by_0_8(tmp_path):
    """N5: density 5/10 and idf 9/18 give old importance 50; now round(0.8 * 50) = 40."""
    lon, lat = -122.419, 37.775
    source = tmp_path / "ov.parquet"
    write_minimal_overture_parquet(source, [("impA", lon, lat, "coffee_shop")])
    density, idf = str(tmp_path / "density.parquet"), str(tmp_path / "idf.parquet")
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE d (tile_qk15 VARCHAR, density_score DOUBLE, tile_xmin DOUBLE, "
                    "tile_ymin DOUBLE, tile_xmax DOUBLE, tile_ymax DOUBLE)")
        con.execute("INSERT INTO d VALUES (?, 5.0, -122.5, 37.7, -122.4, 37.8)",
                    [_quadkey15(lon, lat)])
        con.execute("CREATE TABLE i (category VARCHAR, idf_score DOUBLE)")
        con.execute("INSERT INTO i VALUES ('coffee_shop', 9.0)")
        con.execute(f"COPY d TO '{density}' (FORMAT PARQUET)")
        con.execute(f"COPY i TO '{idf}' (FORMAT PARQUET)")
    finally:
        con.close()
    output = str(tmp_path / "places.parquet")
    stage_import("overture_place", str(source), (-123, 37, -122, 38), output,
                 memory_limit="4GB", density_parquet=density, idf_parquet=idf,
                 density_norm=10.0, idf_norm=18.0, force=True,
                 page_rank_parquet=write_page_rank(tmp_path / "pr.parquet"))
    con = duckdb.connect()
    try:
        assert con.execute(
            f"SELECT importance FROM read_parquet('{output}') WHERE id = 'impA'"
        ).fetchone()[0] == 40
    finally:
        con.close()
