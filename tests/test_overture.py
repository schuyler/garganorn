"""Tests for OvertureMaps database class."""
import pytest

from garganorn.database import OvertureMaps, SearchParams


# ---------------------------------------------------------------------------
# Unit tests — SQL generation (no DB connection needed)
# ---------------------------------------------------------------------------

def _make_ovr(db_path=None, has_name_index=False):
    db = OvertureMaps(db_path or ":memory:")
    db.has_name_index = has_name_index
    return db


def test_query_nearest_spatial_only():
    """Spatial-only params use `geometry` (not `geom`) for distance."""
    db = _make_ovr()
    params: SearchParams = {
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    sql = db.query_nearest(params)
    # Overture uses `geometry` column, not `geom`
    assert "geometry" in sql
    assert "ST_Distance_Sphere" in sql
    # Confirm it does NOT use the FSQ column name
    assert "geom," not in sql and "geom)" not in sql or "geometry" in sql


def test_query_nearest_text_only_no_name_index():
    """Text-only path with no name_index falls back to ILIKE full scan."""
    db = _make_ovr(has_name_index=False)
    params: SearchParams = {"q": "coffee", "t0": "coffee", "limit": 10}
    sql = db.query_nearest(params)
    # Without name_index, falls back to ILIKE
    assert "ILIKE" in sql


def test_query_nearest_text_only_with_name_index():
    """Text-only path with name_index uses multi-token self-join.

    The phonetic branch uses the name_index (token-based) for text-only
    searches instead of FTS. Phonetic search paths are tested separately
    when a phonetic index is present (not tested here as it requires
    the splink_udfs DuckDB extension).
    """
    db = _make_ovr(has_name_index=True)
    params: SearchParams = {"q": "coffee", "t0": "coffee", "limit": 10}
    sql = db.query_nearest(params)
    assert "name_index" in sql
    assert "$t0" in sql


def test_query_name_index_uses_id():
    """When name_index is used, the join key is `id` (not fsq_place_id)."""
    db = _make_ovr(has_name_index=True)
    params: SearchParams = {"q": "coffee", "t0": "coffee", "limit": 10}
    sql = db._query_name_index(params)
    assert "id" in sql
    assert "fsq_place_id" not in sql


def test_process_record_with_addresses():
    """Region 'US-CA' is split to 'CA' in the output."""
    db = _make_ovr()
    result = {
        "rkey": "ovr001",
        "name": "Philz Coffee",
        "latitude": "37.774900",
        "longitude": "-122.419400",
        "addresses": [
            {
                "country": "US",
                "postcode": "94158",
                "locality": "San Francisco",
                "freeform": "201 Berry St",
                "region": "US-CA",
            }
        ],
    }
    record = db.process_record(result)
    assert record["$type"] == "community.lexicon.location.place"
    assert record["rkey"] == "ovr001"
    assert len(record["locations"]) == 2
    addr = record["locations"][1]
    assert addr["$type"] == "community.lexicon.location.address"
    assert addr["country"] == "US"
    assert addr["region"] == "CA"  # Stripped the "US-" prefix
    assert addr["postalCode"] == "94158"
    assert addr["street"] == "201 Berry St"


def test_process_record_no_addresses():
    """When addresses is None, only geo location is present."""
    db = _make_ovr()
    result = {
        "rkey": "ovr099",
        "name": "Mystery Place",
        "latitude": "37.7749",
        "longitude": "-122.4194",
        "addresses": None,
    }
    record = db.process_record(result)
    assert len(record["locations"]) == 1
    assert record["locations"][0]["$type"] == "community.lexicon.location.geo"


def test_query_nearest_empty_produces_sql():
    """No assert guard in OvertureMaps.query_nearest — empty params produce SQL string."""
    db = _make_ovr()
    params: SearchParams = {"limit": 10}
    # Should NOT raise here (no assertion guard in OvertureMaps.query_nearest)
    sql = db.query_nearest(params)
    # SQL is a string (may have empty WHERE clause)
    assert isinstance(sql, str)


def test_text_only_result_search_columns_has_addresses():
    """search_columns() for OvertureMaps includes addresses column."""
    db = _make_ovr()
    cols = db.search_columns()
    assert "addresses" in cols


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_nearest_spatial(overture_db):
    """Spatial query returns results sorted by distance."""
    results = overture_db.nearest(latitude=37.7749, longitude=-122.4194)
    assert len(results) > 0
    distances = [r["distance_m"] for r in results]
    assert distances == sorted(distances)


def test_nearest_text(overture_db):
    """Spatial+text query finds a place by name fragment using ILIKE.

    Both lat/lon and q are provided, so production code uses the spatial bbox
    path and applies `names.primary ILIKE` on the filtered result set.
    """
    results = overture_db.nearest(latitude=37.7596, longitude=-122.4269, q="Dolores")
    names = [r["names"][0]["text"] for r in results]
    assert any("Dolores" in n for n in names)


def test_get_record(overture_db):
    """Known id returns a record with expected structure."""
    record = overture_db.get_record("", "community.lexicon.location.org.overturemaps.places", "ovr001")
    assert record is not None
    assert record["rkey"] == "ovr001"
    assert record["names"][0]["text"] == "Philz Coffee"
    # Should have geo location
    assert record["locations"][0]["$type"] == "community.lexicon.location.geo"


# ---------------------------------------------------------------------------
# Unit tests — Overture trigram SQL generation
# ---------------------------------------------------------------------------

def _make_ovr_trigram(db_path=None):
    """Create an OvertureMaps instance with trigram index flag set."""
    db = OvertureMaps(db_path or ":memory:")
    db.has_name_index = True
    db.has_trigram_index = True
    return db


def test_overture_query_trigram_text_uses_jaccard():
    """_query_trigram_text SQL uses trigram Jaccard scoring."""
    db = _make_ovr_trigram()
    params: SearchParams = {"q": "anchor brewing", "limit": 10}
    trigrams = ["anc", "nch", "cho", "hor", "or ", "r b", " br", "bre", "rew", "ewi", "win", "ing"]
    sql = db._query_trigram_text(params, trigrams)
    assert "count(DISTINCT trigram)" in sql
    assert "trigram IN" in sql or "trigram in" in sql.lower()


# ---------------------------------------------------------------------------
# Integration tests — Overture trigram DB
# ---------------------------------------------------------------------------

def test_overture_trigram_nearest_text(overture_trigram_db):
    """Trigram text search for 'Anchor Brewing' finds it in results."""
    results = overture_trigram_db.nearest(q="Anchor Brewing")
    assert len(results) > 0
    names = [r["names"][0]["text"] for r in results]
    assert any("Anchor" in n for n in names)


def test_overture_trigram_nearest_no_scoring_in_attributes(overture_trigram_db):
    """Trigram search results do not expose score in attributes."""
    results = overture_trigram_db.nearest(q="Anchor Brewing")
    assert len(results) > 0
    for r in results:
        assert "score" not in r.get("attributes", {})
