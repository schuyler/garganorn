"""Tests for FoursquareOSP database class."""
import pytest

from garganorn.database import FoursquareOSP, SearchParams


# ---------------------------------------------------------------------------
# Unit tests — SQL generation (no DB connection needed)
# ---------------------------------------------------------------------------

def _make_fsq(db_path=None, has_name_index=True):
    """Create a FoursquareOSP instance with optional index flags."""
    db = FoursquareOSP(db_path or ":memory:")
    db.has_name_index = has_name_index
    return db


def test_query_nearest_spatial_only():
    """Spatial-only params produce SQL with ST_Distance_Sphere and bbox filter."""
    db = _make_fsq()
    params: SearchParams = {
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    sql = db.query_nearest(params)
    assert "ST_Distance_Sphere" in sql
    assert "bbox" in sql
    assert "$xmin" in sql


def test_query_nearest_text_only_no_name_index():
    """Text-only path with no name_index falls back to ILIKE."""
    db = _make_fsq(has_name_index=False)
    params: SearchParams = {"q": "coffee", "t0": "coffee", "limit": 10}
    sql = db.query_nearest(params)
    assert "ILIKE" in sql


def test_query_nearest_text_only_with_name_index():
    """Text-only path with name_index uses multi-token self-join."""
    db = _make_fsq(has_name_index=True)
    params: SearchParams = {"q": "coffee", "t0": "coffee", "limit": 10}
    sql = db.query_nearest(params)
    assert "name_index" in sql
    assert "$t0" in sql


def test_query_nearest_spatial_plus_text():
    """Spatial + text params produce SQL with bbox filter and ILIKE."""
    db = _make_fsq()
    params: SearchParams = {
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "q": "coffee",
        "limit": 10,
    }
    sql = db.query_nearest(params)
    assert "bbox" in sql
    assert "ILIKE" in sql


def test_query_nearest_requires_centroid_or_q():
    """Neither centroid nor q raises AssertionError."""
    db = _make_fsq()
    params: SearchParams = {"limit": 10}
    with pytest.raises(AssertionError):
        db.query_nearest(params)


def test_query_name_index_single_token():
    """Text-only query with name_index produces SQL with $t0 token param."""
    db = _make_fsq(has_name_index=True)
    params: SearchParams = {"q": "coffee", "t0": "coffee", "limit": 10}
    sql = db.query_nearest(params)
    assert "name_index" in sql
    assert "$t0" in sql


def test_query_name_index_multi_token():
    """Text-only multi-word query: multi-token self-join uses $t0, $t1, etc."""
    db = _make_fsq(has_name_index=True)
    # nearest() binds t0, t1, ... for each token
    params: SearchParams = {"q": "blue bottle coffee", "t0": "blue", "t1": "bottle", "t2": "coffee", "limit": 10}
    sql = db._query_name_index(params)
    assert "name_index" in sql
    assert "$t0" in sql
    assert "$t1" in sql
    assert "$t2" in sql


def test_process_record_full():
    """process_record with full address fields builds correct structure."""
    db = _make_fsq()
    result = {
        "rkey": "fsq001",
        "name": "Blue Bottle Coffee",
        "latitude": "37.774900",
        "longitude": "-122.419400",
        "address": "66 Mint St",
        "locality": "San Francisco",
        "postcode": "94103",
        "region": "CA",
        "country": "US",
        "fsq_place_id": "fsq001",
    }
    record = db.process_record(result)
    assert record["$type"] == "community.lexicon.location.place"
    assert record["rkey"] == "fsq001"
    assert record["names"][0]["text"] == "Blue Bottle Coffee"
    # Geo location
    assert record["locations"][0]["$type"] == "community.lexicon.location.geo"
    # Address location (country required)
    assert len(record["locations"]) == 2
    assert record["locations"][1]["$type"] == "community.lexicon.location.address"
    assert record["locations"][1]["country"] == "US"
    assert record["locations"][1]["postalCode"] == "94103"


def test_process_record_no_country():
    """process_record without country key: no address location appended."""
    db = _make_fsq()
    result = {
        "rkey": "fsq005",
        "name": "Alcatraz Island",
        "latitude": "37.827000",
        "longitude": "-122.423000",
        "address": None,
        "locality": "San Francisco",
        "postcode": "94133",
        "region": "CA",
        "country": None,
    }
    record = db.process_record(result)
    # No address because country is None/falsy
    assert len(record["locations"]) == 1
    assert record["locations"][0]["$type"] == "community.lexicon.location.geo"


def test_process_record_partial_address():
    """process_record with country but missing postcode: address has country only."""
    db = _make_fsq()
    result = {
        "rkey": "fsq099",
        "name": "Mystery Place",
        "latitude": "37.7749",
        "longitude": "-122.4194",
        "address": None,
        "locality": None,
        "postcode": None,
        "region": None,
        "country": "US",
    }
    record = db.process_record(result)
    assert len(record["locations"]) == 2
    addr = record["locations"][1]
    assert addr["country"] == "US"
    assert "postalCode" not in addr


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_nearest_spatial(fsq_db):
    """Spatial query returns results sorted by distance."""
    results = fsq_db.nearest(latitude=37.7749, longitude=-122.4194)
    assert len(results) > 0
    # Verify distance_m present and sorted ascending
    distances = [r["distance_m"] for r in results]
    assert distances == sorted(distances)


def test_nearest_text_name_index(fsq_db):
    """DB with name_index, text query finds place by token."""
    results = fsq_db.nearest(q="tartine")
    names = [r["names"][0]["text"] for r in results]
    assert any("Tartine" in n for n in names)


def test_nearest_text_no_index_ilike(fsq_db_no_index):
    """Text-only query without name_index falls back to ILIKE full scan."""
    results = fsq_db_no_index.nearest(q="Blue Bottle")
    names = [r["names"][0]["text"] for r in results]
    assert any("Blue" in n for n in names)


def test_get_record_found(fsq_db):
    """Known rkey returns a record."""
    record = fsq_db.get_record("", "community.lexicon.location.com.foursquare.places", "fsq001")
    assert record is not None
    assert record["rkey"] == "fsq001"
    assert record["names"][0]["text"] == "Blue Bottle Coffee"


def test_get_record_not_found(fsq_db):
    """Unknown rkey returns None."""
    record = fsq_db.get_record("", "community.lexicon.location.com.foursquare.places", "nonexistent")
    assert record is None


# ---------------------------------------------------------------------------
# Unit tests — trigram SQL generation (no DB connection needed)
# ---------------------------------------------------------------------------

def _make_fsq_trigram(db_path=None):
    """Create a FoursquareOSP instance with trigram index flag set."""
    db = FoursquareOSP(db_path or ":memory:")
    db.has_name_index = True
    db.has_trigram_index = True
    return db


def test_query_trigram_text_uses_jaccard():
    """_query_trigram_text SQL uses trigram Jaccard scoring."""
    db = _make_fsq_trigram()
    params: SearchParams = {"q": "coffee", "limit": 10}
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_text(params, trigrams)
    assert "count(DISTINCT trigram)" in sql
    assert "trigram IN" in sql or "trigram in" in sql.lower()
    assert "AS score" in sql or "as score" in sql.lower()


def test_query_trigram_text_no_limit_5000():
    """_query_trigram_text SQL does not use an intermediate candidate LIMIT."""
    db = _make_fsq_trigram()
    params: SearchParams = {"q": "coffee", "limit": 10}
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_text(params, trigrams)
    assert "5000" not in sql


def test_query_trigram_spatial_uses_jaccard():
    """_query_trigram_spatial SQL uses trigram Jaccard, trigram IN, ST_Distance_Sphere."""
    db = _make_fsq_trigram()
    params: SearchParams = {
        "q": "coffee",
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "count(DISTINCT" in sql
    assert "trigram IN" in sql or "trigram in" in sql.lower()
    assert "ST_Distance_Sphere" in sql


# ---------------------------------------------------------------------------
# Integration tests — FSQ trigram DB
# ---------------------------------------------------------------------------

def test_trigram_nearest_text_exact_match(fsq_trigram_db):
    """Text-only trigram search for 'Tartine Bakery' returns it as first result."""
    results = fsq_trigram_db.nearest(q="Tartine Bakery")
    assert len(results) > 0
    names = [r["names"][0]["text"] for r in results]
    assert names[0] == "Tartine Bakery"


def test_trigram_nearest_text_no_scoring_in_attributes(fsq_trigram_db):
    """Trigram search results do not expose score or jaccard in attributes."""
    results = fsq_trigram_db.nearest(q="Tartine Bakery")
    assert len(results) > 0
    for r in results:
        assert "score" not in r.get("attributes", {})
        assert "jaccard" not in r.get("attributes", {})


def test_trigram_nearest_spatial_with_text(fsq_trigram_db):
    """Spatial + text trigram search returns results with distance_m."""
    results = fsq_trigram_db.nearest(latitude=37.7749, longitude=-122.4194, q="coffee")
    assert len(results) > 0
    assert all(r["distance_m"] >= 0 for r in results)
    # Verify the result has the expected name (Blue Bottle Coffee has "coffee" trigrams)
    names = [r["names"][0]["text"] for r in results]
    assert any("Coffee" in n for n in names)


def test_trigram_nearest_unrelated_query(fsq_trigram_db):
    """Completely unrelated query returns 0 results."""
    results = fsq_trigram_db.nearest(q="xyzqwerty")
    assert len(results) == 0
