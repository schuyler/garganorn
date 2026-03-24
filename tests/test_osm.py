"""Tests for OpenStreetMap database class."""
import pytest

from garganorn.database import OpenStreetMap, SearchParams


# ---------------------------------------------------------------------------
# Unit tests — SQL generation (no DB connection needed)
# ---------------------------------------------------------------------------

def _make_osm(db_path=None):
    """Create an OpenStreetMap instance."""
    db = OpenStreetMap(db_path or ":memory:")
    return db


def test_query_nearest_spatial_only():
    """Spatial-only params produce SQL with ST_Distance_Sphere and bbox filter."""
    db = _make_osm()
    params: SearchParams = {
        "centroid": "POINT(-122.4195 37.7612)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    sql = db.query_nearest(params)
    assert "ST_Distance_Sphere" in sql
    assert "bbox" in sql
    assert "$xmin" in sql
    # OSM uses 'geom' column (like FSQ), not 'geometry' (like Overture)
    assert ".geom" in sql or "geom," in sql or "geom)" in sql


def test_query_nearest_requires_centroid_or_q():
    """Neither centroid nor q raises AssertionError."""
    db = _make_osm()
    params: SearchParams = {"limit": 10}
    with pytest.raises(AssertionError):
        db.query_nearest(params)


def test_query_trigram_text_uses_jaccard():
    """_query_trigram_text SQL uses trigram Jaccard scoring with rkey join."""
    db = _make_osm()
    params: SearchParams = {"q": "tartine", "limit": 10}
    trigrams = ["tar", "art", "rti", "tin", "ine"]
    sql = db._query_trigram_text(params, trigrams)
    assert "count(DISTINCT trigram)" in sql
    assert "trigram IN" in sql or "trigram in" in sql.lower()
    assert "AS score" in sql or "as score" in sql.lower()
    assert "rkey" in sql


def test_query_trigram_spatial_uses_jaccard():
    """_query_trigram_spatial SQL uses trigram Jaccard, ST_Distance_Sphere."""
    db = _make_osm()
    params: SearchParams = {
        "q": "tartine",
        "centroid": "POINT(-122.4195 37.7612)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    trigrams = ["tar", "art", "rti", "tin", "ine"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "count(DISTINCT" in sql
    assert "trigram IN" in sql or "trigram in" in sql.lower()
    assert "ST_Distance_Sphere" in sql


def test_query_record_parses_rkey():
    """query_record SQL parses osm_type from first char and osm_id from remainder."""
    db = _make_osm()
    sql = db.query_record()
    assert "left($rkey, 1)" in sql
    assert "substr($rkey, 2)" in sql


def test_process_record_full_address():
    """process_record with addr:* tags builds address location."""
    db = _make_osm()
    result = {
        "rkey": "n240109189",
        "name": "Tartine Manufactory",
        "latitude": "37.761200",
        "longitude": "-122.419500",
        "primary_category": "amenity=cafe",
        "tags": {"cuisine": "coffee", "addr:street": "Alabama St",
                 "addr:housenumber": "595", "addr:city": "San Francisco",
                 "addr:postcode": "94110", "addr:country": "US"},
    }
    record = db.process_record(result)
    assert record["$type"] == "org.atgeo.place"
    assert record["rkey"] == "n240109189"
    assert record["names"][0]["text"] == "Tartine Manufactory"
    # Geo location present
    assert record["locations"][0]["$type"] == "community.lexicon.location.geo"
    # Address location created because addr:country is present
    assert len(record["locations"]) == 2
    assert record["locations"][1]["$type"] == "community.lexicon.location.address"
    assert record["locations"][1]["country"] == "US"
    assert record["locations"][1]["postalCode"] == "94110"
    addr = record["locations"][1]
    assert addr["locality"] == "San Francisco"
    assert addr["street"] == "595 Alabama St"


def test_process_record_housenumber_prepend():
    """addr:housenumber is prepended to addr:street in the address."""
    db = _make_osm()
    result = {
        "rkey": "n240109189",
        "name": "Tartine Manufactory",
        "latitude": "37.761200",
        "longitude": "-122.419500",
        "primary_category": "amenity=cafe",
        "tags": {"addr:street": "Alabama St", "addr:housenumber": "595", "addr:country": "US"},
    }
    record = db.process_record(result)
    assert len(record["locations"]) == 2
    addr = record["locations"][1]
    assert addr["street"] == "595 Alabama St"


def test_process_record_no_country():
    """Without addr:country, no address location appended."""
    db = _make_osm()
    result = {
        "rkey": "w50637691",
        "name": "Dolores Park",
        "latitude": "37.759600",
        "longitude": "-122.426900",
        "primary_category": "leisure=park",
        "tags": {},
    }
    record = db.process_record(result)
    assert len(record["locations"]) == 1
    assert record["locations"][0]["$type"] == "community.lexicon.location.geo"


def test_process_record_no_address_tags():
    """Tags without addr:* produce only geo location, primary_category parsed into attributes."""
    db = _make_osm()
    result = {
        "rkey": "w88776655",
        "name": "Caltrain Station",
        "latitude": "37.776400",
        "longitude": "-122.394200",
        "primary_category": "railway=station",
        "tags": {},
    }
    record = db.process_record(result)
    assert len(record["locations"]) == 1
    assert record["locations"][0]["$type"] == "community.lexicon.location.geo"
    # With MAP schema, empty tags + primary_category parsed → {"railway": "station"}
    assert record["attributes"] == {"railway": "station"}


def test_process_record_primary_category_in_attributes():
    """The primary_category field is parsed and appears in attributes."""
    db = _make_osm()
    result = {
        "rkey": "n240109189",
        "name": "Tartine Manufactory",
        "latitude": "37.761200",
        "longitude": "-122.419500",
        "primary_category": "amenity=cafe",
        "tags": {"cuisine": "coffee", "addr:country": "US"},
    }
    record = db.process_record(result)
    attrs = record.get("attributes", {})
    assert attrs.get("amenity") == "cafe"
    assert attrs.get("cuisine") == "coffee"


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_nearest_spatial(osm_db):
    """Spatial query returns results sorted by distance."""
    results = osm_db.nearest(latitude=37.7612, longitude=-122.4195)
    assert len(results) > 0
    distances = [r["distance_m"] for r in results]
    assert distances == sorted(distances)


def test_nearest_text(osm_db):
    """Text query finds place by trigram match."""
    results = osm_db.nearest(q="tartine")
    names = [r["names"][0]["text"] for r in results]
    assert any("Tartine" in n for n in names)


def test_nearest_spatial_text(osm_db):
    """Spatial + text query returns results with distance."""
    results = osm_db.nearest(latitude=37.7612, longitude=-122.4195, q="tartine")
    assert len(results) > 0
    assert all(r["distance_m"] >= 0 for r in results)
    names = [r["names"][0]["text"] for r in results]
    assert any("Tartine" in n for n in names)


def test_get_record_found(osm_db):
    """Known rkey like 'n240109189' returns a record."""
    record = osm_db.get_record("", "org.atgeo.places.osm", "n240109189")
    assert record is not None
    assert record["rkey"] == "n240109189"
    assert record["names"][0]["text"] == "Tartine Manufactory"


def test_get_record_not_found(osm_db):
    """Unknown rkey returns None."""
    record = osm_db.get_record("", "org.atgeo.places.osm", "n9999999")
    assert record is None


def test_trigram_nearest_text_exact(osm_db):
    """Text search for 'Tartine Manufactory' returns it first."""
    results = osm_db.nearest(q="Tartine Manufactory")
    assert len(results) > 0
    names = [r["names"][0]["text"] for r in results]
    assert names[0] == "Tartine Manufactory"


def test_trigram_nearest_unrelated(osm_db):
    """Completely unrelated query returns 0 results."""
    results = osm_db.nearest(q="xyzqwerty")
    assert len(results) == 0


def test_trigram_no_scoring_in_attributes(osm_db):
    """Search results don't expose score in attributes."""
    results = osm_db.nearest(q="Tartine Manufactory")
    assert len(results) > 0
    for r in results:
        assert "score" not in r.get("attributes", {})
        assert "jaccard" not in r.get("attributes", {})
