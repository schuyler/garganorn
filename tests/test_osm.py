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


def test_query_trigram_text_uses_jw():
    """_query_trigram_text SQL uses Jaro-Winkler scoring with rkey."""
    db = _make_osm()
    params: SearchParams = {"q": "tartine", "limit": 10}
    trigrams = ["tar", "art", "rti", "tin", "ine"]
    sql = db._query_trigram_text(params, trigrams)
    assert "jaro_winkler_similarity" in sql
    assert "count(DISTINCT trigram)" not in sql
    assert "GROUP BY" not in sql
    assert "with candidates" in sql.lower()
    assert "rkey" in sql


def test_query_trigram_spatial_uses_jw():
    """_query_trigram_spatial SQL uses Jaro-Winkler, ST_Distance_Sphere."""
    db = _make_osm()
    params: SearchParams = {
        "q": "tartine",
        "centroid": "POINT(-122.4195 37.7612)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    trigrams = ["tar", "art", "rti", "tin", "ine"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "jaro_winkler_similarity" in sql
    assert "count(DISTINCT trigram)" not in sql
    assert "GROUP BY" not in sql
    assert "with candidates" in sql.lower()
    assert "ST_Distance_Sphere" in sql
    assert "score >= 0.6" in sql


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
    """Spatial query returns results with distance_m present."""
    results = osm_db.nearest(bbox=(-122.4645, 37.7162, -122.3745, 37.8062))
    assert len(results) > 0
    assert all(r["distance_m"] >= 0 for r in results)


def test_nearest_text(osm_db):
    """Text query finds place by trigram match."""
    results = osm_db.nearest(q="tartine")
    names = [r["names"][0]["text"] for r in results]
    assert any("Tartine" in n for n in names)


def test_nearest_spatial_text(osm_db):
    """Spatial + text query returns results with distance."""
    results = osm_db.nearest(bbox=(-122.4645, 37.7162, -122.3745, 37.8062), q="tartine")
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


# ---------------------------------------------------------------------------
# Token-blending tests (Red phase — these FAIL until token blending is impl.)
# ---------------------------------------------------------------------------

def test_token_blending_text_ranking(osm_db):
    """Token-level JW blending ranks 'Diner North End' above 'North End Pub' for query 'North End Diner'.

    Full-string JW favors 'North End Pub' because it shares the long prefix 'north end'.
    Token-level JW correctly identifies that 'Diner North End' contains all query tokens.
    This test FAILS until token blending is implemented.
    """
    results = osm_db.nearest(q="North End Diner")
    names = [r["names"][0]["text"] for r in results]
    assert "Diner North End" in names, "Diner North End not found in results"
    assert "North End Pub" in names, "North End Pub not found in results"
    diner_idx = names.index("Diner North End")
    pub_idx = names.index("North End Pub")
    assert diner_idx < pub_idx, (
        f"'Diner North End' (pos {diner_idx}) should rank above "
        f"'North End Pub' (pos {pub_idx}) with token-level JW blending"
    )


def test_token_blending_spatial_ranking(osm_db):
    """Spatial + text: token blending ranks 'Diner North End' above 'North End Pub'.

    Both places are co-located within the search bbox; distance does not break the tie.
    Full-string JW favors 'North End Pub'. Token JW correctly favors 'Diner North End'.
    This test FAILS until token blending is implemented.
    """
    results = osm_db.nearest(
        bbox=(-122.4801, 37.7299, -122.3901, 37.8199), q="North End Diner"
    )
    names = [r["names"][0]["text"] for r in results]
    assert "Diner North End" in names, "Diner North End not found in results"
    assert "North End Pub" in names, "North End Pub not found in results"
    diner_idx = names.index("Diner North End")
    pub_idx = names.index("North End Pub")
    assert diner_idx < pub_idx, (
        f"'Diner North End' (pos {diner_idx}) should rank above "
        f"'North End Pub' (pos {pub_idx}) with token-level JW blending"
    )


def test_single_token_finds_existing_place(osm_db):
    """Single-token query 'Caltrain' finds Caltrain Station (regression guard, should PASS)."""
    results = osm_db.nearest(q="Caltrain")
    names = [r["names"][0]["text"] for r in results]
    assert any("Caltrain" in n for n in names)


def test_single_token_no_blending_applied(osm_db):
    """Single-token query returns results without token blending (regression guard, should PASS).

    Single-token queries use full-string JW only per spec. Verify this path
    still works correctly after the blending feature is added.
    """
    results = osm_db.nearest(q="Dolores")
    names = [r["names"][0]["text"] for r in results]
    assert any("Dolores" in n for n in names)


# ---------------------------------------------------------------------------
# norm_name optimization tests (Red phase — FAIL until Optimization 2 impl.)
# ---------------------------------------------------------------------------

def test_osm_query_trigram_text_uses_norm_name():
    """Text path uses norm_name column, not runtime lower(strip_accents(name)).

    FAILS until norm_name optimization is implemented for OpenStreetMap.
    """
    db = _make_osm()
    params: SearchParams = {"q": "tartine", "limit": 10, "importance_floor": 0}
    trigrams = ["tar", "art", "rti", "tin", "ine"]
    sql = db._query_trigram_text(params, trigrams)
    assert "norm_name" in sql, "SQL should reference the norm_name column"
    assert "lower(strip_accents(" not in sql, (
        "SQL should not call lower(strip_accents(...)) at runtime — use norm_name instead"
    )


def test_osm_query_trigram_text_multi_token_uses_norm_name():
    """Multi-token text path name_tokens CTE uses norm_name, not runtime normalization.

    In the multi-token branch, the name_tokens CTE splits candidate names into
    tokens. After Optimization 2, it should split norm_name (pre-computed) rather
    than calling lower(strip_accents(r.name)) at runtime.
    FAILS until norm_name optimization is implemented for OpenStreetMap.
    """
    db = _make_osm()
    params: SearchParams = {
        "q": "north end diner",
        "limit": 10,
        "importance_floor": 0,
        "t0": "north",
        "t1": "end",
        "t2": "diner",
    }
    trigrams = ["nor", "ort", "rth", "th ", "h e", " en", "end", "nd ", "d d", " di", "din", "ine", "ner"]
    sql = db._query_trigram_text(params, trigrams)
    assert "norm_name" in sql, "name_tokens CTE should split norm_name, not lower(strip_accents(r.name))"
    assert "lower(strip_accents(" not in sql, (
        "SQL should not call lower(strip_accents(...)) at runtime — use norm_name instead"
    )


def test_osm_query_trigram_spatial_uses_norm_name():
    """Spatial path uses norm_name column, not runtime lower(strip_accents(name)).

    FAILS until norm_name optimization is implemented for OpenStreetMap.
    """
    db = _make_osm()
    params: SearchParams = {
        "q": "tartine",
        "centroid": "POINT(-122.4195 37.7612)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
        "importance_floor": 0,
    }
    trigrams = ["tar", "art", "rti", "tin", "ine"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "norm_name" in sql, "SQL should reference the norm_name column"
    assert "lower(strip_accents(" not in sql, (
        "SQL should not call lower(strip_accents(...)) at runtime — use norm_name instead"
    )


def test_osm_query_trigram_spatial_multi_token_uses_norm_name():
    """Multi-token spatial path name_tokens CTE uses norm_name, not runtime normalization.

    FAILS until norm_name optimization is implemented for OpenStreetMap.
    """
    db = _make_osm()
    params: SearchParams = {
        "q": "north end diner",
        "centroid": "POINT(-122.4195 37.7612)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
        "importance_floor": 0,
        "t0": "north",
        "t1": "end",
        "t2": "diner",
    }
    trigrams = ["nor", "ort", "rth", "th ", "h e", " en", "end", "nd ", "d d", " di", "din", "ine", "ner"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "norm_name" in sql, "name_tokens CTE should split norm_name, not lower(strip_accents(r.name))"
    assert "lower(strip_accents(" not in sql, (
        "SQL should not call lower(strip_accents(...)) at runtime — use norm_name instead"
    )


def test_osm_query_trigram_text_uses_norm_q():
    """Text path SQL references $norm_q instead of lower(strip_accents($q)).

    After Optimization 2, nearest() pre-normalizes the query as norm_q and
    the SQL references $norm_q (a pre-computed scalar) instead of computing
    lower(strip_accents($q)) repeatedly at runtime.
    FAILS until norm_name optimization is implemented for OpenStreetMap.
    """
    db = _make_osm()
    params: SearchParams = {"q": "tartine", "limit": 10, "importance_floor": 0}
    trigrams = ["tar", "art", "rti", "tin", "ine"]
    sql = db._query_trigram_text(params, trigrams)
    assert "$norm_q" in sql, "SQL should reference $norm_q (pre-computed query string)"
    assert "lower(strip_accents($q))" not in sql, (
        "SQL should not call lower(strip_accents($q)) at runtime — use $norm_q instead"
    )


def test_osm_name_index_has_norm_name_column(osm_db):
    """name_index table has a norm_name column (schema migration for Optimization 2).

    After Optimization 2, the import script pre-computes norm_name for each row.
    This integration test verifies the column exists in the fixture DB.
    FAILS until the conftest fixture (and import script) add the norm_name column.
    """
    rows = osm_db.conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'name_index'"
    ).fetchall()
    column_names = [row[0] for row in rows]
    assert "norm_name" in column_names, (
        f"name_index is missing the 'norm_name' column. "
        f"Found columns: {column_names}. "
        "Add norm_name = lower(strip_accents(name)) to the import script and conftest fixture."
    )


# ---------------------------------------------------------------------------
# Schema normalization tests (Optimization 4 — Red phase)
# These tests FAIL against the current norm_name worktree code and PASS after
# schema normalization removes display columns from name_index.
# ---------------------------------------------------------------------------

def test_osm_name_index_no_display_columns(osm_db):
    """name_index has NO display columns after Optimization 4 schema normalization.

    After Optimization 4, OSM name_index is stripped to only:
        trigram, rkey, name, norm_name, importance
    Display columns (latitude, longitude) are removed — text-only queries JOIN places.
    FAILS until schema normalization is implemented.
    """
    rows = osm_db.conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'name_index'"
    ).fetchall()
    column_names = [row[0] for row in rows]
    display_cols = ["latitude", "longitude"]
    present = [c for c in display_cols if c in column_names]
    assert present == [], (
        f"OSM name_index should not contain display columns after Optimization 4. "
        f"Found: {present}. "
        "Remove these columns — text-only queries should JOIN places."
    )
    expected_cols = {"trigram", "rkey", "name", "norm_name", "importance"}
    assert expected_cols.issubset(set(column_names)), (
        f"OSM name_index is missing expected columns. Found: {column_names}"
    )


def test_osm_query_trigram_text_joins_places(osm_db):
    """Single-token text-only query SQL contains JOIN places after Optimization 4.

    After schema normalization, OSM name_index no longer has latitude/longitude.
    The text-only path must JOIN the places table to retrieve coordinates and
    primary_category.
    FAILS until text-only SQL is updated to JOIN places.
    """
    db = osm_db
    params: SearchParams = {
        "q": "tartine",
        "limit": 10,
        "importance_floor": 0,
        "norm_q": "tartine",
        "g0": "tar", "g1": "art", "g2": "rti", "g3": "tin", "g4": "ine",
    }
    trigrams = ["tar", "art", "rti", "tin", "ine"]
    sql = db._query_trigram_text(params, trigrams)
    assert "JOIN PLACES" in sql.upper().replace("\n", " "), (
        "Single-token OSM text-only SQL should JOIN places after schema normalization."
    )


def test_osm_query_trigram_text_multi_token_joins_places(osm_db):
    """Multi-token text-only query SQL contains JOIN places after Optimization 4.

    FAILS until multi-token OSM text-only SQL is updated to JOIN places.
    """
    db = osm_db
    params: SearchParams = {
        "q": "north end diner",
        "limit": 10,
        "importance_floor": 0,
        "norm_q": "north end diner",
        "t0": "north",
        "t1": "end",
        "t2": "diner",
    }
    trigrams = ["nor", "ort", "rth", "th ", "h e", " en", "end", "nd ", "d d", " di", "din", "ine", "ner"]
    sql = db._query_trigram_text(params, trigrams)
    assert "JOIN PLACES" in sql.upper().replace("\n", " "), (
        "Multi-token OSM text-only SQL should JOIN places after schema normalization."
    )


def test_osm_text_query_returns_primary_category(osm_db):
    """Text-only query returns primary_category after Optimization 4.

    Before Optimization 4, the OSM text-only path does not return primary_category
    because it reads only from name_index (which doesn't store it). After schema
    normalization, text-only queries JOIN places and return p.primary_category.

    Fixture n240109189 (Tartine Manufactory) has primary_category = 'amenity=cafe'.
    FAILS until the text-only path JOINs places and returns primary_category.
    """
    results = osm_db.nearest(q="Tartine Manufactory")
    assert len(results) > 0, "Expected at least one result for 'Tartine Manufactory'"
    tartine = next((r for r in results if "Tartine" in r["names"][0]["text"]), None)
    assert tartine is not None, "Expected to find 'Tartine Manufactory' in results"
    # After JOIN places, primary_category is parsed into attributes
    attrs = tartine.get("attributes", {})
    assert "amenity" in attrs, (
        f"Expected 'amenity' in attributes after joining places for primary_category. "
        f"Got attributes: {attrs}. "
        "Text-only path must JOIN places to return primary_category."
    )


# ---------------------------------------------------------------------------
# Attribute hydration tests (Red phase — FAIL until hydrate_records() is impl.)
# nearest() results currently have empty attributes because search queries
# only select minimal columns (or, for text-only pre-Opt4, missing primary_category).
# hydrate_records() will batch-fetch full record_columns() and merge attributes
# back into search results.
# ---------------------------------------------------------------------------

def test_nearest_text_has_attributes(osm_db):
    """Text search results include non-empty attributes with OSM tags after hydration.

    Fixture n240109189 (Tartine Manufactory) has tags including 'cuisine'.
    After hydration, process_record parses tags into attributes.
    Before hydration, text-only results lack tags (search_columns omits them).
    FAILS until hydrate_records() is implemented.
    """
    results = osm_db.nearest(q="Tartine Manufactory")
    assert len(results) > 0
    tartine = next(
        (r for r in results if "Tartine" in r["names"][0]["text"]), None
    )
    assert tartine is not None, "Expected to find 'Tartine Manufactory' in results"
    assert "cuisine" in tartine.get("attributes", {}), (
        f"Expected 'cuisine' tag in attributes after hydration. "
        f"Got attributes: {tartine.get('attributes')}. "
        "nearest() must call hydrate_records() to populate tags in attributes."
    )


def test_nearest_spatial_has_attributes(osm_db):
    """Spatial-only search results include full tag attributes after hydration.

    Fixture n240109189 (Tartine Manufactory) has tags including 'cuisine=coffee'.
    Spatial search near its coordinates returns it; after hydration, cuisine should
    be present in attributes. Before hydration, search_columns() only returns
    primary_category (not tags), so cuisine is missing.
    FAILS until hydrate_records() is implemented.
    """
    # Search near Tartine Manufactory (has cuisine tag)
    results = osm_db.nearest(bbox=(-122.4645, 37.7162, -122.3745, 37.8062))
    assert len(results) > 0
    tartine = next(
        (r for r in results if "Tartine" in r["names"][0]["text"]), None
    )
    assert tartine is not None, "Expected to find 'Tartine Manufactory' in spatial results"
    assert "cuisine" in tartine.get("attributes", {}), (
        f"Expected 'cuisine' tag in attributes after hydration. "
        f"Got attributes: {tartine.get('attributes')}. "
        "nearest() must call hydrate_records() to populate tags from places.tags."
    )


def test_nearest_attributes_match_get_record(osm_db):
    """Attributes from nearest() match those from get_record() for the same rkey.

    After hydration, the attributes dict in search results should be identical
    to the attributes produced by a direct get_record() lookup.
    FAILS until hydrate_records() is implemented.
    """
    results = osm_db.nearest(q="Tartine Manufactory")
    assert len(results) > 0
    tartine = next(
        (r for r in results if "Tartine" in r["names"][0]["text"]), None
    )
    assert tartine is not None, "Expected to find 'Tartine Manufactory' in nearest() results"
    rkey = tartine["rkey"]

    direct = osm_db.get_record("", "org.atgeo.places.osm", rkey)
    assert direct is not None, f"get_record returned None for rkey={rkey}"

    assert tartine["attributes"] == direct["attributes"], (
        f"attributes from nearest() do not match get_record() for rkey={rkey}. "
        f"nearest attributes: {tartine['attributes']}. "
        f"get_record attributes: {direct['attributes']}. "
        "hydrate_records() must produce the same attributes as get_record()."
    )
