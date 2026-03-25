"""Tests for FoursquareOSP database class."""
import pytest

from garganorn.database import FoursquareOSP, SearchParams


# ---------------------------------------------------------------------------
# Unit tests — SQL generation (no DB connection needed)
# ---------------------------------------------------------------------------

def _make_fsq(db_path=None):
    """Create a FoursquareOSP instance."""
    db = FoursquareOSP(db_path or ":memory:")
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


def test_query_nearest_requires_centroid_or_q():
    """Neither centroid nor q raises AssertionError."""
    db = _make_fsq()
    params: SearchParams = {"limit": 10}
    with pytest.raises(AssertionError):
        db.query_nearest(params)


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
    assert record["$type"] == "org.atgeo.place"
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


def test_nearest_text(fsq_db):
    """Text query finds place by trigram match."""
    results = fsq_db.nearest(q="tartine")
    names = [r["names"][0]["text"] for r in results]
    assert any("Tartine" in n for n in names)


def test_get_record_found(fsq_db):
    """Known rkey returns a record."""
    record = fsq_db.get_record("", "org.atgeo.places.foursquare", "fsq001")
    assert record is not None
    assert record["rkey"] == "fsq001"
    assert record["names"][0]["text"] == "Blue Bottle Coffee"


def test_get_record_not_found(fsq_db):
    """Unknown rkey returns None."""
    record = fsq_db.get_record("", "org.atgeo.places.foursquare", "nonexistent")
    assert record is None


# ---------------------------------------------------------------------------
# Unit tests — trigram SQL generation (no DB connection needed)
# ---------------------------------------------------------------------------

def test_query_trigram_text_uses_jw():
    """_query_trigram_text SQL uses Jaro-Winkler scoring."""
    db = _make_fsq()
    params: SearchParams = {"q": "coffee", "limit": 10}
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_text(params, trigrams)
    assert "jaro_winkler_similarity" in sql
    assert "count(DISTINCT trigram)" not in sql
    assert "GROUP BY" not in sql
    assert "with candidates" in sql.lower()


def test_query_trigram_text_no_limit_5000():
    """_query_trigram_text SQL does not use an intermediate candidate LIMIT."""
    db = _make_fsq()
    params: SearchParams = {"q": "coffee", "limit": 10}
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_text(params, trigrams)
    assert "5000" not in sql


def test_query_trigram_spatial_uses_jw():
    """_query_trigram_spatial SQL uses Jaro-Winkler, trigram IN, ST_Distance_Sphere."""
    db = _make_fsq()
    params: SearchParams = {
        "q": "coffee",
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "jaro_winkler_similarity" in sql
    assert "count(DISTINCT trigram)" not in sql
    assert "GROUP BY" not in sql
    assert "with candidates" in sql.lower()
    assert "ST_Distance_Sphere" in sql
    assert "score >= 0.6" in sql


# ---------------------------------------------------------------------------
# Integration tests — FSQ trigram DB
# ---------------------------------------------------------------------------

def test_trigram_nearest_text_exact_match(fsq_db):
    """Text-only trigram search for 'Tartine Bakery' returns it as first result."""
    results = fsq_db.nearest(q="Tartine Bakery")
    assert len(results) > 0
    names = [r["names"][0]["text"] for r in results]
    assert names[0] == "Tartine Bakery"


def test_trigram_nearest_text_no_scoring_in_attributes(fsq_db):
    """Trigram search results do not expose score or jaccard in attributes."""
    results = fsq_db.nearest(q="Tartine Bakery")
    assert len(results) > 0
    for r in results:
        assert "score" not in r.get("attributes", {})
        assert "jaccard" not in r.get("attributes", {})


def test_trigram_nearest_spatial_with_text(fsq_db):
    """Spatial + text trigram search returns results with distance_m."""
    results = fsq_db.nearest(latitude=37.7749, longitude=-122.4194, q="Blue Bottle Coffee")
    assert len(results) > 0
    assert all(r["distance_m"] >= 0 for r in results)
    names = [r["names"][0]["text"] for r in results]
    assert any("Coffee" in n for n in names)


def test_trigram_nearest_unrelated_query(fsq_db):
    """Completely unrelated query returns 0 results."""
    results = fsq_db.nearest(q="xyzqwerty")
    assert len(results) == 0


# ---------------------------------------------------------------------------
# Token-blending tests (Red phase — these FAIL until token blending is impl.)
# ---------------------------------------------------------------------------

def test_token_blending_text_ranking(fsq_db):
    """Token-level JW blending ranks 'Diner North End' above 'North End Pub' for query 'North End Diner'.

    Full-string JW favors 'North End Pub' because it shares the long prefix 'north end'.
    Token-level JW correctly identifies that 'Diner North End' contains all query tokens.
    This test FAILS until token blending is implemented.
    """
    results = fsq_db.nearest(q="North End Diner")
    names = [r["names"][0]["text"] for r in results]
    assert "Diner North End" in names, "Diner North End not found in results"
    assert "North End Pub" in names, "North End Pub not found in results"
    diner_idx = names.index("Diner North End")
    pub_idx = names.index("North End Pub")
    assert diner_idx < pub_idx, (
        f"'Diner North End' (pos {diner_idx}) should rank above "
        f"'North End Pub' (pos {pub_idx}) with token-level JW blending"
    )


def test_token_blending_spatial_ranking(fsq_db):
    """Spatial + text: token blending ranks 'Diner North End' above 'North End Pub'.

    Both places are co-located within the search bbox; distance does not break the tie.
    Full-string JW favors 'North End Pub'. Token JW correctly favors 'Diner North End'.
    This test FAILS until token blending is implemented.
    """
    results = fsq_db.nearest(
        latitude=37.7749, longitude=-122.4351, q="North End Diner"
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


def test_single_token_finds_existing_place(fsq_db):
    """Single-token query 'Alcatraz' finds Alcatraz Island (regression guard, should PASS)."""
    results = fsq_db.nearest(q="Alcatraz")
    names = [r["names"][0]["text"] for r in results]
    assert any("Alcatraz" in n for n in names)


def test_single_token_no_blending_applied(fsq_db):
    """Single-token query returns results without token blending (regression guard, should PASS).

    Single-token queries use full-string JW only per spec. Verify this path
    still works correctly after the blending feature is added.
    """
    results = fsq_db.nearest(q="Ferry")
    # "Ferry Building Marketplace" contains "ferry" and should appear
    names = [r["names"][0]["text"] for r in results]
    assert any("Ferry" in n for n in names)


# ---------------------------------------------------------------------------
# Multi-token scaling tests (Strategy E — Red phase)
# These test that token blending works correctly at higher token counts (4-6).
# ---------------------------------------------------------------------------

def test_four_token_query_finds_correct_place(fsq_db):
    """4-token query 'North Beach Community Garden' finds 'North Beach Community Garden Center'.

    Verifies that blending works at 4 query tokens. The target place (5 tokens)
    contains all 4 query tokens and should appear in results.
    """
    results = fsq_db.nearest(q="North Beach Community Garden")
    names = [r["names"][0]["text"] for r in results]
    assert "North Beach Community Garden Center" in names, (
        "4-token query should find 'North Beach Community Garden Center'"
    )


def test_four_token_query_ranks_best_match_first(fsq_db):
    """4-token query 'North Beach Community Garden' ranks the 5-token fixture first.

    'North Beach Community Garden Center' has all 4 query tokens. Any partial
    match (e.g., 2-token overlap) should rank lower.
    """
    results = fsq_db.nearest(q="North Beach Community Garden")
    names = [r["names"][0]["text"] for r in results]
    assert len(names) > 0
    assert names[0] == "North Beach Community Garden Center", (
        f"Expected 'North Beach Community Garden Center' first, got '{names[0]}'"
    )


def test_five_token_query_finds_airport(fsq_db):
    """5-token query finds 'San Francisco International Airport Terminal'.

    Verifies blending works at 5 query tokens — matches the fixture exactly.
    """
    results = fsq_db.nearest(q="San Francisco International Airport Terminal")
    names = [r["names"][0]["text"] for r in results]
    assert "San Francisco International Airport Terminal" in names, (
        "5-token query should find 'San Francisco International Airport Terminal'"
    )


def test_five_token_query_ranks_exact_match_first(fsq_db):
    """5-token query 'San Francisco International Airport Terminal' ranks exact match first."""
    results = fsq_db.nearest(q="San Francisco International Airport Terminal")
    names = [r["names"][0]["text"] for r in results]
    assert len(names) > 0
    assert names[0] == "San Francisco International Airport Terminal", (
        f"Expected exact match first, got '{names[0]}'"
    )


def test_six_token_query_finds_best_match(fsq_db):
    """6-token query matches available fixtures via token blending.

    Query 'North Beach Community Garden Center Park' has 6 tokens.
    The best candidate is 'North Beach Community Garden Center' (5/6 token overlap).
    Verifies blending doesn't break at 6 tokens.
    """
    results = fsq_db.nearest(q="North Beach Community Garden Center Park")
    names = [r["names"][0]["text"] for r in results]
    assert "North Beach Community Garden Center" in names, (
        "6-token query should find 'North Beach Community Garden Center' via token blending"
    )


# ---------------------------------------------------------------------------
# Top-N cutoff survival test (Strategy E — Red phase)
# Strategy E introduces a LIMIT in a CTE to cap candidates before the
# expensive token JW scoring step. This test would catch a too-small cutoff.
# ---------------------------------------------------------------------------

def test_cutoff_survival_reordered_name(fsq_db):
    """Reordered name 'Restaurant Park Avenue' appears in results for 'Park Avenue Restaurant'.

    Setup: 25 places named 'Park Avenue <X>' (high full_jw for 'Park Avenue Restaurant')
    + 1 place named 'Restaurant Park Avenue' (low full_jw, perfect token_jw).

    Strategy E optimization: before expensive token scoring, pre-sort candidates by
    full_jw and keep only the top (N * limit) candidates. 'Restaurant Park Avenue'
    has the lowest full_jw of all 26 candidates (reordered words → weak prefix match),
    so it is the first to be dropped by an aggressive cutoff.

    With limit=26 and a 20x cutoff (20*26=520), all 26 candidates survive.
    With a 1x cutoff (1*26=26), the exact boundary — pre-sort order determines
    whether 'Restaurant Park Avenue' survives.
    With a cutoff smaller than 26, 'Restaurant Park Avenue' is dropped.

    This test uses limit=26 so 'Restaurant Park Avenue' can appear in the final
    results when all candidates are scored. It has perfect token_jw (1.0) vs
    partial token_jw for 'Park Avenue X' variants (missing 'restaurant' token),
    so the blended score pushes it into the top results.

    Passes against: current correlated subquery (no cutoff) and Strategy E (20x cutoff).
    Would fail against: a cutoff of ~1x or smaller that drops the reordered candidate
    before token scoring can surface it.
    """
    results = fsq_db.nearest(q="Park Avenue Restaurant", limit=26)
    names = [r["names"][0]["text"] for r in results]
    assert "Restaurant Park Avenue" in names, (
        f"'Restaurant Park Avenue' (perfect token match, low full_jw) should appear "
        f"in results for 'Park Avenue Restaurant' when all candidates are scored. "
        f"Got: {names}"
    )


# ---------------------------------------------------------------------------
# norm_name optimization tests (Red phase — FAIL until Optimization 2 impl.)
# These tests verify that pre-computed norm_name is used instead of runtime
# lower(strip_accents(...)) calls in SQL, and that nearest() pre-computes norm_q.
# ---------------------------------------------------------------------------

def test_query_trigram_text_uses_norm_name():
    """Single-token text path uses norm_name column, not runtime lower(strip_accents(name)).

    After Optimization 2, the SQL should reference the pre-computed norm_name
    column instead of calling lower(strip_accents(c.name)) or lower(strip_accents(name))
    at query time.
    FAILS until norm_name optimization is implemented.
    """
    db = _make_fsq()
    params: SearchParams = {"q": "coffee", "limit": 10, "importance_floor": 0}
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_text(params, trigrams)
    assert "norm_name" in sql, "SQL should reference the norm_name column"
    assert "lower(strip_accents(" not in sql, (
        "SQL should not call lower(strip_accents(...)) at runtime — use norm_name instead"
    )


def test_query_trigram_text_multi_token_uses_norm_name():
    """Multi-token text path name_tokens CTE uses norm_name, not runtime normalization.

    In the multi-token branch, the name_tokens CTE splits candidate names into
    tokens. After Optimization 2, it should split norm_name (pre-computed) rather
    than calling lower(strip_accents(r.name)) at runtime.
    FAILS until norm_name optimization is implemented.
    """
    db = _make_fsq()
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


def test_query_trigram_spatial_uses_norm_name():
    """Single-token spatial path uses norm_name column, not runtime lower(strip_accents(name)).

    FAILS until norm_name optimization is implemented.
    """
    db = _make_fsq()
    params: SearchParams = {
        "q": "coffee",
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
        "importance_floor": 0,
    }
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "norm_name" in sql, "SQL should reference the norm_name column"
    assert "lower(strip_accents(" not in sql, (
        "SQL should not call lower(strip_accents(...)) at runtime — use norm_name instead"
    )


def test_query_trigram_spatial_multi_token_uses_norm_name():
    """Multi-token spatial path name_tokens CTE uses norm_name, not runtime normalization.

    FAILS until norm_name optimization is implemented.
    """
    db = _make_fsq()
    params: SearchParams = {
        "q": "north end diner",
        "centroid": "POINT(-122.4351 37.7748)",
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


def test_query_trigram_text_uses_norm_q():
    """Text path SQL references $norm_q instead of lower(strip_accents($q)).

    After Optimization 2, nearest() pre-normalizes the query as norm_q and
    the SQL references $norm_q (a pre-computed scalar) instead of computing
    lower(strip_accents($q)) repeatedly at runtime.
    FAILS until norm_name optimization is implemented.
    """
    db = _make_fsq()
    params: SearchParams = {"q": "coffee", "limit": 10, "importance_floor": 0}
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_text(params, trigrams)
    assert "$norm_q" in sql, "SQL should reference $norm_q (pre-computed query string)"
    assert "lower(strip_accents($q))" not in sql, (
        "SQL should not call lower(strip_accents($q)) at runtime — use $norm_q instead"
    )



def test_name_index_has_norm_name_column(fsq_db):
    """name_index table has a norm_name column (schema migration for Optimization 2).

    After Optimization 2, the import script pre-computes norm_name for each row.
    This integration test verifies the column exists in the fixture DB.
    FAILS until the conftest fixture (and import script) add the norm_name column.
    """
    rows = fsq_db.conn.execute(
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

def test_name_index_no_display_columns(fsq_db):
    """name_index has NO display columns after Optimization 4 schema normalization.

    After Optimization 4, name_index is stripped to only:
        trigram, fsq_place_id, name, norm_name, importance
    Display columns (latitude, longitude, address, locality, postcode, region,
    country) are removed — text-only queries JOIN places instead.
    FAILS until schema normalization is implemented.
    """
    rows = fsq_db.conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'name_index'"
    ).fetchall()
    column_names = [row[0] for row in rows]
    display_cols = ["latitude", "longitude", "address", "locality", "postcode", "region", "country"]
    present = [c for c in display_cols if c in column_names]
    assert present == [], (
        f"name_index should not contain display columns after Optimization 4. "
        f"Found: {present}. "
        "Remove these columns from name_index — text-only queries should JOIN places."
    )
    expected_cols = {"trigram", "fsq_place_id", "name", "norm_name", "importance"}
    assert expected_cols.issubset(set(column_names)), (
        f"name_index is missing expected columns. Found: {column_names}"
    )


def test_query_trigram_text_joins_places(fsq_db):
    """Single-token text-only query SQL contains JOIN places after Optimization 4.

    After schema normalization, name_index no longer has display columns.
    The text-only path must JOIN the places table to retrieve latitude, longitude,
    address, etc. for the result set.
    FAILS until text-only SQL is updated to JOIN places.
    """
    db = fsq_db
    params: SearchParams = {
        "q": "coffee",
        "limit": 10,
        "importance_floor": 0,
        "norm_q": "coffee",
        "g0": "cof", "g1": "off", "g2": "ffe", "g3": "fee",
    }
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_text(params, trigrams)
    assert "JOIN PLACES" in sql.upper().replace("\n", " "), (
        "Single-token text-only SQL should JOIN places after schema normalization. "
        "name_index no longer has display columns."
    )


def test_query_trigram_text_multi_token_joins_places(fsq_db):
    """Multi-token text-only query SQL contains JOIN places after Optimization 4.

    Same requirement as single-token, but for the multi-token CTE branch.
    FAILS until multi-token text-only SQL is updated to JOIN places.
    """
    db = fsq_db
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
        "Multi-token text-only SQL should JOIN places after schema normalization. "
        "name_index no longer has display columns."
    )


def test_query_trigram_spatial_no_name_index_display_cols():
    """Spatial query SQL does not reference n.latitude, n.longitude, n.address.

    Spatial paths already JOIN places; this verifies they don't reference
    display columns via the name_index alias (n.*). Guards against regression.
    Should PASS both before and after Optimization 4.
    """
    db = _make_fsq()
    params: SearchParams = {
        "q": "coffee",
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
        "importance_floor": 0,
    }
    trigrams = ["cof", "off", "ffe", "fee"]
    sql = db._query_trigram_spatial(params, trigrams)
    # Spatial paths get display cols from p.* (places), not n.* (name_index)
    assert "n.latitude" not in sql, "Spatial SQL should not reference n.latitude (use places instead)"
    assert "n.longitude" not in sql, "Spatial SQL should not reference n.longitude (use places instead)"
    assert "n.address" not in sql, "Spatial SQL should not reference n.address (use places instead)"
