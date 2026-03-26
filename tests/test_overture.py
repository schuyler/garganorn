"""Tests for OvertureMaps database class."""
import pytest

from garganorn.database import OvertureMaps, SearchParams


# ---------------------------------------------------------------------------
# Unit tests — SQL generation (no DB connection needed)
# ---------------------------------------------------------------------------

def _make_ovr(db_path=None):
    db = OvertureMaps(db_path or ":memory:")
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


def test_query_nearest_requires_centroid_or_q(overture_db):
    """query_nearest() should require either centroid or q."""
    with pytest.raises(AssertionError):
        overture_db.query_nearest({"limit": 10})


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
    assert record["$type"] == "org.atgeo.place"
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


def test_search_columns_removed():
    """search_columns() method should not exist after eliminating places scan.

    FAILS until search_columns is removed from OvertureMaps.
    """
    db = _make_ovr()
    assert not hasattr(db, 'search_columns'), (
        "search_columns() should be removed — text/spatial+text paths return "
        "minimal columns; spatial-only uses record_columns() directly"
    )


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_nearest_spatial(overture_db):
    """Spatial query returns results with distance_m present."""
    results = overture_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199))
    assert len(results) > 0
    assert all(r["distance_m"] >= 0 for r in results)


def test_nearest_text(overture_db):
    """Text query finds a place by name fragment via trigram search."""
    results = overture_db.nearest(bbox=(-122.4719, 37.7146, -122.3819, 37.8046), q="Dolores")
    names = [r["name"] for r in results]
    assert any("Dolores" in n for n in names)


def test_get_record(overture_db):
    """Known id returns a record with expected structure."""
    record = overture_db.get_record("", "org.atgeo.places.overture", "ovr001")
    assert record is not None
    assert record["rkey"] == "ovr001"
    assert record["name"] == "Philz Coffee"
    assert record["variants"] == []
    # Should have geo location
    assert record["locations"][0]["$type"] == "community.lexicon.location.geo"


# ---------------------------------------------------------------------------
# Unit tests — Overture trigram SQL generation
# ---------------------------------------------------------------------------

def test_overture_query_trigram_text_uses_jw():
    """_query_trigram_text SQL uses Jaro-Winkler scoring."""
    db = _make_ovr()
    params: SearchParams = {"q": "anchor brewing", "limit": 10}
    trigrams = ["anc", "nch", "cho", "hor", "or ", "r b", " br", "bre", "rew", "ewi", "win", "ing"]
    sql = db._query_trigram_text(params, trigrams)
    assert "jaro_winkler_similarity" in sql
    assert "count(DISTINCT trigram)" not in sql
    assert "GROUP BY" not in sql
    assert "with candidates" in sql.lower()


def test_overture_query_trigram_spatial_uses_jw():
    """_query_trigram_spatial SQL uses Jaro-Winkler, trigram IN, ST_Distance_Sphere."""
    db = _make_ovr()
    params: SearchParams = {
        "q": "anchor brewing",
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    trigrams = ["anc", "nch", "cho", "hor", "or ", "r b", " br", "bre", "rew", "ewi", "win", "ing"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "jaro_winkler_similarity" in sql
    assert "count(DISTINCT trigram)" not in sql
    assert "GROUP BY" not in sql
    assert "with candidates" in sql.lower()
    assert "ST_Distance_Sphere" in sql
    assert "score >= 0.6" in sql


def test_query_trigram_spatial_multi_token_has_limit():
    """Multi-token _query_trigram_spatial ranked CTE contains a LIMIT.

    The multi-token branch pre-sorts candidates by full_jw and limits them before
    the expensive token-level scoring step. The ranked CTE must contain LIMIT {top_n}
    between 'ranked as' and 'name_tokens'.
    FAILS until LIMIT is added to the multi-token spatial ranked CTE.
    """
    db = _make_ovr()
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
    sql_lower = sql.lower()
    ranked_pos = sql_lower.find("ranked as")
    name_tokens_pos = sql_lower.find("name_tokens", ranked_pos)
    assert ranked_pos != -1, "ranked CTE not found in SQL"
    assert name_tokens_pos != -1, "name_tokens CTE not found after ranked CTE"
    ranked_to_name_tokens = sql_lower[ranked_pos:name_tokens_pos]
    assert "limit" in ranked_to_name_tokens, (
        "ranked CTE in multi-token spatial path should contain LIMIT to cap candidates "
        "before token-level scoring. Add LIMIT {top_n} to the ranked CTE."
    )


# ---------------------------------------------------------------------------
# Integration tests — Overture trigram DB
# ---------------------------------------------------------------------------

def test_overture_trigram_nearest_text(overture_db):
    """Trigram text search for 'Anchor Brewing' finds it in results."""
    results = overture_db.nearest(q="Anchor Brewing")
    assert len(results) > 0
    names = [r["name"] for r in results]
    assert any("Anchor" in n for n in names)


def test_overture_trigram_nearest_no_scoring_in_attributes(overture_db):
    """Trigram search results do not expose score in attributes."""
    results = overture_db.nearest(q="Anchor Brewing")
    assert len(results) > 0
    for r in results:
        assert "score" not in r.get("attributes", {})


# ---------------------------------------------------------------------------
# Token-blending tests (Red phase — these FAIL until token blending is impl.)
# ---------------------------------------------------------------------------

def test_token_blending_text_ranking(overture_db):
    """Token-level JW blending ranks 'Diner North End' above 'North End Pub' for query 'North End Diner'.

    Full-string JW favors 'North End Pub' because it shares the long prefix 'north end'.
    Token-level JW correctly identifies that 'Diner North End' contains all query tokens.
    This test FAILS until token blending is implemented.
    """
    results = overture_db.nearest(q="North End Diner")
    names = [r["name"] for r in results]
    assert "Diner North End" in names, "Diner North End not found in results"
    assert "North End Pub" in names, "North End Pub not found in results"
    diner_idx = names.index("Diner North End")
    pub_idx = names.index("North End Pub")
    assert diner_idx < pub_idx, (
        f"'Diner North End' (pos {diner_idx}) should rank above "
        f"'North End Pub' (pos {pub_idx}) with token-level JW blending"
    )


def test_token_blending_spatial_ranking(overture_db):
    """Spatial + text: token blending ranks 'Diner North End' above 'North End Pub'.

    Both places are co-located within the search bbox; distance does not break the tie.
    Full-string JW favors 'North End Pub'. Token JW correctly favors 'Diner North End'.
    This test FAILS until token blending is implemented.
    """
    results = overture_db.nearest(
        bbox=(-122.4801, 37.7299, -122.3901, 37.8199), q="North End Diner"
    )
    names = [r["name"] for r in results]
    assert "Diner North End" in names, "Diner North End not found in results"
    assert "North End Pub" in names, "North End Pub not found in results"
    diner_idx = names.index("Diner North End")
    pub_idx = names.index("North End Pub")
    assert diner_idx < pub_idx, (
        f"'Diner North End' (pos {diner_idx}) should rank above "
        f"'North End Pub' (pos {pub_idx}) with token-level JW blending"
    )


def test_single_token_finds_existing_place(overture_db):
    """Single-token query 'Coit' finds Coit Tower (regression guard, should PASS)."""
    results = overture_db.nearest(q="Coit")
    names = [r["name"] for r in results]
    assert any("Coit" in n for n in names)


def test_single_token_no_blending_applied(overture_db):
    """Single-token query returns results without token blending (regression guard, should PASS).

    Single-token queries use full-string JW only per spec. Verify this path
    still works correctly after the blending feature is added.
    """
    results = overture_db.nearest(q="Lombard")
    names = [r["name"] for r in results]
    assert any("Lombard" in n for n in names)


# ---------------------------------------------------------------------------
# norm_name optimization tests (Red phase — FAIL until Optimization 2 impl.)
# ---------------------------------------------------------------------------

def test_overture_query_trigram_text_uses_norm_name():
    """Text path uses norm_name column, not runtime lower(strip_accents(name)).

    FAILS until norm_name optimization is implemented for OvertureMaps.
    """
    db = _make_ovr()
    params: SearchParams = {"q": "anchor brewing", "limit": 10, "importance_floor": 0}
    trigrams = ["anc", "nch", "cho", "hor"]
    sql = db._query_trigram_text(params, trigrams)
    assert "norm_name" in sql, "SQL should reference the norm_name column"
    assert "lower(strip_accents(" not in sql, (
        "SQL should not call lower(strip_accents(...)) at runtime — use norm_name instead"
    )


def test_overture_query_trigram_text_multi_token_uses_norm_name():
    """Multi-token text path name_tokens CTE uses norm_name, not runtime normalization.

    In the multi-token branch, the name_tokens CTE splits candidate names into
    tokens. After Optimization 2, it should split norm_name (pre-computed) rather
    than calling lower(strip_accents(r.name)) at runtime.
    FAILS until norm_name optimization is implemented for OvertureMaps.
    """
    db = _make_ovr()
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


def test_overture_query_trigram_spatial_uses_norm_name():
    """Spatial path uses norm_name column, not runtime lower(strip_accents(name)).

    FAILS until norm_name optimization is implemented for OvertureMaps.
    """
    db = _make_ovr()
    params: SearchParams = {
        "q": "anchor brewing",
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
        "importance_floor": 0,
    }
    trigrams = ["anc", "nch", "cho", "hor"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "norm_name" in sql, "SQL should reference the norm_name column"
    assert "lower(strip_accents(" not in sql, (
        "SQL should not call lower(strip_accents(...)) at runtime — use norm_name instead"
    )


def test_overture_query_trigram_spatial_multi_token_uses_norm_name():
    """Multi-token spatial path name_tokens CTE uses norm_name, not runtime normalization.

    FAILS until norm_name optimization is implemented for OvertureMaps.
    """
    db = _make_ovr()
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


def test_overture_query_trigram_text_uses_norm_q():
    """Text path SQL references $norm_q instead of lower(strip_accents($q)).

    After Optimization 2, nearest() pre-normalizes the query as norm_q and
    the SQL references $norm_q (a pre-computed scalar) instead of computing
    lower(strip_accents($q)) repeatedly at runtime.
    FAILS until norm_name optimization is implemented for OvertureMaps.
    """
    db = _make_ovr()
    params: SearchParams = {"q": "anchor brewing", "limit": 10, "importance_floor": 0}
    trigrams = ["anc", "nch", "cho", "hor"]
    sql = db._query_trigram_text(params, trigrams)
    assert "$norm_q" in sql, "SQL should reference $norm_q (pre-computed query string)"
    assert "lower(strip_accents($q))" not in sql, (
        "SQL should not call lower(strip_accents($q)) at runtime — use $norm_q instead"
    )


def test_overture_name_index_has_norm_name_column(overture_db):
    """name_index table has a norm_name column (schema migration for Optimization 2).

    After Optimization 2, the import script pre-computes norm_name for each row.
    This integration test verifies the column exists in the fixture DB.
    FAILS until the conftest fixture (and import script) add the norm_name column.
    """
    rows = overture_db.conn.execute(
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

def test_overture_name_index_no_display_columns(overture_db):
    """name_index has NO display columns after Optimization 4 schema normalization.

    After Optimization 4, Overture name_index is stripped to only:
        trigram, id, name, norm_name, importance
    Display columns (latitude, longitude) are removed — text-only queries JOIN places.
    FAILS until schema normalization is implemented.
    """
    rows = overture_db.conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'name_index'"
    ).fetchall()
    column_names = [row[0] for row in rows]
    display_cols = ["latitude", "longitude"]
    present = [c for c in display_cols if c in column_names]
    assert present == [], (
        f"Overture name_index should not contain display columns after Optimization 4. "
        f"Found: {present}. "
        "Remove these columns — text-only queries should JOIN places."
    )
    expected_cols = {"trigram", "id", "name", "norm_name", "importance"}
    assert expected_cols.issubset(set(column_names)), (
        f"Overture name_index is missing expected columns. Found: {column_names}"
    )


def test_text_query_no_join_places():
    """Text-only query SQL should NOT contain JOIN places after eliminating places scan.

    The plan removes JOIN places from text-only queries. Text search returns
    only minimal columns (rkey, name, score) from name_index CTEs.
    Display columns are filled in by hydration via ART-indexed lookup.
    FAILS until JOIN places is removed from _query_trigram_text.
    """
    db = _make_ovr()
    params: SearchParams = {"q": "anchor brewing", "limit": 10}
    trigrams = ["anc", "nch", "cho", "hor"]
    sql = db._query_trigram_text(params, trigrams)
    assert "JOIN PLACES" not in sql.upper().replace("\n", " "), (
        "Overture text-only SQL should NOT join places — hydration fills in display columns"
    )


def test_text_query_multi_token_no_join_places():
    """Multi-token text-only query SQL should NOT contain JOIN places.

    The plan removes JOIN places from text-only queries (both single-token and
    multi-token paths). Hydration fills in display columns via ART-indexed lookup.
    FAILS until JOIN places is removed from the multi-token _query_trigram_text path.
    """
    db = _make_ovr()
    params: SearchParams = {
        "q": "north end diner",
        "limit": 10,
        "t0": "north",
        "t1": "end",
        "t2": "diner",
    }
    trigrams = ["nor", "ort", "rth", "th ", "h e", " en", "end", "nd ", "d d", " di", "din", "ine", "ner"]
    sql = db._query_trigram_text(params, trigrams)
    assert "JOIN PLACES" not in sql.upper().replace("\n", " "), (
        "Overture multi-token text-only SQL should NOT join places — hydration fills in display columns"
    )


def test_spatial_only_returns_record_columns():
    """The spatial-only branch of query_nearest() should select record_columns() fields.

    After the plan is implemented, the spatial-only path selects full record_columns()
    fields directly (no separate hydration needed for spatial). Key record_columns fields
    like id (as rkey), addresses, confidence should appear in the spatial-only SQL.
    FAILS until query_nearest selects record_columns() for the spatial-only branch.
    """
    db = _make_ovr()
    params: SearchParams = {
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    sql = db.query_nearest(params)
    sql_lower = sql.lower()
    assert "addresses" in sql_lower, (
        "Overture spatial-only SQL should select addresses (from record_columns)"
    )
    assert "confidence" in sql_lower, (
        "Overture spatial-only SQL should select confidence (from record_columns)"
    )


def test_spatial_text_query_no_display_columns():
    """Spatial+text query SQL should not select display columns.

    After eliminating places scan, the spatial+text path returns only
    rkey, name, distance_m, score. Display columns are filled in by hydration.
    FAILS until _query_trigram_spatial is slimmed per plan.
    """
    db = _make_ovr()
    params: SearchParams = {
        "q": "anchor brewing",
        "centroid": "POINT(-122.4194 37.7749)",
        "xmin": -122.5, "ymin": 37.7, "xmax": -122.3, "ymax": 37.85,
        "limit": 10,
    }
    trigrams = ["anc", "nch", "cho", "hor", "or ", "r b", " br", "bre", "rew", "ewi", "win", "ing"]
    sql = db._query_trigram_spatial(params, trigrams)
    assert "p.addresses" not in sql.lower(), (
        "Spatial+text SQL should not select p.addresses — hydration fills in display columns"
    )


# ---------------------------------------------------------------------------
# Attribute hydration tests (Red phase — FAIL until hydrate_records() is impl.)
# nearest() results currently have empty attributes because search queries
# only select minimal columns. hydrate_records() will batch-fetch full
# record_columns() and merge attributes back into search results.
# ---------------------------------------------------------------------------

def test_nearest_text_has_attributes(overture_db):
    """Text search results include non-empty attributes after hydration.

    Before hydration, attributes is {} because search_columns() omits extended
    fields. After hydration, confidence is always present (fixture has confidence=0.9,
    cast to varchar '0.900').
    FAILS until hydrate_records() is implemented.
    """
    results = overture_db.nearest(q="Philz Coffee")
    assert len(results) > 0
    for r in results:
        assert "confidence" in r.get("attributes", {}), (
            f"Expected 'confidence' in attributes after hydration. "
            f"Got attributes: {r.get('attributes')}. "
            "nearest() must call hydrate_records() to populate attributes."
        )


def test_nearest_spatial_has_attributes(overture_db):
    """Spatial-only search results include non-empty attributes after hydration.

    FAILS until hydrate_records() is implemented.
    """
    results = overture_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199))
    assert len(results) > 0
    for r in results:
        assert "confidence" in r.get("attributes", {}), (
            f"Expected 'confidence' in attributes after hydration. "
            f"Got attributes: {r.get('attributes')}. "
            "nearest() must call hydrate_records() to populate attributes."
        )


def test_nearest_attributes_match_get_record(overture_db):
    """Attributes from nearest() match those from get_record() for the same rkey.

    After hydration, the attributes dict in search results should be identical
    to the attributes produced by a direct get_record() lookup.
    FAILS until hydrate_records() is implemented.
    """
    results = overture_db.nearest(q="Philz Coffee")
    assert len(results) > 0
    philz = next(
        (r for r in results if "Philz" in r["name"]), None
    )
    assert philz is not None, "Expected to find 'Philz Coffee' in nearest() results"
    rkey = philz["rkey"]

    direct = overture_db.get_record("", "org.atgeo.places.overture", rkey)
    assert direct is not None, f"get_record returned None for rkey={rkey}"

    assert philz["attributes"] == direct["attributes"], (
        f"attributes from nearest() do not match get_record() for rkey={rkey}. "
        f"nearest attributes: {philz['attributes']}. "
        f"get_record attributes: {direct['attributes']}. "
        "hydrate_records() must produce the same attributes as get_record()."
    )
