"""Tests for dedup-rkey fix (TDD red phase).

These tests MUST FAIL against current code because:
- query_nearest SQL lacks a deduped CTE (ROW_NUMBER OVER PARTITION BY rkey)
- Database.nearest() does not deduplicate rkeys before hydration

The bug:
  name_index has multiple rows per place (multilingual name variants).
  When a trigram search matches both the primary name and a variant name
  for the same place, the final SELECT emits one row per (place, name) pair,
  producing duplicate rkeys in the result. This:
    1. Causes fewer unique places than LIMIT requests (duplicates eat slots).
    2. Causes a KeyError in server.py when r.pop("distance_m") hits the
       same dict twice via the shared metadata dict.
"""
import pytest
import duckdb

from garganorn.database import FoursquareOSP, OvertureMaps, OpenStreetMap


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_trigrams(name):
    """Generate distinct trigrams from a place name (lowercased full string)."""
    s = name.lower()
    trigrams = set()
    for i in range(len(s) - 2):
        trigrams.add(s[i:i+3])
    return trigrams


def _create_dedup_fsq_db(db_path):
    """
    Create a minimal FSQ database where ONE place has two name_index entries
    (primary name + one variant) that both match the query 'blue cafe'.
    This reliably triggers the duplicate rkey bug.
    """
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            fsq_place_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            geom GEOMETRY,
            address VARCHAR,
            locality VARCHAR,
            postcode VARCHAR,
            region VARCHAR,
            admin_region VARCHAR,
            post_town VARCHAR,
            po_box VARCHAR,
            country VARCHAR,
            date_created DATE,
            date_refreshed DATE,
            date_closed DATE,
            tel VARCHAR,
            website VARCHAR,
            email VARCHAR,
            facebook_id VARCHAR,
            instagram VARCHAR,
            twitter VARCHAR,
            fsq_category_ids VARCHAR[],
            fsq_category_labels VARCHAR[],
            placemaker_url VARCHAR,
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            importance INTEGER,
            variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT []
        )
    """)

    # One place: "Blue Cafe" with a variant "Cafe Bleu" (French)
    conn.execute("""
        INSERT INTO places VALUES (
            'dd001', 'Blue Cafe', 37.7749, -122.4194,
            ST_Point(-122.4194, 37.7749),
            '1 Main St', 'San Francisco', '94103', 'CA', 'CA', NULL, NULL, 'US',
            '2021-01-01', '2022-01-01', NULL,
            NULL, NULL, NULL, NULL, NULL, NULL,
            ARRAY[]::VARCHAR[], ARRAY[]::VARCHAR[],
            NULL,
            {'xmin': -122.421, 'ymin': 37.773, 'xmax': -122.418, 'ymax': 37.776},
            80,
            [{'name': 'Cafe Bleu', 'type': 'alternate', 'language': 'fr'}]
            ::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
        )
    """)

    # A second, different place so we can test LIMIT behavior.
    # "Blue Cafe Express" scores 0.906 full-JW and 1.0 token-JW (blended 0.953)
    # against query "blue cafe", well above JW_THRESHOLD=0.6.
    conn.execute("""
        INSERT INTO places VALUES (
            'dd002', 'Blue Cafe Express', 37.7800, -122.4100,
            ST_Point(-122.4100, 37.7800),
            '2 Main St', 'San Francisco', '94103', 'CA', 'CA', NULL, NULL, 'US',
            '2021-01-01', '2022-01-01', NULL,
            NULL, NULL, NULL, NULL, NULL, NULL,
            ARRAY[]::VARCHAR[], ARRAY[]::VARCHAR[],
            NULL,
            {'xmin': -122.412, 'ymin': 37.778, 'xmax': -122.408, 'ymax': 37.782},
            75,
            []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
        )
    """)

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            fsq_place_id VARCHAR,
            name VARCHAR,
            norm_name VARCHAR,
            importance INTEGER,
            is_variant BOOLEAN DEFAULT FALSE
        )
    """)

    # Index "Blue Cafe" (primary) for dd001
    primary_name = "Blue Cafe"
    norm_primary = FoursquareOSP._strip_accents(primary_name.lower())
    for trigram in _generate_trigrams(primary_name):
        conn.execute(
            "INSERT INTO name_index VALUES (?, 'dd001', ?, ?, 80, FALSE)",
            [trigram, primary_name, norm_primary],
        )

    # Index "Cafe Bleu" (variant) for dd001 — shares trigrams with "Blue Cafe"
    # Both "blue cafe" and "cafe bleu" contain trigrams: 'blu', 'lue', 'caf', 'afe'
    variant_name = "Cafe Bleu"
    norm_variant = FoursquareOSP._strip_accents(variant_name.lower())
    for trigram in _generate_trigrams(variant_name):
        conn.execute(
            "INSERT INTO name_index VALUES (?, 'dd001', ?, ?, 80, TRUE)",
            [trigram, variant_name, norm_variant],
        )

    # Index "Blue Cafe Express" for dd002
    express_name = "Blue Cafe Express"
    norm_express = FoursquareOSP._strip_accents(express_name.lower())
    for trigram in _generate_trigrams(express_name):
        conn.execute(
            "INSERT INTO name_index VALUES (?, 'dd002', ?, ?, 75, FALSE)",
            [trigram, express_name, norm_express],
        )

    conn.close()


def _create_dedup_osm_db(db_path):
    """
    Create a minimal OSM database where ONE place has multiple name_index entries
    (primary + variants) that all match the query 'tartine'.
    """
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            osm_type VARCHAR,
            osm_id BIGINT,
            rkey VARCHAR,
            name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            geom GEOMETRY,
            primary_category VARCHAR,
            tags MAP(VARCHAR, VARCHAR),
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            importance INTEGER,
            variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT []
        )
    """)

    conn.execute("""
        INSERT INTO places VALUES (
            'n', 240109189, 'n240109189', 'Tartine Manufactory',
            37.7612, -122.4195,
            ST_Point(-122.4195, 37.7612),
            'amenity=cafe',
            MAP()::MAP(VARCHAR, VARCHAR),
            {'xmin': -122.421, 'ymin': 37.759, 'xmax': -122.418, 'ymax': 37.763},
            65,
            [
                {'name': 'Tartine Manufactory SF', 'type': 'alternate', 'language': 'en'},
                {'name': 'Old Tartine', 'type': 'historical', 'language': NULL},
                {'name': 'Tartine MFY', 'type': 'short', 'language': NULL}
            ]::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
        )
    """)

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            rkey VARCHAR,
            name VARCHAR,
            norm_name VARCHAR,
            importance INTEGER,
            is_variant BOOLEAN DEFAULT FALSE
        )
    """)

    all_names = [
        ("Tartine Manufactory", False),
        ("Tartine Manufactory SF", True),
        ("Old Tartine", True),
        ("Tartine MFY", True),
    ]
    for name, is_variant in all_names:
        norm = OpenStreetMap._strip_accents(name.lower())
        for trigram in _generate_trigrams(name):
            conn.execute(
                "INSERT INTO name_index VALUES (?, 'n240109189', ?, ?, 65, ?)",
                [trigram, name, norm, is_variant],
            )

    conn.close()


def _create_dedup_overture_db(db_path):
    """
    Create a minimal Overture database where ONE place has two name_index entries
    (primary name + one variant) that both match the query 'blue cafe'.
    This mirrors _create_dedup_fsq_db for the Overture schema.

    Place ov001: "Blue Cafe" (primary) + "Cafe Bleu" (variant, French)
      - "Blue Cafe" scores 1.0 blended for query "blue cafe"
      - "Cafe Bleu" scores 0.778 blended — above JW_THRESHOLD=0.6
      - Triggers the duplicate rkey bug for ov001

    Place ov002: "Blue Cafe Express"
      - Scores 0.953 blended for query "blue cafe"
      - Shares trigrams with the query ('blu', 'lue', 'caf', 'afe', ...)
      - Provides the second unique place needed to test LIMIT behavior
    """
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            id VARCHAR PRIMARY KEY,
            geometry GEOMETRY,
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            names STRUCT("primary" VARCHAR),
            categories STRUCT("primary" VARCHAR),
            addresses STRUCT(
                country VARCHAR,
                postcode VARCHAR,
                locality VARCHAR,
                freeform VARCHAR,
                region VARCHAR
            )[],
            websites VARCHAR[],
            socials VARCHAR[],
            emails VARCHAR[],
            phones VARCHAR[],
            brand STRUCT(names STRUCT("primary" VARCHAR)),
            confidence DOUBLE,
            version INTEGER,
            sources STRUCT(property VARCHAR, dataset VARCHAR, record_id VARCHAR, confidence DOUBLE)[],
            importance INTEGER,
            variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT []
        )
    """)

    # Place ov001: "Blue Cafe" with variant "Cafe Bleu"
    conn.execute("""
        INSERT INTO places VALUES (
            'ov001',
            ST_Point(-122.4194, 37.7749),
            {'xmin': -122.421, 'ymin': 37.773, 'xmax': -122.418, 'ymax': 37.776},
            {'primary': 'Blue Cafe'},
            {'primary': NULL},
            [{'country': 'US', 'postcode': '94103', 'locality': 'San Francisco',
              'freeform': '1 Main St', 'region': 'US-CA'}],
            NULL, NULL, NULL, NULL, NULL,
            0.9, 1, NULL,
            80,
            [{'name': 'Cafe Bleu', 'type': 'alternate', 'language': 'fr'}]
            ::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
        )
    """)

    # Place ov002: "Blue Cafe Express" — scores 0.953 blended for "blue cafe"
    conn.execute("""
        INSERT INTO places VALUES (
            'ov002',
            ST_Point(-122.4100, 37.7800),
            {'xmin': -122.412, 'ymin': 37.778, 'xmax': -122.408, 'ymax': 37.782},
            {'primary': 'Blue Cafe Express'},
            {'primary': NULL},
            [{'country': 'US', 'postcode': '94103', 'locality': 'San Francisco',
              'freeform': '2 Main St', 'region': 'US-CA'}],
            NULL, NULL, NULL, NULL, NULL,
            0.9, 1, NULL,
            75,
            []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
        )
    """)

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            id VARCHAR,
            name VARCHAR,
            norm_name VARCHAR,
            importance INTEGER,
            is_variant BOOLEAN DEFAULT FALSE
        )
    """)

    # Index "Blue Cafe" (primary) for ov001
    primary_name = "Blue Cafe"
    norm_primary = OvertureMaps._strip_accents(primary_name.lower())
    for trigram in _generate_trigrams(primary_name):
        conn.execute(
            "INSERT INTO name_index VALUES (?, 'ov001', ?, ?, 80, FALSE)",
            [trigram, primary_name, norm_primary],
        )

    # Index "Cafe Bleu" (variant) for ov001
    variant_name = "Cafe Bleu"
    norm_variant = OvertureMaps._strip_accents(variant_name.lower())
    for trigram in _generate_trigrams(variant_name):
        conn.execute(
            "INSERT INTO name_index VALUES (?, 'ov001', ?, ?, 80, TRUE)",
            [trigram, variant_name, norm_variant],
        )

    # Index "Blue Cafe Express" for ov002
    express_name = "Blue Cafe Express"
    norm_express = OvertureMaps._strip_accents(express_name.lower())
    for trigram in _generate_trigrams(express_name):
        conn.execute(
            "INSERT INTO name_index VALUES (?, 'ov002', ?, ?, 75, FALSE)",
            [trigram, express_name, norm_express],
        )

    conn.close()


# ---------------------------------------------------------------------------
# Session-scoped path fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def dedup_fsq_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("dedup_fsq") / "dedup_fsq.duckdb"
    _create_dedup_fsq_db(db_path)
    return db_path


@pytest.fixture(scope="session")
def dedup_osm_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("dedup_osm") / "dedup_osm.duckdb"
    _create_dedup_osm_db(db_path)
    return db_path


@pytest.fixture(scope="session")
def dedup_overture_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("dedup_overture") / "dedup_overture.duckdb"
    _create_dedup_overture_db(db_path)
    return db_path


# ---------------------------------------------------------------------------
# Function-scoped DB instance fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dedup_fsq_db(dedup_fsq_db_path):
    db = FoursquareOSP(dedup_fsq_db_path)
    db.connect()
    yield db
    db.close()


@pytest.fixture
def dedup_osm_db(dedup_osm_db_path):
    db = OpenStreetMap(dedup_osm_db_path)
    db.connect()
    yield db
    db.close()


@pytest.fixture
def dedup_overture_db(dedup_overture_db_path):
    db = OvertureMaps(dedup_overture_db_path)
    db.connect()
    yield db
    db.close()


# ---------------------------------------------------------------------------
# 1. No duplicate rkeys in results
# ---------------------------------------------------------------------------

class TestNoDuplicateRkeys:
    """Results must never contain duplicate rkeys."""

    def test_fsq_no_duplicate_rkeys_text_search(self, dedup_fsq_db):
        """
        Text search for 'blue cafe' against a DB where 'dd001' has both a primary
        name ('Blue Cafe') and a variant ('Cafe Bleu') that share trigrams with
        the query. Both names score above JW_THRESHOLD for this query.
        The result must NOT contain dd001 more than once.
        """
        results = dedup_fsq_db.nearest(q="blue cafe")
        rkeys = [r["rkey"] for r in results]
        assert rkeys.count("dd001") <= 1, (
            f"dd001 appears {rkeys.count('dd001')} times in results: {rkeys}"
        )

    def test_osm_no_duplicate_rkeys_text_search(self, dedup_osm_db):
        """
        Text search for 'tartine' against a DB where 'n240109189' has 4 name_index
        entries (primary + 3 variants). The result must NOT contain the rkey
        for that place more than once.
        OSM expand_rkey converts 'n240109189' → 'node:240109189' in process_record.
        """
        results = dedup_osm_db.nearest(q="tartine")
        rkeys = [r["rkey"] for r in results]
        # OSM rkeys are expanded: "n240109189" → "node:240109189"
        assert rkeys.count("node:240109189") <= 1, (
            f"node:240109189 appears {rkeys.count('node:240109189')} times in results: {rkeys}"
        )

    def test_overture_no_duplicate_rkeys_text_search(self, overture_db):
        """
        Text search for 'de coit tower' against the Overture DB where 'ovr003' has
        both a primary name ('Coit Tower', blended=0.928) and a French variant
        ('Tour de Coit', blended=0.833) that both score above the 0.6 JW threshold.
        The result must NOT contain ovr003 more than once.
        """
        results = overture_db.nearest(q="de coit tower")
        rkeys = [r["rkey"] for r in results]
        assert rkeys.count("ovr003") <= 1, (
            f"ovr003 appears {rkeys.count('ovr003')} times in results: {rkeys}"
        )


# ---------------------------------------------------------------------------
# 2. LIMIT is honored (duplicates must not eat limit slots)
# ---------------------------------------------------------------------------

class TestLimitHonored:
    """When duplicates are present, LIMIT must reflect unique place count."""

    def test_fsq_limit_reflects_unique_places(self, dedup_fsq_db):
        """
        The dedup DB has 2 unique places. With limit=2 and dd001 matching both
        its primary name ('Blue Cafe') and variant ('Cafe Bleu'), the query
        'cafe bleu blue' ranks the variant (blended=0.964) above the primary
        (blended=0.885), and both rank above dd002 'Blue Cafe Express' (0.860).
        Without dedup, limit=2 is consumed entirely by dd001's two entries,
        leaving dd002 out. With dedup, both dd001 and dd002 should appear.
        """
        results = dedup_fsq_db.nearest(q="cafe bleu blue", limit=2)
        rkeys = [r["rkey"] for r in results]
        assert len(rkeys) == len(set(rkeys)), (
            f"Duplicate rkeys detected with limit=2: {rkeys}"
        )
        # Both unique places should fit within the limit
        assert "dd001" in rkeys, f"dd001 missing from results: {rkeys}"
        assert "dd002" in rkeys, f"dd002 missing from results: {rkeys}"

    def test_overture_limit_not_consumed_by_duplicates(self, dedup_overture_db):
        """
        The dedup Overture DB has 2 unique places. Place 'ov001' has both a primary
        name ('Blue Cafe') and a variant ('Cafe Bleu') that both score above
        JW_THRESHOLD for query 'cafe bleu blue'.
        Query 'cafe bleu blue' ranks 'Cafe Bleu' (ov001 variant, blended=0.964)
        and 'Blue Cafe' (ov001 primary, blended=0.885) both above 'Blue Cafe
        Express' (ov002, blended=0.860). With limit=2, the SQL emits 2 duplicate
        rows for ov001 (one per name), consuming both limit slots. After the fix,
        dedup ensures only 1 slot is used for ov001, leaving room for ov002.
        """
        results = dedup_overture_db.nearest(q="cafe bleu blue", limit=2)
        rkeys = [r["rkey"] for r in results]
        assert len(rkeys) == len(set(rkeys)), (
            f"Duplicate rkeys in limit=2 results: {rkeys}"
        )
        # With duplicates gone, 2 unique places should fit
        assert len(results) == 2, (
            f"Expected 2 unique places with limit=2, got {len(results)}: {rkeys}"
        )


# ---------------------------------------------------------------------------
# 3. Best score wins when place has multiple name variants
# ---------------------------------------------------------------------------

class TestBestScoreWins:
    """When a place matches via multiple names, the highest score must be returned."""

    def test_fsq_best_score_returned_for_multi_variant_place(self, dedup_fsq_db):
        """
        'Blue Cafe' (primary) scores higher against query 'blue cafe' than
        'Cafe Bleu' (variant). The deduplicated result for dd001 should carry
        the higher score (from the primary name match), not a lower variant score.
        """
        results = dedup_fsq_db.nearest(q="blue cafe")
        dd001_results = [r for r in results if r["rkey"] == "dd001"]
        assert len(dd001_results) == 1, (
            f"Expected exactly 1 result for dd001, got {len(dd001_results)}"
        )
        # 'blue cafe' matches 'Blue Cafe' with JW ~1.0, 'Cafe Bleu' ~0.67
        # The deduplicated entry must carry the best (higher) score
        score = dd001_results[0].get("score")
        assert score is not None, "score field missing from result"
        # The best score (primary name) is much higher than the variant score
        # JW('blue cafe', 'cafe bleu') ≈ 0.67; JW('blue cafe', 'blue cafe') = 1.0
        # After fix, the deduplicated row should carry the higher score
        assert score > 0.9, (
            f"Expected best score > 0.9 for dd001 (primary name match), got score={score}"
        )

    def test_osm_best_score_returned_for_primary_name(self, dedup_osm_db):
        """
        Query 'tartine manufactory' should match primary name with high JW score.
        After dedup, only the best-scoring name variant's score is retained.
        OSM expand_rkey converts 'n240109189' → 'node:240109189' in process_record.
        """
        results = dedup_osm_db.nearest(q="tartine manufactory")
        # OSM rkeys are expanded: "n240109189" → "node:240109189"
        tartine_results = [r for r in results if r["rkey"] == "node:240109189"]
        assert len(tartine_results) == 1, (
            f"Expected exactly 1 result for node:240109189, got {len(tartine_results)}"
        )
        score = tartine_results[0].get("score")
        assert score is not None, "score field missing from result"
        # 'tartine manufactory' vs 'tartine manufactory' JW = 1.0
        # 'tartine manufactory' vs 'tartine mfy' JW ~ 0.75
        # Best score should be near 1.0
        assert score > 0.95, (
            f"Expected best score > 0.95 for node:240109189 (primary match), got {score}"
        )


# ---------------------------------------------------------------------------
# 4. Python safety net: nearest() deduplicates rkeys list
# ---------------------------------------------------------------------------

class TestPythonDedup:
    """
    Database.nearest() must deduplicate rkeys before building the metadata dict
    and calling query_hydrate. If the SQL returns duplicate rkeys, Python must
    still produce a correct (non-duplicate) result list.

    This test verifies the Python-level safety net described in the design doc:
    dedup `rkeys` list so that metadata dict keys are unique and the hydration
    loop does not attempt to pop 'distance_m' from the same dict twice.
    """

    def test_fsq_nearest_result_has_no_duplicate_rkeys(self, dedup_fsq_db):
        """
        nearest() result list must have no duplicate rkeys regardless of whether
        the SQL or Python layer is responsible for deduplication.
        """
        results = dedup_fsq_db.nearest(q="blue cafe")
        rkeys = [r["rkey"] for r in results]
        assert len(rkeys) == len(set(rkeys)), (
            f"nearest() returned duplicate rkeys: {rkeys}"
        )

    def test_osm_nearest_result_has_no_duplicate_rkeys(self, dedup_osm_db):
        """
        nearest() result list for a multi-variant place must have no duplicate rkeys.
        """
        results = dedup_osm_db.nearest(q="tartine")
        rkeys = [r["rkey"] for r in results]
        assert len(rkeys) == len(set(rkeys)), (
            f"nearest() returned duplicate rkeys: {rkeys}"
        )

    def test_overture_nearest_result_has_no_duplicate_rkeys(self, overture_db):
        """
        nearest() result list for Overture multi-variant place must have no
        duplicate rkeys.
        """
        results = overture_db.nearest(q="coit tower")
        rkeys = [r["rkey"] for r in results]
        assert len(rkeys) == len(set(rkeys)), (
            f"nearest() returned duplicate rkeys: {rkeys}"
        )

    def test_fsq_nearest_no_keyerror_on_duplicate_rkeys(self, dedup_fsq_db):
        """
        When the SQL returns duplicate rkeys, nearest() must not raise a KeyError.
        The bug manifests as KeyError when the second occurrence of a rkey tries
        to pop 'distance_m' from a dict that was already popped (because metadata
        dict key was overwritten by the second duplicate row, and then the
        ordered rkeys list tries to pop from it twice).

        This test exercises that code path by searching in a scenario guaranteed
        to produce duplicate rkeys from the SQL if no dedup is applied.
        """
        # This should not raise KeyError
        try:
            results = dedup_fsq_db.nearest(q="blue cafe")
        except KeyError as e:
            pytest.fail(
                f"nearest() raised KeyError (distance_m pop on duplicate rkey): {e}"
            )
        # And result must be a non-empty list (not silently empty)
        assert len(results) >= 1, "Expected at least 1 result for 'blue cafe'"
