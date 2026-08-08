"""Tests for pending quadtree pipeline SQL fixes (regression guards)."""

import re

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    run_overture_import, run_osm_import,
    make_tile_assignment_db, run_tile_assignments,
    write_minimal_overture_parquet,
)


def _strip_sql_comments(sql: str) -> str:
    """Strip SQL '--' line comments and '/* ... */' block comments.

    This is the single shared implementation used by every structural test
    in this module that pattern-matches raw SQL text against
    garganorn/sql/*.sql. Route every such test through this function (or
    through a helper — like TestOvertureVariantsCteSpillFix._sql() — that
    calls it) rather than re-implementing comment stripping per test: a
    rewrite's explanatory comments (e.g. describing what a fix replaced, or
    why a base CTE is scanned only once) must never be able to cause a
    spurious pass or fail.
    """
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


# ---------------------------------------------------------------------------
# Tests: quadtree pipeline SQL fixes (Red phase — fail against current code)
# ---------------------------------------------------------------------------

class TestQk17PipelineFixes:
    """Red-phase tests for pending fixes to the quadtree pipeline SQL files.

    Fix 1 — overture_place_import.sql: qk17 should be computed inline in the CTAS
             SELECT list; no ALTER TABLE or UPDATE statements.

    Fix 2 — compute_tile_assignments.sql: place_zoom should be an inline CTE,
             not a CREATE TEMP TABLE statement.

    Fix 3 — compute_tile_assignments.sql: tile_assignments CTAS should have
             ORDER BY tile_qk so the output is sorted.

    Fix 4 — overture_place_export_tiles.sql: place_addresses TEMP TABLE eliminated;
             addresses rendered inline via list_transform/list_filter in the VIEW.

    OSM Fix — osm_import.sql: qk17 should be in the CREATE TABLE schema and
              computed inline in each INSERT SELECT; no ALTER TABLE ADD COLUMN.

    Every test here FAILS against the current SQL files and PASSES after the
    corresponding fix is applied.
    """

    # ------------------------------------------------------------------
    # Fix 1: overture_place_import.sql — qk17 inline in CTAS, no ALTER/UPDATE
    # ------------------------------------------------------------------

    def test_fix1_overture_import_no_alter_table(self):
        """overture_place_import.sql must NOT contain ALTER TABLE after fix.

        Currently the script adds qk17 via ALTER TABLE + UPDATE after the CTAS.
        After the fix, qk17 is computed inline in the CTAS SELECT list.
        FAILS until the ALTER TABLE statement is removed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"
        sql = sql_path.read_text()
        assert "ALTER TABLE" not in sql.upper(), (
            "overture_place_import.sql still contains ALTER TABLE. "
            "Fix: compute qk17 inline in the CTAS SELECT list and remove "
            "the ALTER TABLE / UPDATE block."
        )

    def test_fix1_overture_import_no_update_qk17(self):
        """overture_place_import.sql must NOT contain UPDATE places SET qk17 after fix.

        FAILS until the UPDATE statement is removed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"
        sql = sql_path.read_text()
        assert "UPDATE PLACES SET QK17" not in sql.upper(), (
            "overture_place_import.sql still contains 'UPDATE places SET qk17'. "
            "Fix: compute qk17 inline in the CTAS SELECT list."
        )

    def test_fix1_overture_import_qk17_nonnull(self, overture_parquet, tmp_path):
        """After overture_place_import.sql runs, all rows must have a non-null qk17.

        This is a green regression guard: it passes both before and after the
        fix because both the current ALTER TABLE + UPDATE approach and the
        post-fix inline CTAS approach produce non-null qk17 values. It ensures
        that the inline approach doesn't accidentally leave qk17 null.
        """
        db_path = tmp_path / "test_fix1_qk17_nonnull.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        assert "qk17" in cols, "qk17 column missing from places after import"
        null_count = conn.execute(
            "SELECT count(*) FROM places WHERE qk17 IS NULL"
        ).fetchone()[0]
        conn.close()
        assert null_count == 0, (
            f"Expected 0 null qk17 values after fix; got {null_count}. "
            "Inline CTAS must compute qk17 for every row."
        )

    # ------------------------------------------------------------------
    # Fix 2: compute_tile_assignments.sql — place_zoom as inline CTE
    # ------------------------------------------------------------------

    def test_fix2_tile_assignments_no_create_temp_place_zoom(self):
        """compute_tile_assignments.sql must NOT contain CREATE TEMP TABLE place_zoom.

        Currently the script materializes place_zoom as a TEMP TABLE.
        After the fix, place_zoom is an inline CTE inside the tile_assignments CTAS.
        FAILS until CREATE TEMP TABLE place_zoom is removed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "compute_tile_assignments.sql"
        sql = sql_path.read_text()
        assert "CREATE TEMP TABLE place_zoom" not in sql, (
            "compute_tile_assignments.sql still contains 'CREATE TEMP TABLE place_zoom'. "
            "Fix: convert place_zoom to an inline CTE inside the tile_assignments CTAS."
        )

    # ------------------------------------------------------------------
    # Fix 3: compute_tile_assignments.sql — tile_assignments sorted by tile_qk
    # ------------------------------------------------------------------

    def test_fix3_tile_assignments_sql_has_order_by(self):
        """Structural guard: compute_tile_assignments.sql must have ORDER BY tile_qk in the CTAS."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "compute_tile_assignments.sql"
        sql = sql_path.read_text()
        assert "ORDER BY tile_qk" in sql, (
            "compute_tile_assignments.sql missing ORDER BY tile_qk in tile_assignments CTAS."
        )

    def test_fix3_tile_assignments_ordered_by_tile_qk(self, tmp_path):
        """tile_assignments rows must be sorted by tile_qk after fix.

        The fix adds ORDER BY tile_qk to the tile_assignments CTAS so the
        output is sorted for deterministic downstream processing.
        FAILS until ORDER BY tile_qk is added to the CTAS.
        """
        places = [
            # Choose places in different quadkey regions so ordering is testable.
            ("p_nyc",  40.7128, -74.0060),   # NYC — starts with '0'
            ("p_sf1",  37.7749, -122.4194),   # SF  — starts with '0' but different prefix
            ("p_sf2",  37.7750, -122.4195),
            ("p_lon",  51.5074,  -0.1278),    # London — different prefix
        ]

        db_path = tmp_path / "test_fix3_sorted.duckdb"
        conn = duckdb.connect(str(db_path))
        make_tile_assignment_db(conn, places)
        run_tile_assignments(conn, pk_expr="place_id", min_zoom=6, max_zoom=17, max_per_tile=10)

        rows = conn.execute("SELECT tile_qk FROM tile_assignments").fetchall()
        conn.close()

        qk_values = [row[0] for row in rows]
        assert qk_values == sorted(qk_values), (
            f"tile_assignments is not sorted by tile_qk. "
            f"Got order: {qk_values}. "
            "Fix: add ORDER BY tile_qk to the tile_assignments CTAS."
        )

    # ------------------------------------------------------------------
    # Fix 4: overture_place_export_tiles.sql — eliminate place_addresses TEMP TABLE
    # ------------------------------------------------------------------

    def test_no_place_addresses_temp_table(self):
        """overture_place_export_tiles.sql must NOT define place_addresses as a TEMP TABLE.

        After the fix, place_addresses is eliminated entirely and replaced with
        inline list_transform/list_filter in the VIEW. This test FAILS until the
        TEMP TABLE is removed from the SQL file.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_place_export_tiles.sql"
        sql = sql_path.read_text()
        assert "CREATE TEMP TABLE place_addresses" not in sql, (
            "overture_place_export_tiles.sql still defines place_addresses as a TEMP TABLE. "
            "Fix: remove the TEMP TABLE and use inline list_transform/list_filter in the VIEW."
        )

    # ------------------------------------------------------------------
    # OSM Fix: osm_import.sql — qk17 inline, no ALTER TABLE ADD COLUMN
    # ------------------------------------------------------------------

    def test_osm_fix_no_alter_table_add_column_qk17(self):
        """osm_import.sql must NOT contain ALTER TABLE places ADD COLUMN qk17.

        Currently the script adds qk17 via ALTER TABLE after the INSERT statements.
        After the fix, qk17 is in the CREATE TABLE schema and computed inline
        in each INSERT SELECT list.
        FAILS until the ALTER TABLE ADD COLUMN qk17 statement is removed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "osm_import.sql"
        sql = sql_path.read_text()
        assert "ALTER TABLE places ADD COLUMN qk17" not in sql, (
            "osm_import.sql still contains 'ALTER TABLE places ADD COLUMN qk17'. "
            "Fix: add qk17 to the CREATE TABLE schema and compute it inline "
            "in each INSERT SELECT list."
        )
        assert "UPDATE PLACES SET QK17" not in sql.upper(), (
            "osm_import.sql still contains UPDATE places SET qk17 — inline qk17 in INSERT SELECT lists instead."
        )

    def test_osm_fix_qk17_nonnull(self, tmp_path):
        """After osm_import.sql runs against a minimal synthetic fixture, all qk17 values must be non-null.

        This is a green regression guard: it passes both before and after the fix because
        both the current ALTER TABLE + UPDATE approach and the post-fix inline approach
        produce non-null qk17 values.
        """
        import duckdb as _duckdb

        base = tmp_path / "osm_fix_qk17_nonnull"
        base.mkdir()
        node_path = base / "node_data.parquet"
        way_path = base / "way_data.parquet"

        conn = _duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
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
                (1, map(['name','amenity'], ['Test Cafe','cafe']),        37.7612, -122.4195),
                (2, map(['name','leisure'], ['Test Park','park']),        37.7596, -122.4269),
                (3, map(['name','shop'],    ['Test Shop','bakery']),      37.7700, -122.4100)
        """)
        conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")
        conn.execute("""
            CREATE TABLE tmp_ways (
                id   BIGINT,
                tags MAP(VARCHAR, VARCHAR),
                nds  STRUCT(ref BIGINT)[]
            )
        """)
        conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
        conn.close()

        db_path = tmp_path / "test_osm_fix_qk17_nonnull.duckdb"
        conn2 = _duckdb.connect(str(db_path))
        conn2.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn2, str(node_path), str(way_path))
        null_count = conn2.execute(
            "SELECT count(*) FROM places WHERE qk17 IS NULL"
        ).fetchone()[0]
        conn2.close()
        assert null_count == 0, (
            f"Expected 0 null qk17 values after osm_import; got {null_count}. "
            "Both the current UPDATE approach and the post-fix inline approach must produce non-null qk17."
        )


# ---------------------------------------------------------------------------
# Fix 5: overture_place_import.sql — eliminate ov_base re-materialization spill
# ---------------------------------------------------------------------------
#
# Background: ov_base is currently referenced five times beyond its own
# definition (ov_density, ov_idf, common_entries, rule_entries, and the
# final SELECT). Each reference forces DuckDB to re-materialize the full
# width of ov_base (every Overture column) to answer a join or unnest,
# which on the production 22M-row Overture places extract spills >60GB of
# temp disk (a plain scan of the same rows spills 0 bytes). The fix folds
# the density/idf joins directly into the final SELECT and computes
# `variants` as a per-row list expression (list_transform/list_concat/
# list_filter/list_sort) with no unnest, no GROUP BY, and no join-back —
# so ov_base is scanned once.
#
# The structural tests below pin the absence of the spill-causing pattern.
# They FAIL against the current SQL and must PASS once the rewrite lands.
# The characterization tests pin the *semantics* of the variants column as
# currently produced, so the rewrite cannot silently change behavior.
# ---------------------------------------------------------------------------

class TestOvertureVariantsCteSpillFix:
    """Red-phase structural tests for the ov_base/variants CTE-spill fix.

    The ov_base-reference-count and forbidden-helper-CTE checks that used
    to live here have moved to TestImportSqlD6Enforcement below, which
    parametrizes the same checks over a list of import SQL files (D6,
    docs/design-constraints.md D6) instead of hand-writing them
    per file. The tests remaining here (no unnest on names.common/rules, no
    GROUP BY id) are specific to how Overture's `variants` column is
    derived and have no OSM equivalent, so they stay Overture-specific.
    """

    SQL_PATH = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"

    def _sql(self):
        # Comments stripped via the single shared helper (_strip_sql_comments)
        # so every test below is automatically protected — see that
        # function's docstring for why.
        return _strip_sql_comments(self.SQL_PATH.read_text())

    def test_no_unnest_on_names_common(self):
        """No unnest(...) applied to names.common.

        Currently `unnest(map_entries(names.common))` is used in a comma-join
        lateral against ov_base to explode names.common into rows, which
        forces a full re-materialization of ov_base for that join. The fix
        computes variants as a per-row list expression with no unnest.
        FAILS until the unnest(map_entries(names.common)) pattern is removed.
        """
        sql = self._sql()
        assert not re.search(r"unnest\s*\(\s*map_entries\s*\(\s*names\.common", sql, re.IGNORECASE), (
            "overture_place_import.sql still applies unnest() to names.common via "
            "map_entries(). This forces DuckDB to re-materialize the full width of "
            "ov_base for the comma-join lateral, which is the source of the >60GB "
            "temp-disk spill on the full Overture places scale. Fix: compute the "
            "common-name variants as a per-row list expression (e.g. "
            "list_transform over map_entries(names.common)) instead of unnesting."
        )

    def test_no_unnest_on_names_rules(self):
        """No unnest(...) applied to names.rules.

        Currently `unnest(names.rules)` is used in a comma-join lateral
        against ov_base, with the same full re-materialization cost as the
        names.common unnest. FAILS until removed.
        """
        sql = self._sql()
        assert not re.search(r"unnest\s*\(\s*names\.rules", sql, re.IGNORECASE), (
            "overture_place_import.sql still applies unnest() to names.rules. "
            "This forces DuckDB to re-materialize the full width of ov_base for "
            "the comma-join lateral, contributing to the >60GB temp-disk spill "
            "on the full Overture places scale. Fix: compute the rules-name "
            "variants as a per-row list expression (e.g. list_transform over "
            "names.rules) instead of unnesting."
        )

    def test_no_group_by_id(self):
        """No GROUP BY id.

        Currently ov_variants aggregates the unnested/unioned name rows back
        into a per-id list via `GROUP BY id`, which requires a full shuffle/
        sort of all exploded rows keyed by id — the single largest
        contributor to the temp-disk spill. The fix computes variants
        per-row (no explosion, so no re-aggregation is needed).
        FAILS until GROUP BY id is removed.
        """
        sql = self._sql()
        assert not re.search(r"GROUP\s+BY\s+id\b", sql, re.IGNORECASE), (
            "overture_place_import.sql still contains 'GROUP BY id'. This forces "
            "a full shuffle/sort of every exploded name row keyed by id, which is "
            "the largest single contributor to the >60GB temp-disk spill on the "
            "full Overture places scale. Fix: compute variants as a per-row list "
            "expression so no re-aggregation by id is needed."
        )

    # test_helper_cte_not_defined and test_ov_base_referenced_at_most_once_
    # outside_definition used to live here. They are now
    # TestImportSqlD6Enforcement.test_no_forbidden_helper_cte and
    # .test_base_cte_referenced_at_most_once_outside_definition below,
    # parametrized over IMPORT_SQL_FILES (which includes
    # overture_place_import.sql with the same base CTE / forbidden-CTE
    # values used here previously, plus osm_import.sql).


# ---------------------------------------------------------------------------
# D6 enforcement ledger: "No unbounded complex-state aggregation... a hard
# review criterion for every SQL file" (docs/design-constraints.md D6).
#
# D6 was stated as a design constraint but never coded as a test, and never
# tied to a specific, enumerable list of files — so it was never actually
# enforced. IMPORT_SQL_FILES below is that list for the two files known to
# violate it (overture_place_import.sql, osm_import.sql): each has one or
# more base CTE(s) that scan the raw parquet input in full, referenced
# multiple times beyond their own definition by narrow-projection
# "helper" CTEs (density/idf/variant scores) that get LEFT JOINed back on
# a computed key — forcing DuckDB to re-materialize the full-width base CTE
# once per reference, which is what spills temp disk at production scale.
#
# EXEMPT_IMPORT_SQL_FILES is a visible ledger of known debt, not a silent
# omission: every *_import.sql file under garganorn/sql/ must appear in
# exactly one of these two lists (test_all_import_sql_files_are_classified
# enforces this), and every exemption must carry a stated reason
# (test_exempt_files_have_a_reason enforces this). A new import SQL file
# added later is therefore forced to make a deliberate choice.
# ---------------------------------------------------------------------------

IMPORT_SQL_FILES = [
    {
        "filename": "overture_place_import.sql",
        # Base CTE(s) that scan the raw parquet input; each must be
        # referenced at most once beyond its own definition.
        "base_ctes": ["ov_base"],
        # Narrow-projection CTEs that re-scan a base CTE and get LEFT
        # JOINed back on a computed key — the specific pattern that forces
        # full re-materialization and causes the temp-disk spill.
        "forbidden_helper_ctes": [
            "ov_density", "ov_idf", "common_entries", "rule_entries",
            "all_variants", "ov_variants",
        ],
    },
    {
        "filename": "osm_import.sql",
        # filtered (nodes) and way_base (ways) are the two base CTEs;
        # each is currently referenced 4x beyond its own definition
        # (node_density/node_idf/final SELECT for filtered;
        # way_density/way_idf/final SELECT for way_base).
        "base_ctes": ["filtered", "way_base"],
        "forbidden_helper_ctes": [
            "node_density", "node_idf", "way_density", "way_idf",
        ],
    },
]

EXEMPT_IMPORT_SQL_FILES = [
    {
        "filename": "overture_division_import.sql",
        "reason": (
            "division_density aggregates with a scalar avg(density_score) "
            "over ~1M division rows — not list()/string_agg() over exploded "
            "per-row state. A scalar avg accumulator is O(1) per group and "
            "spills gracefully, so this does not exhibit the D6 anti-pattern "
            "(unbounded complex-state aggregation) that overture_place_import.sql "
            "and osm_import.sql have."
        ),
    },
]


class TestImportSqlD6Enforcement:
    """D6 (docs/design-constraints.md) as executable tests,
    parametrized over IMPORT_SQL_FILES rather than hand-written per file.

    FAILS now for both overture_place_import.sql and osm_import.sql (both
    currently violate D6); must PASS after each file's spill fix lands.
    """

    def test_all_import_sql_files_are_classified(self):
        """Every garganorn/sql/*_import.sql file must be enforced or exempt.

        Guards against a new import SQL file silently skipping D6 review:
        adding one requires either adding it to IMPORT_SQL_FILES (enforced)
        or to EXEMPT_IMPORT_SQL_FILES with a stated reason.
        """
        sql_dir = REPO_ROOT / "garganorn" / "sql"
        on_disk = {p.name for p in sql_dir.glob("*_import.sql")}
        enforced = {entry["filename"] for entry in IMPORT_SQL_FILES}
        exempt = {entry["filename"] for entry in EXEMPT_IMPORT_SQL_FILES}
        classified = enforced | exempt

        unclassified = on_disk - classified
        assert not unclassified, (
            f"Import SQL file(s) {sorted(unclassified)} under garganorn/sql/ "
            "are not classified in IMPORT_SQL_FILES or EXEMPT_IMPORT_SQL_FILES. "
            "D6 requires a deliberate choice for every import SQL file: "
            "enforce it, or exempt it with a stated reason."
        )

        stale = classified - on_disk
        assert not stale, (
            f"IMPORT_SQL_FILES/EXEMPT_IMPORT_SQL_FILES reference file(s) that "
            f"no longer exist under garganorn/sql/: {sorted(stale)}. Remove "
            "the stale entry."
        )

    def test_exempt_files_have_a_reason(self):
        """Every EXEMPT_IMPORT_SQL_FILES entry must state why it's exempt."""
        for entry in EXEMPT_IMPORT_SQL_FILES:
            assert entry.get("reason", "").strip(), (
                f"{entry['filename']} is exempt from D6 enforcement but has no "
                "stated reason. The exemption list must be a visible ledger of "
                "known debt, not a silent omission."
            )

    @pytest.mark.parametrize(
        "filename,base_cte",
        [
            (entry["filename"], base_cte)
            for entry in IMPORT_SQL_FILES
            for base_cte in entry["base_ctes"]
        ],
    )
    def test_base_cte_referenced_at_most_once_outside_definition(self, filename, base_cte):
        """<base_cte> in <filename> must be referenced at most once beyond its own definition.

        Each reference beyond the definition forces DuckDB to re-materialize
        the full width of the base CTE (every source column) to answer that
        join, which is what causes the temp-disk spill at production scale.
        Comments are stripped first (via the shared _strip_sql_comments) so
        an explanatory comment about why the base CTE is scanned once cannot
        itself count as a reference.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / filename
        sql = _strip_sql_comments(sql_path.read_text())
        occurrences = len(re.findall(rf"\b{re.escape(base_cte)}\b", sql, re.IGNORECASE))
        # 1 occurrence is the CTE's own definition; at most 1 more occurrence
        # (typically the final SELECT) is allowed.
        assert occurrences <= 2, (
            f"{filename} references '{base_cte}' {occurrences} times; expected "
            "at most 2 (its own definition + at most one more use, typically "
            "the final SELECT). Fold every other computation (density, idf, "
            f"variants, ...) into that single final SELECT so '{base_cte}' is "
            "scanned once."
        )

    @pytest.mark.parametrize(
        "filename,cte_name",
        [
            (entry["filename"], cte_name)
            for entry in IMPORT_SQL_FILES
            for cte_name in entry["forbidden_helper_ctes"]
        ],
    )
    def test_no_forbidden_helper_cte(self, filename, cte_name):
        """<cte_name> must not be defined as a CTE in <filename>.

        This class of CTE re-reads a base CTE in full to compute a narrow
        result (a density/idf score or a name list) that is then LEFT
        JOINed back onto the base CTE by a computed key. Fold this logic
        into the final SELECT instead, so the base CTE is not re-scanned
        and no join-back is needed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / filename
        sql = _strip_sql_comments(sql_path.read_text())
        assert not re.search(rf"\b{re.escape(cte_name)}\s+AS\s*\(", sql, re.IGNORECASE), (
            f"{filename} still defines a '{cte_name}' CTE. Fold its logic "
            "into the final SELECT so the base CTE is not re-scanned and no "
            "join-back by a computed key is needed."
        )


class TestOvertureVariantsCharacterization:
    """Green-phase characterization tests pinning current `variants` semantics.

    These tests run the CURRENT overture_place_import.sql against fixture
    rows added to the `overture_parquet` fixture (ov010-ov016 in
    tests/conftest.py) and assert on the exact `variants` value observed.
    They must PASS both before and after the ov_base/variants CTE-spill fix
    (Fix 5 above) — the fix must not change what `variants` computes, only
    how it computes it.
    """

    def _variants_for(self, conn, place_id):
        row = conn.execute(
            "SELECT variants FROM places WHERE id = ?", [place_id]
        ).fetchone()
        assert row is not None, f"place id {place_id!r} not found in places"
        return row[0]

    def test_rules_only_official_mapping(self, overture_parquet, tmp_path):
        """ov010: names.common IS NULL, names.rules has one 'official' entry.

        Pins: common-NULL/rules-populated combination, and the
        'official' -> 'official' variant-type mapping.
        """
        db_path = tmp_path / "test_variants_ov010.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov010")
        conn.close()
        assert variants == [
            {"name": "Rules Value", "type": "official", "language": "en"},
        ]

    def test_common_only_defaults_to_alternate(self, overture_parquet, tmp_path):
        """ov011: names.common has one entry, names.rules IS NULL.

        Pins: common-populated/rules-NULL combination, and that every
        names.common entry is typed 'alternate'.
        """
        db_path = tmp_path / "test_variants_ov011.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov011")
        conn.close()
        assert variants == [
            {"name": "Nom Commun", "type": "alternate", "language": "fr"},
        ]

    def test_all_rule_variant_types_mapping(self, overture_parquet, tmp_path):
        """ov012: names.rules covers common/official/alternate/short plus an
        unrecognized variant string ('colloquial').

        Pins the full CASE mapping: common->alternate, official->official,
        alternate->alternate, short->short, anything else (colloquial)
        ->alternate. Names are distinct so ORDER BY name has no ties here;
        tie behavior is covered separately by test_order_and_name_tie.
        """
        db_path = tmp_path / "test_variants_ov012.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov012")
        conn.close()
        assert variants == [
            {"name": "Alpha Official", "type": "official", "language": "en"},
            {"name": "Beta Colloquial", "type": "alternate", "language": "en"},
            {"name": "Mid Alternate", "type": "alternate", "language": "en"},
            {"name": "Omega Short", "type": "short", "language": "en"},
            {"name": "Zeta Common", "type": "alternate", "language": "en"},
        ]

    def test_null_and_empty_values_excluded(self, overture_parquet, tmp_path):
        """ov013: every candidate variant value is NULL or ''.

        Pins that NULL values and empty-string values are excluded from
        variants regardless of source (names.common or names.rules), and
        that the result is exactly [] (not NULL) even though both source
        fields are populated with (filtered-out) entries.
        """
        db_path = tmp_path / "test_variants_ov013.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov013")
        conn.close()
        assert variants == []

    def test_neither_common_nor_rules_yields_empty_list_not_null(self, overture_parquet, tmp_path):
        """ov004: names present with common=empty map, rules=empty list.

        ov004 is an existing fixture row (unmodified) already shaped this
        way. Pins that variants is exactly [] and NOT NULL when there are no
        candidate entries at all.
        """
        db_path = tmp_path / "test_variants_ov004.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov004")
        conn.close()
        assert variants == [], f"expected [], got {variants!r}"
        assert variants is not None

    def test_names_struct_itself_null_yields_empty_list_not_null(self, overture_parquet, tmp_path):
        """ov003: names IS NULL entirely (existing fixture row, unmodified).

        Pins that variants is exactly [] and NOT NULL even when the whole
        `names` struct is SQL NULL, not just its common/rules fields.
        """
        db_path = tmp_path / "test_variants_ov003.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov003")
        conn.close()
        assert variants == [], f"expected [], got {variants!r}"
        assert variants is not None

    def test_order_and_name_tie(self, overture_parquet, tmp_path):
        """ov014: names from both names.common and names.rules, including two
        entries that share the same name ('Golden Gate').

        Pins the stable property (ORDER BY name) rather than a fixed order
        between tied entries, since the tie-break order between two rows
        with the same name is not specified by the current SQL (UNION ALL
        followed by ORDER BY name only) and is not guaranteed stable across
        DuckDB versions/plans. Non-tied entries ('Porte Doree', 'Zelle') do
        have a fully-determined order and are asserted exactly.
        """
        db_path = tmp_path / "test_variants_ov014.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov014")
        conn.close()

        assert len(variants) == 4, f"expected 4 variants, got {len(variants)}: {variants}"

        names = [v["name"] for v in variants]
        assert names == sorted(names), (
            f"variants must be ordered by name; got names in order: {names}"
        )

        # The two 'Golden Gate' entries (one from names.common, one from
        # names.rules) must sort first (alphabetically before 'Porte Doree'
        # and 'Zelle'), but their relative order to each other is NOT
        # asserted — only that both are present, as a set of (type, language).
        tied = variants[0:2]
        assert {v["name"] for v in tied} == {"Golden Gate"}
        assert {(v["type"], v["language"]) for v in tied} == {
            ("alternate", "en"),
            ("official", "es"),
        }

        assert variants[2] == {"name": "Porte Doree", "type": "alternate", "language": "fr"}
        assert variants[3] == {"name": "Zelle", "type": "short", "language": "de"}

    def test_duplicate_tuple_within_rules_not_deduped(self, overture_parquet, tmp_path):
        """ov015: the same (name, type, language) tuple appears twice within
        names.rules alone.

        Pins that variants does NOT dedupe: nothing in the current SQL
        (rule_entries -> all_variants UNION ALL -> ov_variants list(...))
        applies DISTINCT, so both identical entries survive into variants.
        The planned rewrite uses list_concat with no DISTINCT either, so
        this must remain true after the fix — if the rewrite silently
        starts deduping, this test will fail and flag the behavior change.
        """
        db_path = tmp_path / "test_variants_ov015.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov015")
        conn.close()
        assert variants == [
            {"name": "Repeat Name", "type": "alternate", "language": "en"},
            {"name": "Repeat Name", "type": "alternate", "language": "en"},
        ], f"expected two identical (undeduped) entries; got {variants!r}"

    def test_duplicate_tuple_across_common_and_rules_not_deduped(self, overture_parquet, tmp_path):
        """ov016: the same (name, type, language) tuple arises once from
        names.common and once from an explicit names.rules 'alternate' entry.

        Pins that variants does NOT dedupe across the two source fields
        either: common_entries and rule_entries are combined with UNION ALL
        (not UNION), so a cross-source duplicate also survives.
        """
        db_path = tmp_path / "test_variants_ov016.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov016")
        conn.close()
        assert variants == [
            {"name": "Repeat Name", "type": "alternate", "language": "en"},
            {"name": "Repeat Name", "type": "alternate", "language": "en"},
        ], f"expected two identical (undeduped) entries; got {variants!r}"

    def test_struct_field_keys_and_order(self, overture_parquet, tmp_path):
        """Each variants element must have exactly the keys name/type/language,
        in that order (as produced by the current struct literal
        `{'name': ..., 'type': ..., 'language': ...}`).

        Checked against both a rules-derived variant (ov010) and a
        common-derived variant (ov011), since names.common and names.rules
        are built by separate CTEs (common_entries vs rule_entries) that
        could independently drift in field order.
        """
        db_path = tmp_path / "test_variants_struct_shape.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        rules_variants = self._variants_for(conn, "ov010")
        common_variants = self._variants_for(conn, "ov011")
        conn.close()

        assert len(rules_variants) == 1
        assert list(rules_variants[0].keys()) == ["name", "type", "language"], (
            f"rules-derived variant: expected struct keys in order "
            f"[name, type, language]; got {list(rules_variants[0].keys())}"
        )

        assert len(common_variants) == 1
        assert list(common_variants[0].keys()) == ["name", "type", "language"], (
            f"common-derived variant: expected struct keys in order "
            f"[name, type, language]; got {list(common_variants[0].keys())}"
        )


# ---------------------------------------------------------------------------
# Fix 5 (continued): density/idf join must not multiply place rows
# ---------------------------------------------------------------------------
#
# Background: run_overture_import() (tests/quadtree_helpers.py) always
# created density_tiles/idf_scores as EMPTY tables, so the density/idf
# LEFT JOINs were never exercised against real matching rows by any test.
# That matters because the fix folds those joins directly into the final
# SELECT: if the join key (tile_qk15, matched via
# `d.tile_qk15 = left(b.qk17, 15)`) is not guaranteed unique, a LEFT JOIN
# against a density_tiles table with a duplicate tile_qk15 key fans out
# the matching place row. No test anywhere in the suite asserts
# COUNT(*) FROM places or `id` uniqueness, so a fan-out bug would pass the
# whole suite silently.
#
# run_overture_import() now accepts optional density_rows/idf_rows to
# populate those temp tables with real data (default behavior unchanged:
# omitting them still creates empty tables, so every existing caller is
# unaffected).
# ---------------------------------------------------------------------------

class TestOvertureDensityJoinNoFanOut:
    """Red-phase regression guard: density_tiles join must not multiply rows.

    This is a real, currently-reproducible bug in overture_place_import.sql,
    not just a hypothetical risk from the rewrite: ov_density joins
    density_tiles to ov_base on `d.tile_qk15 = left(b.qk17, 15)` with no
    guarantee that tile_qk15 is unique in density_tiles. If it isn't, the
    LEFT JOIN produces one ov_density row per matching density_tiles row,
    and the final `LEFT JOIN ov_density od USING (id)` then fans that back
    out into duplicate `places` rows for the same id. The fix must not
    reproduce this: whether by construction (e.g. aggregating density_tiles
    by key before joining) or by strengthening the data contract, exactly
    one places row per input row is required.

    FAILS now: two input places (one matching a duplicated density_tiles
    key, one not) currently produce three places rows.
    """

    def test_row_count_and_id_uniqueness_preserved_with_duplicate_density_key(self, tmp_path):
        base = tmp_path / "dup_density_fixture"
        base.mkdir()
        ov_path = base / "ov_dup.parquet"

        # Two in-bbox places. fanA is placed so its qk17 (computed the same
        # way the import SQL computes it, from the bbox midpoint) is known
        # in advance so the duplicate density_tiles row is guaranteed to match.
        lon_a, lat_a = -122.419, 37.775
        lon_b, lat_b = -122.486, 37.769
        write_minimal_overture_parquet(ov_path, [
            ("fanA", lon_a, lat_a, "coffee_shop"),
            ("fanB", lon_b, lat_b, "park"),
        ])

        probe = duckdb.connect(":memory:")
        probe.execute("INSTALL spatial; LOAD spatial;")
        qk17_a = probe.execute(
            "SELECT ST_QuadKey(?, ?, 17)", [lon_a, lat_a]
        ).fetchone()[0]
        probe.close()
        tile_qk15 = qk17_a[:15]

        # Duplicate tile_qk15 key: two density_tiles rows for the same tile,
        # differing only in density_score. This is the specific scenario
        # that fans out the join.
        density_rows = [
            (tile_qk15, 5.0, -122.5, 37.7, -122.4, 37.8),
            (tile_qk15, 9.0, -122.5, 37.7, -122.4, 37.8),
        ]

        db_path = tmp_path / "test_dup_density.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, str(base / "*.parquet"), density_rows=density_rows)

        total = conn.execute("SELECT COUNT(*) FROM places").fetchone()[0]
        distinct_ids = conn.execute("SELECT COUNT(DISTINCT id) FROM places").fetchone()[0]
        dupes = conn.execute(
            "SELECT id, COUNT(*) FROM places GROUP BY id HAVING COUNT(*) > 1"
        ).fetchall()
        conn.close()

        assert total == 2, (
            f"expected 2 places rows (1 per input place; no fan-out), got {total}. "
            f"Duplicate ids with row counts: {dupes}. "
            "A duplicate tile_qk15 key in density_tiles must not multiply "
            "the matching place's row in the final output."
        )
        assert distinct_ids == 2, f"expected 2 distinct ids, got {distinct_ids}"
        assert total == distinct_ids, (
            f"id must be unique in places; got {total} rows but {distinct_ids} distinct ids: {dupes}"
        )


class TestOvertureIdfJoinNoFanOut:
    """Red-phase regression guard: idf_scores join must not multiply rows.

    Symmetric to TestOvertureDensityJoinNoFanOut above, for the idf side:
    ov_idf joins idf_scores to ov_base on `i.category = b.categories.primary`
    with no guarantee that category is unique in idf_scores. This is the
    same real, currently-reproducible bug as the density case (confirmed by
    running the current SQL, not assumed): a duplicate category key in
    idf_scores fans out the matching place's row through
    `LEFT JOIN ov_idf oi USING (id)` in the final SELECT.

    FAILS now: two input places (one matching a duplicated idf_scores
    category key, one not) currently produce three places rows.
    """

    def test_row_count_and_id_uniqueness_preserved_with_duplicate_idf_key(self, tmp_path):
        base = tmp_path / "dup_idf_fixture"
        base.mkdir()
        ov_path = base / "ov_dup.parquet"

        # Two in-bbox places; fanA's category matches the duplicated
        # idf_scores key, fanB's does not.
        lon_a, lat_a = -122.419, 37.775
        lon_b, lat_b = -122.486, 37.769
        write_minimal_overture_parquet(ov_path, [
            ("fanA", lon_a, lat_a, "coffee_shop"),
            ("fanB", lon_b, lat_b, "park"),
        ])

        # Duplicate category key: two idf_scores rows for the same category,
        # differing only in idf_score. This is the specific scenario that
        # fans out the join.
        idf_rows = [
            ("coffee_shop", 5.0),
            ("coffee_shop", 9.0),
        ]

        db_path = tmp_path / "test_dup_idf.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, str(base / "*.parquet"), idf_rows=idf_rows)

        total = conn.execute("SELECT COUNT(*) FROM places").fetchone()[0]
        distinct_ids = conn.execute("SELECT COUNT(DISTINCT id) FROM places").fetchone()[0]
        dupes = conn.execute(
            "SELECT id, COUNT(*) FROM places GROUP BY id HAVING COUNT(*) > 1"
        ).fetchall()
        conn.close()

        assert total == 2, (
            f"expected 2 places rows (1 per input place; no fan-out), got {total}. "
            f"Duplicate ids with row counts: {dupes}. "
            "A duplicate category key in idf_scores must not multiply "
            "the matching place's row in the final output."
        )
        assert distinct_ids == 2, f"expected 2 distinct ids, got {distinct_ids}"
        assert total == distinct_ids, (
            f"id must be unique in places; got {total} rows but {distinct_ids} distinct ids: {dupes}"
        )
