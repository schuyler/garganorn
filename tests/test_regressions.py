"""Tests for pending quadtree pipeline SQL fixes (regression guards)."""

import re

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    run_overture_import, run_osm_import,
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
# Tests: quadtree pipeline SQL invariants
# ---------------------------------------------------------------------------

class TestQk17PipelineFixes:
    """Tests pinning how qk17 is computed in the quadtree pipeline SQL files.

    overture_place_import.sql: qk17 is computed inline in the CTAS
        SELECT list; no ALTER TABLE or UPDATE statements.

    overture_place_export_tiles.sql: no place_addresses TEMP TABLE;
        addresses are rendered inline via list_transform/list_filter in
        the VIEW.

    osm_import.sql: qk17 is in the CREATE TABLE schema and computed
        inline in each INSERT SELECT; no ALTER TABLE ADD COLUMN.
    """

    # ------------------------------------------------------------------
    # overture_place_import.sql — qk17 inline in CTAS, no ALTER/UPDATE
    # ------------------------------------------------------------------

    def test_fix1_overture_import_no_alter_table(self):
        """overture_place_import.sql must NOT contain ALTER TABLE."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"
        sql = sql_path.read_text()
        assert "ALTER TABLE" not in sql.upper(), (
            "overture_place_import.sql still contains ALTER TABLE. "
            "Fix: compute qk17 inline in the CTAS SELECT list and remove "
            "the ALTER TABLE / UPDATE block."
        )

    def test_fix1_overture_import_no_update_qk17(self):
        """overture_place_import.sql must NOT contain UPDATE places SET qk17."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"
        sql = sql_path.read_text()
        assert "UPDATE PLACES SET QK17" not in sql.upper(), (
            "overture_place_import.sql still contains 'UPDATE places SET qk17'. "
            "Fix: compute qk17 inline in the CTAS SELECT list."
        )

    def test_fix1_overture_import_qk17_nonnull(self, overture_parquet, tmp_path):
        """After overture_place_import.sql runs, all rows must have a non-null qk17."""
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
            f"Expected 0 null qk17 values; got {null_count}. "
            "Inline CTAS must compute qk17 for every row."
        )

    # ------------------------------------------------------------------
    # overture_place_export_tiles.sql — no place_addresses TEMP TABLE
    # ------------------------------------------------------------------

    def test_no_place_addresses_temp_table(self):
        """overture_place_export_tiles.sql must NOT define place_addresses as a TEMP TABLE.

        Addresses are rendered inline via list_transform/list_filter in the VIEW.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_place_export_tiles.sql"
        sql = sql_path.read_text()
        assert "CREATE TEMP TABLE place_addresses" not in sql, (
            "overture_place_export_tiles.sql still defines place_addresses as a TEMP TABLE. "
            "Fix: remove the TEMP TABLE and use inline list_transform/list_filter in the VIEW."
        )

    # ------------------------------------------------------------------
    # osm_import.sql — qk17 inline, no ALTER TABLE ADD COLUMN
    # ------------------------------------------------------------------

    def test_osm_fix_no_alter_table_add_column_qk17(self):
        """osm_import.sql must NOT contain ALTER TABLE places ADD COLUMN qk17.

        qk17 is in the CREATE TABLE schema and computed inline in each
        INSERT SELECT list.
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

    def test_osm_fix_qk17_nonnull(self, tmp_path, empty_relation_parquet):
        """After osm_import.sql runs against a minimal synthetic fixture, all qk17 values must be non-null."""
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
        run_osm_import(conn2, str(node_path), str(way_path), empty_relation_parquet)
        null_count = conn2.execute(
            "SELECT count(*) FROM places WHERE qk17 IS NULL"
        ).fetchone()[0]
        conn2.close()
        assert null_count == 0, (
            f"Expected 0 null qk17 values after osm_import; got {null_count}."
        )


# ---------------------------------------------------------------------------
# overture_place_import.sql: ov_base re-materialization spill
# ---------------------------------------------------------------------------
#
# ov_base is referenced at most once beyond its own definition (the final
# SELECT). Referencing it more forces DuckDB to re-materialize the full
# width of ov_base (every Overture column) to answer a join or unnest,
# which on the production 22M-row Overture places extract spills >60GB of
# temp disk (a plain scan of the same rows spills 0 bytes). Density/idf
# joins and `variants` are folded directly into the final SELECT, with
# `variants` computed as a per-row list expression (list_transform/
# list_concat/list_filter/list_sort) with no unnest, no GROUP BY, and no
# join-back — so ov_base is scanned once.
#
# The structural tests below pin the absence of the spill-causing pattern.
# The characterization tests pin the *semantics* of the variants column,
# so a future rewrite cannot silently change behavior.
# ---------------------------------------------------------------------------

class TestOvertureVariantsCteSpillFix:
    """Structural tests pinning the ov_base/variants CTE-spill fix for
    overture_place_import.sql: no unnest on names.common/rules, no GROUP
    BY id.

    These checks are specific to how Overture's `variants` column is
    derived and have no OSM equivalent, so they stay Overture-specific.
    The ov_base-reference-count and forbidden-helper-CTE checks are
    enforced by TestImportSqlD6Enforcement below, which parametrizes
    the same checks over a list of import SQL files (`gotchas.md`,
    "No unbounded complex-state aggregation").
    """

    SQL_PATH = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"

    def _sql(self):
        # Comments stripped via the single shared helper (_strip_sql_comments)
        # so every test below is automatically protected — see that
        # function's docstring for why.
        return _strip_sql_comments(self.SQL_PATH.read_text())

    def test_no_unnest_on_names_common(self):
        """No unnest(...) applied to names.common.

        `unnest(map_entries(names.common))` in a comma-join lateral against
        ov_base would force a full re-materialization of ov_base for that
        join; variants is computed as a per-row list expression with no
        unnest instead.
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

        `unnest(names.rules)` in a comma-join lateral against ov_base would
        carry the same full re-materialization cost as the names.common
        unnest.
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

        Aggregating unnested/unioned name rows back into a per-id list via
        `GROUP BY id` would require a full shuffle/sort of every exploded
        row keyed by id — the largest single contributor to the temp-disk
        spill. variants is computed per-row instead, with no explosion and
        no re-aggregation.
        """
        sql = self._sql()
        assert not re.search(r"GROUP\s+BY\s+id\b", sql, re.IGNORECASE), (
            "overture_place_import.sql still contains 'GROUP BY id'. This forces "
            "a full shuffle/sort of every exploded name row keyed by id, which is "
            "the largest single contributor to the >60GB temp-disk spill on the "
            "full Overture places scale. Fix: compute variants as a per-row list "
            "expression so no re-aggregation by id is needed."
        )

    # Base-CTE-reference-count and forbidden-helper-CTE checks for this file
    # are enforced by TestImportSqlD6Enforcement.test_no_forbidden_helper_cte
    # and .test_base_cte_referenced_at_most_once_outside_definition below,
    # parametrized over IMPORT_SQL_FILES (which also covers osm_import.sql).


# ---------------------------------------------------------------------------
# Enforcement ledger: no unbounded complex-state aggregation, enforced as
# a hard review criterion for every SQL file.
#
# IMPORT_SQL_FILES below lists the import SQL files this invariant is
# enforced against (overture_place_import.sql, osm_import.sql): each has
# one or more base CTE(s) that scan the raw parquet input in full. A
# narrow-projection "helper" CTE (density/idf/variant scores) that
# re-scans a base CTE and gets LEFT JOINed back on a computed key forces
# DuckDB to re-materialize the full-width base CTE once per reference,
# which is what spills temp disk at production scale.
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
        # filtered (nodes), way_base (ways), and rel_base (relations) are
        # the three base CTEs; node_density/node_idf, way_density/way_idf,
        # and rel_density/rel_idf are the narrow-projection CTEs this
        # guards against re-introducing.
        "base_ctes": ["filtered", "way_base", "rel_base"],
        "forbidden_helper_ctes": [
            "node_density", "node_idf", "way_density", "way_idf",
            "rel_density", "rel_idf",
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
            "spills gracefully, so this does not exhibit the unbounded "
            "complex-state aggregation that overture_place_import.sql "
            "and osm_import.sql have."
        ),
    },
]


class TestImportSqlD6Enforcement:
    """The unbounded-aggregation limit as executable tests, parametrized over
    IMPORT_SQL_FILES rather than hand-written per file.
    """

    def test_all_import_sql_files_are_classified(self):
        """Every garganorn/sql/*_import.sql file must be enforced or exempt.

        Guards against a new import SQL file silently skipping this review:
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
            "Every import SQL file requires a deliberate choice: "
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
                f"{entry['filename']} is exempt from complex-state-aggregation enforcement but has no "
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
    """Characterization tests pinning `variants` semantics.

    These tests run overture_place_import.sql against fixture rows added
    to the `overture_parquet` fixture (ov010-ov016 in tests/conftest.py)
    and assert on the exact `variants` value observed, so a future rewrite
    of how `variants` is computed cannot silently change what it computes.
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

        Pins that variants is exactly [] and NOT NULL when there are no
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

    def test_common_and_rules_both_null_yields_empty_list_not_null(self, overture_parquet, tmp_path):
        """ov003: names.primary present, names.common and names.rules both
        SQL NULL (not just empty).

        Pins that variants is exactly [] and NOT NULL when both source
        fields are NULL, not just empty, complementing ov010 (common NULL,
        rules populated) and ov011 (common populated, rules NULL). A row
        with the whole `names` struct SQL NULL never reaches this code
        path: names.primary would also be NULL, and the ov_base filter
        excludes NULL-name rows before variants is computed.
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

        Pins the stable property (sorted by name) rather than a fixed order
        between tied entries, since list_sort orders structs by their
        fields in order (name first) and does not specify a tie-break
        between two entries with the same name, which is not guaranteed
        stable across DuckDB versions/plans. Non-tied entries ('Porte
        Doree', 'Zelle') do have a fully-determined order and are
        asserted exactly.
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

        Pins that variants does NOT dedupe: list_concat has no DISTINCT,
        so both identical entries survive into variants.
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
        either: the common and rules lists are combined with list_concat,
        which has no DISTINCT, so a cross-source duplicate also survives.
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
        in that order (as produced by the struct literal
        `{'name': ..., 'type': ..., 'language': ...}`).

        Checked against both a rules-derived variant (ov010) and a
        common-derived variant (ov011), since names.common and names.rules
        are built by separate list_transform expressions that could
        independently drift in field order.
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

    def test_invalid_rules_language_nulled(self, overture_parquet, tmp_path):
        """ov017: names.rules has an invalid language ('genitive', the
        real defect named in project memory) alongside a valid one.

        Pins the R1 safety net for overture_place_import.sql: the invalid
        entry's language becomes NULL while name/type survive, and the
        valid entry is untouched (same-fixture regression guard).
        """
        db_path = tmp_path / "test_variants_ov017.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        variants = self._variants_for(conn, "ov017")
        conn.close()
        assert variants == [
            {"name": "Invalid Language Name", "type": "official", "language": None},
            {"name": "Valid Language Name", "type": "alternate", "language": "en"},
        ], variants


# ---------------------------------------------------------------------------
# density/idf join must not multiply place rows
# ---------------------------------------------------------------------------
#
# ov_density and ov_idf join density_tiles/idf_scores to ov_base on a
# computed key (tile_qk15, category) with no uniqueness guarantee on that
# key in the joined table; a duplicate key would fan the matching place's
# row out through the final `LEFT JOIN ... USING (id)`. Exercising this
# requires real density_tiles/idf_scores rows, which run_overture_import()
# accepts via optional density_rows/idf_rows parameters (omitting them
# still creates empty tables, matching every other caller).
# ---------------------------------------------------------------------------

class TestOvertureDensityJoinNoFanOut:
    """Regression guard: a duplicate tile_qk15 key in density_tiles must
    not multiply a place's row in the final output.

    ov_density joins density_tiles to ov_base on
    `d.tile_qk15 = left(b.qk17, 15)` with no guarantee that tile_qk15 is
    unique in density_tiles. If it isn't, the LEFT JOIN would produce one
    ov_density row per matching density_tiles row, and the final
    `LEFT JOIN ov_density od USING (id)` would fan that back out into
    duplicate `places` rows for the same id. Exactly one places row per
    input row is required regardless of construction.
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
    """Regression guard: a duplicate category key in idf_scores must not
    multiply a place's row in the final output.

    Symmetric to TestOvertureDensityJoinNoFanOut above, for the idf side:
    ov_idf joins idf_scores to ov_base on `i.category = b.categories.primary`
    with no guarantee that category is unique in idf_scores. A duplicate
    category key would fan the matching place's row out through
    `LEFT JOIN ov_idf oi USING (id)` in the final SELECT.
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
