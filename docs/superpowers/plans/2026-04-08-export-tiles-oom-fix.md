# Fix export_tiles OOM: per-record SQL + Python-side grouping

**Date:** 2026-04-08
**Branch:** feat/quadtree
**Status:** Green TDD complete — ready for Documentation

## Problem

The quadtree export pipeline OOMs at 32GB during `CREATE TABLE tile_export AS ...` in `fsq_export_tiles.sql`. The SQL uses `GROUP BY tile_qk` with `list()` to aggregate all records per tile, then `to_json()` to serialize. DuckDB must hold the hash aggregation state for ALL groups simultaneously — 38.7M records across ~40K+ tiles. The `list()` aggregate accumulates variable-length struct data that doesn't spill to disk effectively.

## Approach

Split the work: SQL produces one row per record (no `GROUP BY`, no `list()`), Python groups consecutive rows by `tile_qk` and builds the JSON envelope.

**Why this works:** A flat join+sort spills to disk reliably. Memory usage is bounded to one tile's worth of records at a time in Python.

## SQL Changes

### All three files: common transformation

Remove the outer `GROUP BY tile_qk` and the `list({...})` aggregate. Remove `${attribution}` substitution (attribution moves to Python). Keep `${repo}` for URI construction. Add `ORDER BY ta.tile_qk` **inside the VIEW body** as an advisory hint. Output schema: `(tile_qk VARCHAR, record_json VARCHAR)` — one row per place.

**ORDER BY note (I1):** The load-bearing ORDER BY is on the **cursor SELECT** in Python (`con.execute("SELECT tile_qk, record_json FROM tile_export ORDER BY tile_qk")`), not inside the VIEW. DuckDB does not guarantee that ORDER BY in a VIEW body is preserved when the view is queried without an outer ORDER BY. The SQL files include ORDER BY for readability only; the Python cursor SELECT is the authoritative sort.

### fsq_export_tiles.sql

Current: `DROP TABLE IF EXISTS tile_export; CREATE TABLE tile_export AS SELECT ... list({...}) ... GROUP BY ta.tile_qk`

New: `CREATE OR REPLACE VIEW tile_export AS SELECT ta.tile_qk, to_json({uri: ..., value: {...}})::VARCHAR AS record_json FROM places p JOIN tile_assignments ta ON ta.place_id = p.fsq_place_id ORDER BY ta.tile_qk`

### overture_export_tiles.sql

Keep `place_addresses` TEMP TABLE unchanged — it aggregates addresses per-place (one row per place_id), which is a different aggregation unrelated to the tile OOM. Remove outer `GROUP BY tile_qk` + `list()` only. Change VIEW to output `(tile_qk, record_json)`.

### osm_export_tiles.sql

Already a VIEW. Remove `GROUP BY ta.tile_qk`, add `ORDER BY ta.tile_qk`, change `tile_json` alias to `record_json`, remove `list()` wrapper.

## Python Changes (quadtree.py)

### export_tiles() rewrite

```python
def export_tiles(con, output_dir: str, source: str) -> dict:
    """Query DuckDB for per-record JSON, group by tile_qk, write gzipped files.

    Streams results via fetchmany(1000) to keep memory bounded. One tile's
    records are accumulated in-memory at a time; on tile boundary, flushes to disk.
    Returns {qk: record_count}.
    """
    sql_dir = Path(__file__).parent / "sql"
    raw = (sql_dir / f"{source}_export_tiles.sql").read_text()
    sql = string.Template(raw).safe_substitute(repo=REPO)  # attribution removed
    con.execute(sql)  # creates VIEW — no materialization
    cursor = con.execute("SELECT tile_qk, record_json FROM tile_export ORDER BY tile_qk")

    manifest = {}
    current_qk = None
    accumulated = []

    def flush_tile(qk, records):
        envelope = {"attribution": ATTRIBUTION[source], "records": [json.loads(r) for r in records]}
        subdir = os.path.join(output_dir, qk[:6])
        os.makedirs(subdir, exist_ok=True)
        with gzip.open(os.path.join(subdir, f"{qk}.json.gz"), "wb") as f:
            f.write(json.dumps(envelope).encode("utf-8"))
        manifest[qk] = len(records)

    tile_count = 0
    while True:
        batch = cursor.fetchmany(1000)
        if not batch:
            break
        for tile_qk, record_json in batch:
            if tile_qk != current_qk:
                if current_qk is not None:
                    flush_tile(current_qk, accumulated)
                    tile_count += 1
                    if tile_count % 1000 == 0:
                        log.info("export: wrote %d tiles", tile_count)
                current_qk = tile_qk
                accumulated = []
            accumulated.append(record_json)

    if current_qk is not None:
        flush_tile(current_qk, accumulated)

    log.info("export: wrote %d tiles total", len(manifest))
    return manifest
```

Key differences from current:
- `string.Template.safe_substitute(repo=REPO)` — `attribution` no longer a parameter
- Nested `flush_tile()` builds envelope with `ATTRIBUTION[source]`
- `manifest[qk] = len(records)` — no JSON re-parsing for count
- Final tile flushed after loop with `if current_qk is not None`

## Test Changes

### TestFsqExportTiles (existing tests requiring full rewrites — C2)

The following tests all currently loop over `tile_json["records"]` (the old tile-envelope format). Each test must be **fully rewritten**, not just have column names updated:

- **`test_tile_export_is_table_not_view`**: Invert assertion from `row[0] == "BASE TABLE"` to `row[0] == "VIEW"`. **Also update the docstring** (C3) — currently says "after implementation converts it to TABLE, test passes"; should say "after implementation changes to VIEW, test passes".

- **`test_export_produces_rows`**: Update `SELECT *` to `SELECT tile_qk, record_json`. Assert `len(rows) >= len(_FSQ_EXPORT_PLACES)` (number of fixture records, not 1). This catches that output is now per-record.

- **`test_tile_json_structure`**: Full rewrite. Old assertion: `json.loads(tile_json)` has `attribution` and `records` keys. New assertion: `SELECT tile_qk, record_json FROM tile_export`; `json.loads(record_json)` has `uri` and `value` keys (per-record structure, not tile envelope).

- **`test_record_schema`**: Full rewrite. Old: reads `tile_json` column, iterates `parsed["records"]`. New: reads `record_json` column, parses each row directly. Assert `uri`, `value.$type`, `value.name`, `value.importance`, `value.locations`, `value.variants`, `value.attributes`, `value.relations` are present on each parsed record.

- **`test_geo_location`**: Full rewrite. Old: `tile_json` → `parsed["records"]` → `rec["value"]["locations"]`. New: `record_json` → `parsed["value"]["locations"]`.

- **`test_address_location_when_country_present`**: Full rewrite. Old: nested loop over `tile_json["records"]`. New: iterate `record_json` rows directly; parse each row; check `value.locations[1]` for records with country.

- **`test_no_address_when_country_null`**: Full rewrite. Same pattern — iterate `record_json` rows directly.

### TestExportTiles (existing tests to update — C1)

These tests mock the cursor. The mock must supply **per-record JSON** (what `to_json({uri:..., value:{...}})::VARCHAR` produces), not tile-envelope JSON.

**Mock row format:**
```python
# OLD (incorrect after redesign):
record_a = json.dumps({"attribution": "test", "records": [{"$type": "place"}]})
all_rows = [(tile_qk_a, record_a), (tile_qk_b, record_b)]

# NEW (correct):
record_a = json.dumps({"uri": "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001",
                        "value": {"$type": "org.atgeo.place", "rkey": "fsq001", "name": "Test"}})
all_rows = [(tile_qk_a, record_a), (tile_qk_b, record_b)]
```

The mock rows must contain `uri` + `value` keys so that `flush_tile()` produces a correct envelope when it calls `json.loads(r)` per row.

Additionally, mock-based tests must **assert on written file content** to verify the envelope structure, not just that files were written. Specifically: read the written `.json.gz`, verify `parsed["attribution"]` is set and `parsed["records"]` is a list of `{uri, value}` objects (not envelopes).

Tests to update:
- `test_uses_fetchmany_not_fetchall`: update mock row payload; add content assertion
- `test_progress_log_format_no_total`: update mock row payload
- `test_post_loop_log_uses_manifest_len`: update mock row payload

Also: the `fake_sql` in these tests says `"SELECT tile_qk, tile_json FROM tile_export"` — update to `"SELECT tile_qk, record_json FROM tile_export"`.

### `_SUBS` dict in TestFsqExportTiles

`_SUBS` currently contains `{"attribution": "Foursquare Open Source Places", "repo": "https://example.com"}`. Since `_load_sql` uses `string.Template.safe_substitute`, the extra `attribution` key is harmless after the SQL files drop `${attribution}`. No change required, but it is acceptable to remove `attribution` from `_SUBS` for clarity.

### New tests

- **`test_python_groups_records_by_tile_qk`**: Mock cursor returns rows across two tiles: `[(qk_a, rec1_json), (qk_a, rec2_json), (qk_b, rec3_json)]`. Verify 2 `.json.gz` files written; `qk_a` tile has 2 records, `qk_b` tile has 1. Read and verify file content.

- **`test_attribution_in_envelope`**: Mock cursor returns one row. Verify written `.json.gz` has `attribution == ATTRIBUTION["fsq"]` in the envelope.

- **`test_single_record_tile`**: Mock cursor returns exactly one row for one tile. Verify file written; envelope has 1 record.

- **`test_tile_boundary_across_fetchmany_batches`**: Mock `fetchmany` side_effect: first call `[(qk_a, rec1)]`, second call `[(qk_a, rec2), (qk_b, rec3)]`, third call `[]`. This forces `qk_a`'s records across two `fetchmany` calls. Verify both tiles written correctly — `qk_a` file has 2 records, `qk_b` file has 1.

## Edge Cases

- **Empty result**: loop never enters, `current_qk` stays None, final-flush guard fires safely.
- **Single-record tile**: `accumulated` has one element; flush writes correct JSON.
- **Tile spanning batch boundary**: state machine (`current_qk` + `accumulated`) carries state across `fetchmany` calls.
- **Final tile**: explicit flush after loop exits.

## Output Format

Each `.json.gz` file contains:
```json
{"attribution": "https://...", "records": [{uri: "...", value: {...}}, ...]}
```

This matches the current format exactly. Only the assembly point changes (Python vs SQL).

## Verification

1. `pytest tests/` — all tests pass
2. Server test with `--memory-limit 32GB` against full FSQ dataset (38.7M places)
3. Diff sample tiles against current output to verify format compatibility

---

## Implementation Status

### Completed

**Design** ✓ (reviewed, gated, one fix loop)

**Baseline** ✓
- Full suite (`tests/`): 533 tests, 532 passed, 1 xfailed
- No failures before Red TDD changes

**Pre-existing bug fix** ✓
- `garganorn/sql/overture_import.sql` line 8: comment contained the text "ALTER TABLE" (in `-- inline avoids ALTER TABLE + UPDATE`), causing `TestQk17PipelineFixes::test_fix1_overture_import_no_alter_table` to fail. Changed to `-- inline avoids the two-pass approach`.

**Red TDD** ✓ (reviewed, gated, one fix loop)

Uncommitted changes on `feat/quadtree` (relative to HEAD `2efd234`):
- `garganorn/sql/overture_import.sql` — comment fix (1 line)
- `tests/test_quadtree.py` — Red TDD changes (+277 / -105 lines)

Current test state: **537 tests, 523 passed, 14 failing (all intentional Red), 1 xfailed**

### Failing tests (Red — all intentional, all in test_quadtree.py)

**TestFsqExportTiles (7 rewrites):**
- `test_tile_export_is_view_not_table` — inverted: asserts VIEW not TABLE
- `test_export_produces_rows` — asserts `>= len(_FSQ_EXPORT_PLACES)` rows (per-record)
- `test_record_json_structure` (was `test_tile_json_structure`) — asserts `{uri, value}` per row
- `test_record_schema` — iterates `record_json` rows directly
- `test_geo_location` — iterates `record_json` rows directly
- `test_address_location_when_country_present` — iterates `record_json` rows directly
- `test_no_address_when_country_null` — iterates `record_json` rows directly

**TestExportTiles (3 updated + 4 new):**
- `test_uses_fetchmany_not_fetchall` — updated mock payload + added content assertion
- `test_progress_log_format_no_total` — updated mock payload
- `test_post_loop_log_uses_manifest_len` — updated mock payload
- `test_python_groups_records_by_tile_qk` — NEW
- `test_attribution_in_envelope` — NEW
- `test_single_record_tile` — NEW
- `test_tile_boundary_across_fetchmany_batches` — NEW

**Green TDD** ✓ (reviewed, gated)

All 14 Red failures turned green. Final test state: **536 passed, 1 xfailed (537 collected)**.

Implementation notes from code review:
- The `tile_count` variable in the plan pseudocode was replaced with `len(manifest) % 1000 == 0` inside `flush_tile()`. This avoids a separate counter and uses the manifest dict (which is already updated on every flush) as the source of truth for the tile count.

### Post-merge cleanup (cosmetic)

- `test_tile_export_is_view_not_table` docstring: still describes the Red state ("after implementation changes to VIEW, test passes") — update to describe what the test asserts in steady state.
- `TestFsqExportTiles` class docstring: may reference old tile-envelope format — review and update if stale.
