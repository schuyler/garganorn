# Pipeline Checkpoint/Resume Design (MVP)

## Problem

The quadtree pipeline runs 7 stages against an on-disk DuckDB file. If it crashes mid-run, all progress is lost — the working DB is left behind but there's no way to resume from the last completed stage. For FSQ (38.7M places), re-running from scratch wastes significant time and I/O.

## Approach

The working DuckDB file already lives on disk at `<tile_dir>/{source}.duckdb`. Instead of deleting it on failure and starting fresh, we keep it and use a **sentinel table** inside the DB to track which stages completed. On restart, we find the incomplete run, read the sentinel, and pick up where we left off.

The DuckDB file IS the checkpoint. No separate metadata files, no JSON manifests, no ATTACH/CTAS dance. Trust DuckDB.

## Sentinel Table

```sql
CREATE TABLE IF NOT EXISTS _pipeline_progress (
    stage VARCHAR PRIMARY KEY,
    completed_at VARCHAR
);
```

After each stage completes:

```python
con.execute("DELETE FROM _pipeline_progress WHERE stage = ?", [stage_name])
con.execute("INSERT INTO _pipeline_progress VALUES (?, ?)",
            [stage_name, datetime.now(timezone.utc).isoformat()])
con.execute("CHECKPOINT")
```

`CHECKPOINT` flushes the WAL to the main DB file, ensuring the sentinel row is durable. Cost is minimal — the WAL accumulates during the stage itself, and DuckDB auto-checkpoints when it exceeds 16MB anyway.

Stage names match the `stage_*` function names (without the `stage_` prefix). The stage order differs by source:

```python
STAGE_ORDER = {
    'default': [
        'import', 'importance', 'variants', 'tile_assignment',
        'containment', 'export', 'manifest',
    ],
    'overture_division': [
        'import', 'boundary_export', 'division_importance_backfill',
        'tile_assignment', 'containment', 'export', 'manifest',
    ],
}
```

The sentinel and stage-skipping logic must use the source-specific ordering.

## Resume Discovery

In `run_pipeline()`, before creating a new timestamp:

1. Scan `<source_dir>/` for timestamped directories
2. For each, check if it has a `{source}.duckdb` file but no `manifest.json`
3. Most recent match is the resumable run
4. If found: open the working DB, read `_pipeline_progress`, skip completed stages
5. If not found: create a new timestamped directory as usual

Helper function:

```python
def _find_incomplete_run(source_dir, source):
    """Find the most recent timestamped dir with a work DB but no manifest."""
    candidates = []
    for d in sorted(os.listdir(source_dir), reverse=True):
        if not _TIMESTAMP_RE.match(d):
            continue
        ts_dir = os.path.join(source_dir, d)
        if not os.path.isdir(ts_dir) or os.path.islink(ts_dir):
            continue
        db_path = os.path.join(ts_dir, f".{source}_work.duckdb")
        manifest = os.path.join(ts_dir, "manifest.json")
        if os.path.exists(db_path) and not os.path.exists(manifest):
            # Verify the DB is readable (not corrupted)
            try:
                test_con = duckdb.connect(db_path)
                test_con.execute("SELECT 1")
                test_con.close()
                candidates.append(ts_dir)
            except duckdb.Error:
                # Corrupted DB, skip this candidate
                log.warning("Corrupted working DB at %s, skipping", db_path)
    return candidates[0] if candidates else None
```

## Sentinel Helper Functions

```python
def _ensure_sentinel_table(con):
    """Create sentinel table if it doesn't exist. Call once after connect."""
    try:
        con.execute("SELECT 1 FROM _pipeline_progress LIMIT 1")
    except duckdb.CatalogException:
        con.execute("""
            CREATE TABLE _pipeline_progress (
                stage VARCHAR PRIMARY KEY,
                completed_at VARCHAR
            )
        """)

def _read_sentinel(con):
    """Return set of completed stage names."""
    rows = con.execute("SELECT stage FROM _pipeline_progress").fetchall()
    return {row[0] for row in rows}

def _mark_complete(con, stage):
    """Mark a stage as complete and checkpoint."""
    con.execute("DELETE FROM _pipeline_progress WHERE stage = ?", [stage])
    con.execute("INSERT INTO _pipeline_progress VALUES (?, ?)",
                [stage, datetime.now(timezone.utc).isoformat()])
    con.execute("CHECKPOINT")
```

## Stage Idempotency

Most stages are already idempotent. They use the CTAS + DROP + RENAME pattern:

```sql
DROP TABLE IF EXISTS places_scored;
CREATE TABLE places_scored AS SELECT ... FROM places ...;
DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
```

**Fix required**: `compute_containment()` in `stages.py:295` uses plain `CREATE TABLE place_containment`. This fails on re-run because the table already exists. The fix: add `DROP TABLE IF EXISTS place_containment` before the `CREATE TABLE`:

```python
# stages.py, compute_containment(), around line 294:
con.execute("DROP TABLE IF EXISTS place_containment")
con.execute("""
    CREATE TABLE place_containment (
        place_id       VARCHAR,
        relations_json VARCHAR
    )
""")
```

## `run_pipeline()` Integration

After opening the DuckDB connection, unconditionally ensure the sentinel table exists:

```python
db_path = os.path.join(tile_dir, f".{source}_work.duckdb")
con = duckdb.connect(db_path)
_ensure_sentinel_table(con)  # Always create sentinel table on first open
```

For resumed runs, read the sentinel to determine which stages to skip:

```python
if incomplete_dir:
    completed = _read_sentinel(con)
    log.info("[%s] Resuming from %s, completed stages: %s",
             source, incomplete_dir, sorted(completed))
else:
    completed = set()
```

After each stage completes, mark it:

```python
stage_import(con, source, parquet_glob, bbox, memory_limit, t0)
_mark_complete(con, 'import')
```

Skip stages that are already complete:

```python
if 'importance' not in completed:
    stage_importance(con, source, t0, density_parquet)
    _mark_complete(con, 'importance')
else:
    log.info("[%s] Skipping 'importance' stage (already complete)", source)
```

## Stage Name Verification

The stage names in `STAGE_ORDER` exactly match the function names in `stages.py` and the call sites in `run_pipeline()`:

| STAGE_ORDER entry | Function name | Call site (quadtree.py) |
|-------------------|---------------|-------------------------|
| `import` | `stage_import` | line 126 |
| `importance` | `stage_importance` | line 135 |
| `variants` | `stage_variants` | line 136 |
| `boundary_export` | `stage_boundary_export` | line 132 |
| `division_importance_backfill` | `stage_division_importance_backfill` | line 133 |
| `tile_assignment` | `stage_tile_assignment` | line 139 |
| `containment` | `stage_containment` | line 141 |
| `export` | `stage_export` | line 142 |
| `manifest` | `stage_manifest` | line 143 |

**Note**: `stage_density_extract` is NOT part of `run_pipeline()`'s stage order — it's a standalone utility stage called from the CLI with its own mtime caching logic.

## `--force` Behavior

- Without incomplete run: start fresh (current behavior)
- With incomplete run: delete the working DB and start fresh

## What NOT To Do

- **No separate checkpoint files.** The DuckDB file IS the checkpoint. No JSON, no YAML, no sidecar files.
- **No ATTACH/CTAS dance.** Work directly on the on-disk connection. DuckDB handles persistence natively.
- **No column-existence introspection.** The sentinel table is the source of truth for what completed. Don't check if `importance` column exists on `places` — check the sentinel.
- **No `--resume` flag.** Auto-discovery from incomplete runs is sufficient. The user doesn't need to specify which timestamp to resume.
- **No partial stage resume.** If a stage crashes, it re-runs from the beginning of that stage. The sentinel is only written after the stage completes. There's no checkpointing within a stage.

## Implementation Checklist

1. Add `_find_incomplete_run(source_dir, source)` to `quadtree.py` with DB readability check
2. Add sentinel helpers (`_ensure_sentinel_table`, `_read_sentinel`, `_mark_complete`) and `STAGE_ORDER` dict to `quadtree.py`
3. Modify `run_pipeline()` to call `_ensure_sentinel_table(con)` unconditionally after `duckdb.connect()`
4. Modify `run_pipeline()` to check for incomplete runs and read sentinel before running stages
5. Add `_mark_complete()` call after each stage in `run_pipeline()`
6. Add stage-skipping logic based on `_read_sentinel()` result set
7. Fix `compute_containment` idempotency: add `DROP TABLE IF EXISTS place_containment`
8. Update `--force` to delete working DB of incomplete runs

## Changes from Original Design

### Critical Fix #1: Sentinel table creation

**Original design**: Called `_ensure_sentinel_table` only from `_read_sentinel`, which was only called when resuming an incomplete run. For new runs, the sentinel table was never created, causing the first `_mark_complete` call to fail.

**Fix**: Call `_ensure_sentinel_table(con)` unconditionally right after `duckdb.connect(db_path)`, before any sentinel reads or writes. This ensures the table exists for both new and resumed runs.

### Important Fix #2: Corrupted DB handling

**Original design**: `_find_incomplete_run` only checked if the DB file exists, not whether it's readable. A corrupted DB would be selected as the resume candidate, causing a crash when trying to open it.

**Fix**: In `_find_incomplete_run`, try to open each candidate DB with `duckdb.connect(db_path)` and execute a simple query (`SELECT 1`). Skip candidates that fail to open or raise `duckdb.Error`. Log a warning for each corrupted DB found.

### Important Fix #3: Stage name verification

**Original design**: Listed `STAGE_ORDER` but didn't verify the stage names matched the actual function names and call sites in `run_pipeline()`.

**Fix**: Verified all stage names in `STAGE_ORDER` exactly match the `stage_*` function names in `stages.py` and the call sites in `quadtree.py` (lines 126-143). Added a verification table documenting the correspondence. Noted that `stage_density_extract` is NOT part of the pipeline stage order.

## Implementation Status

**Status: Complete** (2026-04-13)

The checkpoint/resume feature is fully implemented and tested. Pipeline runs that crash mid-execution can now resume from the last completed stage without re-running earlier work.

### What Was Implemented

1. **Sentinel table** (`_pipeline_progress`) tracks completed stages within the working DuckDB file
2. **Three helper functions** in `quadtree.py`:
   - `_ensure_sentinel_table(con)`: Creates the sentinel table if it doesn't exist
   - `_read_sentinel(con)`: Returns set of completed stage names
   - `_mark_complete(con, stage)`: Marks a stage complete and calls `CHECKPOINT`
3. **Resume discovery** via `_find_incomplete_run()`: Scans for the most recent timestamped directory with a working DB but no manifest.json
4. **Stage skipping logic** in `run_pipeline()`: Each stage checks the sentinel before executing
5. **Idempotency fix** for `compute_containment()`: Added `DROP TABLE IF EXISTS place_containment`
6. **mtime-based caching**: Uses `_is_output_fresh()` to skip pipeline runs when manifest.json is newer than input files
7. **`--force` flag**: Deletes incomplete run's working DB and starts fresh when provided

### Key Design Decisions

- **The DuckDB file IS the checkpoint** — no separate metadata files or sidecars
- **Auto-discovery** — users don't specify which timestamp to resume; the pipeline finds the most recent incomplete run automatically
- **Stage-level granularity** — if a stage crashes, it re-runs from the beginning of that stage (no intra-stage checkpoints)
- **Corrupted DB handling** — candidate working DBs are tested for readability before being selected for resume
- **Symlink exclusion** — both symlinks and their targets are skipped during incomplete run discovery

### How To Use

**Resume behavior**: If a pipeline run crashes, re-run the same command. The pipeline will automatically detect the incomplete run, read the sentinel, and resume from the first incomplete stage.

**`--force` flag**: Use `--force` to skip mtime caching and discard any incomplete run, starting from scratch. Example:

```bash
python -m garganorn.quadtree --source foursquare --parquet /data/fsq/*.parquet --output /tiles --force
```

**Test coverage**: 26 tests in `tests/test_checkpoint.py` cover sentinel helpers, stage order, incomplete run discovery, symlink handling, corrupted DB handling, and `compute_containment` idempotency.
