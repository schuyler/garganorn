# Pipeline Restructuring: Global Prerequisites + Single-CTAS Import

## Context

The pipeline currently uses a multi-pass approach: import creates `places`, then separate stages use CTAS + table-swap (CREATE TABLE places_scored AS SELECT...; DROP TABLE places; ALTER TABLE places_scored RENAME TO places) to add `importance` and `variants` columns. IDF is computed inline during importance. DuckDB is columnar — each table-swap is a full rewrite. The checkpoint/resume feature we just built works but the stage structure doesn't match the operational workflow.

The goal: restructure into a three-layer pipeline where global prerequisites (density tiles, boundaries) are computed once, category IDF is cached per-dataset, and the per-dataset import is a single CTAS that produces the final `places` table with all columns. INSERT or CTAS only — never UPDATE or ALTER TABLE.

## Architecture

### Layer 1: Global Prerequisites (run once, cache for long periods)

| Prerequisite | Input | Output | Existing code |
|---|---|---|---|
| Density tiles | Overture places parquet | `density_tiles.parquet` (z15, `ln(1+count)`) | `stage_density_extract` + `density_extract.sql` |
| Boundaries | Overture divisions parquet | `boundaries.duckdb` (R-tree indexed) | `stage_boundary_export` logic |

Both use mtime-based freshness checks. Change only when Overture source parquets change.

### Layer 2: Per-Dataset Category IDF (cacheable)

For each dataset, compute `idf_{source}.parquet` with schema `(category, n_places, idf_score)`.

- **FSQ**: `unnest(fsq_category_ids)` from FSQ parquet directly
- **Overture places**: `categories.primary` from Overture places parquet directly
- **OSM**: `primary_category` from OSM parquet directly

IDF formula: `ln(N_total / count_per_category)`. Computed from source parquet, not from the `places` table — no ephemeral import needed.

**Division importance**: Keep current hybrid formula — localities: 60% bbox-averaged density + 40% `ln(1+population)/pop_norm`, non-localities: `ln(1+population)/pop_norm` only. No IDF for divisions. Density is only meaningful for localities (bbox-averaged z15 tiles); for large admin areas it's too diluted. Population is the right signal for admin areas and is generally available for non-localities. Divisions don't receive an `idf_parquet` parameter.

**Note on source differences**: Each source has its own import SQL file handling its category format (FSQ `INTEGER[]` arrays, Overture `categories.primary` struct, OSM `VARCHAR`, divisions `subtype`/`admin_level`). The "uniform" aspect is the stage list and pattern (single CTAS), not a single SQL template.

### Layer 3: Per-Dataset Pipeline (single CTAS import)

```
STAGE_ORDER = ['import', 'tile_assignment', 'containment', 'export', 'manifest']
```

Same for all sources. The `import` stage is a single CTAS:

```
CREATE TABLE places AS
SELECT <source columns>,
       <importance computation joining density_tiles + category_idf>,
       <variants computation>
FROM read_parquet(...)
WHERE <bbox filter>
```

No subsequent ALTER TABLE, UPDATE, or table-swap. The `importance` and `variants` stages are eliminated. `tile_assignment`, `containment`, `export`, `manifest` are unchanged (already CTAS).

## Files Changed

### New SQL files (3)
- `garganorn/sql/foursquare_idf.sql` — IDF from FSQ categories, reads parquet directly
- `garganorn/sql/overture_place_idf.sql` — IDF from Overture categories, reads parquet directly
- `garganorn/sql/osm_idf.sql` — IDF from OSM categories, reads parquet directly

### Modified SQL files (4)
- `garganorn/sql/foursquare_import.sql` — Absorb density join + IDF join + importance + variants. Single CTAS.
- `garganorn/sql/overture_place_import.sql` — Same pattern as FSQ
- `garganorn/sql/osm_import.sql` — Same pattern (currently has `importance=0` placeholder)
- `garganorn/sql/overture_division_import.sql` — Absorb hybrid density+population importance formula, inline `variants=[]`

### Deleted SQL files (7)
- `foursquare_importance.sql` — absorbed into import
- `overture_place_importance.sql` — absorbed into import
- `osm_importance.sql` — absorbed into import
- `foursquare_variants.sql` — absorbed into import
- `overture_place_variants.sql` — absorbed into import
- `osm_variants.sql` — absorbed into import
- `division_importance_backfill.sql` — absorbed into import

### Python changes

**`garganorn/stages.py`**:
- Add: `stage_idf(source, parquet_glob, output_path, t0, force=False)` — ephemeral DuckDB, reads parquet directly, computes IDF, writes parquet
- Add: `stage_boundary_export_standalone(...)` — extracted from current `stage_boundary_export`, callable without a pipeline DB connection
- Modify: `stage_import` — accept `density_parquet` and `idf_parquet` params (idf_parquet not passed for divisions), pass to SQL as `${density_parquet}` and `${idf_parquet}`
- Remove: `stage_importance`, `stage_variants`, `stage_division_importance_backfill`
- Keep: `stage_boundary_export` as thin wrapper for backward compat during migration
- Keep: `stage_tile_assignment`, `stage_containment`, `stage_export`, `stage_manifest` (unchanged)

**`garganorn/quadtree.py`**:
- Simplify `STAGE_ORDER` to single list: `['import', 'tile_assignment', 'containment', 'export', 'manifest']`
- Simplify `run_pipeline()` stage loop — no source-specific branching
- Add `--idf-parquet` CLI flag (same pattern as `--density-parquet`)
- Remove source-specific branching in pipeline body (all branching encapsulated in SQL)

**`tests/test_checkpoint.py`**:
- Update `TestStageOrder` — both sources now same stage list
- Add tests for `stage_idf` (output schema, mtime caching, correctness)
- Update integration tests for unified import (importance + variants present after import)

### Net SQL count: 17 → 13 (3 new, 7 deleted)

## Migration: 3 Phases

**Phase 1: Add IDF stage** (non-breaking)
- Create `sql/{source}_idf.sql` files
- Add `stage_idf()` to stages.py
- Add `--idf-parquet` CLI flag
- Add IDF tests
- Existing pipeline still works

**Phase 2: Merge importance + variants into import** (breaking)
- Rewrite `sql/{source}_import.sql` to include density + IDF join + importance + variants. For divisions: inline the hybrid density+population formula from `division_importance_backfill.sql` into `overture_division_import.sql` (with density join), then delete the backfill SQL.
- **Extract OSM category extraction to a shared snippet**: The CASE expression for `primary_category` (14 WHEN branches) is duplicated across `osm_import.sql` (2x) and `osm_idf.sql` (2x). Extract to `sql/_osm_category_case.sql` and interpolate via `${osm_category_case}` using the existing substitution pattern. No connection setup needed — works identically in ephemeral and persistent connections.
- Delete `sql/{source}_importance.sql`, `sql/{source}_variants.sql`, `division_importance_backfill.sql`
- **Sentinel migration**: Changing STAGE_ORDER means old incomplete runs with sentinel entries for 'importance', 'variants', etc. won't resume correctly. Fix: use `--force` to discard old incomplete runs, or add migration logic to map old stage names to new ones.
- Update `stage_import` signature
- Remove `stage_importance`, `stage_variants`, `stage_division_importance_backfill`
- Simplify `STAGE_ORDER` and `run_pipeline()`
- Update all tests

**Phase 3: Extract boundary export** (small cleanup)
- Move `stage_boundary_export` to standalone prerequisite function
- Remove from STAGE_ORDER
- Update CLI help/docs

Phases 2 and 3 could be combined.

**Phase 4: Fix density tile spatial join** (bug fix)
- `density_extract.sql` currently emits `centroid_lon`/`centroid_lat` (average of place coords per z15 tile). The division import joins tiles to localities using `centroid BETWEEN min/max` — a centroid-in-bbox check.
- **Bug**: For localities smaller than a z15 tile (~1.2km), tile centroids often fall outside the locality bbox, causing dense localities to receive zero density score. Edge tiles are also systematically excluded for larger localities.
- **Fix**: Store tile bounding box (`tile_xmin`, `tile_ymin`, `tile_xmax`, `tile_ymax`) derived from the z15 quadkey in the density parquet. Change the division join to bbox-overlap-bbox instead of centroid-in-bbox. Replace `centroid_lon`/`centroid_lat` columns with the tile bounds.
- **Files**: `density_extract.sql` (schema), `overture_division_import.sql` (join logic), `stages.py` (empty CTE schema), `quadtree_helpers.py` (test helpers), density-related tests.

## Verification

1. Run each IDF SQL against a small sample parquet — verify output schema and IDF values
2. Run unified import CTAS — verify `places` table has `importance` and `variants` columns populated
3. Run full pipeline end-to-end for one source (FSQ with bbox) — verify tiles produced
4. `pytest tests/` — all tests pass including updated checkpoint tests
5. Verify no SQL file contains `ALTER TABLE`, `UPDATE`, or `ADD COLUMN`
