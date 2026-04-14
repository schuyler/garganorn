# Garganorn Design Constraints & Invariants

Living reference of DuckDB behaviors, pipeline invariants, and architectural
rules learned from production experience. Each entry includes the constraint,
why it exists, and where it applies.

---

## DuckDB Engine Constraints

### D1: R-tree indexes only activate in top-level WHERE clauses

R-tree spatial indexes are not used in JOIN ON conditions or subqueries.
Workaround: materialize filtered results to a temp table via a top-level
WHERE clause, then join against the temp table.

**Applies to**: `stages.py:116-128` (containment pre-filter), `boundaries.py:48`
(point-in-polygon query)

**Why it matters**: Without this workaround, spatial queries degrade to
full-table scans against large geometry columns, causing 100x+ latency.

### D2: Zone maps require sorted columns

DuckDB zone maps (min/max statistics per row group) only accelerate queries
when the filtered column is physically sorted in the parquet file. Removing
ORDER BY from a CTAS that writes parquet can cause 10-24x query regression
on filtered reads.

**Applies to**: All `*_import.sql` files, density_extract.sql, manifest writes

**Why it matters**: The pipeline uses parquet as its interchange format.
Unsorted parquet defeats DuckDB's built-in zonemap pushdown.

### D3: CTAS is fast, UPDATE/ALTER TABLE is slow

DuckDB's columnar storage means every column mutation is a full table copy.
The pipeline must be INSERT/CTAS-only. Never UPDATE a column or ALTER TABLE
ADD COLUMN on large tables.

**Applies to**: All SQL files, `stages.py`

**Why it matters**: An UPDATE on a 38M-row table can take as long as the
original import.

### D4: CTAS ORDER BY can OOM during sort

CTAS with ORDER BY uses multi-threaded sorting but can exhaust memory on
large datasets. Lower `memory_limit` below system RAM to leave headroom.

**Applies to**: `overture_division_import.sql` (ST_Hilbert sort),
`export_boundaries_db()` (Hilbert sort)

**Why it matters**: OOM during sort kills the pipeline. The memory limit
parameter exists to prevent this.

### D5: unnest() on NULL arrays silently drops the row

When unnesting a NULL or empty array, the row disappears from the result
set entirely — no error, no NULL output row. This is correct SQL semantics
but can cause unexpected row count changes.

**Applies to**: `foursquare_import.sql:34-35` (category unnesting),
`foursquare_idf.sql:12` (same), `overture_place_import.sql:38-55`
(name variant unnesting)

**Why it matters**: Places with NULL category arrays get no IDF score
(defaults to 0 via coalesce). This is intentional but should be documented.

---

## Pipeline Architecture Constraints

### P1: All spatial indexing uses quadkeys

Quadkeys (Bing tile system) at z17 for places, z15 for density tiles.
S2 cell IDs are eliminated from the pipeline. ST_QuadKey() computes
spatial keys from lon/lat coordinates.

**Applies to**: All `*_import.sql` (qk17 column), `compute_tile_assignments.sql`,
`density_extract.sql`

### P2: Importance scoring varies by entity type

- **Places (FSQ, Overture, OSM)**: `60% density + 40% IDF`
  - Formula: `round(60 * least(density/density_norm, 1.0) + 40 * least(idf/idf_norm, 1.0))`
  - Defaults: `density_norm=10.0`, `idf_norm=18.0`
- **Localities (Overture divisions, subtype=locality)**: `60% density + 40% population`
  - Formula: `round(60 * least(avg_density/density_norm, 1.0) + 40 * least(ln(1+population)/pop_norm, 1.0))`
  - Defaults: `density_norm=10.0`, `pop_norm=20.0`
- **Non-locality divisions**: `40% population` only
  - Formula: `round(40 * least(ln(1+population)/pop_norm, 1.0))`

**Applies to**: `foursquare_import.sql:41-44`, `overture_place_import.sql:71-74`,
`osm_import.sql:139-142`, `overture_division_import.sql:94-100`

### P3: Density tiles use bbox-overlap join

The density spatial join for division localities uses bbox-overlap-bbox
rather than centroid-in-bbox. A density tile contributes to a locality's
score if the tile bbox intersects the locality bbox.

**Applies to**: `overture_division_import.sql:84-88`

**Why it matters**: Centroid-based joins systematically under-scored small
localities and edge tiles. Fixed in Phase 4 of pipeline restructuring.

### P4: STAGE_ORDER is uniform across all sources

```
STAGE_ORDER = ['import', 'tile_assignment', 'containment', 'export', 'manifest']
```

Boundary export is a special case outside this loop — called directly for
`overture_division` only, between import and tile_assignment.

**Applies to**: `quadtree.py:42`, `quadtree.py:242-247`

### P5: IDF is computed from source parquet, not imported places table

IDF computation reads raw parquet directly (ephemeral DuckDB connection),
not the imported `places` table. This avoids an ephemeral import step and
makes IDF cacheable per-dataset.

**Applies to**: `stages.py:601-649`, `*_idf.sql`

### P6: Containment uses adaptive quadtree with two-phase optimization

Phase 1: ST_Contains(geometry, tile_envelope) identifies boundaries that
fully contain a tile. All places in that tile get assigned via CROSS JOIN
(no per-point geometry test).

Phase 2: ST_Contains(geometry, point) runs per-point only for "edge"
boundaries that overlap but don't fully contain the tile. Bbox pre-filter
on lat/lon columns reduces ST_Contains calls.

Tiles are subdivided from z6 seeds when boundary count exceeds 200, up to
z14 maximum.

**Applies to**: `stages.py:89-186` (`_run_containment`, `_process_tile`,
`compute_containment`)

**Correctness invariant**: Each place belongs to exactly one leaf tile
(determined by the appropriate prefix of its qk17). The CROSS JOIN in
phase 1 assigns all phase-1 boundaries to every place in that tile.

---

## Query Path Constraints

### Q1: JW scoring blends field-level and token-level similarity

`JW_TOKEN_ALPHA = 0.5` — 50/50 blend of full-name JW score and
token-level JW score. Token matching splits query and name on spaces,
matches tokens greedily by highest JW score, and averages match scores.

**Applies to**: `database.py:42-44`, JW scoring functions in Database subclasses

### Q2: Importance floor scales with search area

`compute_importance_floor(area_km2)` returns `min(4 * ln(1 + area_km2/K), 50)`
where `K=1000`. This prevents low-importance results from dominating
large-area searches. Applied only when a text query is present.

**Applies to**: `database.py:14-22`, `database.py:243-244`

### Q3: Containment gracefully degrades

If boundary lookup fails for any reason, the place is served without
`within` relations. The exception is caught and logged, not propagated.

**Applies to**: `server.py:88-105`

---

## Normalization Constants

| Constant | Default | Used by |
|----------|---------|---------|
| `density_norm` | 10.0 | Importance density component (all place sources) |
| `idf_norm` | 18.0 | Importance IDF component (FSQ, Overture, OSM) |
| `pop_norm` | 20.0 | Importance population component (Overture divisions) |
| `IMPORTANCE_FLOOR_K` | 1000 | Importance floor scaling for search area |
| `JW_THRESHOLD` | 0.6 | Minimum JW score for name matching |
| `JW_TOKEN_ALPHA` | 0.5 | Field/token blending weight |
| `max_boundaries` | 200 | Containment subdivision threshold |
| `max_zoom` | 14 | Maximum containment subdivision depth |
| `max_per_tile` | 1000 | Maximum records per export tile |

## Coordinate System

- All coordinates are WGS84 (EPSG:4326), longitude/latitude order
- Quadkey zoom levels: z17 for places, z15 for density, z6-z17 for tiles
- Coordinate precision in export: `DECIMAL(10,6)` → 6 decimal places (~0.1m)
- Bbox privacy grid: 0.01° (~1km) enforced by `_check_bbox_precision()`
