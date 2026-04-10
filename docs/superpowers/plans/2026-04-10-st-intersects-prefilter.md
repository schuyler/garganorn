# ST_Intersects Pre-filter with Geometry Clipping

**Date:** 2026-04-10
**Branch:** feat/quadtree
**Status:** Design
**Predecessor:** [Two-Phase Containment](2026-04-10-two-phase-containment.md)

## Problem

The production WoF boundaries database contains ~1M boundaries with highly
complex geometries:

| Level | Count   | Avg vertices | Max vertices |
|-------|---------|--------------|--------------|
| 50    | 765,000 | 201          | 240,000      |
| 35    | 42,000  | 1,456        | 283,000      |
| 25    | 4,219   | 6,049        | 927,196      |

The current two-phase containment processes each z6 tile as follows:

- **Phase 1:** `ST_Contains(geom, tile_envelope)` with R-tree index finds
  boundaries that fully contain the tile. For z6 tiles (~5.6 x 4.4 degrees),
  this returns **0 boundaries** because z6 tiles are too large for any boundary
  to fully contain them.

- **Phase 2:** Per-point `ST_Contains` runs against ALL ~1M boundaries (since
  phase 1 matched nothing). Each `ST_Contains` call traverses the boundary's
  full vertex list. A boundary with 927K vertices costs 927K vertex traversals
  per point tested.

Two distinct problems:

1. **Too many boundaries in phase 2.** All 1M boundaries fall through. An
   `ST_Intersects` pre-filter reduces this to ~2,140 for a San Francisco z6
   tile in ~13ms.

2. **Too many vertices per boundary.** Even after filtering to ~2,140
   boundaries, a country-spanning boundary carries its full geometry into
   ST_Contains. Clipping to the tile envelope reduces a 927K-vertex boundary
   to just the portion overlapping the tile -- potentially hundreds of vertices.

## Design

Add a new Step 0 before the existing two phases. Step 0 creates a temp table
of boundaries that intersect the tile, with geometries clipped to the tile
envelope. Phases 1 and 2 then operate against this small, vertex-reduced temp
table instead of the full `wof.boundaries` table.

### Step 0: Pre-filter and clip boundaries by tile intersection

```sql
CREATE OR REPLACE TEMP TABLE tile_boundaries AS
SELECT rkey, name, level,
       ST_Intersection(geom, ST_MakeEnvelope(?, ?, ?, ?)) AS geom,
       greatest(min_latitude, ?) AS min_latitude,
       least(max_latitude, ?)    AS max_latitude,
       greatest(min_longitude, ?) AS min_longitude,
       least(max_longitude, ?)    AS max_longitude
FROM wof.boundaries
WHERE ST_Intersects(geom, ST_MakeEnvelope(?, ?, ?, ?))
```

Parameters (12 total, derived from `quadkey_to_bbox(z6)` which returns
`(lon_min, lat_min, lon_max, lat_max)`):

```python
bbox = quadkey_to_bbox(z6)  # (lon_min, lat_min, lon_max, lat_max)
params = [
    bbox[0], bbox[1], bbox[2], bbox[3],   # ST_Intersection envelope (xmin, ymin, xmax, ymax)
    bbox[1], bbox[3], bbox[0], bbox[2],   # bbox clamping (lat_min, lat_max, lon_min, lon_max)
    bbox[0], bbox[1], bbox[2], bbox[3],   # ST_Intersects WHERE (xmin, ymin, xmax, ymax)
]
```

Key properties:

- **R-tree activation:** `ST_Intersects` is in a top-level WHERE clause, so
  the R-tree index on `wof.boundaries.geom` activates. This is the fast path
  to narrow ~1M boundaries to the few thousand that overlap the tile.

- **Geometry clipping:** `ST_Intersection(geom, envelope)` clips each boundary
  to the tile rectangle. A country boundary spanning thousands of kilometers
  is reduced to the polygon fragment within the tile. Subsequent ST_Contains
  calls in phases 1 and 2 operate on this clipped geometry with far fewer
  vertices.

- **Bbox clamping:** The `min_latitude`, `max_latitude`, `min_longitude`,
  `max_longitude` columns are clamped to the tile bounds using
  `greatest`/`least`. This tightens the BETWEEN pre-filters in phase 2 to
  match the clipped geometry rather than the original full-extent bbox.

### Step 1: Phase 1 -- boundaries fully containing the tile

```sql
CREATE OR REPLACE TEMP TABLE phase1 AS
SELECT rkey, name, level FROM tile_boundaries
WHERE ST_Contains(geom, ST_MakeEnvelope(?, ?, ?, ?))
```

Parameters: bbox (4 values).

This no longer queries `wof.boundaries` directly. Since `tile_boundaries`
contains only ~2K rows, no R-tree is needed -- a sequential scan over ~2K
clipped geometries is fast. R-tree activation now matters only in Step 0,
where it narrows ~1M boundaries to the tile-intersecting subset. The
ST_Contains check runs against the clipped geometry, which is correct (see
correctness argument).

### Step 2: Phase 2 -- per-point containment for edge boundaries

Change the `edge_matches` CTE to reference `tile_boundaries` instead of
`wof.boundaries`:

```sql
edge_matches AS (
    SELECT p.{pk_expr} AS pk,
           'org.atgeo.places.wof:' || b.rkey AS rkey,
           b.name, b.level
    FROM places p
    JOIN tile_boundaries b
        ON p.{lat_expr} BETWEEN b.min_latitude AND b.max_latitude
       AND p.{lon_expr} BETWEEN b.min_longitude AND b.max_longitude
       AND ST_Contains(b.geom, ST_Point(p.{lon_expr}, p.{lat_expr}))
    WHERE LEFT(p.qk17, 6) = ?
      AND NOT EXISTS (
          SELECT 1 FROM phase1 ph WHERE ph.rkey = b.rkey
      )
)
```

The rest of the query (bulk_assign, all_matches, INSERT INTO
place_containment) is unchanged.

### Logging

Update the per-tile log line to include the tile boundary count:

```python
log.info("compute_containment: tile %d/%d z6=%s boundaries=%d phase1=%d (%.1fs)",
         i, total, z6, tile_boundary_count, phase1_count, elapsed)
```

Where `tile_boundary_count` is obtained from
`SELECT count(*) FROM tile_boundaries` after Step 0.

### Cleanup

Add `DROP TABLE IF EXISTS tile_boundaries` to the finally block:

```python
finally:
    con.execute("DROP TABLE IF EXISTS tile_boundaries")
    con.execute("DROP TABLE IF EXISTS phase1")
    con.execute("DETACH wof")
```

## Correctness argument

### Clipped geometry preserves ST_Contains for in-tile points

**Claim:** For any place P within the tile and any boundary B,
`ST_Contains(clipped_B, P) = ST_Contains(full_B, P)`.

**Proof:** Let `E` be the tile envelope and `clipped_B = ST_Intersection(B, E)`.

- If `ST_Contains(full_B, P)` is true: P is inside B. P is also inside E
  (because P's qk17 prefix places it in this tile). Therefore P is inside
  `B ∩ E = clipped_B`. So `ST_Contains(clipped_B, P)` is true.

- If `ST_Contains(full_B, P)` is false: P is outside B. Since
  `clipped_B ⊆ B`, P is also outside clipped_B. So
  `ST_Contains(clipped_B, P)` is false.

**Prerequisite:** P must be strictly within E. This holds because places are
assigned to tiles by qk17 prefix, and each place belongs to exactly one z6
tile. The quadkey system partitions the world such that a place's coordinates
fall within its tile's bbox.

### Phase 1 with clipped geometry

**Claim:** `ST_Contains(clipped_B, E) = ST_Contains(full_B, E)`.

- If `ST_Contains(full_B, E)` is true: E is inside B. Then
  `B ∩ E = E` (since E ⊆ B). So `ST_Contains(clipped_B, E)` =
  `ST_Contains(E, E)` = true.

- If `ST_Contains(full_B, E)` is false: Some part of E is outside B.
  Then `clipped_B = B ∩ E ⊊ E`, so clipped_B does not contain E.
  `ST_Contains(clipped_B, E)` is false.

### Pre-filter completeness

**Claim:** `ST_Intersects` does not exclude any boundary that would match a
point in the tile.

If boundary B contains point P which is inside tile E, then B and E share at
least point P, so `ST_Intersects(B, E)` is true. No false negatives.

### Clamped bbox correctness

The clamped bbox columns are used only as pre-filters for the BETWEEN clause
in phase 2. Clamping tightens the filter but cannot exclude valid matches:
if a point is inside the clipped geometry, it is necessarily within the
clamped bbox (since the clamped bbox is the intersection of the original bbox
and the tile bbox, which contains the clipped geometry's extent).

## Python-level changes

All changes are in `compute_containment()` in `garganorn/quadtree.py`.

### Summary of changes

1. **Add Step 0 query** -- `CREATE OR REPLACE TEMP TABLE tile_boundaries` with
   ST_Intersects filter and ST_Intersection clipping. One new `con.execute`
   call and one `count(*)` fetch.

2. **Retarget Phase 1** -- change `FROM wof.boundaries` to
   `FROM tile_boundaries` in the phase1 CREATE TABLE.

3. **Retarget Phase 2** -- change `JOIN wof.boundaries b` to
   `JOIN tile_boundaries b` in the edge_matches CTE.

4. **Update log line** -- add `boundaries=%d` field with `tile_boundary_count`.

5. **Update cleanup** -- add `DROP TABLE IF EXISTS tile_boundaries` to finally.

No other changes to the function signature, table schema, or output format.

## Files modified

- `garganorn/quadtree.py` -- `compute_containment()` only

## Verification plan

### Unit tests

Existing tests should pass unchanged. The optimization produces identical
output for any point within its tile. Run `pytest tests/` to confirm.

### Production verification

1. Run the pipeline against the full WoF boundaries DB on the server.
2. Compare `place_containment` output row-for-row against pre-optimization
   baseline. Any difference indicates a correctness bug.
3. Log analysis: verify `boundaries=N` in per-tile log lines. Expected:
   - Ocean tiles: boundaries=0
   - SF z6 tile: ~2,140
   - Mid-continent tile: low thousands
4. Timing: compare per-tile elapsed times against baseline.

## Risk assessment

### ST_Intersection producing empty geometry

If a boundary only touches the tile edge (shares a line or point but no area
overlap), `ST_Intersection` produces a zero-area geometry. `ST_Contains` on
such geometry returns false for any point. This is correct behavior -- the
boundary doesn't actually contain any tile points. The degenerate row in
`tile_boundaries` is harmless.

### ST_Intersection performance

More expensive than ST_Intersects alone, but bounded by the ~2K filtered
boundaries. The 13ms ST_Intersects baseline suggests clipping adds modest
overhead (likely under 100ms). Savings in phase 2 from reduced vertex counts
far exceed this cost.

### Geometry type changes after clipping

`ST_Intersection` can change geometry types (Polygon → MultiPolygon if clip
splits the boundary, or GeometryCollection for mixed types). `ST_Contains`
behavior on GEOMETRYCOLLECTION results has not been verified in DuckDB's
spatial extension. During implementation, test `ST_Contains` on
GEOMETRYCOLLECTION geometries empirically. If it returns incorrect results
or errors, add a WHERE clause to `tile_boundaries` filtering out empty or
degenerate geometries (e.g., `WHERE ST_GeometryType(geom) != 'GEOMETRYCOLLECTION'`
or `WHERE NOT ST_IsEmpty(geom)`).

### Antimeridian tiles

Quadkeys partition the Mercator projection without wrapping. `quadkey_to_bbox`
computes coordinates from tile indices, so `lon_min < lon_max` always holds.
No risk.
