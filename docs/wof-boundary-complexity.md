# WoF Boundary Complexity Reference

Production boundaries DB: **1,005,557 boundaries** across 15 WoF place
type levels.

## Vertex counts by level

| level | count   | avg vertices | max vertices |
|-------|---------|-------------|-------------|
| 0     | 15      | 28,390      | 138,561     |
| 5     | 11      | 15,794      | 40,259      |
| 10    | 214     | 33,675      | 778,840     |
| 15    | 140     | 2,229       | 33,511      |
| 20    | 502     | 9,112       | 571,505     |
| 25    | 4,219   | 6,049       | 927,196     |
| 30    | 575     | 6,570       | 172,670     |
| 35    | 41,619  | 1,456       | 283,374     |
| 45    | 127,470 | 599         | 226,232     |
| 50    | 765,105 | 201         | 240,740     |
| 55    | 442     | 1,097       | 11,084      |
| 60    | 1,230   | 592         | 19,799      |
| 65    | 59,588  | 135         | 65,832      |
| 70    | 1,928   | 208         | 15,728      |
| 75    | 2,499   | 3,833       | 811,842     |

## Performance implications

### Filtering alone is insufficient

`ST_Intersects(geom, tile_envelope)` with R-tree reduces 1M boundaries
to ~2,140 for a SF z6 tile in ~13ms. But the surviving boundaries still
carry their full vertex counts. A level-25 boundary with 927K vertices
costs 927K vertex traversals per `ST_Contains` call — even if only a
sliver of that geometry overlaps the tile.

### Geometry clipping

`ST_Intersection(geom, tile_envelope)` clips each boundary to the tile
bbox. A country-spanning boundary becomes just the portion overlapping
the tile — potentially reducing 927K vertices to hundreds.

For point-in-polygon queries where all points are within the tile (our
case: places are constrained by qk17 prefix), `ST_Contains(clipped, pt)`
is equivalent to `ST_Contains(full, pt)`.

### R-tree activation

DuckDB R-tree indexes only activate for **top-level WHERE clauses**.
They do not activate inside CTEs, subqueries, or JOIN ON conditions.
Any query that needs R-tree must be a standalone statement, typically
materialized to a temp table.

### Phase 1 returned 0 for z6 tiles (before clipping)

z6 tiles are ~5.6° x 4.5°. Before the ST_Intersection clipping
optimization (see "Solution: Pre-filter and clip" below), no real WoF
boundary fully contained an area that large, so
`ST_Contains(geom, tile_envelope)` returned 0 matches and all
boundaries fell through to phase 2 (per-point containment). With
clipping, boundaries are trimmed to the tile envelope, so a boundary
whose clipped geometry fills the entire tile now matches in phase 1.

## Query to reproduce

```sql
SELECT level, count(*) as cnt,
       avg(ST_NPoints(geom))::int as avg_verts,
       max(ST_NPoints(geom)) as max_verts
FROM boundaries GROUP BY level ORDER BY level;
```

Measured 2026-04-10 against production WoF boundaries DB.

## Solution: Pre-filter and clip

Implemented 2026-04-10 on branch `feat/quadtree` in
`compute_containment()` in `garganorn/quadtree.py`.

The optimization has two parts, both executed as step 0 before the
phase 1/phase 2 containment logic:

1. **ST_Intersects pre-filter**: Uses the R-tree index to reduce ~1M
   boundaries to those intersecting the tile envelope (~2K boundaries
   for a typical z6 tile, ~13ms).

2. **ST_Intersection geometry clipping**: Clips each surviving boundary
   to the tile envelope. Boundaries that span large areas (countries,
   regions) are reduced from their full vertex count to only the portion
   overlapping the tile. A level-25 boundary with 927K vertices becomes
   just the clipped fragment — potentially hundreds of vertices.

The clipped boundaries are materialized to a temp table
(`tile_boundaries`). Phase 1 and phase 2 containment queries run
against this table instead of the full boundaries table. Since all
places in a tile fall within the tile envelope, `ST_Contains(clipped,
point)` is equivalent to `ST_Contains(full, point)` — clipping does
not affect correctness.
