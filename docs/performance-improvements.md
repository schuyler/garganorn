# Performance improvements

Proposed performance work on code that already works correctly —
open-ended, one section per idea, each with its own status. This doc
holds performance-improvement ideas generally, not just containment; add
new sections here rather than starting another file. Nothing here is
scoped for implementation until its section says so.

## Containment computation: polygon tiling, and correct division tile assignment

Status: scoped, not started. No design has been reviewed. Scoping
conversation 2026-08-08 merged this with the division tile assignment and
containment names problem (formerly tracked separately) once both turned
out to need the same mechanism — see Solution below. No longer purely a
performance nice-to-have: it also fixes a real correctness bug (next
section), which settles the old "is it worth doing yet" question.

### Problem: containment performance

Log analysis of the 2026-08-08 build (`compute-containment.log`,
`tile-build.log`) shows `compute_containment`'s per-batch cost is
near-linear in candidate count `n` in aggregate (log-log slope 0.71-0.75 for
n≥1000, both the overture_place and osm runs) — but a consistent set of
~20 quadtree cells run **10-38x slower than their own `n` predicts**,
independently, in both runs. Every one of them falls on a complex coastline
or archipelago: Chesapeake Bay, Cuba/Bahamas, Nova Scotia/Gulf of St.
Lawrence, Maine coast, BC fjords, Norway fjords, Finland lakes, Hawaii,
Indonesia, Sicily, Río de la Plata. `032010` (Chesapeake/DC-MD-VA-DE) is the
worst outlier in both runs (~23-35x) and also a top-10 cell by raw `n`.

This is the same mechanism `compute_containment.sql`'s own header already
documents for the Nunavut case (~200k vertices): the edge-arm join's
`ST_Contains`-style test costs scale with boundary polygon **vertex count**,
not candidate point count. Right now every candidate point tested against a
boundary pays that boundary's full vertex count, no matter how far from the
complex part of the coastline the point actually is.

### Problem: division tile assignment is a correctness bug

`overture_division_import.sql` computes, per division:

```sql
-- qk17 placed at the geometry centroid for tile assignment
ST_QuadKey(
    (ST_XMin(ma.geometry) + ST_XMax(ma.geometry)) / 2.0,
    (ST_YMin(ma.geometry) + ST_YMax(ma.geometry)) / 2.0, 17
) AS qk17,
```

Two distinct bugs, both from indexing an area by a single point:
1. The comment claims "centroid"; the computation is the bbox midpoint,
   which is not the centroid and for crescent-shaped, multipart, or
   overseas-territory geometries (Norway, Chile, Indonesia) is not inside
   the division at all.
2. The depth is hardcoded to z17 regardless of the division's size, so
   every division — a neighborhood or Russia — gets one z17 leaf-tile
   assignment. A client's bbox query finds whichever divisions' single
   z17 point happens to fall inside it: it misses the division the user
   is standing in whenever that point lands outside the query bbox, and
   it misses every division larger than a z17 tile unconditionally,
   which in practice is most of them.

Divisions do not go through `compute_tile_assignments.sql` (the
record-density coarsest-fitting-zoom assigner used for places) —
`stages.py`'s dispatch returns to `stage_division_import` before that path
is reached. The `qk17` column above is the division collection's entire,
independent tile-assignment mechanism today.

### Solution

**Pre-split large/geometrically-complex boundary polygons against a coarse
global grid at import time** — a one-time cost paid once per boundary —
and use the resulting fragments for two purposes at once: bound the
containment edge-arm test's cost to a fragment's vertex count instead of
the whole polygon's, and give every division a correct set of tile
references instead of one bbox-derived point. Divisions are referenced in
every tile they overlap, computed from real geometry, not the bounding
box. Two tiers, separated by one test (no level-based gating needed — a
sub-locality's polygon is never large enough to trigger the first tier):

- **Tier A — not wholly contained within one cell at the decomposition
  zoom.** Intersect the polygon against the grid, store one fragment per
  overlapping cell, reference the division in each cell whose fragment is
  non-empty. Fixes Norway/Chile/Indonesia for free, since real geometry
  replaces the bbox, and bounds the edge-arm test to that fragment's
  vertex count.
- **Tier B — wholly contained in one cell at that zoom.** Assign to the
  deepest tile that wholly contains the division's bbox — the same rule
  as today, minus the midpoint-instead-of-centroid bug and the z17 cap.
  This is where sub-localities land, generally much deeper than the
  Tier A decomposition zoom.
- **Straddling needs no special case.** A division touching a cell
  boundary at its tier's placement zoom is simply referenced in each cell
  it touches, same mechanism as Tier A.
- **Does not collide with the record-density tile assigner.** Confirmed
  by reading `stages.py`: divisions never route through
  `compute_tile_assignments.sql`'s `max_per_tile` splitter to begin with
  (the division dispatch returns before that code path). This design
  extends the division collection's existing, separate, geometry-only
  assignment mechanism.

**Schema consequence**: `qk17` today is a scalar column, one tile per
division row. A division mapping to N tiles needs a one-to-many
structure — likely a separate division-to-tile table rather than a
column, paralleling how `tile_assignments` already relates places to
tiles. Not designed here.

**Dedup becomes a client requirement.** Divisions can now legitimately
appear in more than one tile, where before (bugs aside) a record appeared
in exactly one. `atgeo-spec.md` needs a normative statement that a client
concatenating results from more than one tile MUST dedup division
relation records by rkey — write this once the mechanism above actually
ships, not before, since the spec would otherwise describe behavior that
doesn't exist yet.

Already shipped, ahead of the tile-assignment fix, because they didn't
depend on it: `relations.within` entries now carry `name` and `level`
(`compute_containment.sql`), and containment now reaches every level in
`LEVEL_VOCAB` (country through microhood, not just locality and coarser —
the `WHERE level <= 50` filter in `stages.py` had no principled reason to
exist once sub-localities were in scope, and is gone).

### Open questions

- [ ] What triggers tiling a polygon — a vertex-count threshold, or the
      observed slowdown-vs-`n` ratio, or both?
- [ ] Decomposition zoom: reuse the existing qk17 / `partition_zoom=6`
      quadtree partitioning already used for containment batching, or
      something independent? Same parameter for both the performance and
      tile-assignment uses — resolve once, here.
- [ ] Does decomposition produce and store the clipped fragment geometry
      (needed for the containment-cost fix) or only cell membership
      (sufficient for tile-reference correctness alone)? A
      cell-membership-only version is a smaller first cut if the
      correctness fix is wanted before the performance fix — confirm
      whether that staging is acceptable or whether the two should ship
      together.
- [ ] What structure holds the division-to-tile mapping (table shape,
      where it's built relative to `stage_division_import` and
      `compute_containment`)?
- [ ] Parity/correctness testing: tiled/multi-tile division output must
      match a from-scratch geometric answer — same containment and
      discoverability, not just "doesn't crash." D7 (antimeridian
      OR-logic in `compute_containment.sql`) must be preserved through
      decomposition, not just the single-lobe case.
- [ ] Where does the one-time decomposition cost land — `stage_division_
      import`, or the `boundaries.duckdb` build? Whichever it is has to
      fit the existing artifact pipeline without restructuring
      `boundaries.duckdb`.
- [ ] Sub-locality inclusion (already shipped, see above) roughly doubled
      the level vocabulary considered by `bnd.places` — worth a
      size/cost sanity check against the evidence below before
      implementing the rest, not a blocker.

### Evidence

Full cell-by-cell numbers (prefix, n, duration, actual÷predicted) live in
the 2026-08-08 log-analysis session, not reproduced here — re-run the same
analysis against a fresh build log before implementing, since this data is a
snapshot of one build.

A candidate second cost driver was investigated and ruled out — whether
same-level boundary overlap density (as opposed to per-boundary vertex
complexity) also drives the slow cells above. It doesn't, and while
investigating it, a data characteristic surfaced that isn't performance
work at all (source boundaries duplicated under two different IDs). Both
now live in `known-data-quality-issues.md`.
