# Fragment containment: implementation design

Implementation-level design for the `fragment-containment` shippable unit
described in `performance-improvements.md`'s containment/tiling section.
That document states the problem and the requirements this design is
checked against, and pins the Discovery constants (`V = 5000`, depth cap
16); this document turns them into a concrete plan against the current
codebase.

The unit's goal is a cost bound: the edge arm's point-in-polygon test
must cost a *fragment's* vertex count, not the whole boundary's. Today a
point tested against Nunavut pays ~200k vertices; Discovery measured
`ST_NPoints` up to 345,467.

## The covering loop

`stage_covering` (`covering.py`) seeds `l_current` from
`covering_seed.sql`: one row per (boundary, z4 tile) that intersects,
carrying `geom = ST_Intersection(boundary, qk_env(tile))`, filtered by
`ST_IsValid(geom) AND ST_Area(geom) > 0`. It then runs `covering_level.sql`
once per zoom from `cover_min_zoom` to `cover_max_zoom`, inclusive, with no
special case for the terminal zoom: materialize `is_interior =
ST_Contains(geom, qk_env(tile_qk))` into `l_flagged`, emit interior rows to
`covering_out`, emit edge leaves — non-interior rows satisfying that
level's `${leaf}` predicate — to `covering_out` with their clipped
fragment geometry, expand the rest ×4 by appending a quadkey digit,
re-clip each child to its own envelope, drop degenerate clips, rename
`l_next` → `l_current`. `${leaf}` is `FALSE` below `cover_min_leaf_zoom`,
`npoints <= V` between the floor and `cover_max_zoom`, and `TRUE` at
`cover_max_zoom` (see "The level SQL is uniform across zooms" below).

`covering_out` is `(tile_qk, boundary_id, level, kind, geom)` — `geom` is
the edge leaf's clipped fragment, `NULL` for interior rows. It is written
per z4 prefix, `ORDER BY tile_qk, boundary_id`, with `_meta.json` written
last as the freshness sentinel; the whole directory is swapped in
atomically.

One property of the output is load-bearing downstream:

- **Antichain.** A cell is either emitted (interior at any level, edge at
  whatever level the leaf rule fires) or expanded, never both, so no
  boundary's covering row is a quadkey-prefix descendant of another of its
  rows. `test_no_covering_tile_descends_from_interior_tile` and
  `test_antichain_generalized_over_all_kinds` assert it, and
  `pipeline-artifacts.md` records the consequence: a place matches each
  boundary at most once downstream, so no `DISTINCT` is needed.

Edge leaves are *not* depth-uniform. They sit anywhere in
`[cover_min_leaf_zoom, cover_max_zoom]`, which is why the edge arm is one
join per zoom rather than a single fixed-length prefix match. The
antichain is what keeps that fan of joins from double-counting: a point's
`qk17` has exactly one ancestor at each zoom, and at most one of those
ancestors is a leaf of any given boundary.

`compute_containment` (`stages.py`) reads the covering's `_meta.json` for
all four zoom/capacity params, generates one interior arm per zoom in
`[cover_min_zoom, cover_max_zoom]` and one edge arm per zoom in
`[cover_min_leaf_zoom, cover_max_zoom]` (13 and 5 at the current
defaults), materializes `cov` (the whole covering parquet for one z4
prefix) and `p` (one batch of candidate points) as temp tables, and
substitutes both into `compute_containment.sql`.

## Where recursion stops

Two conditions, not one. The adaptive capacity rule bounds the *test*
cost; a depth floor bounds the *join* fan-out. A leaf is emitted at zoom
`z` when

```
NOT is_interior AND (z = cover_max_zoom OR (z >= cover_min_leaf_zoom AND npoints <= V))
```

and expanded otherwise. Four parameters govern it:

| param | value | role |
| --- | --- | --- |
| `cover_min_zoom` | 4 | seed zoom |
| `cover_min_leaf_zoom` | 12 | shallowest zoom an edge leaf may be emitted at |
| `cover_max_zoom` | 16 | hard depth cap |
| `cover_vertex_capacity` | 5000 | `V` |

`cover_min_leaf_zoom = 12` is the value `COVER_MAX_ZOOM` held before this
unit split it into a shallower fan-out floor and a deeper depth cap, so
the edge join's fan-out at z12 is exactly what production already
produces, and recursion below z12 behaves exactly as it did before the
split. The capacity rule governs only whether a fragment descends *past*
z12, which is what Discovery measured: fragments fully resolve by z15 —
three levels past the former `COVER_MAX_ZOOM = 12` cap.

**The floor is not optional.** Without it, a simple inland division
(p50 = 123 vertices) is under capacity at z4 and stops there, so
`covering/1202.parquet` would hold a z4 edge leaf for each of the 130,269
divisions Discovery found in that one z4 cell. The edge arm's level-4
join keys on `left(p.qk17, 4) = c.tile_qk`, so every candidate point in
that cell would join all 130,269 of them — the join emits the cross
product regardless of how cheap the predicate on top of it is. See
"Why it works this way" below.

A fragment that reaches `cover_max_zoom` still above `V` is emitted as an
edge leaf anyway. There is no error path; `stats["over_capacity_leaves"]`
counts them so the pair (`V`, cap) can be recalibrated from a real run.
At `V = 5000` Discovery expects that count to be zero.

## The level SQL is uniform across zooms

`l_flagged` materializes both per-row scalars — the existing comment's
"do not evaluate twice" discipline applies to `ST_NPoints` for the same
reason it applies to `ST_Contains`:

```sql
CREATE TEMP TABLE l_flagged AS
SELECT *, ST_Contains(geom, qk_env(tile_qk)) AS is_interior,
          ST_NPoints(geom) AS npoints
FROM l_current;
```

Interior emission carries a `NULL::GEOMETRY`. Edge emission lives in the
level SQL, gated on a `${leaf}` predicate that `stage_covering` substitutes
per level — `FALSE` below `cover_min_leaf_zoom`, `npoints <= ${V}` between
the floor and the cap, `TRUE` at the cap:

```sql
INSERT INTO covering_out
SELECT tile_qk, boundary_id, level, 'edge', geom
FROM l_flagged WHERE NOT is_interior AND (${leaf});
```

and the expansion adds `AND NOT (${leaf})` to its existing
`WHERE NOT is_interior`. The `ST_IsValid(geom) AND ST_Area(geom) > 0`
filter on `l_next` stops a border running along a grid line from
producing line or point fragments, which would otherwise be *stored* as
well as referenced.

`stage_covering`'s loop runs `covering_level.sql` for every zoom in
`[cover_min_zoom, cover_max_zoom]` with no special case, and drops
`l_current` after the loop.

## Persisted geometry and the covering schema

`covering_out` and each `covering/<qk4>.parquet` carry one column beyond
membership:

```
tile_qk VARCHAR, boundary_id VARCHAR, level INTEGER, kind VARCHAR, geom GEOMETRY
```

`geom` is the edge leaf's clipped fragment, and `NULL` for interior rows
(an interior cell's geometry is its own envelope; storing it would be
redundant). It is inline in the existing rows: the covering is keyed
`(boundary_id, tile_qk)`, sharded per z4 prefix, and the edge arm reads
those rows directly — no separate file, key, or join. DuckDB 1.4.4's
parquet writer takes `GEOMETRY` directly: it writes GeoParquet, round-trips
as `GEOMETRY` when spatial is loaded, decodes as a WKB `BLOB` when it
isn't, and projections that don't select `geom` work with no spatial
extension at all.

Sizing: every edge leaf carries geometry, so the artifact is roughly the
size of `boundaries.duckdb`'s geometry column plus the vertices clipping
introduces at cell crossings — not the 4.05M vertices Discovery reports,
which count only the fragments produced *below* z12 (see "Why it works
this way" below). `stage_covering`'s returned `stats` dict carries two
top-level keys alongside `total`/`interior`/`edge`/`per_level`:
`edge_vertices` (`SUM(ST_NPoints(geom))` over `kind = 'edge'`) and
`over_capacity_leaves` (edge rows at `cover_max_zoom` with
`npoints > V`). Both are computed in the stats query block and land in
`_meta.json`'s `stats`, so a real run measures the artifact instead of
estimating it.

Compute cost is near-unchanged: the z12 clips being persisted are
computed as part of the same level loop regardless of whether they're
written, and descent past z12 applies to a small set of over-capacity
fragments. The added cost is the write — the per-prefix `COPY`'s
`ORDER BY tile_qk, boundary_id` carries geometry through the sort. The
sort stays per prefix, bounded by one z4 shard, where a single global
sort over the whole payload is the pattern `knowledge/export_sort_spill_ceiling.md`
records blowing the 250GB spill cap and that the tile export was moved
away from.

## The variable-depth edge arm

Edge leaves exist at every zoom in
`[cover_min_leaf_zoom, cover_max_zoom]`, so `compute_containment.sql`'s
edge arm is a generated `UNION ALL` over that range, mirroring
`interior_arms`. `compute_containment` builds an `edge_arms` string the
same way and substitutes it for `${edge_arms}` in the template:

```python
edge_arms = "\nUNION ALL\n".join(
    f"    SELECT p.place_id, c.boundary_id, c.level\n"
    f"    FROM p JOIN cov c\n"
    f"      ON c.kind = 'edge' AND len(c.tile_qk) = {L}\n"
    f"     AND left(p.qk17, {L}) = c.tile_qk\n"
    f"    WHERE ST_Covers(c.geom, ST_Point(p.lon, p.lat))"
    for L in range(cover_min_leaf_zoom, cover_max_zoom + 1)
)
```

Interior arms are generated over `[cover_min_zoom, cover_max_zoom]` (4 to
16) because a deep-split edge cell's children can themselves be interior:
thirteen interior arms and five edge arms in total.

The arm has no `bnd.places` join: the fragment *is* the geometry to test,
so there is no boundary bbox pre-filter or separate geometry lookup;
`level` comes directly from `cov`. `bnd.places` is joined once, in the
final SELECT, for `names."primary"`. There is no fragment-bbox pre-filter
either: the point is already known to be in the fragment's tile, and the
fragment is at most tile-sized and at most `V` vertices, which is the
bound this unit exists to establish.

## `ST_Covers`, and why no dedup

Fragments are tested with `ST_Covers`, not `ST_Contains`. Splitting
creates interior seams that were never boundaries of the original
polygon; `ST_Contains` excludes boundaries, so a point on a seam would be
inside the whole division and inside neither fragment.

With `ST_Covers` the semantics are exact, not approximate. For a point
`x` and boundary `b`, let `F` be the leaf cell of `b`'s covering that
contains `x` (the antichain guarantees at most one). If `x` is strictly
inside `b`, `F` exists and covers `x`. If `x` is on `b`'s boundary, `F`'s
clip contains that boundary segment, so `ST_Covers` is true. If `x` is
outside `b`, no fragment can cover it, since every fragment is a subset
of `b`. So the fragment result equals `ST_Covers(b.geometry, x)` exactly,
and differs from a whole-polygon `ST_Contains(b.geometry, x)` only on
`b`'s own boundary — a measure-zero set where that answer is arbitrary
anyway.
Everything else is floating-point noise from `ST_Intersection`, order
1e-15 degrees.

`matches` stays `UNION ALL`. A point's quadkey chain meets at most one
leaf per boundary — the same antichain property `pipeline-artifacts.md`
already relies on for the current no-`DISTINCT` claim, and this unit does
not weaken it: leaves appear at multiple zooms, but they are still
mutually non-descending, and `ST_Covers` widens the predicate, not the set
of cells a point can join. Adding a `DISTINCT` would guard a state the
build cannot reach; the invariant is instead held by tests (below).

## Params and freshness

`stage_covering`'s freshness gate compares recorded `cover_min_zoom` and
`cover_max_zoom` against the arguments and skips when they match and
`_meta.json` is newer than `boundaries_db`. `cover_min_leaf_zoom` and
`cover_vertex_capacity` are part of both the `meta` dict written at the
end and the gate's comparison, so changing `V` cannot silently reuse a
stale artifact. A `_meta.json` written before those keys existed lacks
them entirely, so `recorded.get(...)` returns `None`, the comparison
fails, and the covering rebuilds.

`compute_containment` reads the covering's `_meta.json` for
`cover_min_zoom`/`cover_max_zoom` and puts them in `params`. It also reads
`cover_min_leaf_zoom` (needed to generate the edge arms) and
`cover_vertex_capacity` (not needed for the query, but `params` is the
artifact's record of what produced it, and mtime-based freshness is
defeated whenever an artifact directory is copied between hosts with
timestamps preserved). Both are read from `covering/_meta.json` the same
way as the two zooms, with the same fallback-to-default-on-missing
behaviour — never from a `compute_containment` keyword argument.
`cover_min_leaf_zoom` decides which levels the edge arms cover, so a
value sourced from anywhere but the artifact that was actually built
would silently drop arms and lose matches.

The literal fallback defaults at the top of `compute_containment` must
track `covering.py`'s constants: a stale `cover_max_zoom = 12` there
against a covering built to z16 would silently drop the z13–z16 arms and
lose matches. `covering.py` imports from `stages.py`, so a module-level
import back would be a cycle; `compute_containment` instead uses a
function-local `from garganorn.covering import ...`.

`quadtree.py`'s `covering` subcommand has `--min-leaf-zoom` and
`--vertex-capacity` alongside `--min-zoom`/`--max-zoom`, which makes
recalibrating `V` against real data possible without editing source.
`run_pipeline` and `_cmd_all` pass no zoom arguments.

## Bounded memory

The decomposition works fragment-by-fragment, level-by-level: each
level's work is `l_flagged`-sized, each clip is one fragment against one
child envelope, and `ST_NPoints` is a scalar over an already-materialized
row. Peak `l_current` size is bounded by each level's expanding set: only
over-capacity fragments recurse past z12; interior cells and
under-capacity edge cells stop earlier.

Three places where a naive implementation violates the bound:

- **Inlining the covering read into the edge arm.**
  `compute_containment.sql`'s header documents a 100x+ memory blowup
  (~150MB vs 30GB+) from letting the planner see a CTE over a full
  backing relation instead of a pre-materialized subset. `cov` must stay
  a per-z4-prefix temp table built with `SELECT *`; since it carries
  geometry, the same mistake costs more.
- **Computing the capacity predicate over an unmaterialized expression.**
  Referencing `ST_NPoints(ST_Intersection(...))` inline lets the planner
  duplicate the intersection across the emit and expand branches.
  `l_flagged` materializes it once.
- **Verifying mass balance with `ST_Union_Agg`.** Unioning a boundary's
  fragments back together is exactly the whole-polygon × whole-grid shape
  the bounded-memory requirement forbids. It is also unnecessary — see
  below.

Two resident sets are large enough to name:

- **`covering_out`.** `stage_covering` accumulates every interior and
  edge row, for every boundary and every zoom, in one temp table —
  carrying the entire persisted geometry set, order 10 GB — before
  partitioning it into per-prefix files, and spills to the stage's
  owned, `max_temp_directory_size`-bounded spill dir on the output
  volume. The single accumulator is kept: emitting per prefix instead is
  not the local change it looks like. Rows for a given prefix are
  produced at every level, parquet cannot be appended, and the
  alternatives that avoid the accumulator (one file per prefix *and*
  level, or a `PARTITION_BY` copy) either break the
  `covering/<qk4>.parquet` contract `compute_containment` depends on or give up
  the per-file sort, since the stage sets `preserve_insertion_order =
  false` and a partitioned write therefore cannot preserve an upstream
  `ORDER BY`. The accepted price is the write loop's 256 scans of the
  spilled accumulator, one per prefix, each reading geometry:
  low-single-digit TB of sequential spill reads against a build already
  measured in hours. If that measures worse than it reads, the fallback
  is a `PARTITION_BY` copy with the per-file sort dropped.
- **`cov`.** Skewed by the same z4 clustering Discovery measured: the
  `1202` prefix holds 21% of all divisions, so its `cov` carries a
  correspondingly large share of the stored geometry for the duration of
  that z4 group's batches. Within the 48GB default limit, and spillable.

## Antimeridian

`compute_containment.sql`'s edge arm has no `CASE` on
`b.min_longitude <= b.max_longitude` with an OR-branch for
antimeridian-crossing boundaries — that branch belonged to the
`bnd.places` bbox pre-filter, and the pre-filter is gone with the join.
Correctness comes from the geometry test, `ST_Covers` against a fragment
whose extent is at most one tile — a two-lobe bbox cannot arise.

The antimeridian two-lobe handling remains live on the build side (see
[design-constraints.md](design-constraints.md#d7-antimeridian-bboxes-are-two-lobes)).
`covering_seed.sql`'s join keeps its own `min_longitude > max_longitude`
case, which is what stops gap tiles between the lobes from being seeded,
and `bbox_to_quadkeys` keeps its two-lobe logic for
`overlap-tile-references`.

What a test asserts: with a constructed two-lobe boundary (the
import-side bbox filter drops ±180-crossers, so the fixture must be
built, not found), a point in each lobe matches and a point in the gap
does not; and `compute_containment.sql` contains no `min_longitude`
reference, as a regression guard against the pre-filter being
reintroduced with the fragment test.

## Verification

**Structural invariants** (exact, cheap, no reference run):

1. *Mass balance, per boundary.* Leaves are pairwise disjoint (antichain
   ⇒ no ancestor relation ⇒ disjoint tiles), so their areas sum; no union
   is needed. For each boundary,
   `SUM(ST_Area(qk_env(tile_qk)))` over interior rows plus
   `SUM(ST_Area(geom))` over edge rows equals
   `ST_Area(ST_Intersection(b.geometry, merc_extent))` within a relative
   tolerance of 1e-6, where `merc_extent` is the ±85.05112878 envelope.
   Clipping to the Mercator extent on the right-hand side is what makes
   the identity exact for polar boundaries rather than requiring them to
   be excluded. Catches dropped and duplicated fragments.
2. *No orphaned boundary.* Every boundary whose geometry intersects
   `merc_extent` has at least one covering row.
3. *Antichain.* No boundary's covering row is a quadkey-prefix descendant
   of another of its rows. Generalizes
   `test_no_covering_tile_descends_from_interior_tile`, which today only
   tests interior ancestors. This is the invariant standing in for the
   absent `DISTINCT`, together with the existing
   `test_no_duplicate_rkeys_in_within`.
4. *Leaf depth.* No `'edge'` row is shallower than `cover_min_leaf_zoom`
   and none is deeper than `cover_max_zoom`. This unit falsifies
   `test_edge_kind_only_at_max_zoom` in `tests/test_covering.py`; that
   test is deleted and this one takes its place.
5. *Capacity.* Every `'edge'` row shallower than `cover_max_zoom` has
   `ST_NPoints(geom) <= V`. Every `'interior'` row has `geom IS NULL`.

**Sampled behavioral diff** (one-time, at migration). Run the current and
new containment over a stratified sample — the ~20 known-slow z6 cells
from `compute-containment.log`, the seam and antimeridian synthetics, and
a random background sample of z6 cells — and diff the
`(place_id, boundary_id)` match sets. Triage each disagreement by
`ST_Distance` from the point to `ST_Boundary(b.geometry)`. Since the two
answers differ only by `ST_Contains` vs `ST_Covers` on the boundary
itself, every legitimate flip sits within floating-point noise of the
boundary: the band is 1e-9 degrees, not the tens of metres the
containment-parity requirement allows for. Anything outside the band is a
failure with no tolerance.
Direction matters in triage: a *gained* match at zero distance is the
expected `ST_Covers` widening, while a *lost* match at any distance
indicates a dropped or misattributed fragment.

**Synthetic unit tests** (recurring), against a small fixture DB with the
covering built at reduced zooms so a fragment split is reachable:

- Seam point: a boundary spanning a cell edge, point exactly on the
  internal seam — matched, exactly once.
- Grid-aligned border: a boundary whose edge lies along a cell edge —
  no zero-area fragment is stored, no spurious reference is produced.
- Antimeridian: as described under "Antimeridian" above.
- Depth cap: a synthetic high-vertex polygon with a small `V` and small
  cap — leaves exist at the cap above capacity, and `stats` counts them.
- Capacity: with a small `V`, a boundary that would be one leaf at
  default `V` produces several, and each is under capacity.
- Freshness: changing `cover_vertex_capacity` rebuilds the covering;
  changing nothing does not.

`test_containment_covering.py`'s brute-force oracle (`ST_Contains`) stays
as it is — its sample points are interior, where the two predicates agree.

## What this unit does not change

- Tile assignment. `stage_tile_assignment`, `tile_assignments.parquet`,
  and the division tile-reference policy belong to
  `overlap-tile-references`.
- Tile JSON shape. Fragments never leave the build; no record gains,
  loses, or reshapes a field.
- `boundaries.duckdb`. Unchanged schema, unchanged import path.
- The containment artifact's schema, batching, dir-swap, or Q3
  degradation path.
- The interior arms' semantics — only their zoom range.
- `qk_env_macro.sql`, `covering_seed.sql`'s join conditions, and
  `bbox_to_quadkeys`.

One thing it does change outside the covering: division `qk17` derives
from the geometry's interior point rather than its bbox midpoint
(`overture_division_import.sql`). The edge arm fetches the fragment
clipped to the leaf `qk17` names and tests the containment point against
it, so the two must describe the same location. They did not for
divisions — `qk17` came from the bbox midpoint while the containment
point is `ST_PointOnSurface` — and a division whose midpoint fell in a
different tile lost the match silently.

## Interaction with `overlap-tile-references`

`fragment-containment` has shipped; `overlap-tile-references` has not
started. What it will need to know about the covering as it now stands,
checked against `overlap-tile-references-design.md` rather than assumed:

Tier A truncates the covering to z4 with
`SELECT boundary_id, left(tile_qk, 4) ... GROUP BY 1, 2`, resting on the
property that a child quadkey never rewrites its parent's prefix. Leaf
depth and the added `geom` column don't disturb this: prefix truncation
is invariant to leaf depth, and the `GROUP BY` collapses rows that now
exist at more zooms. The query projects named columns and never
`SELECT *`, so the `geom` column is not read.

Tier B reads `bnd.places` bbox extents and `covering.py`'s
`bbox_to_quadkeys`/`bbox_fits_in_one_cell`, none of which this unit
touches. The two units share `covering.py` and `stages.py`, so if
`overlap-tile-references` work overlaps with further changes here, the
two must be sequenced rather than run as parallel streams.

## Why it works this way

1. **The depth floor bounds join fan-out, not test cost.** Stopping
   recursion purely once a fragment's vertex count drops under `V` would
   leaf a typical 123-vertex division at z4, and the edge join's level-4
   arm would then pair every candidate point in that z4 cell with every
   division leafed there — 130,269 of them in cell `1202`, by Discovery's
   count, emitted as a cross product before any predicate can filter it.
   `cover_min_leaf_zoom = 12` reproduces production's fan-out at z12
   exactly and confines the adaptive rule to descent *past* z12, which is
   also the range Discovery's own measurement addresses: fragments fully
   resolve by z15, three levels past the former `COVER_MAX_ZOOM = 12` cap.

2. **Persisted geometry has to cover every edge leaf, not only fragments
   produced by splitting below z12.** The 4.05M stored-vertex figure
   Discovery measured counts only fragments produced below z12 (2,244 of
   them at `V = 5000`, ≈1,805 vertices each). But the cost bound this
   unit exists to establish requires every edge leaf to carry its own
   clipped geometry, including the z12 leaves that never split —
   otherwise a Nunavut z12 cell whose clip is 500 vertices would still be
   tested against the 345k-vertex whole polygon, and the fix would buy
   nothing there. The 40-70x worst-case reduction is itself
   345,467 ÷ 5,000: the bound obtained precisely when every leaf's own
   geometry is stored and capped at `V`. So the artifact grows by
   roughly `boundaries.duckdb`'s geometry column plus clip overhead,
   which is what the `edge_vertices` stat measures on the first run.

## Decided

**Points beyond ±85.05° are clipped, and that is accepted.** Fragments
are clipped at the Mercator extent, so a point outside it inside a polar
boundary loses its `within` relations, where a whole-polygon `ST_Contains`
test would still match it. This follows from deriving containment from
quadtree tiles at all: the Bing tile system defines the map only within
±85.05112878°, because that bound is what makes the Mercator world square
and therefore quadkey-subdividable.

The affected population is 200 records across both sources — OSM has 1
above 85.05°N and 199 below 85.05°S (Amundsen–Scott, IceCube, the Jack
F. Paulus Skiway, and named Antarctic peaks); Overture has none.

## Open items

- `cover_min_leaf_zoom = 12` is inherited from the pre-split
  `COVER_MAX_ZOOM`, not measured. It trades edge-join fan-out against
  stored fragment count; the first real run's `edge_vertices` and
  per-level stats are what would justify moving it. A floor of 12 holds
  edge-join fan-out at what the pre-split covering already produced, which
  is known to work.

## Out of scope, but real

Those 199 southern records are not merely excluded today — they are
misplaced. The import SQL guards `ST_QuadKey` with `latitude BETWEEN -90
AND 90`, which validates geographic range rather than the range
`ST_QuadKey` is defined over, and DuckDB's `ST_QuadKey` *wraps* rather
than clamping. So they currently receive northern-hemisphere quadkeys
and pollute unrelated Arctic tiles. `covering.py`'s own `lonlat_to_tile`
clamps correctly, per Bing; the two implementations of the same
projection disagree. Neither caused nor fixed by this unit; it has its
own tranche.
