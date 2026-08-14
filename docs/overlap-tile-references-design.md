# Division tile references: implementation design

Implementation-level design for the `overlap-tile-references` shippable
unit, shipped as `stage_division_tile_references`. `performance-improvements.md`'s
containment/tiling section states the requirements this design is checked
against and pins the reference zoom.

## Outcomes

What the unit has to be true of, restated from those requirements as
things that are either so or not. Each is asserted in
`tests/test_division_tile_references.py`.

1. **Discoverability.** For every client bbox query B and every division D
   whose geometry intersects B, at least one tile referencing D intersects
   B. This is the invariant the single-interior-point assignment violated,
   and the reason this unit is a correctness fix. Checked against
   `ST_Intersects` over the real geometry, not against the same bbox
   arithmetic the stage used.
2. **Bounded overshoot.** A division may be referenced by a tile its
   geometry doesn't reach — references are derived from the bbox — but
   never by one its bbox doesn't touch. There is a floor as well as a
   ceiling: a division inside one cell is referenced once, since a
   duplicate reference is a duplicated record in another tile.
3. **Record shape unchanged.** `tile_assignments.parquet` keeps its schema
   (`place_id VARCHAR, tile_qk VARCHAR`) and its `(tile_qk, place_id)`
   sort, which is what `stage_export`'s streaming cursor groups on. A
   division gains N rows where it had one, and all N copies of its record
   must be byte-identical across tiles — the spec's dedup-by-rkey rule
   silently drops all but one, so per-tile divergence is data loss.
4. **Placement by size.** A division is placed by its own extent, not by
   record density and not by where a seam happens to fall. The
   record-density splitter, and `max_per_tile` with it, applies only to
   the point sources.
5. **One-time cost.** Re-runs only when its input changes, under the same
   `artifact_fresh` discipline as every other stage.

## What this replaces

`overture_division_import.sql` computes `qk17` from each division's
interior point (`ST_PointOnSurface` of the merged geometry), so the point
is inside the division — but it is still one point. `run_pipeline`
(`quadtree.py`) calls `stage_tile_assignment` (the record-density
`max_per_tile` splitter) unconditionally for every source, feeding it that
single point, so a division is discoverable only through the one tile
containing it. This design forks the call: `overture_division` routes to a
new function; `overture_place` and `osm` keep using
`stage_tile_assignment` unchanged, since their one-point-per-record model
is still correct for point data. `qk17` itself is untouched by this
design.

## Placement

One rule: place the division at the deepest zoom where its bbox still fits
inside one cell, floored at z4 and capped at z17, then reference every cell
it touches at that zoom.

`covering.py` already had `bbox_to_quadkeys`, an antimeridian-aware *touch*
test (which cells does this bbox intersect at zoom Z, handling crossings
via the two-lobe `min_longitude > max_longitude` case; see
[gotchas.md](gotchas.md#antimeridian-bboxes-are-two-lobes)). It gains
`placement_zoom`, the matching *size* test, built on `_bbox_spans` (which
carries the same two-lobe rule) and `_lat_to_yfrac`, factored out of
`lonlat_to_tile` so the tile lookup and the size test share one projection
rather than two copies of the same arithmetic.

Size, not containment: a bbox no larger than a cell still straddles up to
four of them, which is why the zoom is paired with `bbox_to_quadkeys`
rather than treated as naming one tile. Placing each division in the
deepest cell that *contains* it would give one reference apiece, but would
drag a division straddling a shallow seam into a continent-sized tile for
no reason but a coordinate accident.

The floor is what handles divisions bigger than a reference cell: they fit
at no zoom, so they are placed at z4 and reference every z4 cell their
bbox touches. z4 is the floor because Discovery measured
cells-touched-per-division there at p99 = 1.67 with total duplication
under 1.2%, while every zoom from z5 up puts Russia or Canada over the
client's `max_tiles=50` cap. z17 is the cap, matching `qk17`'s role as the
finest granularity elsewhere in the pipeline.

**What this gives up, deliberately.** For a division bigger than a
reference cell, the exact set of cells its *geometry* touches is already
available — it is the covering artifact, truncated to z4, since the
covering recurses by appending digits and so every row carries its seed
cell as a prefix. Reading it would remove the handful of cells a big
division's bbox touches but its coastline doesn't. That is precisely the
false positive requirement 2 permits ("a reference derived from the
division's bbox rather than its geometry can do that... false positives
cost tile bytes, not correctness"), and buying it back costs a covering
read, a two-tier split, and a dependency on `stage_covering` having run.
Not worth it unless a real build shows the extra references mattering; the
stage logs its busiest tiles so that is measurable rather than assumed.

## The new tile-reference stage

`stage_division_tile_references`, added alongside (not replacing)
`stage_tile_assignment` in `stages.py`: freshness gate on
`boundaries_db`, one pass over the flattened bbox extents in `bnd.places`
applying the rule above, the rows loaded back into DuckDB and written
`ORDER BY tile_qk, place_id` to match `stage_tile_assignment`'s sort
convention, then the same atomic tmp-write-then-rename finalize every
other stage uses. No geometry is read and none is computed.

It logs its busiest tiles: `max_per_tile` no longer bounds a division
tile's size — geometry does — so nothing else would say how large one
got. It also logs the widest fan-out and the division responsible, which
is what surfaces a garbage geometry (`known-data-quality-issues.md`)
without a threshold anyone has yet calibrated. Divisions with a NULL or
NaN extent are dropped with a warning.

`run_pipeline`'s `stage_tile_assignment` call becomes a fork:

```python
if source == "overture_division":
    stage_division_tile_references(bnd_path, ta_parquet, ...)
else:
    stage_tile_assignment(places_parquet, ta_parquet, source,
                          max_per_tile=max_per_tile, ...)
```

`max_per_tile` stops being passed to divisions at all — the splitter is
gone for that source.

Division tiles can now be as shallow as z4 where place tiles bottom out at
z6. `qk[:6]` is the subdirectory prefix at both the write and the read
site and returns the whole quadkey for anything shorter, so the two still
agree and nothing downstream needs to know.

`tile_assignments.parquet`'s schema is unchanged
(`place_id VARCHAR, tile_qk VARCHAR`); divisions simply gain N rows
instead of 1.

## Why most downstream consumers don't need to change

- `compute_containment.sql` joins `tile_assignments` and groups by
  `(tile_qk, place_id)`; `relations_json` is built from `place_id`/
  `boundary_id`/`level` alone, never `tile_qk` — so N tile-assignment
  rows fan out to N *identical* `relations_json` rows automatically.
- `write_manifest_db` copies `tile_assignments` verbatim into the
  manifest's `record_tiles` table; a rkey under multiple `tile_qk`
  values is exactly what a multi-tile manifest should hold.
- `TileManifest.get_tiles_for_bbox` reads `SELECT DISTINCT tile_qk`
  from that manifest table — unrelated to per-record cardinality.

The one place that did assume one row per place — the export join —
needed a real fix, covered next.

## The export-join fan-out bug

`overture_division_export_tiles.sql` joins `places`, `tile_assignments`,
and `place_containment` to build each tile's export rows. Its containment
join currently keys on `place_id` alone:

```sql
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.id
LEFT JOIN place_containment pc ON pc.place_id = p.id
```

Once a division has N tile-assignment rows *and* N containment rows
(one per tile, same content, different `tile_qk` — exactly what
`compute_containment.sql`'s `(tile_qk, place_id)` grouping produces),
that join fans out to **N² rows per division** instead of N: every `ta`
row matches every `pc` row sharing the same `place_id`. The fix is one
join condition:

```sql
LEFT JOIN place_containment pc ON pc.place_id = p.id AND pc.tile_qk = ta.tile_qk
```

The `ON` clause references columns by name, so column ordering between
the containment artifact's real output and the fallback described below
doesn't matter. This is a general
invariant, not a division-specific patch: any query joining
`tile_assignments` against another per-place artifact that can also
carry multiple tile-scoped rows for the same `place_id` must key on
`(place_id, tile_qk)` together. `overture_place_export_tiles.sql` and
`osm_export_tiles.sql` don't need this — their `tile_assignments` and
`place_containment` rows stay one-per-place, so their existing
`place_id`-only join stays correct.

**This bug is dormant today, not live** — `compute_containment` runs
with `boundaries_db=None` for the division source (`quadtree.py`'s
`_cmd_all`), so `place_containment` has zero rows for divisions right
now, and the fan-out condition can't trigger. It must still be fixed as
part of this unit, because the byte-identical-copies requirement from
`performance-improvements.md` (one record per division per referencing
tile) has to hold structurally — not by accident of today's empty
containment data. This doc's other
units are explicitly working toward populating division containment,
at which point an unfixed join would become a live, silent bloat bug
with nothing in the test suite positioned to catch it.

### Every site that needs to change

`stage_export` (`stages.py`) substitutes a `containment_expr` into every
source's export SQL — shared code, not division-specific. When
`compute_containment` produced no output (the live path for divisions
today), it falls back to an empty placeholder subquery that only
projects `(place_id, relations_json)`:

```python
containment_expr = (
    "(SELECT NULL::VARCHAR AS place_id, NULL::VARCHAR AS relations_json WHERE 1=0)"
)
```

Once the division export SQL's join references `pc.tile_qk`, this
placeholder needs to project it too, or the query fails to bind:

```python
containment_expr = (
    "(SELECT NULL::VARCHAR AS place_id, NULL::VARCHAR AS relations_json, "
    "NULL::VARCHAR AS tile_qk WHERE 1=0)"
)
```

An exhaustive repo-wide sweep (grepping for `place_containment`,
`relations_json`-shaped fixtures, and every load site of the division
export SQL) found two more places that hand-build a `place_containment`-
shaped table for testing purposes and are exercised against the division
export SQL, each needing the same `tile_qk VARCHAR` column added to
their `CREATE TABLE`: `test_overture_division.py`'s
`_make_division_export_db` helper, and `test_tile_flatten.py`'s
`_make_export_db` helper. Neither call site passes containment rows
today, so both tables stay empty — this is a schema-only addition, no
call-site signature changes. `test_export.py` builds a
`place_containment`-shaped table in three more places —
`_make_overture_export_db`, the inline `CREATE TABLE` in
`_run_full_pipeline`, and `_create_place_containment` — all checked and
confirmed *not* to need a change, since every call site of all three
loads only the place or OSM export SQL, never the division one.

### Regression coverage

No existing test builds a division with more than one tile reference, so
nothing in the suite would catch a regression of the join predicate
above. A new test, `TestDivisionMultiTileContainmentJoin` in
`test_overture_division.py` (placed after `TestExportStripJsonNulls`),
builds one division with three `tile_assignments` rows (distinct
`tile_qk` values) and three matching `place_containment` rows (same
`place_id`, matching `tile_qk`, identical `relations_json`, mirroring
what `compute_containment.sql` actually produces), runs the division
export SQL, and asserts exactly three `tile_export` rows come out — nine
would mean the old N² join regressed back in; fewer than three would
mean rows were dropped.

## Open items

- **The referencing-count guardrail is not built.** It remains
  `performance-improvements.md`'s open question. What ships instead is a
  log line naming the widest fan-out and the division responsible, which
  gives an operator the same signal without inventing thresholds nobody
  has calibrated. Discovery's two concrete cases — Antarctica at 71 cells,
  and one level-50 division at 27 cells against a level-50 median of 1 —
  are the calibration data a real guardrail would want, and it wants a
  production run first. A hard fail is not the answer either way:
  Antarctica is legitimate geography, and a failing check would need a
  growing exemption list to keep real builds running.
- **Per-tile division counts are unbounded by design.** Discovery accepted
  this (one z4 cell touched by 130,269 divisions, though most of those are
  small enough to be placed deeper). The busiest-tile log is how a real
  build would show it mattering.
- **The bbox-versus-geometry overshoot is unmeasured.** See "What this
  gives up" above. One query over `bnd.places` extents against the
  covering would size it, no pipeline run needed.
