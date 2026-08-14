# Division tile references: implementation design

Implementation-level design for the `overlap-tile-references` shippable
unit described in `performance-improvements.md`'s containment/tiling
section. That document states the requirements and pins the Discovery
constants (reference zoom, fragment capacity) this design is checked
against; the Tier A/Tier B policy shape is this document's own.

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

## Referencing every cell a division overlaps (Tier A)

For divisions bigger than one cell at the reference zoom (z4, pinned by
Discovery), the tile references come from truncating the division's
existing covering artifact — no new geometry computation.

The covering (`stage_covering`, `covering_seed.sql`/`covering_level.sql`)
seeds at z4 and recurses by appending digits to each parent tile's
quadkey; a child's quadkey never rewrites its parent's prefix. That means
every row a boundary ever gets in its covering output, at any zoom,
shares the same leading four characters as its z4 seed tile. So
`DISTINCT boundary_id, left(tile_qk, 4)` over the covering output is
exactly "which z4 cells does this division's real geometry intersect":

```sql
SELECT boundary_id AS place_id, left(tile_qk, 4) AS tile_qk
FROM read_parquet('<covering_dir>/*.parquet')
WHERE boundary_id IN (<Tier A id set>)
GROUP BY boundary_id, left(tile_qk, 4)
```

The `WHERE` restriction to the Tier A id set matters: a Tier B division
(small) can still have covering rows spanning more than one z4 cell if
it straddles a z4 boundary, and that's legitimately Tier B's job (a
deeper, more precise placement), not this coarse truncation.

## Placement for divisions that fit in one cell (Tier B)

`covering.py` already has `bbox_to_quadkeys`, an antimeridian-aware
*touch* test (which cells does this bbox intersect at zoom Z, handling
antimeridian crossings via the two-lobe `min_longitude > max_longitude`
case; see
[gotchas.md](gotchas.md#antimeridian-bboxes-are-two-lobes)).
Tier B additionally needs a *size* test that doesn't exist yet: at what's the
deepest zoom where this bbox still fits inside one cell. Proposed new
function in `covering.py`:

```python
def bbox_fits_in_one_cell(min_lon, min_lat, max_lon, max_lat, zoom):
    lon_span = (180 - min_lon) + (max_lon + 180) if min_lon > max_lon else max_lon - min_lon
    if lon_span > 360.0 / 2 ** zoom:
        return False
    y_top = _lat_to_yfrac(max_lat)
    y_bot = _lat_to_yfrac(min_lat)
    return (y_bot - y_top) <= 1.0 / 2 ** zoom
```

`_lat_to_yfrac` factors the continuous (pre-floor) half of the existing
`lonlat_to_tile`'s Web Mercator projection (`asinh(tan(lat))`, clamped to
`_MERC_LAT_MAX`) into its own function, reused by both the existing
`lonlat_to_tile` and this new size test — same math, not reimplemented.

Placement search per Tier B division: start at the reference zoom (4),
descend one zoom at a time while `bbox_fits_in_one_cell` still holds,
stop at a proposed depth cap of z17 (matches `qk17`'s existing role as
the finest granularity used elsewhere in the pipeline — proposed, not
Discovery-pinned). Call the existing `bbox_to_quadkeys` at the final
zoom to get the (at most four) cells to reference.

Tier assignment: `bbox_fits_in_one_cell(..., zoom=4)` true means Tier B
(search deeper); false means Tier A.

## The new tile-reference stage

Proposed function `stage_division_tile_references`, added alongside
(not replacing) `stage_tile_assignment` in `stages.py`:

1. Freshness gate (`artifact_fresh` pattern) keyed on `boundaries_db`
   and the covering's `_meta.json`.
2. Read per-division bbox extents from `bnd.places` — already-flattened
   columns, no geometry touched.
3. One Python pass over the division set (Discovery measured 617,734
   divisions) applying the Tier A/B size test and, for Tier B, the
   placement search above.
4. Load the Tier A id list and Tier B `(place_id, tile_qk)` rows into
   DuckDB temp tables (bulk registration — a naive per-row
   `executemany` is the wrong choice at this row count; the exact
   registration mechanism is an implementation-time detail).
5. `COPY` combining Tier A's covering-truncation query (restricted to
   Tier A ids) `UNION ALL` the Tier B rows, `ORDER BY tile_qk, place_id`
   — matching `stage_tile_assignment`'s existing sort convention.
6. The referencing-count guardrail (below), then the same atomic
   tmp-write-then-rename finalize pattern every other stage uses.

`run_pipeline`'s `stage_tile_assignment` call becomes a fork:

```python
if source == "overture_division":
    stage_division_tile_references(bnd_path, covering_dir, ta_parquet, ...)
else:
    stage_tile_assignment(places_parquet, ta_parquet, source,
                          max_per_tile=max_per_tile, ...)
```

`run_pipeline`'s division branch derives the covering directory path at
the `stage_covering` call and would otherwise re-derive the same path
again here; `covering_dir` should be assigned once, at the
`stage_covering` call, and reused at this fork point instead.
`max_per_tile` stops being passed to divisions at all — the splitter is
gone for that source.

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

## Guardrail: implausible referencing-cell counts

Open question in `performance-improvements.md`, not fully settled here.
Proposed shape: after writing the artifact, compute each division's
reference count, group by `level` (`levels.py`'s `LEVEL_VOCAB`), and log
(never fail) any division whose count is a large outlier relative to its
level's typical count — Antarctica (71 cells at z4) and one level-50
division (27 cells at z7 against a level-50 median of 1) are Discovery's
two concrete examples this should catch without flagging the mainstream
distribution. A hard fail isn't proposed: Antarctica is legitimate
geography, and a fail-based guardrail would need a growing exemption
list to avoid breaking real builds. Whatever specific threshold gets
implemented should be treated as provisional and recalibrated against
measured per-level distributions the first time this stage runs against
production data, not baked in as a permanent default.

## Open items for implementation

- The z17 depth cap for Tier B's placement search — proposed, not
  Discovery-pinned.
- Guardrail threshold values — proposed shape, needs real calibration
  on first run.
- Bulk-load mechanism for Tier B's rows into DuckDB — implementation
  detail, flagged so it isn't rediscovered as a performance surprise.
