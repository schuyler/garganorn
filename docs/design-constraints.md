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

**Applies to**: `sql/covering_seed.sql` (containment pre-filter)

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

**Applies to**: `overture_place_import.sql` (name variant unnesting)

**Why it matters**: Places with NULL category arrays get no IDF score
(defaults to 0 via coalesce). This is intentional but should be documented.

### D6: No unbounded complex-state aggregation

DuckDB cannot spill intermediate state for `list()`, `string_agg()`, and other
holistic aggregates. Any query using them must run over a bounded partition —
per-tile, or per qk4 prefix.

**Applies to**: every SQL file; `compute_containment` partitions at z6 for
exactly this reason

**Why it matters**: it is the most reliable way to exhaust memory on the 72 GB
target, and it has done so. Treat it as a review criterion for new SQL, not a
thing to discover at scale.

### D7: Antimeridian bboxes are two lobes

Where a bbox filter is derived from a geometry's min/max longitude,
`min_longitude > max_longitude` means the feature crosses ±180°, and a naive
`BETWEEN` drops it. The correct filter is `(lon >= min OR lon <= max)`, or a
tile-seed set built as the union of the two lobes.

**Applies to**: `covering.py` (two-lobe split) and `stages.py`
(`bboxes_intersect` wrap branches) implement this but have no test coverage.
The import-side bbox filter does not, so features crossing ±180° are dropped
there.

**Why it matters**: it affects Pacific data — eastern Russia, Fiji,
Antarctica. A global build makes it reachable where a CONUS-bounded one did
not. Deliberately deferred, and recorded here so the rule is known when the
code is next touched.

### D8: `strip_json_nulls()` is vulnerable to special key characters

The custom `strip_json_nulls()` helper may fail on JSON keys containing `{`,
`}`, `"`, or `,`. No failures observed in practice.

**Applies to**: `sql/overture_place_export_tiles.sql`,
`sql/overture_division_export_tiles.sql`

**Why it matters**: it exists only because DuckDB has no native
`json_strip_nulls()` (PR #21748). Replace it with the native function when
that lands rather than hardening the workaround.

### D9: `ST_Union_Agg` is a memory-pressure point

`ST_Union_Agg` merges a division's multiple geometry rows into one geometry
and can consume substantial memory on large multi-part divisions.

**Applies to**: `sql/overture_division_import.sql` (`ST_Union_Agg` in the
`merged_areas` CTE)

**Why it matters**: it is a place to look first when the division import
runs out of memory, alongside the CTAS sort in D4.

### D10: DuckDB sizes join memory off the source relation, not the filtered one

A query that filters a large TEMP TABLE inline (`WHERE ... = '${prefix}'`)
and then joins the result against a relation with expensive per-row cost
(e.g. a GEOS-backed spatial predicate against a complex multi-vertex
geometry) can blow up memory 100x+ even when the filtered row count is tiny.
DuckDB's join planner appears to size against the source relation's full
cardinality, not the true post-filter selectivity.

**Applies to**: `garganorn/stages.py:compute_containment()`, where `p` is
materialized as its own `CREATE TEMP TABLE` (not left as a CTE over
`places_slim`) for exactly this reason before joining to boundary geometries
in the `edge` arm; hit by Canada/Nunavut's 200k-vertex Arctic Archipelago
coastlines at global scale, not by any data defect

**Why it matters**: rewriting the filter for zone-map pruning or reducing
thread count does not help — only materializing the filtered relation into
its own `CREATE TEMP TABLE` before the join does, same pattern as the `cov`
CTE materialization this codebase already uses elsewhere. Any new query that
filters a large relation and then joins it against expensive-per-row
predicates needs the same treatment. Two open upstream
DuckDB issues make this plausible as an engine limitation rather than a bug
here: [duckdb/duckdb#14087](https://github.com/duckdb/duckdb/issues/14087),
[duckdb/duckdb#18330](https://github.com/duckdb/duckdb/issues/18330).

### D11: DuckDB version spread — dev pins 1.5.1, production runs 1.4.4

All SQL must run unchanged on both. Two syntax consequences: `ATTACH 'p'
AS x (READ_ONLY)` is the only attach form that parses on both versions
(`... READ_ONLY AS` parses on neither); `COPY`'s `KV_METADATA` option is
1.4+ only, which is why parquet/DB metadata lives in JSON sidecars
(`.meta.json`, `_meta.json`, `manifest.json`) rather than embedded
key-value metadata.

**Applies to**: all SQL files; run `.venv/bin/pytest` to test against the
dev pin.

---

## Pipeline Architecture Constraints

### P1: All spatial indexing uses quadkeys

Quadkeys (Bing tile system) at z17 for places, z15 for density tiles.
S2 cell IDs are eliminated from the pipeline. ST_QuadKey() computes
spatial keys from lon/lat coordinates.

**Applies to**: All `*_import.sql` (qk17 column), `compute_tile_assignments.sql`,
`density_extract.sql`

### P2: Importance scoring varies by entity type

- **Places (Overture, OSM)**: `60% density + 40% IDF`
  - Formula: `round(60 * least(density/density_norm, 1.0) + 40 * least(idf/idf_norm, 1.0))`
  - Defaults: `density_norm=10.0`, `idf_norm=18.0`
- **Localities (Overture divisions, subtype=locality)**: `60% density + 40% population`
  - Formula: `round(60 * least(avg_density/density_norm, 1.0) + 40 * least(ln(1+population)/pop_norm, 1.0))`
  - Defaults: `density_norm=10.0`, `pop_norm=20.0`
- **Non-locality divisions**: `40% population` only
  - Formula: `round(40 * least(ln(1+population)/pop_norm, 1.0))`

The norm divisors (`density_norm=10.0`, `idf_norm=18.0`, `pop_norm=20.0`)
are not arbitrary: each comes from a statistical analysis of that
variable's distribution, chosen so the term scales to roughly `[0, 1)`
before weighting. The 60/40 split rests on no such analysis — it was
chosen by eyeballing search results across categories and picking
coefficients that made the ranking look subjectively reasonable. Don't
conflate the two when tuning or explaining this formula.

**Applies to**: `overture_place_import.sql`,
`osm_import.sql`, `overture_division_import.sql`

### P3: Density tiles use bbox-overlap join

The density spatial join for division localities uses bbox-overlap-bbox
rather than centroid-in-bbox. A density tile contributes to a locality's
score if the tile bbox intersects the locality bbox.

**Applies to**: `overture_division_import.sql` (`division_density` CTE)

**Why it matters**: Centroid-based joins systematically under-scored small
localities, which may contain no tile centroid at all and so scored zero.

The tradeoff is deliberate and unfixed: a tile that barely touches a
locality's bbox still contributes to its density average, so density is
over-estimated near division edges. Weighted intersection area would be more
accurate but needs an `ST_Intersection` per tile per locality. The impact is
negligible — density is one component of a composite importance score, so a
few percent of noise moves rankings very little.

### P4: Five independent, freshness-gated CLI subcommands

There is no single `STAGE_ORDER` constant. `garganorn.quadtree` exposes five
subcommands (`quadtree.py`): `density`, `idf`, `covering`, `run`
(one source), `all` (every source). Each stage is independently gated by
artifact freshness, so there's no shared ordered pipeline object — `all`
sequences density → idf per source → division → remaining sources, while
`density`, `idf` and `covering` can be invoked in any order and simply no-op
if their inputs aren't ready. `run` carries the one ordering constraint: with
`--boundaries` it requires an already-fresh sibling `covering/` and raises
`RuntimeError` otherwise, since containment computed against a missing or
stale covering would silently ship tiles with no `relations.within`. Freshness itself is one mechanism
everywhere: an artifact is fresh iff it exists, its meta sidecar exists
and parses, the sidecar's recorded `params` matches the caller's exactly,
the sidecar is strictly newer than every resolved input, and
`mtime(artifact) <= mtime(meta)` (so a crash between artifact rename and
meta write reads as stale). Editing a stage's SQL text does not itself
invalidate its artifact — SQL is neither an input nor hashed — so
`--force` (CLI) / `force=True` (tests) is the remedy after a SQL edit; it
invalidates by deleting outputs, nothing else. See pipeline-artifacts.md's
"Shared machinery" for the freshness/atomicity implementation this
describes.

**Applies to**: `quadtree.py`

### P5: IDF is computed from source parquet, not imported places table

IDF computation reads raw parquet directly (ephemeral DuckDB connection),
not the imported `places` table. This avoids an ephemeral import step and
makes IDF cacheable per-dataset.

**Applies to**: `stages.py` (`stage_idf`), `*_idf.sql`

### P6: Containment is a precomputed covering joined by quadkey prefix

Replaced the old per-tile recursive containment (adaptive quadtree,
CROSS JOIN per contained tile, z6 seeds subdividing past a 200-boundary
threshold to z14) with a two-artifact approach. The reason a plain R-tree
prefilter isn't enough on its own: `ST_Contains`-style cost scales with
the boundary polygon's vertex count, not the candidate count, so a
900K-vertex boundary costs the same per test whether the candidate point
is a sliver away or on the other side of the tile (see also D10). Clipping
each boundary to a tile's own envelope during descent trims that cost
without changing the answer, since every candidate in that tile is inside
it: `ST_Contains(clipped, point) ⟺ ST_Contains(full, point)`.

1. **Covering** (`covering.py` `stage_covering`, `COVER_MIN_ZOOM=4`,
   `COVER_MAX_ZOOM=16`): level-by-level descent z4→z16 precomputes which
   tiles each boundary fully contains (`interior`, emitted at every level)
   vs. merely overlaps (`edge`, emitted between `COVER_MIN_LEAF_ZOOM` and
   `COVER_MAX_ZOOM` as fragment vertex count falls to
   `COVER_VERTEX_CAPACITY`), clipping geometry to each tile's own envelope
   as it descends and storing each edge leaf's clipped fragment.
2. **Containment join** (`stages.py` `compute_containment`): two arms,
   `UNION ALL`. *Interior arm* — an equi-join of a place's qk17 prefix
   against interior covering tiles, no geometry test. *Edge arm* — one
   per-zoom leg over `COVER_MIN_LEAF_ZOOM`..`COVER_MAX_ZOOM`, testing
   `ST_Covers` against the fragment the covering stored for that leaf, so
   the test costs the fragment's vertex count rather than the whole
   boundary's. `ST_Covers` rather than `ST_Contains` because splitting
   introduces seams that were never the boundary's own edge, and a point
   exactly on one is inside the boundary but on the fragment's border.

**Applies to**: `covering.py` (`stage_covering`), `stages.py`
(`compute_containment`)

**Correctness invariant**: a boundary's emitted leaves form an antichain —
none is a quadkey-prefix descendant of another — and a place's qk17 has
exactly one ancestor per zoom, so a place matches each boundary at most
once (no `DISTINCT` needed). Verified against an in-suite brute-force `ST_Contains` oracle, not
a captured baseline (the old per-tile code never worked correctly in
production, so no valid baseline existed to compare against).

### P7: Tile serving uses three distinct, deliberately separate namespaces

NSID dotted (`org.atgeo.places.overture.place`, a config key) → disk
`source_key` snake_case (`overture_place`, private, never appears in a
URL) → public `slug` kebab-case (`overture-place`, in the tile URL path).
The serving route is `/tiles/<slug>/<path:tile_path>`, resolving a public
slug to that collection's own `tiles_dir` — not a route rooted at a
source directory, which would let `GET .../places.parquet` or
`containment/` resolve on disk (`safe_join` blocks `..`, not sibling
paths). This also means public URLs never mirror the on-disk snake_case
layout. `base_url` must end with `/<slug>` or `getCoverage` emits URLs no
route can serve — checked at startup, not discovered at request time.

**Applies to**: `garganorn/__main__.py` (`serve_tile` route, `base_url`
check)

---

## Compatibility Policy

Backwards compatibility with prior tile/record formats is a non-goal —
the atgeo lexicons have never been in production use and the API has
only ever been published as beta. This is what unblocks format changes
(envelope shape, level vocabulary, tile layout) without a consumer
migration path. The one compatibility that does matter is internal: the
server's own coupling to the tiles it reads, which is why a deploy ships
server-then-re-export in that order, not the other way around.

---

## Licensing Posture

The OSM tileset is an ODbL Derivative Database: it is served under ODbL with
attribution, which the tile envelope's `source` and `license` links provide.
The density artifact is a Produced Work, not a database extraction, so
blending Overture-derived density scores into OSM importance does not make
the OSM tileset a derivative of Overture data. This is why per-source score
derivation is unnecessary.

## Normalization Constants

| Constant | Default | Used by |
|----------|---------|---------|
| `density_norm` | 10.0 | Importance density component (all place sources) |
| `idf_norm` | 18.0 | Importance IDF component (Overture, OSM) |
| `pop_norm` | 20.0 | Importance population component (Overture divisions) |
| `COVER_MIN_ZOOM` | 4 | Covering descent start level |
| `COVER_MIN_LEAF_ZOOM` | 12 | Shallowest level an edge leaf may be emitted at; bounds edge-join fan-out |
| `COVER_MAX_ZOOM` | 16 | Covering descent end level; a fragment still over capacity here is emitted anyway |
| `COVER_VERTEX_CAPACITY` | 5000 | Vertex count at or below which an edge fragment stops recursing |
| `max_per_tile` | 1000 | Maximum records per export tile |
| `max_temp_directory_size` | 250GB | Ceiling on DuckDB spill, applied independently of `temp_directory` |

## Coordinate System

- All coordinates are WGS84 (EPSG:4326), longitude/latitude order
- Quadkey zoom levels: z17 for places, z15 for density, z6-z17 for tiles
- Coordinate precision in export: `DECIMAL(10,6)` → 6 decimal places (~0.1m)
- Bbox privacy grid: 0.01° (~1km) enforced by `_check_bbox_precision()`
