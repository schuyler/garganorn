# Garganorn Architectural Invariants

Living reference of pipeline invariants and architectural rules garganorn's
own code must satisfy. Each entry includes the constraint, why it exists,
and where it applies. For behaviors of the third-party tools garganorn
builds on, see `gotchas.md`.

---

## Pipeline Architecture Constraints

### All spatial indexing uses quadkeys

Quadkeys (Bing tile system) at z17 for places, z15 for density tiles.
S2 cell IDs are eliminated from the pipeline. ST_QuadKey() computes
spatial keys from lon/lat coordinates.

**Applies to**: All `*_import.sql` (qk17 column), `compute_tile_assignments.sql`,
`density_extract.sql`

### Importance scoring varies by entity type

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

### Density tiles use bbox-overlap join

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

### Five independent, freshness-gated CLI subcommands

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

### IDF is computed from source parquet, not imported places table

IDF computation reads raw parquet directly (ephemeral DuckDB connection),
not the imported `places` table. This avoids an ephemeral import step and
makes IDF cacheable per-dataset.

**Applies to**: `stages.py` (`stage_idf`), `*_idf.sql`

### Containment is a precomputed covering joined by quadkey prefix

The reason a plain R-tree prefilter isn't enough on its own:
`ST_Contains`-style cost scales with the boundary polygon's vertex count,
not the candidate count, so a 900K-vertex boundary costs the same per
test whether the candidate point is a sliver away or on the other side of
the tile (see `gotchas.md`, "Join memory is sized off the source relation,
not the filtered one"). Clipping each boundary to a tile's own envelope
during descent trims that cost without changing the answer, since every
candidate in that tile is inside it: `ST_Contains(clipped, point) ⟺
ST_Contains(full, point)`. Containment is tested with `ST_Covers` rather
than `ST_Contains`, because splitting introduces seams that were never
the boundary's own edge, and a point exactly on one is inside the
boundary but on the fragment's border.

**Applies to**: `covering.py` (`stage_covering`), `stages.py`
(`compute_containment`)

**Correctness invariant**: a boundary's emitted leaves form an antichain —
none is a quadkey-prefix descendant of another — and a place's qk17 has
exactly one ancestor per zoom, so a place matches each boundary at most
once (no `DISTINCT` needed). Verified against an in-suite brute-force `ST_Contains` oracle, not
a captured baseline (the old per-tile code never worked correctly in
production, so no valid baseline existed to compare against).

### A record may be referenced by more than one tile

Divisions are polygons: `stage_division_tile_references` references a
division from every grid tile its geometry overlaps, not just the one
tile holding its interior point. Places are points and keep the single
tile `stage_tile_assignment` assigns them.

Every copy of a multi-tile record must be byte-identical across the
tiles carrying it. The spec's dedup-by-rkey rule keeps one arbitrary
copy and drops the rest, so a per-tile divergence is data loss with no
error to surface it.

Any join of the tile-assignment artifact (`tile_assignments.parquet` or
`tile_references.parquet`) against another per-place artifact that can
itself carry tile-scoped rows must key on `(place_id, tile_qk)` together,
never `place_id` alone — `compute_containment` groups by `(tile_qk,
place_id)`, so a `place_id`-only join fans out to N² rows for an N-tile
record. `overture_place_export_tiles.sql` and `osm_export_tiles.sql` keep
the `place_id`-only join deliberately: both sides stay one row per place
for those sources.

**Applies to**: `stages.py` (`stage_division_tile_references`,
`compute_containment`), `overture_division_export_tiles.sql`

### Tile serving uses three distinct, deliberately separate namespaces

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
| `max_per_tile` | 1000 | Maximum records per tile in the assignment grid, every source; does not bound a division tile's exported count |
| `max_temp_directory_size` | 250GB | Ceiling on DuckDB spill, applied independently of `temp_directory` |

## Coordinate System

- All coordinates are WGS84 (EPSG:4326), longitude/latitude order
- Quadkey zoom levels: z17 for places, z15 for density, z6-z17 for tiles
- Coordinate precision in export: `DECIMAL(10,6)` → 6 decimal places (~0.1m)
- Bbox privacy grid: 0.01° (~1km) enforced by `_check_bbox_precision()`
