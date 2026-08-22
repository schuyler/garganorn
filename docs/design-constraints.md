# Garganorn Architectural Invariants

Non-trivial implications of garganorn's design requirements and its
implementation choices taken together — consequences neither the
requirement nor the code states on its own. An entry earns its place only
if it can't be read off the file it binds; if it can, it belongs in a
comment there. Each entry gives the constraint, why it exists, and where it
applies.

Above this sits the normative layer: `tile-privacy-design.md` states the
principles that override implementation choices. Below it, `gotchas.md`
records behaviors of the third-party tools garganorn builds on.

---

## Pipeline Architecture Constraints

### All spatial indexing uses quadkeys

Quadkeys (Bing tile system) at z17 for places, z15 for density tiles.
S2 cell IDs are eliminated from the pipeline. The `qk17` macro
(`garganorn/sql/qk_env_macro.sql`) computes spatial keys from lon/lat
coordinates; it is the only place `ST_QuadKey()` appears.

**Applies to**: `garganorn/sql/qk_env_macro.sql` (macro definition); every
`*_import.sql` (qk17 column) and `density_extract.sql` call `qk17(...)`,
never `ST_QuadKey()` directly. Tile assignment builds its quadkey prefixes
inline in `stages.py:stage_tile_assignment`; `compute_tile_assignments.sql`
is the reference implementation the parity test compares it against, not
pipeline SQL.

### Importance scoring varies by entity type

- **Places (Overture, OSM)**: `60% density + 40% IDF`
  - Formula: `round(60 * least(density/density_norm, 1.0) + 40 * least(idf/idf_norm, 1.0))`
  - Defaults: `density_norm=10.0`, `idf_norm=18.0`
  - OSM's IDF term reads node, way, and relation parquet in both the
    numerator and the denominator (`osm_idf.sql`), so every OSM
    importance value shifts whenever the relation arm's counts change.
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

### OSM category branches are appended, never interleaved

`_osm_category_case.sql`'s `CASE` expression is ordered: each key's branch
must come after every key that was already in the whitelist when it was
added, never inserted earlier. This is what keeps every already-importable
record's `primary_category` unchanged as the whitelist grows — the CASE
still stops at the same branch for those records regardless of what's
appended below it. A branch's position therefore records when its key was
added, not how strong a signal it is. `building` is the weakest signal in
the expression and is also the most recent, so an element carrying
`building` and any other whitelisted key categorizes by the other key; a
later key weaker still than `building` would sit below it and break that
coincidence without breaking the invariant.

**Applies to**: `garganorn/sql/_osm_category_case.sql`

### OSM building import is way-and-relation, subtractive; the node arm has none

Named buildings are reached through `tags['building'] IS NOT NULL AND
tags['building'] NOT IN (...)` in the way and relation arms of
`osm_import.sql`'s whitelist — there is no `building` branch in the node
arm. Stage 0's building chain (`w/building` → `w/name` in
`extract-osm-parquet.sh`) selects ways; the relation selector list
includes `r/building` directly. `filtered.osm.pbf`'s building-derived
nodes are untagged way members plus, via `osmium getid --add-referenced`,
the member nodes and ways of every named qualifying relation.

**Applies to**: `garganorn/sql/osm_import.sql`, `scripts/extract-osm-parquet.sh`

### OSM relation geometry ignores nested relation members and mirrors way bbox conventions

Member resolution in `osm_import.sql`'s relation pipeline resolves only
node and way members; a relation-type member is never followed, so a
relation reachable only through nested relations imports nothing (measured
at 0.38% of qualifying relations, against the 2026-03-27 planet extract,
probed 2026-08-19). A relation's `bbox` uses the same
point±0.0001 convention as ways rather than its true member extent —
consistency with ways was chosen over precision; true-extent bbox would
apply equally to ways and is out of scope.

**Applies to**: `garganorn/sql/osm_import.sql`

### Stage 0 merges to one filtered.osm.pbf, relying on osmium's dedup

`extract-osm-parquet.sh` produces two further chains beyond the tag
filter — the buildings chain (`w/building` → `w/name`) and the relation
closure chain (`osmium getid --add-referenced` over the named-relation
subset) — whose outputs are merged back into the same `filtered.osm.pbf`
with `osmium merge`, which drops objects duplicated across its three
inputs by type/id/version. That dedup is what keeps the pipeline at one
parquet dataset with no SQL-side dedup: a way matched by both the tag
filter and the building filter (e.g. `building=yes` + `amenity=restaurant`)
would otherwise import twice and collide on rkey `w<id>`, a node present in
both would double-count in `way_centroids`, and an object pulled in by
both the buildings chain and the relation closure chain would double the
same way.

**Applies to**: `scripts/extract-osm-parquet.sh`

### The relation extraction chain is kept separate from the existing filter passes

`extract-osm-parquet.sh` resolves relation member closure (`osmium getid
--add-referenced`) as its own chain rather than folding `r/` selectors
into the main tag-filter pass. Folding it in would resolve closure for
every key-matched relation (6.1M), not just the ~1.4M named ones, carrying
millions of unnamed water bodies and building multipolygons into every
downstream scan for no time saving.

**Applies to**: `scripts/extract-osm-parquet.sh`

### Stage 0's cache key includes the selector list, not just the PBF's mtime

Editing `extract-osm-parquet.sh`'s selector list doesn't touch the planet
PBF's mtime, so a plain freshness check would silently reuse a
`filtered.osm.pbf` built under the old selectors. Stage 0 writes the
concatenation of its node/way and relation selector arrays to
`filter-selectors.txt` on success, and re-derives every intermediate —
`filtered-tags.osm.pbf`, `filtered-buildings.osm.pbf`, the relation
closure intermediates, and `filtered.osm.pbf` — when that file is missing
or its content differs from the current list.

**Applies to**: `scripts/extract-osm-parquet.sh`

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
the tile (see `gotchas.md`, "A join is sized off the source relation, not
the filtered one"). Clipping each boundary to a tile's own envelope
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

The capacity rule alone would leaf a typical inland division (p50 = 123
vertices) at z4: the arm's z4 join keys on `left(p.qk17, 4) = c.tile_qk`,
so every candidate point in that cell pairs with every division leafed
there — 130,269 of them in cell `1202`, 21% of all divisions — a cross
product no predicate filters. `COVER_MIN_LEAF_ZOOM` is what stops that.

The value 12 is a known-working default rather than a measured one: it
holds edge-join fan-out at a level the covering has already run at
safely. Moving it either way trades that fan-out against stored fragment
count, and wants a real run's `per_level` stats and measured artifact
size to justify.

### Division containment comes from Overture hierarchies, not geometry

`stage_division_containment` derives a division's `relations.within` from
Overture's `hierarchies` column (`division_containment.sql`), not from
`compute_containment`'s geometric covering join. The geometric route was
considered and rejected: a representative point is the wrong containment
test for divisions — a region's point lands in exactly one county without
the region being inside it, and every division's own point is inside
itself. The covering machinery is untouched and still required for places
and OSM, whose records carry no division references of their own.

An ancestor is kept only if it survived the import's own filters (`is_land`,
a bbox-scoped build) — an emitted rkey must resolve via `getRecord`, so an
ancestor dropped from the imported set is dropped from `relations.within`
too. Containment fans out over `tile_assignments_combined.parquet`, the same
union of tile references and summary tile references that `stage_export`
reads, so a division's summary-band copy and its regular-grid copies carry
identical `relations`.

**Applies to**: `garganorn/sql/division_containment.sql`, `stages.py`
(`stage_division_containment`), `quadtree.py` (`run_pipeline`)

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
record. The summary band (below) unions a second assignment row into the
same artifact for every top-N place, so a place can carry two rows even
in sources where the regular band assigns it once; all three
`*_export_tiles.sql` files join containment on `(place_id, tile_qk)` for
this reason.

**Applies to**: `stages.py` (`stage_division_tile_references`,
`compute_containment`, `stage_division_containment`),
`overture_division_export_tiles.sql`, `overture_place_export_tiles.sql`,
`osm_export_tiles.sql`

### The summary band is a coarse tile tier for region-less resolution

Each source's tileset carries a z1-z5 band alongside the regular z6-z17 grid,
holding only the top-N=10,000 records by `importance` DESC (ties by
`<pk>` ASC), so a client with no region can resolve a name without
prefetching the whole tileset. Divisions additionally include `subtype IN
('country', 'region', 'dependency')` unconditionally, additive on top of
the top-N cut and deduplicated against it — a pure importance cut crowds
every non-locality subtype out below N≈50,000.

z0 is excluded because its quadkey is the empty string, which degenerates
the `qk[:6]` partition and URL slice; z6 is excluded because it would
collide with the regular band's filenames. Quadkey length under six
characters is what marks a tile summary-band, both on disk and in
`TileManifest.get_tiles_for_bbox`. z5 is a hard floor — tiling cannot
split a fine cell further — so a z5 tile may hold more than
`max_per_tile` records; this is expected, not an error.

`get_tiles_for_bbox` answers from the regular band first; when that
answer would exceed `max_tiles`, it returns the intersecting summary-band
tiles instead of raising `BboxTooLarge`. The summary answer is uncapped —
the band is bounded by construction, a few dozen tiles per source at
N=10,000 — and is deliberately incomplete rather than an error.

**Applies to**: `stages.py` (`stage_summary_tile_assignment`,
`stage_summary_division_tile_references`), `quadtree.py`
(`TileManifest.get_tiles_for_bbox`)

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

### The bbox precision check is a backstop, not the mechanism

Clients must snap to the 0.01° grid before sending; `_check_bbox_precision`
refuses anything finer anyway, so a conforming client would never trip the
check — which is not the same as redundant. Two things follow for anyone
editing it. Don't replace the refusal with server-side snapping: that
silences the only signal that a client is sending raw GPS. Don't fold it
into `max_tiles`, which bounds a request's cost rather than its precision
(see "The summary band is a coarse tile tier for region-less resolution");
the two answer different threats and neither subsumes the other.
`tile-privacy-design.md` holds the reasoning.

**Applies to**: `garganorn/server.py` (`_check_bbox_precision`),
`garganorn/lexicon/getCoverage.json`

### Optional-but-not-nullable lexicon fields must be omitted, not null

A DuckDB struct literal always carries every declared key, so building
JSON for a lexicon field that's optional (not `required`) but not marked
`nullable` — e.g. `variant.language` — from a possibly-NULL source column
emits `"language": null` instead of omitting the key, which `lexrpc`'s
validator rejects. This only matters where a struct gets serialized to
JSON text (export SQL); a typed struct column at rest is fine, since the
distinction doesn't exist until serialization. Wrap the `to_json(...)`
call in `strip_json_nulls` (`garganorn/sql/json_macros.sql`) for any new
optional-but-not-nullable field built this way — per element via
`list_transform` for a list field, directly for a single struct.

**Applies to**: `garganorn/sql/json_macros.sql`,
`garganorn/sql/overture_place_export_tiles.sql`,
`garganorn/sql/overture_division_export_tiles.sql`,
`garganorn/sql/osm_export_tiles.sql`

### `variant.language` invalid values are nulled at import, not export

`atgeo_valid_language` — identical `CREATE OR REPLACE MACRO` in each of
the three import scripts — nulls any `language` value that fails
lexrpc's `format: language` check before it reaches the `variants`
struct. Each import script runs as an isolated DuckDB process with no
shared preamble, so duplicating the two-line macro three times is
cheaper than inventing one. It's a correctness backstop, not the primary
mechanism: OSM's `osm_variant_suffix` and Overture's
`names.rules`/`names.common` derivations classify what they can into a
real BCP-47 code first, so the backstop only catches whatever shape
nobody has classified yet. Because it runs at import, the NULL it
produces still passes through the neighboring
optional-but-not-nullable-lexicon-fields entry above at export —
`strip_json_nulls` is what turns that NULL into an omitted key on the
wire, not this macro.

**Applies to**: `garganorn/sql/osm_import.sql` (`atgeo_valid_language`,
`osm_variant_suffix`), `garganorn/sql/overture_division_import.sql`,
`garganorn/sql/overture_place_import.sql`

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
  plus a z1-z5 summary band
- Coordinate precision in export: `DECIMAL(10,6)` → 6 decimal places (~0.1m)
- Bbox privacy grid: 0.01° (~1km) enforced by `_check_bbox_precision()`
- Latitude for a quadkey is clamped to ±85.05101030905541, the centre of the
  outermost z17 row — not the Mercator limit, which wraps (see the
  `ST_QuadKey` entry in `gotchas.md`). Safe at any zoom ≤ 17: a point inside
  the outermost z17 row is inside the outermost row at every coarser zoom
- Outermost tile rows reach ±90, so a bbox inside a polar cap matches a tile
