# Pipeline Artifacts

What each tile-build stage writes, in what shape, and why. Source of truth
is the code (`garganorn/stages.py`, `garganorn/quadtree.py`,
`garganorn/covering.py`, `garganorn/sql/*.sql`); this document describes
current behavior only.

## Shared machinery

Every stage below (except `stage_export`, which uses its own symlink+keep-2
scheme) uses the same freshness/atomicity pattern:

- **Freshness**: `artifact_fresh()` / `_is_output_fresh()` skip a rebuild
  when the output exists, is strictly newer than every input, and (for
  `artifact_fresh`) a `.meta.json` sidecar's recorded `params`/`inputs`
  match the caller's. Equal mtimes count as stale.
- **Atomicity**: each stage writes to a `.tmp` path/dir, then
  `finalize_artifact()` (single files) or a directory rename swap
  (`covering/`, `containment/`) promotes it. The completion marker
  (`.meta.json`, `_meta.json`, or `manifest.json`) is always written last,
  so a crash mid-stage never leaves a fresh-looking but partial artifact.

## 1. `stage_density_extract` (`garganorn/stages.py`)

**Writes**: `density_tiles.parquet` (Parquet), typically at
`shared/density_tiles.parquet`.

**Schema**: `tile_qk15 VARCHAR, density_score DOUBLE, tile_xmin DOUBLE,
tile_ymin DOUBLE, tile_xmax DOUBLE, tile_ymax DOUBLE`.

**Sort**: `ORDER BY tile_qk15`. Every import stage loads this file
wholesale (`CREATE TEMP TABLE density_tiles AS SELECT * FROM
read_parquet(...)`) with no predicate, so nothing is pruned; the
downstream join — a `left(qk17, 15)` prefix equality
(`overture_place_import.sql`, `osm_import.sql`) or a bbox-overlap join on
`tile_xmin`/`tile_xmax`/`tile_ymin`/`tile_ymax`
(`overture_division_import.sql`) — runs against an already-resident temp
table. Sorting here is for deterministic output, not zone-map pruning,
same as `stage_idf`'s entry.

**Shape**: Runs globally over the raw Overture place parquet (no bbox
filter) on an ephemeral in-memory connection — density feeds importance
scoring for every source and every bbox subset, so it is computed once and
shared rather than per-source or per-bbox. `density_score = ln(1 + count)`
per z15 tile. Tile bounds are precomputed here via the `qk_env()` SQL
macro (`garganorn/sql/qk_env_macro.sql`) and stored as flat
`tile_xmin`/`tile_ymin`/`tile_xmax`/`tile_ymax` columns, so the
bbox-overlap join against localities in `overture_division_import.sql`
is a plain numeric comparison rather than a per-row macro or geometry
call. `qk_env()` is verified to agree with the Python `quadkey_to_bbox()`
to within 1e-9 over 10,000 random quadkeys.

## 2. `stage_idf` (`garganorn/stages.py`)

**Writes**: `idf.parquet` per place source — `overture_place/idf.parquet`,
`osm/idf.parquet`. Not run for `overture_division` (divisions use a
population-based formula, no IDF term).

**Schema**: `category VARCHAR, n_places BIGINT, idf_score DOUBLE` where
`idf_score = ln(N_total / n_places)`.

**Sort**: `ORDER BY category`. One row per distinct category value in the
source — sorting here is for deterministic output, not zone-map pruning at
this size.

**Shape**: Reads the raw source parquet directly rather than the imported
`places` table, on its own ephemeral connection — this avoids needing an
import pass just to compute IDF, and makes the artifact cacheable
independently of any particular bbox or import run. OSM's category
expression is substituted from the same `_osm_category_case.sql` snippet
used by `osm_import.sql`, so IDF categories and import categories can't
drift apart. OSM's IDF reads node, way, and relation parquet — three
sources feeding both the numerator and the denominator. The relation
denominator arm counts `building` the same way the way arm does; the
import's node arm has no `building` branch at all, which is an
import-whitelist rule (`design-constraints.md`) rather than an IDF one.

## 3. `stage_division_import` (`garganorn/stages.py`)

**Writes**: two artifacts under `overture_division/` — `places.parquet`
and `boundaries.duckdb`.

**Schema** (`places.parquet`, geometry excluded): `id, names, subtype,
country, region, level INTEGER, wikidata, population, parent_division_id,
bbox STRUCT(xmin, ymin, xmax, ymax), qk17 VARCHAR, min_latitude,
max_latitude, min_longitude, max_longitude, interior_lon, interior_lat,
importance INTEGER, variants STRUCT(name, type, language)[]`. `level`
comes from `garganorn.levels.LEVEL_VOCAB` via a generated `CASE` with no `ELSE`
branch — an unmapped `subtype` must surface as NULL, not a silent default.

**Schema** (`boundaries.duckdb`, table `places`): `id, geometry GEOMETRY,
level, names, subtype, country, region, wikidata, population,
min_latitude, max_latitude, min_longitude, max_longitude, importance,
variants`, unfiltered — every level in `LEVEL_VOCAB` (country through
microhood) is present — plus an `RTREE` index on `geometry`.

**Sort**: `places.parquet` is `ORDER BY qk17 NULLS LAST` — same rationale
as `stage_import`'s equivalent COPY, below. `boundaries.duckdb`
is `ORDER BY ST_Hilbert(geometry, <world bbox>)` — a Hilbert-curve sort
keeps geometrically nearby boundaries physically close on disk, which
matters for the R-tree index build and for containment queries that scan
many boundaries per z4/z6 batch.

**Shape**: Two artifacts because the two downstream consumers need
different things — `stage_covering`/`compute_containment` need the full
polygon geometry (kept in `boundaries.duckdb`, ATTACHed read-only), while
tile assignment and export need only the flat, geometry-free columns
every source's `places.parquet` shares. Fail-loud subtype validation runs
after the import CTAS but before any artifact write (before the `COPY`
and before the `boundaries.duckdb` `ATTACH`), so a release with an
unmapped subtype never produces a partial artifact. `boundaries.duckdb`
is promoted (fsync + `os.replace`) before `places.parquet`'s
`finalize_artifact()` call, so a crash between the two leaves a fresh
`boundaries.duckdb` but a stale `places.parquet` meta — the next run
rebuilds both, which is safe because the whole stage is idempotent.

## 4. `stage_covering` (`garganorn/covering.py`)

**Writes**: `<division>/covering/<qk4-prefix>.parquet` (one file per
distinct z4 quadkey prefix present in the output) plus `_meta.json`.

**Schema**: `tile_qk VARCHAR, boundary_id VARCHAR, level INTEGER, geom
GEOMETRY`. `geom` is never NULL: an interior tile's `geom` is its own
tile envelope (`qk_env(tile_qk)`) — the whole tile is already known to be
inside the boundary, so this is the trivial geometry to test it against —
and a non-interior tile's `geom` is the boundary's geometry clipped to
that tile.

**Sort**: each per-prefix file is `ORDER BY tile_qk, boundary_id`, for
deterministic per-run output. `compute_containment` loads each file
wholesale into a temp table, so its equi-join does not depend on the
order.

**Shape**: Descends z4 → z16 (`COVER_MIN_ZOOM`..`COVER_MAX_ZOOM`),
clipping each boundary's geometry to each tile's own envelope as it goes.
At every level, tiles the geometry fully contains are flagged interior
and emitted (removed from further descent); the rest are split into four
children and re-clipped.

A non-interior tile is emitted as a leaf when it reaches
`COVER_MAX_ZOOM`, or when it is at least `COVER_MIN_LEAF_ZOOM`(12) deep
and its clipped fragment has no more than `COVER_VERTEX_CAPACITY`(5000)
vertices. So edge leaves sit at varying depths: complex coastlines
recurse past z12 until their fragments are small enough, while a simple
boundary stops at the floor. The floor exists to bound the edge join's
fan-out — without it a simple division would leaf at z4, and every
candidate point in that z4 cell would join every division leafed there.
A fragment still over capacity at `COVER_MAX_ZOOM` is emitted anyway;
`stats["over_capacity_leaves"]` counts those so the pair can be
recalibrated from a real run.

Emitted leaves form an antichain — no leaf is a quadkey-prefix
descendant of another leaf of the same boundary — so a place matches each
boundary at most once downstream and no `DISTINCT` is needed. Output is
split per z4 prefix so `compute_containment` can load only the covering
data relevant to the batch of places it is currently processing, bounding
memory. The stage never `rmtree`s a caller-supplied `temp_directory`
wholesale — it only creates and destroys its own subdirectory under it —
so a shared spill directory can't be destroyed out from under a sibling
stage.

## 5. `stage_import` (`garganorn/stages.py`)

Handles `overture_place` and `osm` directly; dispatches to
`stage_division_import` for `overture_division`.

**Writes**: `<source>/places.parquet`.

**Schema** (`overture_place`): the Overture Places parquet schema minus
`geometry`, plus computed columns `qk17 VARCHAR`, `importance INTEGER`,
`variants STRUCT(name, type, language)[]`. Overture's own columns pass
through unchanged (`SELECT * EXCLUDE (geometry)`).

**Schema** (`osm`): explicit — `osm_type VARCHAR, osm_id BIGINT, rkey
VARCHAR, name VARCHAR, latitude DOUBLE, longitude DOUBLE, primary_category
VARCHAR, tags MAP(VARCHAR, VARCHAR), bbox STRUCT(xmin, ymin, xmax, ymax),
importance INTEGER, qk17 VARCHAR, variants STRUCT(name, type,
language)[]`. `osm_type` takes `'n'`, `'w'`, or `'r'`; `rkey` correspondingly
takes an `n`-, `w`-, or `r`-prefixed form (`osm_type || osm_id`), expanded to
`node:`/`way:`/`relation:` at export (`osm_export_tiles.sql`). `geom
GEOMETRY` is computed during import but excluded from the Parquet output
(`_GEOM_COL["osm"] = "geom"`). `osm_import.sql` also builds an `idx_rkey`
index on the ephemeral in-memory `places` table; grepping the codebase
shows no query anywhere ever looks up `places` by `rkey` — the relation
pipeline's two duplicate-suppression anti-joins do read `places` before
the connection closes, but key on `(osm_type, osm_id)` and on `(name,
longitude, latitude)`, never `rkey` — so the index itself is apparently
dead weight, not confirmed in scope here.

**Sort**: both `ORDER BY qk17 NULLS LAST`, kept though nothing prunes this
parquet by a qk17 range — dropping it makes `places_slim`'s re-sort real,
costing ~160s of a 9,659.8s build (measured 2026-08-13), and plausibly
costs compression here too, though that half is unmeasured.

**Shape**: `importance = round(60 * least(density/density_norm, 1.0) + 40
* least(idf/idf_norm, 1.0))`, computed inline in the same `CREATE TABLE
... AS` that imports the source data — one pass, not an import followed
by a separate scoring pass. `density_tiles`/`idf_scores` are loaded as
standalone temp tables *before* the source SQL runs. `idf_scores`' join-key
uniqueness is checked here (`_assert_unique_key`); `density_tiles`' was
already checked by `_assert_density_parquet_unique`, ahead of the
`overture_division` dispatch — a duplicate key would otherwise silently
fan out place rows through the `LEFT JOIN`. Both lookup tables are also
pre-deduplicated (`GROUP BY ... any_value(...)`) at the join site as a second line of
defense. `overture_place`'s `ov_base` CTE and OSM's `filtered`/`way_base`/`rel_base`
CTEs are each scanned exactly once beyond their own definition — geometry
is dropped as early as possible (before any join) since `qk17` comes from
`bbox`, not `geometry`, avoiding a full-width shuffle of the widest
columns at global-Overture scale. `places` is additionally scanned twice
more during the relation pipeline's duplicate suppression, once per
anti-join. OSM's way and relation pipelines have known debt here:
`qualifying_ways` and `way_node_refs` are each scanned twice beyond their
own definition (by `way_node_refs`/`way_base` and by
`needed_node_ids`/`way_centroids`); `qualifying_relations`,
`rel_way_member_node_refs`, and `rel_node_member_refs` are each scanned
twice beyond their own definition the same way, and `rel_members` is
scanned four times, feeding the way-id list, the member-node/member-way
splits, and the suppression anti-join. The
zero-bytes-of-temp-disk measurement (10.7M OSM rows, 32GB memory limit)
predates the relation INSERT and doesn't cover it — the relation
pipeline's spill behavior is not yet measured, and nothing structural
holds either finding. OSM nodes carry their own `lat`/`lon` coordinates
straight through; ways and relations each derive a centroid by averaging
their referenced or member nodes' coordinates (`way_centroids`,
`rel_centroids`). All three pipelines apply the same category allow-list,
keeping only named POIs in a fixed set of OSM tag categories.

**Variants**: all three sources produce them.

`overture_place` computes them per row inside the same CTAS, by concatenating
two `list_transform`s — one over `map_entries(names.common)`, whose entries
are always typed `alternate` with the map key as the language, and one over
`names.rules`, whose `variant` string maps `common` and `alternate` to
`alternate`, `official` to `official`, `short` to `short`, and anything
unrecognized to `alternate`. Entries with a NULL or blank name are filtered
out; the result is `list_sort`ed. Duplicates between the two sources are
deliberately not deduplicated.

`overture_division` computes them the same way, from `names.common` and
`names.rules` on the division parquet.

`osm` computes them per row the same way, from the raw OSM tags map via two
macros defined in `osm_import.sql` (`osm_dropped_suffix`,
`osm_variant_type_lang`): `name:{lang}` and six other name-family tags each
map to a type from a fixed table, an annotation-suffix drop-list is applied,
values are split on `;`, and a variant equal (after trim) to the primary name
is dropped. The result is `list_sort`ed the same way as `overture_place`'s.

## 6. `stage_tile_assignment` (`garganorn/stages.py`)

**Writes**: `<source>/tile_assignments.parquet`.

**Schema**: `place_id VARCHAR, tile_qk VARCHAR`.

**Sort**: `ORDER BY tile_qk, place_id`, for deterministic output. No
downstream read depends on it: `stage_export` joins this file wholesale and
pass 2 re-sorts each partition with its own `ORDER BY tile_qk, place_id`,
which is what establishes the order its cursor consumes.

**Shape**: assigns each place to the coarsest quadtree tile (z6..z17)
whose place count is at or below `max_per_tile`, by building a per-zoom
`tile_counts` table and picking, per place, the minimum zoom level among
tiles that satisfy the cap (falling back to z17 — the place's own leaf
tile — if no coarser tile qualifies). This sizes the grid without a fixed
global zoom: dense areas get fine tiles, sparse areas get coarse ones. It
bounds records per export tile for the point sources, whose export reads
this artifact; a division tile's exported count follows geometry instead,
since a division's per-tile membership comes from `tile_references.parquet`,
which folds into `tile_assignments_combined.parquet` before `stage_export`
reads it. Places with a NULL or malformed `qk17` are dropped (logged as
a warning, not a failure); a post-write duplicate-assignment check exists
because the join logic should make a place-in-two-tiles outcome
impossible, but a silent double-export would be a worse failure mode than
a loud check.

**Division-only extension**: `stage_division_tile_references`
(`garganorn/stages.py`) writes `<source>/tile_references.parquet` for
`overture_division`, same schema and sort. It expands the grid above to
every tile a division's geometry overlaps — read from the covering
artifact, not `qk17` — so a division gets one row per referencing tile
instead of one. This feeds `tile_assignments_combined.parquet` (the union
with the summary band's own tile references) in place of
`tile_assignments.parquet`; other sources are unaffected.

**Summary-band extension**: `stage_summary_tile_assignment`
(`garganorn/stages.py`) writes `<source>/summary_tile_assignments.parquet`,
same schema, restricted to the top-N places by `importance DESC, place_id
ASC` (default N=10000) — for `overture_division`, every `country`/`region`/
`dependency` subtype is additionally included, additive on top of N
(`docs/design-constraints.md`, "The summary band is a coarse tile tier for
region-less resolution"). It reuses this stage's coarsest-fit algorithm
over that subset, but in a z1-z5 band instead of z6-z17; z5 is a hard
floor, so a z5 tile may exceed `max_per_tile`.

For `overture_division`, `stage_summary_division_tile_references`
(`garganorn/stages.py`) writes `<source>/summary_tile_references.parquet`,
the summary-band analog of `stage_division_tile_references` above: it
expands the summary grid the same overlap-from-covering way, then
restricts the result to the
`place_id`s already in `summary_tile_assignments.parquet` — without that
restriction every division on earth would be referenced into the z1-z5
tiles, destroying the top-N cut.

`_union_tile_assignments` (`garganorn/quadtree.py`, not `stages.py` — it
needs no source-specific schema, just the shared `place_id, tile_qk`
columns) then `UNION ALL`s the regular assignments (or division
references) with their summary-band counterpart into
`tile_assignments_combined.parquet`, `ORDER BY tile_qk, place_id`.
`UNION ALL` rather than `UNION` is safe because the regular band is z6-z17
and the summary band is z1-z5, so no `(place_id, tile_qk)` pair can appear
on both sides. `compute_containment`/`stage_division_containment` and
`stage_export` both read this combined artifact instead of the plain one,
which is what makes a summary-band record byte-identical to its regular
copy — same containment, same everything, it just also appears at a
coarser tile.

## 7. `stage_export` (`garganorn/stages.py`)

**Writes**: a new timestamped run directory,
`<tiles_root>/<YYYYMMDDTHHMMSS>/<qk[:6]>/<qk>.json.gz` (one gzip file per
tile), plus `manifest.duckdb` and `manifest.json` in the run dir, then
swaps the `<tiles_root>/current` symlink to point at it. `qk[:6]` returns
the whole key for the summary band's sub-6-char quadkeys, so those tiles
land at `<qk>/<qk>.json.gz` in the same layout.

**Schema** (tile payload, per `envelope.py`): `{collection, source,
license, generated_at, records: [{uri, cid: null, value: <record>}]}`.
`value` is one `org.atgeo.place` record per `<source>_export_tiles.sql`
(`rkey, name, importance, locations[], variants[], attributes, relations`).
`manifest.duckdb` has `record_tiles(rkey VARCHAR, tile_qk VARCHAR)` and
`metadata(source, collection, generated_at)`. `manifest.json` is just
`{generated_at}`.

**Sort**: two passes. Pass 1 copies the export query's output into a
staging directory, Hive-partitioned by `left(tile_qk,
export_partition_zoom)`, with no `ORDER BY` — this bounds peak spill to
one partition rather than the whole dataset. `left(s, 6)` on a summary
tile's shorter key returns the whole key, so those tiles partition by
their own quadkey rather than a 6-char prefix. Pass 2 reads one partition
at a time with `ORDER BY tile_qk, place_id` and streams it to the flush
loop below. `place_id` is a deterministic tiebreaker with no meaning
downstream, kept solely so repeated runs over identical inputs produce
byte-identical gzip output. `manifest.duckdb`'s `record_tiles` is sorted
by `rkey`, since lookups against it are by record key.

**Shape**: the staging directory is private to the stage, which creates
and destroys it, so it never appears in `Writes` above. It sits beside
the spill directory either way: under `temp_directory` when the caller
supplies one, otherwise beside the run dir on the tiles volume. Sizing
the tiles volume therefore has to account for it when no
`temp_directory` is given. Pass 2 streams each partition's query via
`fetchmany(1000)`; flush work (JSON wrap + gzip + write) is handed to a
thread pool with inflight futures capped at `2 * workers`
(`max_inflight`), each holding one tile's records, plus the tile still
accumulating from the cursor — so up to `max_inflight + 1` tiles' records
are buffered in Python memory at once, regardless of source size.
`manifest.json` is written last
— after every tile file and after `manifest.duckdb` — so its presence is
the run's sole completeness marker: freshness gating and the keep-2
retention sweep both key off it, and a crash mid-export leaves a run dir
without `manifest.json`, which the next invocation deletes before writing
a new one. The `current` symlink is swapped via write-to-temp-name then
`os.rename` (atomic on POSIX), so readers never observe a half-updated
symlink. Retention keeps only the two newest *complete* (manifest.json-
bearing) run dirs, so an interrupted run never displaces good history from
the keep-2 count.

**Why the staging write stays**: it runs an estimated 25–40 GiB on a
global build, and the bar for a pipeline disk write is that spill stays
bounded — a few dozen GB is fine, hundreds is not. Writing nothing at all
is reachable for `overture_place` and `osm` by filtering both sides of
the export join on a quadkey prefix range, since for those sources
`tile_qk` is always a prefix of the place's own `qk17` and both sides
prune by zone map. It does not generalise to divisions, whose tile
references come from the covering artifact rather than their own `qk17`,
so a places-side filter would drop them silently. A second export
mechanism is not worth buying back a few dozen GB.

**`collection.json`**: `write_collection_json` runs between
`write_manifest_db` and `write_manifest`, writing the `org.atgeo.collection`
record value as `collection.json`, sibling to `manifest.json` in the run
dir. This seam is the only point where every input is simultaneously in
hand: the per-tile histogram, the freshly written `record_tiles`,
`places.parquet`, the containment artifacts, and the export's single
`run_now`. `write_manifest` still runs last, so `manifest.json` stays the
sole completeness marker — an interrupted run can leave `collection.json`
on disk without `manifest.json`, same as any other partial output. A
zero-tile run omits `collection.json` entirely rather than fabricate a
required `extent`. `idf.parquet` joins `stage_export`'s freshness inputs
for `overture_place` and `osm` (divisions have no IDF artifact), so a
category-vocabulary refresh forces a re-export.

Most fields are read straight from artifacts already open for the tile
export (`source`/`license` from the source class, `recordCount` from
`record_tiles`, `extent` from the regular-band quadkey histogram,
`containmentLevels` from the containment artifacts mapped through
`LEVEL_VOCAB`). `categories` comes from `idf.parquet` for `overture_place`
and `osm`, and from `GROUP BY subtype` over `places.parquet` for
`overture_division`. `locationTypes` is `community.lexicon.location.geo`
for `overture_place` (plus `.address` iff at least one exported record has
a country-bearing address) and for `osm` (unconditional, one location per
record), and `.bbox` for `overture_division` (unconditional). `attributes`
is source-specific: `overture_place` and
`overture_division` capture it directly from `tile_export`'s rendered JSON
on the export connection (`json_keys(record_json, '$.attributes')`) — the
actual served shape, immune by construction to any upstream column
filtering. `osm` instead reads `places.parquet`'s `tags` column directly,
to avoid a full JSON-materialization scan over planet-scale data; because
`osm_import.sql` deliberately strips each record's primary-category key out
of `tags` (it's already carried in the separate `primary_category` column,
and `osm_export_tiles.sql` re-adds it at export time via `map_concat`),
`write_collection_json`'s OSM branch closes the same gap by unioning in the
category keys already fetched from `idf.parquet` — reusing that read rather
than re-deriving from `tags` a second time.

## How the stages compose

Each stage above is independently freshness-gated, so the `density`, `idf` and
`covering` subcommands can be invoked in any sequence and simply no-op if their
inputs aren't ready yet. `run` is the exception: `run_pipeline()`
for a source with a `boundaries_db` requires `<dirname(boundaries_db)>/covering/`
to already exist and be strictly newer than `boundaries_db` — it raises
`RuntimeError` otherwise rather than building it. So the `overture_division`
pipeline, or a standalone `quadtree covering`, must run before any other source
that passes `--boundaries`.

`run_pipeline()` sequences one source through: `stage_import` (dispatching to
`stage_division_import` for `overture_division`) → `stage_covering`
(division only, from the `boundaries.duckdb` it just wrote) → the covering
freshness check described above (other sources, when a `boundaries_db` is
supplied) → `stage_tile_assignment` → `stage_division_tile_references`
(division only, expanding `tile_assignments.parquet` into
`tile_references.parquet`) → `stage_summary_tile_assignment` →
`stage_summary_division_tile_references` (division only) →
`_union_tile_assignments`, producing `tile_assignments_combined.parquet`
→ containment (in `stages.py`, not one of the
seven stages above): `stage_division_containment` for `overture_division`,
joining Overture's `hierarchies` against the imported places
(`design-constraints.md`, "Division containment comes from Overture
hierarchies, not geometry"); `compute_containment` for every other source,
joining a source's places against the covering artifact and
`boundaries.duckdb`. Both write `<source>/containment/*.parquet`, which
`stage_export` reads for each record's `relations` → `stage_export`. The
containment step and `stage_export` both take
`tile_assignments_combined.parquet` — the union of the regular tile
assignments/references above and their summary-band counterparts — for
every source, not just `overture_division`.

The `all` subcommand (`quadtree.py:_cmd_all`) runs the full set for every
configured source: `stage_density_extract` once (shared,
`overture_place`-derived), `stage_idf` once per place source, then
`run_pipeline("overture_division", ...)` first — since it produces
`boundaries.duckdb` and `covering/`, which every other source's
containment step requires — followed by `run_pipeline` for the
remaining place sources, now supplied
`boundaries_db`, the shared `density_tiles.parquet`, and each source's own
`idf.parquet`.
