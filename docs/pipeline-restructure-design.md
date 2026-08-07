---
category: Design
tags: [garganorn, duckdb, pipeline, quadtree, privacy]
last_updated: 2026-08-03
confidence: design-complete, validated at CONUS scale (not yet global)
status: Phases 1, 2, and 2b merged and deployed as of 2026-08-03. Decisions
  condensed in pipeline-implementation-decisions.md. Phase 3
  (searchRecords/getCoverage removal) and Phase 4 (global validation) are
  not started; current tentative order is Phase 4 before Phase 3 (see
  pipeline-restructure memory notes, 2026-07-09). Current operational
  state: pipeline-status.md.
---

# Garganorn Pipeline Restructure: Pure-Parquet Stages and Boundary-Centric Containment

This document specifies a restructuring of the `feat/quadtree` pipeline into
pure parquet-to-parquet stages, replaces the per-tile containment recursion
with a precomputed boundary covering, and removes the `searchRecords` and
`getCoverage` endpoints in favor of fully static tile serving. It is written
as an execution spec for a coding agent: every artifact, schema, algorithm,
deletion, and acceptance check is named concretely. Where a decision is
deferred to measurement, the measurement is specified.

Base branch: `feat/quadtree` at commit `ece56fc`.

## 1. Goals

1. Every pipeline stage is a pure function from input files to one output
   artifact, with no shared mutable database state.
2. Containment cost scales with boundary complexity (computed once per
   division release), not with places × tiles per run.
3. The pipeline completes on a 72 GB machine with spill-to-disk, using
   documented DuckDB settings. No operation may require an unspillable
   memory structure of unbounded size.
4. The serving path becomes static files plus one dynamic endpoint
   (`com.atproto.repo.getRecord`). `searchRecords`, the trigram index, and
   `getCoverage` are removed.
5. Delete the sentinel/resume machinery, the per-tile containment recursion,
   and ~400 lines of query code in `database.py`.

### Non-goals

- Client SDK design (coverage-manifest consumption, local search, prefetch).
  Out of scope; §7.2 defines only the manifest format the SDK will read.
- Lexicon changes beyond marking `searchRecords` removed.
- Automated old-run cleanup policy changes (keep current keep-2 behavior).
- Non-Latin search. It becomes a client-side concern once `searchRecords`
  is gone.

## 2. Artifact graph

All artifacts live under a base output directory. `<src>` ranges over
`foursquare`, `overture_place`, `osm`. Divisions are a special source that
additionally produces boundary artifacts consumed by the others.

```
INPUTS (downloaded, read-only)
  cache/fsq/<release>/*.parquet
  cache/overture/<release>/place/*.parquet
  cache/overture/<release>/division.parquet, division_area.parquet
  cache/osm/<region>/{nodes,ways}/*.parquet

SHARED ARTIFACTS
  shared/density_tiles.parquet          <- overture place parquet
  <src>/idf.parquet                     <- source parquet (per source)

DIVISION ARTIFACTS (rebuilt per Overture division release)
  overture_division/places.parquet      <- division import
  overture_division/boundaries.duckdb   <- division import (geometry + R-tree)
  overture_division/covering/*.parquet  <- boundaries.duckdb   [NEW]

PER-SOURCE ARTIFACTS
  <src>/places.parquet                  <- source parquet + density + idf
  <src>/tile_assignments.parquet        <- places.parquet
  <src>/containment/*.parquet           <- places + tile_assignments
                                           + covering + boundaries.duckdb
  <src>/tiles/<timestamp>/**/qk.json.gz <- places + tile_assignments
                                           + containment
  <src>/tiles/<timestamp>/manifest.json
  <src>/tiles/<timestamp>/manifest.duckdb
  <src>/tiles/current -> <timestamp>    (atomic symlink swap, unchanged)
```

Rules that apply to every artifact:

- **Write to `<path>.tmp`, fsync, atomically rename.** A crash leaves either
  the old artifact or nothing. This replaces the sentinel table,
  `_ensure_sentinel_table`, `_read_sentinel`, `_mark_complete`,
  `_find_incomplete_run`, and the corrupted-working-DB detection — all
  deleted (§9).
- **Freshness is mtime-based.** A stage is skipped when its output exists
  and is newer than every input. Reuse `_is_output_fresh()` from
  `stages.py:41`; it is now the *only* caching mechanism.
- **Every parquet artifact declares a sort order** (listed per schema
  below). Sort order is a correctness-adjacent invariant: DuckDB zonemap
  pruning depends on it (constraint D2). Any CTAS/COPY producing an
  artifact must carry an explicit `ORDER BY`.
- **Every stage opens its own DuckDB connection** — in-memory
  (`duckdb.connect()`) with `SET temp_directory` pointed at scratch space,
  since in-memory connections cannot spill without it. No working
  `.duckdb` file persists between stages. `boundaries.duckdb` and
  `manifest.duckdb` are *artifacts*, not working state: written once,
  attached read-only thereafter.

### Constraints carried forward

D1–D5 from `design-constraints.md` remain in force. Add:

- **D6: No unbounded complex-state aggregation.** DuckDB cannot spill
  intermediate state for `list()`, `string_agg()`, and other holistic
  aggregates. Any query using them must run over a bounded partition
  (per-tile, or per qk4 prefix as in §5.3). This is the only way to OOM
  the 72 GB target, so it is a hard review criterion for every SQL file.
- **D7: Antimeridian bboxes are two lobes.** Wherever a bbox filter is
  derived from a geometry's min/max longitude, `min_longitude >
  max_longitude` means the feature crosses ±180°; the filter must be
  `(lon >= min OR lon <= max)` or the tile-seed set must be the union of
  the two lobes. This resolves DATA-4 structurally rather than as a patch.

## 3. Stage specifications

Stages are listed with signature, inputs, output, and mechanism. SQL files
keep the existing `string.Template` parameter convention.

### 3.1 `density_extract` — unchanged in substance

`stage_density_extract()` already writes a standalone parquet with tile
bounds. Changes: output path becomes `shared/density_tiles.parquet`; adopt
tmp+rename; drop its bespoke `force` handling in favor of the shared
freshness check. Output sorted by `tile_qk15`.

### 3.2 `idf` — unchanged in substance

`stage_idf()` already reads source parquet on an ephemeral connection
(constraint P5) and writes `idf.parquet`. Changes: tmp+rename; shared
freshness check. Output sorted by `category`.

### 3.3 `import` — `<src>/places.parquet`

Current `stage_import()` and the four `*_import.sql` files survive almost
intact; the destination changes from a `places` table in a working DB to a
parquet artifact:

```sql
COPY (
    <existing import CTE, unchanged>
    ORDER BY qk17
) TO '${output_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD);
```

Schema: existing source-specific columns plus `qk17 VARCHAR`,
`importance INTEGER`, `variants VARCHAR[]`. **Sorted by `qk17`** — this is
the load-bearing sort: tile assignment, containment partitioning, and
export all become prefix-range scans over it.

The geometry column is dropped from the parquet for `overture_place`
(coordinates come from `bbox` midpoints, as the export SQL already does);
`foursquare` and `osm` keep their scalar lat/lon columns. Divisions are
handled in §3.4. Rationale: serialized geometries are the widest column by
far and nothing downstream of division import reads them.

`SET preserve_insertion_order = false` is safe and recommended in every
import (order is established by the explicit `ORDER BY`).

### 3.4 `division_import` — `places.parquet` + `boundaries.duckdb`

`overture_division_import.sql` and `export_boundaries_db()` merge into one
stage with two artifacts. `boundaries.duckdb` keeps its current schema
except that `admin_level` is replaced by `level INTEGER` — the atgeo
containment level vocabulary (atgeo-spec.md's Containment levels section), mapped from Overture's
`subtype` via a CASE expression at import. This is the single place the
mapping is applied; covering and containment copy `level` downstream. The
import must `SELECT DISTINCT subtype` first and fail loudly on any subtype
absent from the mapping table rather than defaulting. Schema: `id,
geometry, level, min_latitude, max_latitude, min_longitude,
max_longitude`, Hilbert-sorted, R-tree indexed — it remains the only
place full geometries live. The division `places.parquet` (tile records:
subtype, country, wikidata, population, importance, qk17) follows §3.3.

### 3.5 `covering` — `overture_division/covering/*.parquet` [NEW]

The centerpiece. For every boundary, compute a quadkey covering: the set of
tiles the boundary fully contains (*interior*) and the set it partially
overlaps (*edge*). Computed once per division release; every source's
containment stage consumes it read-only.

**Output schema**, one row per (boundary, tile):

```
tile_qk      VARCHAR   -- length 4..COVER_MAX_ZOOM
boundary_id  VARCHAR
level        INTEGER   -- atgeo containment level (copied from boundaries.duckdb)
kind         VARCHAR   -- 'interior' | 'edge'
```

Written as one parquet file per z4 prefix under `covering/`
(`covering/<qk4>.parquet`), each sorted by `(tile_qk, boundary_id)`.
Per-prefix files give the containment stage natural partition boundaries
and per-file zonemaps.

**Parameters**: `COVER_MIN_ZOOM = 4`, `COVER_MAX_ZOOM = 12`. With the rule
below, interior tiles appear at any zoom in [4, 12]; edge tiles appear
only at exactly z12 (everything not interior and not disjoint is
subdivided until max zoom). z12 tiles are ~9.8 km at the equator; a z12
edge band around every boundary keeps edge candidate sets small without
exploding row counts. These are constants in one place, tunable after the
measurement in §10.

**Algorithm** — level-by-level, set-based (no Python recursion):

1. **Seed (Python).** For each boundary row in `boundaries.duckdb`, compute
   the z4 tiles intersecting its bbox using tile arithmetic (port of the
   inverse of `quadkey_to_bbox()`, `stages.py:455`). Apply D7: if
   `min_longitude > max_longitude`, seed both lobes. Insert
   `(boundary_id, tile_qk)` seed pairs into DuckDB via Arrow. ~1M
   boundaries, trivial.
2. **Level loop (SQL, z = 4 … 11).** Maintain a working table
   `L(boundary_id, level, tile_qk, geom)` where `geom` is the
   boundary geometry clipped to the tile's *parent* envelope. Each
   iteration is one CTAS:
   - `interior_z`: rows where `ST_Contains(geom, qk_env(tile_qk))` →
     append to output with `kind='interior'`.
   - discard rows where `NOT ST_Intersects(geom, qk_env(tile_qk))`.
   - remaining rows expand ×4 (`tile_qk || d for d in '0123'`) into
     `L_{z+1}`, with `geom` re-clipped:
     `ST_Intersection(geom, qk_env(child_qk))`. Clipping at each level is
     the same vertex-count optimization the current per-tile code uses
     (`stages.py:116-128`); clipping to the parent envelope does not
     change containment results for child tiles (child ⊆ parent).
3. **Terminal (z = 12).** Surviving rows → `kind='edge'`.
4. Partition-write the accumulated output by `left(tile_qk, 4)`.

Correctness of the interior test after clipping: for `tile_env ⊆
parent_env`, `ST_Contains(geom ∩ parent_env, tile_env) ⟺
ST_Contains(geom, tile_env)`. State this as a comment; test it (§10).

**`qk_env` macro.** Implement `quadkey_to_bbox()` as a DuckDB SQL macro
(`qk_env(qk)` returning a geometry via `ST_MakeEnvelope`). The mercator
inverse needs only `atan`, `exp`, `pi()` — all available. The Python
function at `stages.py:455` is the reference implementation; a required
unit test compares the two across 10,000 random quadkeys of mixed length
(exact envelope-coordinate agreement to 1e-9).

**Memory note.** Row counts along the level loop grow with total boundary
perimeter ÷ tile edge, not area — the interior emission at each level
prunes the interiors of large polygons. The clipped `geom` column is the
dominant width; each CTAS level is spillable (sorts/joins only, no
holistic aggregates). If a level's CTAS is slow, that is a temp-disk
bandwidth problem, not a memory problem.

### 3.6 `tile_assignment` — `<src>/tile_assignments.parquet`

`compute_tile_assignments.sql` survives with `places` read from
`places.parquet` instead of a table, and output via `COPY ... ORDER BY
tile_qk` (already its sort order) to parquet. Schema unchanged:
`(place_id VARCHAR, tile_qk VARCHAR)`.

### 3.7 `containment` — `<src>/containment/*.parquet` [REWRITTEN]

Replaces `compute_containment()`, `_process_tile()`, `_run_containment()`
(`stages.py:89-330`) entirely. No temp tables, no per-tile SQL statements,
no R-tree dependency in the join path (SPATIAL-6 becomes moot).

**Output schema**, one row per place that is inside at least one boundary:

```
tile_qk        VARCHAR   -- the place's export tile (from tile_assignments)
place_id       VARCHAR
relations_json VARCHAR   -- {"within":[{"rkey":...}, ...]} ordered by level (atgeo vocabulary, atgeo-spec.md's Containment levels section)
```

One parquet file per qk4 prefix (`containment/<qk4>.parquet`), sorted by
`(tile_qk, place_id)`.

**Execution: a Python loop over qk4 prefixes** (the distinct
`left(qk17, 4)` values present in `places.parquet` — at most 256 non-polar
prefixes, in practice far fewer). Per prefix, one query:

```sql
COPY (
WITH p AS (
    SELECT ${pk} AS place_id, qk17, ${lon} AS lon, ${lat} AS lat
    FROM read_parquet('${places}')
    WHERE left(qk17, 4) = '${prefix}'          -- zonemap range scan
),
cov AS (
    SELECT * FROM read_parquet('${covering_dir}/${prefix}.parquet')
),
interior AS (                                   -- one arm per level 4..12
    SELECT p.place_id, c.boundary_id, c.level
    FROM p JOIN cov c
      ON c.kind = 'interior' AND len(c.tile_qk) = ${L}
     AND left(p.qk17, ${L}) = c.tile_qk
    -- UNION ALL over L in 4..12, generated by the Python driver
),
edge AS (
    SELECT p.place_id, c.boundary_id, c.level
    FROM p
    JOIN cov c ON c.kind = 'edge' AND left(p.qk17, 12) = c.tile_qk
    JOIN bnd.places b ON b.id = c.boundary_id
    WHERE p.lat BETWEEN b.min_latitude AND b.max_latitude
      AND (CASE WHEN b.min_longitude <= b.max_longitude
                THEN p.lon BETWEEN b.min_longitude AND b.max_longitude
                ELSE p.lon >= b.min_longitude OR p.lon <= b.max_longitude
           END)                                  -- D7
      AND ST_Contains(b.geometry, ST_Point(p.lon, p.lat))
),
matches AS (SELECT * FROM interior UNION ALL SELECT * FROM edge)
SELECT ta.tile_qk, m.place_id,
       to_json({within: list({rkey: '${collection_prefix}:' || m.boundary_id}
                             ORDER BY m.level ASC)})::VARCHAR
       AS relations_json
FROM matches m
JOIN read_parquet('${tile_assignments}') ta ON ta.place_id = m.place_id
GROUP BY ta.tile_qk, m.place_id
ORDER BY ta.tile_qk, m.place_id
) TO '${output_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD);
```

`boundaries.duckdb` is attached read-only as `bnd` for the edge arm's
geometry lookup only. The `list()` aggregation satisfies D6 because it is
bounded per prefix: group count ≤ places in one qk4 cell, state per group
≤ containing-boundary count (typically 4–10 admin levels). The prefix loop
is embarrassingly parallel if wanted later; ship it sequential.

The interior arm replaces phase 1 of the old design (bulk assignment
without geometry tests); the edge arm replaces phase 2, with the covering
having pre-restricted candidates to the boundary's own z12 edge band —
strictly tighter than the old per-tile `ST_Intersects` prefilter.

If `covering/` is absent (no boundaries configured), the stage writes an
empty artifact and export degrades gracefully, preserving Q3.

### 3.8 `export` — tiles + manifests

`export_tiles()` (`stages.py:330`) keeps its streaming
cursor/ThreadPoolExecutor/backpressure structure unchanged. The
`*_export_tiles.sql` views change their FROM clause: `places`,
`tile_assignments`, and `place_containment` become
`read_parquet('.../places.parquet')`,
`read_parquet('.../tile_assignments.parquet')`, and
`read_parquet('.../containment/*.parquet')` respectively; the
`relations` field reads `pc.relations_json` exactly as today. Because all
three inputs are sorted by tile order, the `ORDER BY ta.tile_qk` in the
export view is a merge over presorted inputs rather than a fresh global
sort. The export query contains **no** aggregation (D6 satisfied by
construction: relations were assembled per-prefix in §3.7).

The export SQL and `flush_tile()` also adopt the atgeo v1 envelope
(companion design, §1.2): tile payload gains `"atgeo": 1` and
`generated_at`; each record is wrapped as `{uri, cid, value}` with `uri`
in the canonical https form (`https://{repo}/{collection}/{rkey}`) and
`cid: null`. Gazetteer records are not repository data and must not carry
`at://` URIs. Small diff to `*_export_tiles.sql` and the payload dict in
`export_tiles()`; land it before the SDK conformance vectors freeze the
format.

`write_manifest()` gains one field (§7.2). `write_manifest_db()` is kept
as-is — `manifest.duckdb`'s `record_tiles` table is what backs `getRecord`
(§7.3) — but reads `tile_assignments.parquet` instead of a table.

## 4. Orchestration

**Decision: a thin Python orchestrator with per-stage subcommands. No
Makefile.**

The argument, honestly weighed: `make` gives DAG semantics and mtime
freshness for free, and this document's artifact graph maps onto it
cleanly. It loses on three practical points. First, the stages take
non-file parameters — bbox, norm constants, memory limit, per-source
parquet *tuples* (OSM's node/way pair, divisions' division/area pair) —
which in make become environment-variable contortions or generated
fragments. Second, the pipeline needs Python inside stages anyway (covering
seed generation, export streaming), so make would be a second language
orchestrating the first for a graph with seven nodes. Third,
`_is_output_fresh()` already implements make's one important idea. When a
7-node DAG needs make, the DAG has a growth problem, not a tooling problem.

Concretely: `quadtree.py` (name kept to limit diff noise) exposes
subcommands —

```
python -m garganorn.quadtree density  --parquet ... --output ...
python -m garganorn.quadtree idf      --source ... --parquet ... --output ...
python -m garganorn.quadtree covering --boundaries ... --output ...
python -m garganorn.quadtree run      --source ... [existing args]   # full DAG for one source
python -m garganorn.quadtree all      --config config.yaml           # everything, dependency order
```

`run` executes import → tile_assignment → containment → export → manifest
for one source, checking freshness per artifact. `all` orders sources:
divisions (import, covering) first, then places sources, density/idf as
leaves. `--force` invalidates by deleting outputs, nothing else. The
sentinel/resume path is gone: an interrupted run reruns unfinished stages
from their start, which the atomic-rename rule makes safe and the stage
granularity makes cheap (§6 for why this trade is acceptable).

## 5. Memory and disk budget

Target machine: 72 GB RAM available, NVMe scratch.

- `SET memory_limit = '48GB'` (raise to 56 only after a clean global run;
  allocations outside the buffer manager are why not 70).
- `SET temp_directory = '<nvme-scratch>/duckdb_tmp'`,
  `SET max_temp_directory_size = '250GB'`. Provision ≥ 200 GB free: the
  qk17 sort of a global source (~35–50 GB uncompressed for Overture
  places) plus join intermediates.
- `SET preserve_insertion_order = false` in every stage.
- Sources run sequentially; peak memory is the largest single stage, and
  every heavy operator in this design (sorts, hash joins, hash aggregates
  with scalar state) spills. The only non-spillable construct permitted is
  the per-prefix `list()` in §3.7 (bounded, D6).
- Expected spill points: import qk17 sort (largest), covering level-loop
  CTAS for perimeter-heavy levels (z10–11). Both are sequential-I/O
  friendly on NVMe. No stage should OOM; an OOM is a D6 violation and a
  bug.

## 6. What is lost, deliberately

Within-stage resume. The old sentinel machinery could resume a killed run
mid-pipeline; the new design reruns the interrupted stage. Worst case is
rerunning a global import sort — tens of minutes on this hardware, monthly
cadence. Traded for deleting `_ensure_sentinel_table`, `_read_sentinel`,
`_mark_complete`, `_find_incomplete_run`, the symlink-target bookkeeping,
and the corrupted-DB probe (~150 lines and the entire class of
"half-finished working DB" states).

## 7. Server changes

### 7.1 Remove `searchRecords` and the trigram system

Delete from `database.py`: `_strip_accents`, `_compute_trigrams`,
`_query_trigram_text`, `query_nearest`, `nearest`, the `JW_THRESHOLD` /
`JW_TOKEN_ALPHA` / `MAX_QUERY_TRIGRAMS` constants,
`compute_importance_floor`, and the `name_index` schema checks
(`database.py:99-113`). Delete `search_records` from `server.py` and the
`searchRecords` registration in `Server.methods`. Remove
`searchRecords.json` from the lexicon directory (or mark it withdrawn in
the file, whichever the lexicon-community convention prefers — one-line
decision at implementation time). What remains of `database.py` is the
per-source class metadata the pipeline consumes (`source_key`,
`source_pk`, `collection`, `attribution`, coordinate expressions); if that
is all that remains, fold it into `stages.py` and delete the file.

Consequences absorbed: EXPORT-1/12 (non-Latin search), SCORE-6/7/8/9, the
616M-row `name_index`, and Q1/Q2 from design-constraints (retired, not
carried).

### 7.2 Replace `getCoverage` with a static manifest

`manifest.json` already carries `{source, generated_at, quadkeys:
sorted([...])}`. Add one field:

```json
"tile_url_template": "{base}/{qk6}/{qk}.json.gz"
```

where `{qk6}` is the first six characters of the quadkey (the existing
shard layout). Clients fetch `<base>/current/manifest.json`, intersect
their region with the quadkey list locally, and construct URLs — the
server receives no bbox, ever. The 0.01° precision grid, `BboxTooPrecise`,
and `max_coverage_tiles` become obsolete server-side; `BboxTooLarge`
semantics move to the client SDK as a local politeness limit.

Delete from `server.py`: `get_coverage`, `_check_bbox_precision`,
`_parse_bbox` (verify no other caller first — `getRecord` does not use
it), `max_coverage_tiles`. Delete from `quadtree.py`: `TileManifest`,
`BboxTooLarge`.

Manifest size is the one open risk: a global source at
`max_per_tile=1000` plausibly yields 10⁵–10⁶ tiles; sorted quadkeys
share long prefixes and gzip hard, but this is an estimate, not a
measurement. §10 requires measuring it; if the gzipped manifest exceeds
~5 MB, the fallback is per-z4-prefix manifest shards (same format,
`manifest/<qk4>.json`), which is a client-visible format change and should
be decided from data, not speculation.

### 7.3 `getRecord` survives

It is already tile-backed: `manifest.duckdb`'s `record_tiles` maps rkey →
tile_qk and `tile_reader.py` pulls the record from the tile file. Keep it,
along with lexicon serving (`list_records`) and `/health`. This is the
entire remaining dynamic surface. **Open question, not for this
iteration:** whether `getRecord` can also go static (rkey-sharded index
files) — defer until something actually needs it.

## 8. Execution phases

Each phase is independently mergeable and ends with its acceptance checks
green.

**Phase 0 — Baseline.** Capture current-branch output for a fixed test
bbox (suggest the SF extent already used in scripts:
`-122.5137 37.7099 -122.3785 37.8101`) for `overture_place` +
`overture_division` with containment: tile file set, per-tile record
counts, and the full set of `(place_id, within-rkey)` pairs, extracted to
a comparison-friendly format (sorted CSV). Record wall time per stage.
**[Amendment 2026-07-06: Baseline capture dropped — containment never
worked on the current production server, so no valid baseline exists.
Correctness rests on the test suite instead (see Phase 1 amendment).]**
Write the `qk_env`-vs-`quadkey_to_bbox` unit test (it tests existing code;
it can land first). **[Done.]**

**Phase 1 — Covering + containment rewrite.** Implement §3.5 and §3.7
against the *existing* working-DB pipeline (covering reads
`boundaries.duckdb`, which already exists; containment output feeds the
existing export unchanged, since `place_containment(place_id,
relations_json)` and the new artifact are join-compatible). ~~Acceptance:
`(place_id, rkey)` pair set over the test bbox is identical to Phase 0
baseline, except for places lying exactly on boundary edges (SPATIAL-7
allows arbitrary assignment; log and eyeball any diffs, require them to be
edge cases). Delete the old containment code only after parity.~~
**[Amendment 2026-07-06: Parity acceptance replaced — no baseline exists
(see Phase 0). Acceptance: test suite green, including brute-force oracle
parity tests in `tests/test_containment_covering.py`. Old containment code
deleted on that basis.]**

**Phase 2 — Parquet artifacts + orchestrator.** Convert import,
tile_assignment, export to the artifact scheme (§3.3, §3.6, §3.8);
implement subcommands (§4); delete sentinel/resume machinery (§6).
Acceptance: byte-comparable tile JSON (modulo `generated_at`) against
Phase 1 output on the test bbox; a `kill -9` mid-import followed by rerun
produces a correct, fresh result.

**Phase 3 — Server removals.** §7.1 and §7.2. `searchRecords` and
`getCoverage` are documented public API on atgeo.org with (per the
operator) zero external users, so removal is plain removal — but the same
change must update the atgeo.org API and Usage pages (site punch list in
the execution plan), since abandoned documentation outlives endpoints.
Acceptance: `getRecord`, lexicon listing, and `/health` work;
`searchRecords` and `getCoverage` return XRPC method-not-found (automatic
once unregistered from lexrpc); test suite green with search tests
deleted, not skipped; site pages updated in the same change set.

**Phase 4 — Global validation.** §10 in full, on the production box.

## 9. Deletion ledger

For the agent's convenience, the complete list, with current locations:

| Symbol / file | Location | Phase |
|---|---|---|
| `_run_containment`, `_process_tile`, `compute_containment` (old bodies) | `stages.py:89-330` | 1 |
| `_ensure_sentinel_table`, `_read_sentinel`, `_mark_complete` | `quadtree.py:45-71` | 2 |
| `_find_incomplete_run` + resume branches in `run_pipeline` | `quadtree.py:74-108, 196+` | 2 |
| `TileManifest`, `BboxTooLarge` | `quadtree.py:316+` | 3 |
| Trigram/JW/search machinery, `name_index` checks | `database.py` (§7.1 list) | 3 |
| `search_records`, `get_coverage`, `_check_bbox_precision`, `_parse_bbox`, `max_coverage_tiles` | `server.py` | 3 |
| `searchRecords.json` | `garganorn/lexicon/` | 3 |
| Design-constraint entries Q1, Q2; pipeline-status items EXPORT-1/12, SCORE-6/7/8/9, SPATIAL-6, DATA-4 | `docs/` (update, noting resolution) | 3–4 |

## 10. Validation plan (Phase 4)

Run against a full global `overture_division` + global `overture_place`
build on the 72 GB machine. Record, and add to `docs/` as the new
baseline:

1. **Covering size and shape**: total rows, rows per kind, per-level
   interior counts, p50/p99/max covering rows per boundary. Sanity bound:
   total covering rows should land in the 10⁷–10⁸ range; an order of
   magnitude above that means `COVER_MAX_ZOOM` needs revisiting.
2. **Covering correctness sample**: 10,000 random places; assert the
   §3.7 join result equals direct
   `ST_Contains(boundary, point)` over all boundaries whose bbox contains
   the point (brute force via R-tree). Zero mismatches allowed except
   documented SPATIAL-7 edge cases.
3. **D6 audit**: max `list()` group size in containment (max boundaries
   containing one place) and max per-tile record payload at export.
   Assert `duckdb_memory()` / peak RSS stayed under limit per stage.
4. **Manifest size**: gzipped `manifest.json` bytes per source. Decide
   §7.2's shard question from this number.
5. **Wall time per stage vs Phase 0 baseline**, and temp-directory
   high-water mark (`duckdb_temporary_files()` sampled per stage).
   Expectation to confirm or refute: containment total time drops by an
   order of magnitude; import time roughly flat; covering build is a
   division-release-cadence cost, not a monthly one.

## 11. Open questions

- **`COVER_MAX_ZOOM = 12`** is chosen by area/perimeter reasoning, not
  measurement. If validation item 1 shows bloated edge sets for small
  localities (whose entire covering may be edge tiles at z12), consider
  emitting edges at the boundary's "natural" zoom instead of forcing
  descent to 12 — a small algorithm change confined to §3.5 step 3.
- **Manifest sharding** (§7.2) — decide from validation item 4.
- **Static `getRecord`** (§7.3) — deferred.
- **Foursquare source disposition**: confirmed stale, not speculative —
  atgeo.org notes FSQ OS Places hasn't been updated since late 2025, and
  an S3 listing of `fsq-os-places-us-east-1` on 2026-07-02 returned only
  LICENSE and NOTICE objects, so `download-fsq.sh` auto-discovery is
  presumed broken. Decision needed (human): pin the last release from a
  mirror/local cache and mark the collection's `generated_at`
  accordingly, or drop the source. The pipeline design is indifferent;
  the conflation/licensing doc should treat FSQ as frozen data either
  way.
