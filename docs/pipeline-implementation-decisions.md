---
category: Design
tags: [garganorn, pipeline, duckdb, quadtree, containment, covering, envelope]
last_updated: 2026-08-03
status: reference — condensed decision record for the shipped tile pipeline
---

# Pipeline Implementation Decisions

Why the shipped tile pipeline is built the way it is. This condenses the
design records for the pipeline restructure (Phases 1, 2, 2b), the
serving-path migration (OQ-P2-5), and the boundary-containment performance
work that predates them. It records **decisions and their reasoning**, not
the development history. For DuckDB quirks, R-tree activation rules, and
quadkey conventions referenced throughout, see `docs/design-constraints.md`.

The pipeline produces, per source, a chain of mtime-tracked parquet/DB
artifacts (`places.parquet` → `tile_assignments.parquet` → `containment/`
→ `tiles/`), driven by an orchestrator with per-stage freshness. Sources:
`foursquare`, `overture_place`, `osm`, `overture_division` (the last also
produces the boundary data the others' containment reads).

## Cross-cutting decisions

- **Backwards compatibility is a non-goal** (operator decision, 2026-07-07).
  The atgeo lexicons were never in production use and the API was only
  published in beta. This unblocks format changes (envelope, level
  vocabulary) that would otherwise need consumer migration. The only
  compatibility that matters is the server's own coupling to the tiles it
  reads (see "OQ-P2-1 — record envelope adoption" below).
- **DuckDB version pin: 1.5.1** (dev `.venv`); constructs verified stable
  across [1.2.1, 1.5.1] and the production box runs 1.4.4. All SQL must run
  on both 1.2.1 (`venv/bin/pytest`) and 1.5.1. Consequences baked into the
  design: no `KV_METADATA` in COPY (1.4+ only — the reason metadata lives in
  JSON sidecars, not parquet), and `ATTACH 'p' AS x (READ_ONLY)` is the only
  attach form that parses (the `... READ_ONLY AS` order parses on neither).
- **One caching mechanism: mtime + params.** Every stage skips iff its
  artifact is fresh. Freshness = artifact exists, meta sidecar exists and
  parses, `meta['params']` matches exactly, meta is strictly newer than
  every resolved input, and `mtime(artifact) <= mtime(meta)` (so a crash
  between artifact rename and meta write reads as stale). Editing a stage's
  SQL text does **not** invalidate its artifact — SQL is neither an input
  nor hashed; `--force` is the remedy after SQL edits.
- **`--force` invalidates by deleting outputs, nothing else** (spec §4).
  The `force=True` kwarg (tests only) bypasses the gate without deleting.

## Background: boundary-containment performance

The containment problem is point-in-polygon of ~10⁸ places against ~10⁶
boundaries carrying up to ~900K vertices each. Two insights, established
2026-04-10 and carried through every later design:

- **Filtering alone is insufficient.** An R-tree `ST_Intersects` prefilter
  cuts ~1M boundaries to ~2K for a tile in ~13ms, but the survivors still
  carry their full vertex counts — a 900K-vertex boundary costs 900K vertex
  traversals per `ST_Contains`, even where only a sliver overlaps the tile.
- **Clip to the tile envelope.** `ST_Intersection(geom, tile_env)` trims a
  country-spanning boundary to the fragment overlapping the tile (900K
  vertices → hundreds). This is correctness-preserving for point-in-tile
  queries: since every candidate place lies inside the tile, `ST_Contains(
  clipped, point) ⟺ ST_Contains(full, point)`. This equivalence is the
  foundation of the covering algorithm's per-level clipping (Phase 1).

(R-tree indexes activate only in top-level WHERE clauses, never inside
CTEs/subqueries/JOIN conditions — see `docs/design-constraints.md`. This is
why the covering and containment join paths avoid the R-tree entirely and
work by bbox arithmetic + quadkey prefix instead.)

## Phase 1 — covering + containment rewrite (merged `5328e4e`)

Replaces the old per-tile recursive containment (3 SQL statements per leaf
tile across thousands of tiles, one ATTACH per tile) with a two-artifact
approach: a precomputed **covering** of each boundary by quadtree tiles,
then a **containment join** of places against that covering by quadkey
prefix.

### Covering: quadtree decomposition of boundaries

- **Algorithm: level-by-level set-based descent**, z4 → z12
  (`COVER_MIN_ZOOM=4`, `COVER_MAX_ZOOM=12`), not per-tile recursion. Seed
  256 z4 tiles, then at each level flag tiles whose envelope the (clipped)
  boundary fully contains as `interior` and emit them; expand the rest ×4
  into child tiles, re-clipping geometry to each child envelope; repeat. At
  z12 the survivors become `edge` tiles.
- **Interior emitted at every zoom in [4,12]; edge only at z12.** This
  resolved a spec ambiguity (OQ-1): running the interior test at z12 too is
  cheaper downstream (an interior tile misclassified as edge only costs a
  per-point `ST_Contains` at containment time) and matches the schema
  intent.
- **Clip to the tile's own envelope at expansion, not the parent's.** One
  level tighter than the spec wording; valid by the clip-equivalence above.
  Degenerate clips (`NOT ST_IsValid OR ST_Area = 0`) are dropped — a
  boundary that merely touches a tile edge can contain no point.
- **`ST_Contains` materialized once per (boundary, tile) per level** via an
  `is_interior` flag column — it is the dominant cost and must not be
  evaluated twice.
- **Output: `covering/<qk4>.parquet`**, columns `(tile_qk, boundary_id,
  level, kind)`, sorted `(tile_qk, boundary_id)`, one file per z4 prefix,
  `_meta.json` written last. Per-file `COPY ... ORDER BY` (not
  `PARTITION_BY`, which does not guarantee per-partition sort order).
- **No R-tree, no pyarrow.** Seeding joins boundaries to a 256-row z4 tile
  table by bbox arithmetic (spec said "Python seeds via Arrow"; pyarrow is
  not a dependency — same seed set, OQ-5).
- **`qk_env` SQL macro** ports `quadkey_to_bbox`. The single likeliest
  transcription error is the tile-y / y+1 asymmetry (tile y increases
  southward, so north and south envelope edges use different `t`); a
  10,000-quadkey agreement test guards it.

### Containment: prefix-join against the covering

- **Two arms, UNION ALL.** *Interior arm*: a place whose qk17 prefix equals
  an interior covering tile is inside that tile, hence inside the boundary —
  one equi-join arm per level L in 4..12 on `left(qk17, L) = tile_qk`, no
  geometry test. *Edge arm*: for z12 edge tiles, join to `bnd.places`
  geometry and run the full-geometry `ST_Contains(boundary, point)`, gated
  by a cheap bbox prefilter.
- **Edge arm tests the full boundary geometry, not the tile-clipped one.**
  This is strictly *more correct* than the old per-tile code (a point on an
  old leaf-tile clip edge failed `ST_Contains` even when truly inside);
  candidate narrowing to the z12 edge band never changes which pairs pass
  the final test. Watch item: full geometries are re-processed per candidate
  point (DuckDB does not cache prepared geometries across join rows); a
  clip-to-z12 contingency exists but is parity-affecting and not adopted
  speculatively.
- **No DISTINCT needed.** Interior and edge tile sets are disjoint by
  construction (interior tiles are removed before expansion), so a place
  matches each boundary at most once.
- **Antimeridian (D7).** A boundary bbox with `min_lon > max_lon` is treated
  as two lobes; both seed SQL and edge-arm bbox prefilter branch on it.
  Latent until division import encodes lobed bboxes, but tested with
  synthetic rows.
- **Output: `containment/<qk4>.parquet`**, `(tile_qk, place_id,
  relations_json)`. `relations_json` is `{"within":[{"rkey":...}]}` ordered
  by `level ASC`.
- **Graceful degradation (Q3):** no `boundaries_db`, or empty/absent
  covering → empty containment, export still runs (places with no
  relations).

### Correctness basis

The old per-tile code never worked on the production box, so there was no
valid baseline to capture (OQ-8, resolved/moot). Correctness rests entirely
on an **in-suite brute-force oracle**: for fixture boundaries and sampled
points, the covering-based result must equal direct `ST_Contains(boundary,
point)` over all boundaries. This oracle is independent of the deleted code
and survives it.

## Phase 2 — parquet artifacts + orchestrator (merged `69767f6`)

Eliminates the per-run working DuckDB entirely. Each stage becomes a
self-gating producer of a stable, mtime-tracked artifact.

- **No more working DB.** The old `.<src>_work.duckdb` (FSQ baseline ~49GB,
  holding places + assignments + containment + a progress table) is gone.
  Each stage opens its own ephemeral in-memory `duckdb.connect()`, reads
  inputs via `read_parquet`, writes one artifact. No `.duckdb` is opened
  read-write by more than one stage; `boundaries.duckdb` and
  `manifest.duckdb` are written once and attached READ_ONLY thereafter.
- **Artifacts and their sorts** (the sort is a load-bearing invariant, D2 —
  it turns downstream prefix filters into zonemap range scans):
  `shared/density_tiles.parquet` (by tile_qk15), `<src>/idf.parquet` (by
  category), `<src>/places.parquet` (qk17-sorted, NULLS LAST),
  `<src>/tile_assignments.parquet` (`(tile_qk, place_id)`-sorted),
  `<src>/containment/`, `<src>/tiles/<timestamp>/`.
- **`place_id` added as tile_assignments secondary sort key.** Makes the
  assignment artifact — and through export's ordering, the tile files —
  deterministic run-to-run. This is a new invariant Phase 1 lacked.
- **Geometry column dropped from `places.parquet`** (`EXCLUDE geometry`/
  `geom`). Verified nothing downstream reads it: export builds points from
  bbox midpoints (overture) or scalar lat/lon (fsq/osm), tile assignment
  uses only qk17. For OSM the `DELETE ... WHERE geom IS NULL` filter runs
  *before* the EXCLUDE, so dropping the column doesn't change the row set.
- **Rows with NULL/invalid qk17 are kept** (sorted last) and filtered at
  tile assignment exactly as before, preserving the dropped-count warning.
- **Freshness via meta sidecars + resolved-input recording.** Generalizes
  Phase 1's `_meta.json`. `params` records only what each stage actually
  consumes (e.g. `pop_norm` only on the division import row, so a
  `pop_norm` change doesn't spuriously invalidate the three place sources).
  Recording resolved `inputs` closes the glob hole: a glob that now matches
  different/older files compares unequal and forces a rebuild.
- **Division import merged into one stage.** `stage_division_import`
  produces both `places.parquet` and `boundaries.duckdb` from a single
  `division_all` CTAS; the places meta is written last and gates both
  (its freshness additionally requires `boundaries.duckdb` to exist and be
  no newer). `boundaries.duckdb` **keeps `admin_level` in Phase 2** — the
  level-vocabulary swap was deferred to 2b so the byte-comparability
  acceptance could verify Phase 2 as a pure no-op.
- **Containment relocated to `<src>/containment/`** (resolves Phase 1 OQ-3).
  Phase 1 put it under the timestamped run dir because its input was the
  per-run `places` table; now `places.parquet` and
  `tile_assignments.parquet` are stable inputs, so it moves to the source
  root with the covering dir-swap atomicity pattern (retiring the Phase 1
  per-file rename, which could leave a mixed old/new file set after a crash).
- **Tiles move under `<src>/tiles/<timestamp>/`**, `tiles/current` symlink.
  Changes serving paths (OQ-P2-5).
- **Crash recovery is three patterns, no sentinel.** The sentinel table,
  resume branches, and corrupted-DB probe are deleted; recovery is always
  "next run finds the stage stale, rebuilds from its start." (1) Single
  file: build at `.tmp`, fsync, `os.replace`, then meta sidecar; stage start
  deletes stale `.tmp` (and, for `boundaries.duckdb`, a stale `.tmp.wal` —
  else ATTACH replays a WAL against a fresh empty DB). (2) Directory
  (covering, containment): `.tmp`/`.old`/`.spill` clobber + swap,
  `_meta.json` last. (3) Timestamped run dir (tiles): complete iff
  `manifest.json` exists (written last); incomplete non-`current` dirs
  deleted at export start.
- **CLI: five subcommands** (`density`, `idf`, `covering`, `run`, `all`),
  no default — bare legacy invocation errors pointing at `run`. Per-stage
  subcommands beyond these are deliberately omitted: per-artifact freshness
  makes "rerun one stage" a no-op-plus-one invocation. `all` runs density →
  idf per source → division → remaining sources, driven by a new `pipeline:`
  config section (the `tiles:` section becomes serving-side only).

## Phase 2b — record envelope + level vocabulary (merged `af3c9dd`)

Two orthogonal format changes shipped together. Both break byte-parity with
Phase 2 by design (envelope shape changes; `level` values replace
`admin_level`), so the byte-parity acceptance is retired and replaced by a
fixture checklist. Orthogonality: OQ-P2-2 changes the division record's
`attributes.level` *value*; OQ-P2-1 changes everything *around* the value.

### OQ-P2-2 — containment level vocabulary

- **Key on Overture `subtype`, drop `admin_level`.** `admin_level` is NULL
  for ~96% of features (only country/county/region/dependency carry it) and
  is semantically inconsistent across countries (OSM-inherited). Ordering
  containment by it is undefined for the majority of boundaries — a
  NULL-`admin_level` locality wrongly sorts last. `subtype` is a complete,
  closed 9-value set (0 NULLs measured). The prior server ordering was only
  *accidentally* correct because the boundary filter excluded everything
  below locality, coincidentally putting localities last; the vocabulary
  makes ordering correct *by design*.
- **`LEVEL_VOCAB` mapping** (single Python constant, single source of truth,
  renders the import CASE and drives the validator so they can't drift):
  country 10, dependency 15, region 25, county 35, localadmin 45, locality
  50, borough 55, macrohood 60, neighborhood 65, microhood 70.
- **Hoods renumbered on uniform stride-5** — a protocol amendment to
  atgeo-spec.md's Containment levels section. macrohood (60) and microhood (70) were absent from the
  normative table and had to be added; rather than wedge macrohood into the
  narrow 55–60 gap (breaking the stride, leaving no insertion room), the hoods were
  renumbered, which **moves normative neighborhood from 60 to 65**. Chosen
  over the minimal-change alternative (macrohood at 57) because a clean
  stride is easier to explain and preserves insertion room; no consumer
  depends on the old value. Placement follows WoF descent (macrohood
  contains neighborhoods → sorts above; microhood sits inside → below).
  Obligates an atgeo.org Lexicon page update.
- **County collapses to a single level 35; `admin_level` not preserved as a
  tiebreak.** A spot check found zero cross-`admin_level` county nesting
  (no al=1⊃al=2), so nothing orderable is lost; the 499 same-level enclave
  nestings weren't orderable by admin_level before either. Raw `subtype`
  stays in `boundaries.duckdb` and tile attributes for anyone needing the
  distinction.
- **Boundary filter becomes `WHERE level <= 50`** (country..locality),
  expressed via the constant so it can't drift. Two deliberate output
  deltas, both signed off: **+200 counties** (admin_level=3 ones the old
  filter arbitrarily excluded) and **+21,380 localadmin** (a genuine
  municipal tier at level 45, previously excluded only as a NULL-admin_level
  side effect). Boundary count ≈616.1k vs the old ≈594.6k. Hoods stay
  excluded (a future one-constant change if neighborhoods should participate).
- **Fail-loud on unmapped subtypes.** Before any artifact write,
  `stage_division_import` raises listing all unknown subtypes at once; the
  import CASE has no ELSE, and a post-CTAS `count WHERE level IS NULL = 0`
  assert is belt-and-braces. Never default or guess a level.
- **Containment tie-break: `ORDER BY level ASC, boundary_id ASC`** (the
  `id ASC` server variant was dropped in the merge). `level` is total by
  construction, so no NULLS-last handling anywhere.
- **Tile and `boundaries.duckdb` `level` agree by construction** — the
  mapping is applied exactly once in the `division_all` CTAS, from which
  both are cut and covering/containment copy the column.

### OQ-P2-1 — record envelope adoption

- **Tile payload gains the atgeo v1 envelope:** `{atgeo:1, collection,
  attribution, generated_at, records:[{uri, cid, value}]}`. `value` is
  byte-for-byte the previous record JSON (value schemas are the existing
  lexicons, unchanged).
- **`cid` is literally `null`, never computed** (approved). A real CID would
  require re-encoding every DuckDB-emitted record as canonical DAG-CBOR
  (~10⁸ times/run) in the flush threads, and the atproto data model bans
  floats — raw Overture `sources` structs pass floats through, which would
  turn a caching nicety into an import-data audit. Most decisively, the hash
  verifies nothing without a signed commit chaining it to a DID: a client
  recomputing it learns only that the producer hashed what TLS already
  delivered. Kept as explicit `null` (not omitted) so every record has
  exactly three keys regardless of producer; the byte cost vanishes under
  gzip. Rejected alternative: dropping per-record `uri` for gazetteer tiles
  — it would fork the envelope into gazetteer/AppView variants, the exact
  thing the unified contract prevents.
- **`uri` = `https://{repo}/{collection}/{rkey}`**, using the post-transform
  rkey (OSM `node:/way:/relation:` form). This is exactly what `getRecord`
  returns, so tile and XRPC URIs agree by construction. Not `at://` —
  gazetteer records are not repository data (no MST, no signed commit) and
  must not mint URIs they can't verify. `REPO` stays hardcoded
  (`places.atgeo.org`); becomes config the day a second deployment exists.
- **`generated_at` is one run-scoped value**, derived from the run-dir
  timestamp, identical across every tile and both manifests. A naive
  per-flush `now()` would make two exports of identical inputs byte-different
  in every file, destroying the determinism the parity harness relies on.
  Normalized to RFC 3339 `Z`, seconds precision. Never per-record. Protocol
  amendment P1 pins this so a second producer can't stamp per-flush times.
- **`cache.immutable: false`** (approved), deliberately contradicting atgeo
  §1.3's `true`. The deployed slug route serves via the `current` symlink,
  so the *same* URL returns new bytes after a run; `immutable: true` would
  let CDNs serve stale tiles for the full max-age. Immutable is deferred to
  a Phase 3 serving path that embeds a run-unique URL segment (protocol
  amendment P2). `tile_url_template` and manifest `attribution` (amendment
  P3) ship now.
- **Envelope wrapping lives in Python, not SQL** (new `garganorn/
  envelope.py`, `ATGEO_VERSION=1`). Centralizes the version constant, run
  timestamp, URI construction, and the (never-taken) CID seam in one place
  the AppView can share; the four export SQLs only expose `rkey` as a
  column. `wrap_record` composes the wrapper by string interpolation (no
  `json.loads` per record), which also stops `ensure_ascii`-escaping DuckDB's
  UTF-8 — JSON-equivalent, byte-different, acceptable since byte-parity is
  already retired.
- **Server ships atomically with the pipeline.** New server + old tiles →
  `KeyError: 'value'`. Covered by a temporary `record.get("value", record)`
  tolerance in `tile_reader.py` (removed after the first re-export) plus
  deploy-code-then-re-export ordering. This is internal correctness, not
  external compatibility.

## OQ-P2-5 — serving-path migration (merged `fd0ff8d`)

- **Collection-slug-aware tile route** `/tiles/<slug>/<path>`, replacing the
  global `serve_dir` route. Chosen to fix two problems with just repointing
  paths: (1) **over-exposure** — a `serve_dir` rooted at `<src>/` makes
  `GET .../places.parquet` and `containment/` resolve on disk (`safe_join`
  blocks `..`, not siblings); (2) **ugly URL coupling** — a shared root
  forces public URLs to mirror disk, producing a doubled `tiles/.../tiles/`.
  The slug route resolves a public kebab slug → that collection's own
  `tiles_dir`, so the route can only reach inside a registered tile dir
  (closing the parquet exposure) and the snake_case disk names never appear
  in a URL.
- **Three deliberately distinct namespaces:** NSID dotted (config key), disk
  `source_key` snake_case (private, never in a URL — which is why the
  internal `tiles/current` doubling is acceptable), public slug kebab.
- **The Phase 2 on-disk layout is unchanged** (it was byte-validated by the
  Phase 2 acceptance; not touched).
- **Fail fast if `base_url` doesn't end with `/<slug>`** — otherwise
  `getCoverage` emits URLs no route can serve. Serving is gated on the same
  manifest-exists condition as the rest of the collection config, so dev
  checkouts (no manifests) stay disabled via the existing warn-and-skip path.
- **`Cache-Control: public, max-age=<cache_ttl>`, not `immutable`** (same
  reasoning as the manifest `cache` object above). `cache_ttl` was
  previously dead config.
- WoF is **not** involved: place-tile containment uses `overture_division`
  boundaries built in-run, not WoF. WoF is only the server's separate,
  deprecated reverse-geocode lookup.
- **Ansible `tiles:` wiring and the `config.yaml.j2` `type: overture` bug**
  (should be `overture_place`) were deferred at merge time until prod
  actually served tiles; both shipped as the OQ-P2-5 infra follow-up
  (`atgeo-server-config` `380cbb3`) with the 2026-08-03 production deploy —
  see `pipeline-status.md`. Disk source dirs stay snake_case (renaming
  reopens the accepted on-disk contract for a client-invisible gain).

## Phase 2 acceptance (recorded)

Both conditions passed on-box 2026-07-09 (Overture release `2026-06-17.0`,
box DuckDB 1.4.4), per the Phase 2 acceptance criteria in
`pipeline-restructure-design.md` §8: byte-comparability — zero diffs on both
`overture_division` and `overture_place` (with containment); `kill -9`
mid-import — the killed run left no partial/fresh-looking artifact, and an
unmodified rerun (no cleanup, no `--force`) rebuilt from import and matched a
never-killed control. Caveat: idf/density were empty on both sides, so
idf/density-driven ranking was not exercised by that run (the scoring SQL was
byte-identical between the compared commits, so the parity verdict holds).
Phase 2b's byte-parity acceptance was retired; it is verified instead by
fixtures asserting the new envelope shape, `attributes.level` values,
`within` ordering, no-NULL-levels, cross-artifact level agreement, and the
±boundary-count delta.
