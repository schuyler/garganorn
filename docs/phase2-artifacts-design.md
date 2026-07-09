---
category: Design
tags: [garganorn, duckdb, pipeline, quadtree, parquet, orchestration]
last_updated: 2026-07-07
status: draft — implementation design for Phase 2 of pipeline-restructure-design.md
---

# Parquet Artifacts + Orchestrator: Phase 2 Implementation Design

Implements §3.3 (import artifact), §3.4 (division import, structural half —
see OQ-P2-2), §3.6 (tile_assignment artifact), §3.8 (export from artifacts),
§4 (orchestrator subcommands), and §6/§9 (sentinel/resume deletion) of
`docs/pipeline-restructure-design.md`. Base branch `feat/pipeline-phase2` at
`5328e4e` (squash-merged Phase 1: covering stage + covering-based
containment, per `docs/covering-containment-design.md`).

Out of scope: server removals (§7.1/§7.2, Phase 3), the §3.8 envelope
adoption and the §3.4 level-vocabulary mapping (both deferred to
"Phase 2b", an immediate follow-on change set after Phase 2 merges —
§10, OQ-P2-1/OQ-P2-2), global validation (Phase 4).

Backwards compatibility is a non-goal (operator decision, 2026-07-07):
the ATGeo lexicons are not in use and the API was only ever published in
beta form. The Phase 2b deferrals exist so the byte-comparability
acceptance can verify this refactor as a no-op — not to protect
consumers.

Acceptance (spec §8, as amended 2026-07-06): tile JSON byte-comparable
(modulo `generated_at` and within-tile record order, §9.1) against Phase 1
output on the SF test bbox; `kill -9` mid-import followed by rerun produces
a correct, fresh result (§9.2).

## 1. Spec-to-code mapping

| Spec requirement | Code change |
|---|---|
| §3.1/§3.2 density, idf: tmp+rename + shared freshness | MODIFY `stage_density_extract()`, `stage_idf()` — adopt §2.2 helpers |
| §3.3 import → `<src>/places.parquet`, qk17-sorted | REWRITE `stage_import()`; MODIFY all four `*_import.sql` (fsq/overture: CTAS → `COPY ... ORDER BY qk17`; osm: TEMP-table build + trailing COPY — §3.2) |
| §3.4 division import → `places.parquet` + `boundaries.duckdb`, one stage | NEW `stage_division_import()` absorbing `export_boundaries_db()`; DELETE `stage_boundary_export()` |
| §3.6 tile_assignment → `<src>/tile_assignments.parquet` | REWRITE `stage_tile_assignment()`; MODIFY `compute_tile_assignments.sql` |
| §3.7 OQ-3: containment relocates to `<src>/containment/` | MODIFY `compute_containment()` — parquet inputs, ephemeral connection, dir-swap atomicity (resolves Phase 1 OQ-3) |
| §3.8 export reads parquet artifacts | MODIFY `stage_export()`, `export_tiles()`, `write_manifest_db()`, all `*_export_tiles.sql` FROM clauses |
| §4 subcommands | REWRITE `main()` in `quadtree.py`: `density`, `idf`, `covering`, `run`, `all` (resolves Phase 1 OQ-4) |
| §6/§9 deletions | DELETE sentinel/resume machinery in `quadtree.py`; DELETE/port `tests/test_checkpoint.py` (§6, §7.9) |
| Acceptance harnesses | NEW `scripts/tile_parity.py`, NEW `tests/crash_harness.py` + `tests/test_crash_recovery.py` |

### Files touched

- MODIFY `garganorn/quadtree.py` (run_pipeline rewrite, subcommand CLI,
  deletions), `garganorn/stages.py` (all stage signatures, helpers),
  `garganorn/covering.py` (none expected; consumed as-is)
- MODIFY `garganorn/sql/{foursquare,overture_place,osm,overture_division}_import.sql`,
  `compute_tile_assignments.sql`, `*_export_tiles.sql`,
  `compute_containment.sql` (FROM clause only)
- NEW `scripts/tile_parity.py`, `tests/crash_harness.py`
- NEW `tests/test_artifacts.py`, `tests/test_crash_recovery.py`,
  `tests/test_cli_subcommands.py`
- MODIFY/DELETE tests per §7.9
- UPDATE `docs/pipeline-restructure-design.md` (mark Phase 2 landed),
  `docs/design-constraints.md` (P-constraints describing working DB)

## 2. Artifact graph delta

### 2.1 Layout: before → after

Before (current, per-run working DB):

```
<output>/<src>/<timestamp>/.<src>_work.duckdb   places, tile_assignments,
                                                place_containment, _pipeline_progress
<output>/<src>/<timestamp>/containment/*.parquet  (Phase 1, OQ-3 interim)
<output>/<src>/<timestamp>/**/qk.json.gz, manifest.json, manifest.duckdb
<output>/<src>/current -> <timestamp>
<output>/overture_division/boundaries.duckdb
<output>/overture_division/covering/*.parquet, _meta.json
```

After (spec §2):

```
<output>/shared/density_tiles.parquet  (+ .meta.json)
<output>/<src>/idf.parquet             (+ .meta.json)         [fsq, overture_place, osm]
<output>/<src>/places.parquet          (+ .meta.json)         qk17-sorted
<output>/<src>/tile_assignments.parquet (+ .meta.json)        (tile_qk, place_id)-sorted
<output>/<src>/containment/<qk4>.parquet, _meta.json          (tile_qk, place_id)-sorted
<output>/<src>/tiles/<timestamp>/<qk6>/<qk>.json.gz
<output>/<src>/tiles/<timestamp>/manifest.json, manifest.duckdb
<output>/<src>/tiles/current -> <timestamp>
<output>/overture_division/boundaries.duckdb                  (schema unchanged, §3.3)
<output>/overture_division/covering/                          (unchanged, Phase 1)
```

Notes:

- **Tiles move under `<src>/tiles/`** per spec §2. This changes the serving
  paths in `config.yaml` (`manifest`, `tiles_dir`, `base_url` targets) and
  the Ansible deploy. One-time production migration: old
  `<src>/<timestamp>/` dirs are inert and can be deleted manually; the
  pipeline never scans them again (the timestamp scan moves to
  `<src>/tiles/`). Flagged OQ-P2-5 for operator sign-off.
- The working DB (`.<src>_work.duckdb`) no longer exists in any form. No
  `.duckdb` file is opened read-write by more than one stage;
  `boundaries.duckdb` and `manifest.duckdb` are written once and attached
  `(READ_ONLY)` thereafter (both DuckDB 1.2.1 and 1.5.1 accept
  `ATTACH 'p' AS x (READ_ONLY)`; the `ATTACH ... READ_ONLY AS` order parses
  on neither — carried Phase 1 lesson).
- Density path becomes `<output>/shared/density_tiles.parquet` when driven
  by `all`; the `density`/`run` subcommands keep explicit `--parquet` /
  `--density-parquet` flags, so nothing hardcodes the location.

### 2.2 Freshness and the meta sidecar (shared helpers)

`_is_output_fresh()` (`stages.py:41`) is necessary but not sufficient:
several stages take non-file parameters (bbox, norm constants,
max_per_tile) whose change must invalidate output even when mtimes are
fresh. The covering stage already solved this with `_meta.json`
(`covering.py:134-149`); Phase 2 generalizes it. Two helpers in
`stages.py`:

```python
def artifact_fresh(artifact: str, inputs: list[str], params: dict) -> bool:
    """True iff:
      - artifact exists, and
      - meta (= artifact + '.meta.json' for files, artifact/'_meta.json'
        for directories) exists and parses, and
      - meta['params'] == params (exact dict equality), and
      - _is_output_fresh(meta_path, inputs)  (meta strictly newer than
        every input; inputs all exist), and
      - mtime(artifact) <= mtime(meta)   (meta written after artifact —
        a crash between artifact rename and meta write reads as stale).
    """

def finalize_artifact(tmp_path: str, artifact: str,
                      params: dict, stats: dict | None = None) -> None:
    """fsync(tmp_path); os.replace(tmp_path, artifact); write
    artifact+'.meta.json' via its own tmp+os.replace; fsync the containing
    directory. meta = {stage, params, inputs, stats, generated_at}."""
```

Every single-file parquet artifact (density, idf, places,
tile_assignments) goes through `finalize_artifact`. Directory artifacts
(covering — already done; containment — §3.5) use the Phase 1
`.tmp`-dir/swap/`_meta.json`-last pattern instead, with `artifact_fresh`
pointed at `<dir>/_meta.json`. `params` per stage:

| Artifact | `params` recorded | file inputs |
|---|---|---|
| `shared/density_tiles.parquet` | `{}` (glob resolved into `inputs`) | source parquet files |
| `<src>/idf.parquet` | `{}` | source parquet files |
| `<src>/places.parquet` | `bbox, density_norm, idf_norm` | source parquet, density, idf |
| division `places.parquet` (gates both §3.3 artifacts) | `bbox, density_norm, pop_norm` | division + division_area parquet, density |
| `<src>/tile_assignments.parquet` | `max_per_tile, min_zoom, max_zoom` | places.parquet |
| `<src>/containment/_meta.json` | `collection_prefix, cover_min_zoom, cover_max_zoom` | places.parquet, tile_assignments.parquet, covering/_meta.json, boundaries.duckdb |
| `covering/_meta.json` | (unchanged, Phase 1) | boundaries.duckdb |
| `tiles/current/manifest.json` (freshness anchor for export; no sidecar — `generated_at` inside it serves as build record) | — | places.parquet, tile_assignments.parquet, containment/_meta.json |

Each row records only the params its stage actually consumes:
`stage_import` passes `pop_norm` to the division import alone
(`stages.py:490-520`; the fsq/overture_place/osm import SQL substitutes
only `${density_norm}`/`${idf_norm}`), so `pop_norm` appears only in the
division row — a `pop_norm` change must not spuriously invalidate the
three place sources.

Recording resolved `inputs` in the meta also closes a latent hole in pure
mtime checking: a changed glob that now matches *different, older* files
compares unequal to `meta['inputs']` and forces a rebuild.
(`artifact_fresh` compares the resolved input path list against
`meta['inputs']` as part of the params check.)

### 2.3 Crash-semantics rule per artifact class

Three patterns, no others:

1. **Single file** (density, idf, places, tile_assignments,
   boundaries.duckdb, manifest.duckdb, manifest.json): build at
   `<path>.tmp`, then fsync → `os.replace`. The parquet artifacts
   (density, idf, places, tile_assignments) go through
   `finalize_artifact`, which adds the `.meta.json` sidecar after the
   rename; boundaries.duckdb, manifest.duckdb, and manifest.json get the
   fsync+rename but **no** meta sidecar (their freshness is anchored
   elsewhere: boundaries.duckdb by the division places meta, §3.3;
   the manifests by `manifest.json` itself, §2.2 table). A crash leaves
   the old artifact intact plus at most a stale `<path>.tmp` and/or a
   meta older than the artifact — both read as stale; stage start
   unconditionally deletes `<path>.tmp` before building. For a `.duckdb`
   artifact built at a `.tmp` path outside a disposable directory
   (boundaries.duckdb is the only one — manifest.duckdb lives inside a
   run dir that rule 3 deletes wholesale), the stage-start cleanup also
   deletes the `<path>.tmp.wal` sibling: a `kill -9` during the
   ATTACH-and-build leaves both files, and a fresh ATTACH at the same
   tmp path beside a stale `.wal` would attempt WAL replay against a
   new empty database and fail or corrupt.
2. **Directory** (covering, containment): Phase 1's `stage_covering`
   sequence verbatim (`covering.py:151-160, 305-312`): clobber stale
   `.tmp`/`.old`/`.spill` at build start; build under `.tmp`; `_meta.json`
   written last inside `.tmp`; rename dir aside to `.old`, rename `.tmp`
   in, remove `.old`. Crash windows all resolve to "stale, rebuilt from
   inputs" (analysis in the Phase 1 design §2.5 applies unchanged).
3. **Timestamped run dir** (tiles): a run dir is complete iff
   `manifest.json` exists (written last, §3.6). At export start, any
   `tiles/<timestamp>/` lacking `manifest.json` and not the `current`
   target is deleted (crash leftover — this replaces `_find_incomplete_run`
   resume with cleanup). `current` symlink swap keeps its existing
   tmp-symlink + rename.

This is the entirety of the crash story; the sentinel table, resume
branches, and corrupted-DB probe are deleted (§6). §5 walks `kill -9`
through every stage against these rules.

## 3. Stage specifications

Connection discipline for every stage (spec §2 rules): own ephemeral
`duckdb.connect()` (in-memory), then immediately
`SET temp_directory = '<resolved>'` (default: `<artifact>.spill` sibling,
Phase 1 convention, removed after finalize), `SET memory_limit`,
`SET preserve_insertion_order = false`, `LOAD spatial` only where needed
(import, containment). All SQL must run on **both** DuckDB 1.2.1
(`venv/bin/pytest`) and 1.5.1 (app `.venv`); no `KV_METADATA` in COPY (1.4+
only — reason the meta lives in JSON sidecars), no bare `READ_ONLY`
attach modifier before `AS`.

### 3.1 `density_extract` and `idf` — deltas only

Both already use ephemeral connections and `_is_output_fresh`. Changes:
route output through `.tmp` + `finalize_artifact`; `stage_density_extract`
gains `ORDER BY tile_qk15` on its COPY (spec §3.1 sort; today the
executemany insert order happens to be sorted — make it explicit);
`stage_idf` output sorted by `category` (spec §3.2). Signatures gain
`memory_limit="48GB", temp_directory=None` kwargs (today neither
function takes either — `stages.py:523, 592`; the §3 connection
discipline requires every stage to set both), otherwise unchanged.

### 3.2 `import` — `<src>/places.parquet`

```python
def stage_import(source, parquet_glob, bbox, output_path, *,
                 memory_limit="48GB", temp_directory=None,
                 density_parquet=None, idf_parquet=None,
                 density_norm=10.0, idf_norm=18.0, pop_norm=20.0,
                 force=False) -> None:
```

(`con` parameter dropped; dispatches to `stage_division_import` for
`overture_division`, §3.3.)

SQL diff, `foursquare_import.sql` and `overture_place_import.sql` (both
are a single `CREATE TABLE places AS <CTE chain>`):

- Delete `DROP TABLE IF EXISTS places;` and `CREATE TABLE places AS`.
- Keep the `SET memory_limit`, `INSTALL spatial; LOAD spatial`,
  `${density_cte}`/`${idf_cte}` temp-table preamble unchanged.
- The final CTE-select becomes:

```sql
COPY (
    <existing import CTE body, unchanged>
    ORDER BY qk17 NULLS LAST
) TO '${output_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD);
```

- The outer SELECT gains `EXCLUDE` for the geometry column (see
  **Schema** below).

SQL diff, `osm_import.sql` — **not** a single CTAS: it is
`CREATE TABLE places (...)` with an explicit schema
(`osm_import.sql:12-26`), two `INSERT INTO places` statements (nodes at
line 28, ways at line 156), and a trailing
`DELETE FROM places WHERE geom IS NULL` (line 317). The conversion keeps
that sequence as a TEMP table and appends a COPY:

- `CREATE TABLE places (...)` → `CREATE TEMP TABLE places (...)` (same
  explicit column list); drop the leading `DROP TABLE IF EXISTS places;`
  (a temp table on a fresh ephemeral connection cannot pre-exist).
- Both `INSERT INTO places` statements and the
  `DELETE FROM places WHERE geom IS NULL` run **unchanged** — the
  NULL-geom filter executes against the temp table before the export
  below, so its semantics (dropping way centroids/nodes that produced no
  point) are preserved exactly.
- Delete `CREATE INDEX idx_rkey ON places(rkey)` (line 319): the
  artifact is parquet; nothing indexes the temp table before the single
  full-scan COPY.
- Append:

```sql
COPY (
    SELECT * EXCLUDE (geom) FROM places
    ORDER BY qk17 NULLS LAST
) TO '${output_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD);
```

Both variants add `SET preserve_insertion_order = false;` to the
preamble (safe: order is established by the explicit ORDER BY; required
so the sort can use all threads without insertion-order bookkeeping).

**Schema**: the existing per-source `places` columns, minus the GEOMETRY
column each source carries today, dropped via `EXCLUDE`:
`geometry` for `overture_place` (`overture_place_import.sql:14`, spec
§3.3 rationale — export uses `bbox` midpoints), `geom` for `foursquare`
(`foursquare_import.sql:14-15`) and `osm` (`osm_import.sql:19/116/281`).
Verified that nothing downstream of import reads the column for any
source: the fsq/osm export SQL contains no `geom` reference, containment
builds points from `_coord_exprs` (`stages.py:76-87` —
`longitude`/`latitude` columns for fsq/osm, `bbox` midpoints for
overture), and `compute_tile_assignments.sql` uses only `qk17` and the
pk. (The `geom` reads in `garganorn/database.py` are against the
server's search database, a separate artifact outside this pipeline.)
Note the OSM ordering subtlety above: the `DELETE ... WHERE geom IS
NULL` filter runs before the `EXCLUDE`, so dropping the column does not
change which rows are kept. `foursquare` and `osm` keep scalar
`latitude`/`longitude`. Rows with NULL/invalid `qk17` are **kept**
(sorted last) — they are filtered at tile assignment exactly as today,
preserving the EXPORT-6 dropped-count warning.

**Sort**: `qk17 NULLS LAST` is the load-bearing invariant (D2):
tile_assignment's `left(qk17, L)` grouping, containment's per-qk4 prefix
scans, and export's tile-ordered merge all become zonemap range scans over
it. The import sort is the largest single operation in the pipeline (§8).

Freshness/atomicity: `artifact_fresh(places.parquet, [source files,
density, idf], {bbox, norms})`; build to `places.parquet.tmp`;
`finalize_artifact`.

### 3.3 `division_import` — `places.parquet` + `boundaries.duckdb`, one stage

Merges `overture_division_import.sql` and `export_boundaries_db()`
(spec §3.4, structural half). One ephemeral connection, sequence:

1. Preamble as today (`${density_cte}`, spatial, settings), then
   `CREATE TEMP TABLE division_all AS <existing import CTE, unchanged,
   geometry included>`. Spillable CTAS.
2. `COPY (SELECT * EXCLUDE (geometry) FROM division_all ORDER BY qk17
   NULLS LAST) TO '<places.parquet.tmp>' (FORMAT PARQUET, COMPRESSION
   ZSTD)`. Implementer note: verify against
   `overture_division_export_tiles.sql` that every column it reads (in
   particular the `bbox` struct feeding `_coord_exprs`) survives the
   EXCLUDE — the export-view test (§7.5) pins this.
3. Delete any stale `boundaries.duckdb.tmp` **and**
   `boundaries.duckdb.tmp.wal` (§2.3 rule 1 — a prior `kill -9` during
   this step leaves both, and ATTACH beside a stale `.wal` replays it
   against the new empty DB); then
   `ATTACH '<boundaries.duckdb.tmp>' AS bnd;` then the existing
   `export_boundaries_db` body verbatim: `CREATE TABLE bnd.places AS
   SELECT id, geometry, admin_level, names, subtype, country, region,
   wikidata, population, min/max lat/lon, importance, variants FROM
   division_all WHERE admin_level BETWEEN 0 AND 2 OR subtype = 'locality'
   ORDER BY ST_Hilbert(...)`; R-tree index; `DETACH bnd`.
4. `os.replace(boundaries.duckdb.tmp, boundaries.duckdb)` (fsync first).
5. `finalize_artifact(places.parquet.tmp, places.parquet, params)` — the
   places meta is written **last** and gates the whole stage; its
   freshness check additionally requires `boundaries.duckdb` to exist and
   be no newer than the meta.

Crash between 4 and 5: new `boundaries.duckdb`, stale/missing places meta
→ stage stale → rerun rebuilds both (idempotent; covering then sees a
newer `boundaries.duckdb` mtime and rebuilds too — consistent, not
wasteful in the crash-recovery case). The server holding the old
`boundaries.duckdb` open across the rename keeps its inode — same
behavior as today's `export_boundaries_db`.

**`boundaries.duckdb` schema is unchanged in Phase 2** — it keeps
`admin_level`; the spec §3.4 `level` (atgeo §1.7 vocabulary) replacement
is deferred to Phase 2b. Rationale (this re-defers Phase 1's OQ-2):

1. The mapping changes `within` ordering (a NULL-`admin_level` locality
   sorts last today; at atgeo level 50 it sorts before neighborhoods),
   which breaks the Phase 2 byte-comparability acceptance by design.
2. `garganorn/boundaries.py` (server lookup path) reads and orders by
   `admin_level` — the change set must include the server code. With no
   API consumers the deploy itself is trivial, but it belongs in its own
   reviewable change, not inside the plumbing refactor.
3. The spec requires `SELECT DISTINCT subtype` verification against a
   current division parquet and loud failure on unmapped subtypes; that is
   an operator-involved measurement (execution plan WS-2 note), not a
   design-time decision.

Phase 2b lands it together with the envelope (both are output-format
changes with their own small acceptance; not gated on WS-1; see
OQ-P2-1/2).

### 3.4 `tile_assignment` — `<src>/tile_assignments.parquet`

```python
def stage_tile_assignment(places_parquet, output_path, source, *,
                          max_per_tile=1000, min_zoom=6, max_zoom=17,
                          memory_limit="48GB", temp_directory=None,
                          force=False) -> dict:   # {total, assigned, dropped}
```

`compute_tile_assignments.sql` diff: `FROM places` →
`FROM read_parquet('${places_parquet}')` (three occurrences); the final
`CREATE TABLE tile_assignments AS` becomes

```sql
COPY (
    <existing assignment query>
    ORDER BY tile_qk, place_id
) TO '${output_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD);
```

The added `place_id` secondary sort key makes the artifact — and, through
§3.6's export ORDER BY, the tile files — deterministic run-to-run (new
invariant, §9.1). The EXPORT-6 dropped-place warning and EXPORT-7
duplicate check move into the Python driver as queries over the written
parquet (`read_parquet` count vs places count; GROUP BY place_id HAVING
count > 1). Freshness: `artifact_fresh(output, [places_parquet],
{max_per_tile, min_zoom, max_zoom})`.

### 3.5 `containment` — relocated to `<src>/containment/` (resolves OQ-3)

Phase 1 wrote containment under the timestamped run dir because its input
(`places` table) was per-run. `places.parquet` and
`tile_assignments.parquet` are now stable, mtime-tracked inputs, so the
artifact moves to `<src>/containment/` as the spec's final graph requires.
Delta from the Phase 1 implementation (`stages.py:89-261`):

```python
def compute_containment(places_parquet, tile_assignments_parquet,
                        boundaries_db, pk_expr, lon_expr, lat_expr,
                        containment_dir, *,
                        collection_prefix="org.atgeo.places.overture.division",
                        covering_dir=None, memory_limit="48GB",
                        temp_directory=None, force=False) -> None:
```

- **Own ephemeral connection** (the `con` parameter and every
  `place_containment` VIEW/TABLE concern move out — export composes its
  own FROM clause, §3.6). The DROP VIEW/TABLE dance and `_make_empty()`
  are deleted.
- **Inputs via `read_parquet`**: the `places_slim` temp CTAS is dropped;
  the per-prefix `p` CTE reads
  `read_parquet('${places_parquet}') WHERE left(qk17, 4) = '${prefix}'`
  plus the existing qk17-validity filter — a zonemap range scan, since
  places.parquet is qk17-sorted (this is spec §3.7's query verbatim). The
  prefix list comes from one
  `SELECT DISTINCT left(qk17, 4) ... WHERE <validity>` pass. The final
  join reads `read_parquet('${tile_assignments}')` instead of the table.
- **Directory atomicity upgraded to the covering pattern**: build all
  prefix files under `containment.tmp/`, write `_meta.json` last
  (params: collection_prefix + the covering zoom range actually used;
  inputs list), swap with `.old` handling, exactly `covering.py`'s
  sequence. The Phase 1 per-file `.tmp`+rename inside a live directory is
  retired — it could leave a mixed old/new file set after a crash.
- **Q3 degradation**: `boundaries_db is None`, covering absent/empty, or
  zero rows → the stage still writes `containment/` containing only
  `_meta.json` (with `"empty": true`). Export treats a containment dir
  with no `*.parquet` as "no relations" (§3.6). The empty artifact keeps
  freshness semantics uniform (a later-added boundaries_db makes it
  stale via the params/inputs change).
- Freshness: `artifact_fresh(containment/_meta.json, [places.parquet,
  tile_assignments.parquet, covering/_meta.json, boundaries.duckdb],
  params)`; when `boundaries_db is None` the input list is just the two
  parquets.
- Query text, interior-arm generation, D6/D7 analysis, and the
  brute-force-oracle correctness argument are unchanged from Phase 1
  (covering-containment design §3); only the plumbing changes.

`stage_containment` keeps its thin-wrapper role with the new signature.

### 3.6 `export` — tiles + manifests from artifacts

```python
def stage_export(source, places_parquet, tile_assignments_parquet,
                 containment_dir, tiles_root, *,
                 memory_limit="48GB", export_workers=None,
                 force=False) -> str:   # returns the new timestamped dir
```

Mechanics:

1. Freshness gate: `_is_output_fresh(tiles_root/current/manifest.json,
   [places_parquet, tile_assignments_parquet, containment_dir/_meta.json])`
   → skip. This replaces `run_pipeline`'s whole-pipeline gate with a
   per-stage one (upstream stages have their own).
2. Cleanup: delete any `tiles/<timestamp>/` lacking `manifest.json`
   (crash leftovers, §2.3 rule 3), excluding the `current` target.
3. Create `tiles/<timestamp>/`; open ephemeral in-memory connection; run
   the source's `*_export_tiles.sql` with new substitutions:
   `${places}` → `read_parquet('<places.parquet>')`, `${tile_assignments}`
   → `read_parquet('<tile_assignments.parquet>')`, `${containment}` →
   `read_parquet([<explicit file list>])` or, when the containment dir has
   no parquet files, `(SELECT NULL::VARCHAR AS place_id, NULL::VARCHAR AS
   relations_json WHERE 1=0)` (explicit list, not a glob — `read_parquet`
   on an empty glob errors; Phase 1 lesson, `stages.py:246-254`). The view
   body — record JSON construction, `relations` from `pc.relations_json`
   — is otherwise byte-identical to today. The view's final ordering
   becomes `ORDER BY ta.tile_qk, ta.place_id` (determinism, §9.1). All
   three inputs are presorted on tile order, so this is a merge, not a
   fresh global sort (spec §3.8); the export query contains no aggregation
   (D6 by construction).
4. `export_tiles()` streaming/ThreadPoolExecutor/backpressure body
   unchanged, but its preamble changes (consistent with §1's MODIFY
   listing): the view creation moves to the step-3 substitutions above,
   and the tile-count log query
   (`SELECT COUNT(DISTINCT tile_qk) FROM tile_assignments`,
   `stages.py:274-276`) becomes a `read_parquet` over
   `tile_assignments.parquet`. **Envelope: unchanged in Phase 2** — payload stays
   `{collection, attribution, records:[...]}`. The §3.8 amendment
   (`atgeo: 1`, `generated_at`, `{uri, cid, value}` wrapping) is deferred
   to Phase 2b so the byte-comparability acceptance can verify this
   refactor as a no-op. Not gated on WS-1: with no API consumers (see
   header note), a later WS-1 adjustment to the shape is a cheap
   follow-up edit, not a format-version event (OQ-P2-1).
5. Manifests, in completion order: `write_manifest_db()` reads
   `read_parquet('<tile_assignments.parquet>')` instead of the table
   (OSM rkey transform unchanged), still tmp+rename; **then**
   `write_manifest()` — which gains tmp+rename (today it writes in place)
   — lands `manifest.json` last as the run-dir completeness marker; then
   the `current` symlink swap (existing code); then keep-2 sweep over
   complete run dirs.

`write_manifest()`'s JSON shape is unchanged (no `tile_url_template` —
that is §7.2, Phase 3).

One documented leftover: a crash after `manifest.json` lands but before
the symlink swap leaves a *complete* run dir that `current` never points
at. Step 2's cleanup ignores it (it has a `manifest.json`), so it
persists until the next actual export's keep-2 sweep collects it —
harmless: it costs disk for one run's tiles, never serves, and never
affects freshness (the export gate keys on `current/manifest.json`, §5).

## 4. Orchestration

### 4.1 Subcommand grammar (spec §4)

`python -m garganorn.quadtree <subcommand>`, argparse subparsers, no
default subcommand — the old flag-only invocation errors with a message
naming `run`. Grammar:

```
density   --parquet GLOB --output PATH
          [--memory-limit S] [--temp-directory D] [--force]
idf       --source {foursquare,overture_place,osm}
          (--parquet GLOB | --parquet-dir DIR)   # dir form: osm only
          --output PATH
          [--memory-limit S] [--temp-directory D] [--force]
covering  --boundaries PATH [--output DIR]        # default: sibling covering/
          [--min-zoom N] [--max-zoom N]
          [--memory-limit S] [--temp-directory D] [--force]
run       --source {foursquare,overture_place,osm,overture_division}
          (--parquet GLOB | --parquet-dir DIR |
           --division-parquet P --division-area-parquet P)
          --output DIR [--bbox XMIN YMIN XMAX YMAX] [--config FILE]
          [--memory-limit S] [--max-per-tile N] [--boundaries PATH]
          [--export-workers N] [--density-parquet P] [--idf-parquet P]
          [--temp-directory D] [--force]
all       --config FILE [--force]
```

`run` keeps today's flag names and per-source validation (`main()`'s
existing checks move under the subparser), minimizing churn for the
operator's on-box invocations. The current IDF-mode branch of `main()`
(triggered by `--idf-parquet` doubling as an output path) is deleted;
`idf` is the only IDF entry point, and `run --idf-parquet` is
unambiguously an input again. Per-stage subcommands beyond these five are
deliberately not added: `run`'s per-artifact freshness makes "rerun just
one stage" a no-op-plus-one-stage invocation, and the spec's DAG-growth
argument (§4) cuts against surface area.

### 4.2 What `run_pipeline` becomes

Signature: unchanged plus `temp_directory=None` (tests call it
positionally today; keeping it stable contains test churn). Body, in
full:

```
source_dir = output_dir/source; tiles_root = source_dir/tiles
if force: delete force-set (§4.4)
stage_import(...)                                # self-gating, §3.2/§3.3
if source == overture_division:
    stage_covering(source_dir/boundaries.duckdb, source_dir/covering,
                   memory_limit=..., ...)        # self-gating (Phase 1)
covering_dir = ensure_covering(boundaries_db) if boundaries_db else None
stage_tile_assignment(...)
stage_containment(...)                           # §3.5
stage_export(...)                                # §3.6, incl. manifests + symlink
```

No connection is opened by the orchestrator; no sentinel is read or
written; an interrupted run reruns unfinished stages from their start
(spec §6 — the accepted trade). `_collect_input_files` is deleted; each
stage owns its input list.

`all --config` executes, in order: `density` (from the overture_place
parquet) → `idf` per configured place source → `run overture_division` →
`run` for each remaining configured source. Config gains a `pipeline`
section (the existing `tiles` section is untouched):

```yaml
pipeline:
  output: /data/tiles
  memory_limit: 48GB
  temp_directory: /scratch/duckdb_tmp
  max_per_tile: 1000
  bbox: null                    # or [xmin, ymin, xmax, ymax]
  sources:
    overture_division: {division_parquet: ..., division_area_parquet: ...}
    overture_place:    {parquet: ...}
    osm:               {parquet_dir: ...}
    foursquare:        {parquet: ...}   # omit to skip (FSQ frozen, spec §11)
```

Density/idf/boundaries paths are derived (`shared/density_tiles.parquet`,
`<src>/idf.parquet`, `overture_division/boundaries.duckdb`); absent
`overture_division` config means `boundaries_db=None` for all sources
(Q3 degradation end to end).

Both `run --config` and `all --config` read the `pipeline:` section
only. Today `main()` reads `tiles.memory_limit`, `tiles.max_per_tile`,
and `tiles.boundaries` (`quadtree.py:445-448`); those keys move under
`pipeline:` (backwards compatibility non-goal, header note). After this
change `tiles:` is serving-side config exclusively (`manifest`,
`tiles_dir`, `base_url`), never consulted by the CLI. CLI flags still
override config values, config overrides hardcoded defaults, as today.

### 4.3 Freshness rules (consolidated)

Stage skips iff its `artifact_fresh` gate (§2.2 table) passes. mtime
chains compose: a rebuilt `places.parquet` is newer than
`tile_assignments.meta.json`, which invalidates assignment, whose rebuild
invalidates containment, then export. There is exactly one caching
mechanism in the pipeline after this change.

Explicitly out of scope for freshness: **editing a stage's SQL file does
not invalidate its artifact.** Inputs are data files and params are
values (bbox, norms, zooms); the SQL text is neither an input nor
hashed. This matches the current pipeline (mtime-only over data inputs)
and the spec's mtime rule. After changing stage SQL during development,
`--force` (or `force=True`) is the remedy.

### 4.4 `--force` semantics

Spec §4: "`--force` invalidates by deleting outputs, nothing else." Exact
deletion sets, then a normal (self-gating) run follows:

- `density`/`idf`/`covering --force`: that artifact + its meta (covering:
  the dir).
- `run --force`: the source's `places.parquet(+meta)`,
  `tile_assignments.parquet(+meta)`, `containment/`; for
  `overture_division` additionally `boundaries.duckdb` and `covering/`.
  Never touches `tiles/` history (export always writes a new timestamped
  dir; deleting mtime-upstream artifacts already guarantees it runs).
- `all --force`: applies the above to every configured stage/source.

The `force=True` kwarg on stage functions (kept for tests) bypasses the
freshness gate without deleting anything — same net effect for
single-file artifacts (tmp+rename overwrites), and the dir-swap pattern
handles its own clobbering.

## 5. Crash semantics: `kill -9` walk-through

Every row must hold for the acceptance test (§9.2). "Recovery" is always:
next `run` finds the stage stale, rebuilds from its start; no state is
consulted other than artifacts + metas.

| Killed during | Disk state left | Why next run is correct |
|---|---|---|
| import COPY | partial `places.parquet.tmp`; old artifact+meta intact | gate compares meta vs inputs as before; stale `.tmp` deleted at stage start, rebuilt |
| import, between artifact rename and meta write | new `places.parquet`, meta stale or missing | `mtime(artifact) <= mtime(meta)` clause fails → stale → rebuild (idempotent overwrite) |
| division import, during boundaries build (§3.3 step 3) | `boundaries.duckdb.tmp` + `boundaries.duckdb.tmp.wal`; old artifact + places meta intact | stage gate stale (places.parquet meta was never finalized this run — or, on a first run, absent); §3.3 step 3 deletes both `.tmp` and `.tmp.wal` before re-ATTACH, avoiding WAL replay against a fresh empty DB |
| division import, between boundaries rename and places finalize | new `boundaries.duckdb`, stale places meta | stage gate keyed on places meta → both rebuilt; covering sees newer boundaries mtime → rebuilds after |
| covering | per Phase 1 §2.5 (`.tmp`/`.old`/`.spill` leftovers) | unchanged, already tested |
| tile_assignment | partial `.tmp` / meta gap | same as import rows |
| containment | `containment.tmp/` partial; or `.old` present with dir missing | covering-pattern recovery: build-start clobber; `_meta.json`-last means a partial dir is never fresh |
| export tile writing | `tiles/<ts>/` with tile files, no `manifest.json`; `current` untouched | dir deleted at next export start (§3.6 step 2); serving unaffected throughout |
| export, after `manifest.duckdb`, before `manifest.json` | same as above (still incomplete) | same |
| export, after `manifest.json`, before symlink swap | complete run dir, `current` → previous run | export gate (keyed on `current/manifest.json`) is stale → new export; orphaned complete dir falls to keep-2. Costs one re-export; correctness unaffected |
| symlink swap itself | `current.tmp` leftover | existing code removes/overwrites it; `os.rename` of the symlink is atomic |

No fsync gap is load-bearing for the `kill -9` acceptance (process death
flushes nothing but loses nothing already written); the fsyncs in
`finalize_artifact` are for the power-loss case per the spec's global
rule.

## 6. Deletion ledger (spec §6, §9 rows for Phase 2)

| Symbol / file | Location | Disposition |
|---|---|---|
| `_ensure_sentinel_table`, `_read_sentinel`, `_mark_complete` | `quadtree.py:46-71` | delete |
| `_find_incomplete_run` | `quadtree.py:74-109` | delete |
| `STAGE_ORDER` + comment | `quadtree.py:40-43` | delete (nothing consumes it after resume removal) |
| `_collect_input_files` | `quadtree.py:112-136` | delete (per-stage input lists) |
| `run_pipeline` working-DB/resume/sentinel body: `db_path`, `con`, `completed`, `incomplete_dir` branches, `_mark_complete` calls, work-DB deletion | `quadtree.py:196-314` | replaced by §4.2 body |
| `main()` IDF-mode branch | `quadtree.py:404-420` | delete (→ `idf` subcommand) |
| `_pipeline_progress` table | (created at runtime) | ceases to exist; no migration needed — old working DBs are deleted with their run dirs |
| `stage_boundary_export` (path-guessing wrapper) | `stages.py:750-837` | delete |
| `export_boundaries_db` | `stages.py:693-747` | body absorbed into `stage_division_import` (§3.3); standalone function deleted |
| `stage_manifest` | `stages.py:687-690` | absorbed into `stage_export` (§3.6 step 5 — manifests are part of the run-dir completion sequence); standalone function deleted |
| `compute_containment` working-DB plumbing: `con` param, `place_containment` VIEW/TABLE creation, DROP dance, `_make_empty` | `stages.py:125-160, 246-261` | replaced per §3.5 |
| `_run_sql`'s `SELECT count(*) FROM places` | `stages.py:72` | delete (`_run_sql` shrinks to read/substitute/execute; stages log their own counts from artifacts) |
| `tests/test_checkpoint.py` | whole file | `TestSentinelTableHelpers`, `TestStageOrder`, `TestFindIncompleteRun` die with their subjects; `TestPhase2Restructuring` (guards of an already-completed migration) deleted; `TestComputeContainmentIdempotency` ported to `test_containment_covering.py` with the §3.5 signature; file removed |
| Old-layout timestamp scan (`<src>/<ts>` at source root) | `quadtree.py:327-334` | moves to `tiles/`; old dirs become inert (migration note, §2.1) |

Explicitly **not** deleted in Phase 2 (later phases): `TileManifest`,
`BboxTooLarge`, trigram/search machinery, `get_coverage` (§9 rows marked
Phase 3).

## 7. Test spec (red/green order)

Framework: `venv/bin/pytest tests/` (DuckDB 1.2.1) must be fully green at
every gate; new SQL constructs additionally smoke-checked on the app
`.venv` (1.5.1) — one parametrized construct test (7.1.5) encodes this
instead of manual checks. Fixtures reuse `tests/quadtree_helpers.py` and
the synthetic-boundary builders from `test_containment_covering.py`.

### 7.1 `tests/test_artifacts.py` — helpers, written first, red

1. `artifact_fresh` truth table: missing artifact / missing meta /
   unparsable meta / params mismatch / resolved-inputs mismatch / input
   newer than meta / meta older than artifact → all stale; the happy path
   → fresh. Equal mtimes stale (matches `_is_output_fresh`).
2. `finalize_artifact`: artifact and meta land; meta mtime ≥ artifact
   mtime; tmp gone; meta contents `{stage, params, inputs, stats,
   generated_at}`.
3. Stale-`.tmp` clobber: a garbage `<artifact>.tmp` present before a stage
   run does not corrupt output.
4. Directory-artifact recovery reuses the existing covering atomicity
   tests as the spec; containment gets its own (7.4.3).
5. DuckDB construct pinning (both-version guard): `ATTACH ... (READ_ONLY)`
   form, `COPY (SELECT ...) TO ... (FORMAT PARQUET, COMPRESSION ZSTD)`,
   `read_parquet(['a','b'])` list form, `ORDER BY ... NULLS LAST` inside
   COPY — executed against the running interpreter's DuckDB; documented
   one-liners for the 1.5.1 side.
6. §3.1 sort pins (extend `test_density_extract.py` /
   `test_idf_stage.py`): `density_tiles.parquet` non-decreasing on
   `tile_qk15`; `idf.parquet` non-decreasing on `category`.

### 7.2 Import artifact tests (extend `test_import_fsq.py` /
`test_import_overture.py` / `test_import_osm.py` / `test_overture_division.py`)

1. `stage_import` writes `places.parquet` + meta; no working DB anywhere
   under the output tree (assert no `*.duckdb` except boundaries/manifest).
2. qk17 sort: `SELECT qk17 FROM read_parquet(...)` non-decreasing, NULLs
   last; invalid-qk17 fixture rows present in the artifact.
3. Schema: **no** GEOMETRY column in any source's `places.parquet`
   (`geometry` absent for overture_place/overture_division, `geom`
   absent for fsq/osm — §3.2 EXCLUDE); fsq/osm keep lat/lon; row set
   equals the old CTE's over the same fixture (column-wise compare on a
   small extract). For OSM specifically, rows the current pipeline
   removes via `DELETE FROM places WHERE geom IS NULL`
   (`osm_import.sql:317`) are absent from the artifact (filter runs
   before the EXCLUDE, §3.2).
4. Freshness: second call no-op (artifact mtime unchanged); touching a
   source parquet rebuilds; changing `bbox` (or a norm) with fresh mtimes
   rebuilds; `force=True` rebuilds.
5. Division (§3.3): both artifacts written; `boundaries.duckdb` schema
   byte-for-byte today's (columns incl. `admin_level`; R-tree present via
   `duckdb_indexes()`); places artifact has no geometry but retains every
   column `overture_division_export_tiles.sql` references; single meta
   gates both (delete `boundaries.duckdb` → stage stale).

### 7.3 Tile-assignment artifact tests (extend `test_tile_assignment.py`)

1. Reads `places.parquet`, writes sorted `(tile_qk, place_id)` artifact;
   schema `(place_id VARCHAR, tile_qk VARCHAR)`.
2. Assignment semantics unchanged: identical `(place_id, tile_qk)` set vs
   the current SQL over the same fixture (port existing behavior tests).
3. `max_per_tile` param change with fresh mtimes → rebuild.
4. Dropped/duplicate diagnostics still emitted (caplog).

### 7.4 Containment relocation tests (extend `test_containment_covering.py`)

1. New signature: no `con`; artifact under `<src>/containment/`;
   brute-force-oracle parity tests re-pointed at parquet inputs (the
   oracle itself is unchanged and stays red→green through the port).
2. Freshness against all four inputs (touch each → rebuild) and params.
3. Dir-swap atomicity matrix (mirror of covering §7.1.7 tests): leftover
   `.tmp`, leftover `.old` with dir missing, partial `.tmp` from a
   simulated crash → next build correct.
4. Q3: `boundaries_db=None` → dir with `_meta.json` only, export runs and
   emits `relations: {}`; ported idempotency tests from
   `test_checkpoint.py::TestComputeContainmentIdempotency`.

### 7.5 Export tests (extend `test_export.py`, `test_pipeline.py`)

1. Export view executes against `read_parquet` substitutions; record JSON
   for a fixture place byte-identical to the current view's output (per
   source — this is the in-suite miniature of the parity acceptance).
2. Empty-containment relation substitution (no files) works on 1.2.1.
3. Determinism: two forced exports of the same artifacts →
   gunzipped-byte-identical tile files (pins the `ORDER BY tile_qk,
   place_id` invariant).
4. Run-dir lifecycle: partial dir (no manifest.json) deleted at next
   export; `manifest.json` written last (assert mtime ordering vs
   manifest.duckdb); symlink swap; keep-2 counts only complete dirs.
5. `write_manifest_db` from parquet: rkey/tile_qk rows equal current
   behavior incl. OSM rkey transform.

### 7.6 CLI tests — NEW `tests/test_cli_subcommands.py`

1. Each subcommand parses its grammar; `run` re-validates per-source flag
   rules (port `main()` validation tests); bare legacy invocation exits
   with an error mentioning `run`.
2. `all`: stage-call order (monkeypatched stages) is density → idfs →
   division → others; derived paths correct; missing sources skipped;
   missing `overture_division` → `boundaries_db=None` everywhere.
3. `--force` deletion sets exactly as §4.4 (create artifacts, invoke,
   assert survivors — in particular `tiles/` history survives `run
   --force`).

### 7.7 Crash tests — NEW `tests/test_crash_recovery.py` + `tests/crash_harness.py`

State-based matrix (deterministic, no subprocess): construct each §5 disk
state directly against fixture artifacts, run `run_pipeline`, assert the
final output equals a clean-run control (canonical tile compare, §9.1
function) and no stale intermediates remain. The matrix includes the
boundaries-build row: plant a `boundaries.duckdb.tmp` plus a
`boundaries.duckdb.tmp.wal` (a real stale WAL, produced by opening and
killing a scratch DuckDB attach) and assert the division rerun clobbers
both and completes.

Subprocess `kill -9` test (the acceptance miniature):
`tests/crash_harness.py` is a driver script executed via
`subprocess.run([sys.executable, harness, ...])`. It reads
`GARGANORN_CRASH_POINT` (e.g. `import:mid-copy`,
`import:pre-meta`, `export:pre-manifest-json`), monkeypatches the named
seam (`os.replace` / `finalize_artifact` / `write_manifest`) to
`os.kill(os.getpid(), signal.SIGKILL)` when the target path matches, then
calls `run_pipeline` on the fixture. The test asserts
`returncode == -signal.SIGKILL`, classifies the disk state against §5,
reruns `run_pipeline` in-process, and compares against the control run.
SIGKILL at a patched seam is a genuine uncatchable kill at a
deterministic instant — no sleeps, no timing.

### 7.8 Parity harness — NEW `scripts/tile_parity.py` (unit-tested canonicalizer)

`capture <tiles_dir> <out_dir>` / `diff <ref_dir> <tiles_dir>`.
Canonical form (§9.1): per tile, gunzip → parse → sort `records` by
`rkey` (Phase 1's within-tile order is not guaranteed deterministic) →
`json.dumps(obj, sort_keys=True, separators=(",", ":"),
ensure_ascii=False)`. `manifest.json` compared with `generated_at`
removed; `manifest.duckdb` compared as `SELECT rkey, tile_qk ORDER BY
rkey, tile_qk`. Unit tests: pairs differing only in record order / key
order / manifest `generated_at` compare equal; any value difference is
reported with tile + rkey. `diff` exits nonzero on any difference — zero
tolerance (identical containment code on both sides; the SPATIAL-7
allowance from Phase 1 does not apply here).

### 7.9 Modified/deleted tests

- `tests/test_checkpoint.py`: per the §6 ledger row (delete; port
  idempotency class).
- Audit set (files that touch `run_pipeline`, working-DB tables, or stage
  signatures). Generated by running, at `5328e4e` on 2026-07-07:
  `grep -rl 'run_pipeline\|_work\.duckdb\|place_containment\|tile_assignments\|stage_import\|stage_export\|stage_tile_assignment\|stage_containment\|compute_containment\|export_boundaries_db\|stage_boundary_export' tests/`
  — 19 files:
  `quadtree_helpers.py` (shared fixture helpers — updated in place, not
  ported), `test_audit_export.py`, `test_audit_scoring.py`,
  `test_audit_spatial_processing.py`, `test_checkpoint.py` (§6 ledger
  row), `test_containment_covering.py` (§7.4),
  `test_coord_exprs_bug.py`, `test_export.py` (§7.5),
  `test_integration_quadtree.py`, `test_overture_division.py` (§7.2),
  `test_phase3_boundary_export.py`, `test_phase3_containment.py`,
  `test_phase4_density_spatial_join.py`, `test_pipeline.py` (§7.5),
  `test_regressions.py`, `test_source_key_unification.py`,
  `test_stages.py`, `test_tile_assignment.py` (§7.3),
  `test_tile_flatten.py`.
  (`test_import_pipeline.py`, listed in an earlier draft, has zero
  matches and is dropped from the audit; the per-source import tests it
  would have suggested are covered by §7.2's named files.) Rule
  (same as Phase 1 §7.3): behavior tests are ported to artifact
  inputs/outputs; implementation-shape tests (asserts on working-DB
  tables, sentinel rows, resume paths, `con`-taking signatures) die with
  the implementation. `test_phase3_boundary_export.py`'s
  `export_boundaries_db` behavior tests port to `stage_division_import`.
- No test may be skipped rather than ported or deleted; the full suite
  (`venv/bin/pytest tests/`) is the gate at every pipeline phase.

## 8. Memory and disk budget deltas (spec §5)

Target machine unchanged: 72 GB RAM, NVMe scratch, `memory_limit` default
48 GB, `temp_directory` always set (every connection is in-memory now and
cannot spill without it — the orchestrator threads one `temp_directory`
setting through all stages; per-artifact `.spill` siblings only where no
global scratch is configured).

- **New cost: the import qk17 sort.** Today's import CTAS has no global
  ORDER BY; Phase 2 adds one over the full source (~35–50 GB uncompressed
  for global Overture places). It is a spillable external sort;
  `COPY ... ORDER BY` runs multi-threaded but is memory-hungry — if the
  global run OOMs here, the remedy is *lowering* `--memory-limit` for the
  import stage (observed DuckDB behavior: allocations outside the buffer
  manager during parallel sort; see `feedback_duckdb_ctas_sort`), not
  raising it. Provision ≥ 200 GB scratch as the spec already requires;
  the sort is sequential-I/O friendly.
- **Removed cost: the working DB.** No more per-run `.duckdb` (FSQ
  baseline ~49 GB) holding places + assignments + WAL. Replaced by
  ZSTD parquet artifacts — expected net disk reduction per source
  (measure in Phase 4). Transient peak during a rebuild is old artifact +
  new `.tmp` + spill, all on the artifact volume/scratch respectively.
- **Steady-state disk adds**: `places.parquet`, `tile_assignments.parquet`,
  `containment/` persist between runs (that is the point — they are the
  cache). Tiles keep-2 unchanged.
- **No new D6 exposure**: the only holistic aggregate remains the bounded
  per-prefix `list()` in containment (Phase 1 analysis unchanged); export
  still contains no aggregation; every new operator introduced by this
  phase (import sort, assignment sort, export merge) spills.
- Stages continue to run sequentially per source and source-by-source
  under `all`; peak = largest single stage (the import sort).

## 9. Acceptance procedures

### 9.1 Byte-comparability vs Phase 1 (operator-run, scripted)

"Byte-comparable modulo `generated_at`" is interpreted as: canonical
forms (§7.8) byte-identical; `generated_at` appears only in
`manifest.json`/`manifest.duckdb` metadata (the Phase 2 tile payload
carries no timestamp — envelope unchanged, §3.6). Within-tile record
order is additionally canonicalized because Phase 1's
`ORDER BY ta.tile_qk` leaves tie order unspecified; Phase 2 is strictly
deterministic (7.5.3) but Phase 1 need not be. Procedure:

1. At `5328e4e` (before merging Phase 2), run the current pipeline on the
   SF test bbox (`-122.5137 37.7099 -122.3785 37.8101`) for
   `overture_place` + `overture_division` with containment;
   `scripts/tile_parity.py capture` the tile dir. (The harness script is
   the one Phase 2 artifact allowed to be cherry-picked back for the
   capture run, or the capture runs from the Phase 2 worktree against a
   5328e4e checkout — either way, record the commit and data release used.)
2. After Phase 2: same inputs, `run` subcommand, `tile_parity.py diff`.
   Zero differences required for both sources' tile sets, canonical tile
   bytes, manifest quadkey lists, and `manifest.duckdb` row sets.

### 9.2 `kill -9` acceptance

In-suite: §7.7 subprocess test (crash at `import:mid-copy`), plus the
state matrix. Operator-level (on the box, same session as 9.1): start
`run` for `overture_place`, `kill -9` it during the import COPY (visible
in logs), rerun the identical command; assert the rerun rebuilds from
import (log inspection), completes, and `tile_parity.py diff` against a
never-killed control run is clean. No sentinel, no resume, no manual
cleanup between the kill and the rerun.

## 10. Open questions register

| ID | Question | Status | Owner |
|---|---|---|---|
| OQ-P2-1 | §3.8 envelope adoption (`atgeo: 1`, `{uri,cid,value}`, per-tile `generated_at`; manifest §1.3 fields). Resolved 2026-07-07: Phase 2 ships the current envelope (preserves the byte-comparability acceptance); Phase 2b follows immediately after Phase 2 merges, **not** gated on WS-1 — backwards compatibility is a non-goal (header note). | resolved | operator |
| OQ-P2-2 | Level vocabulary (Phase 1 OQ-2, re-deferred): `boundaries.duckdb` keeps `admin_level` through Phase 2 (§3.3 rationale). Resolved 2026-07-07: lands in Phase 2b, same change set as the envelope; the remaining precondition is the on-box `SELECT DISTINCT subtype` verification, not compatibility. | resolved | operator |
| OQ-P2-3 | Phase 1 OQ-3 (containment location): resolved — relocates to `<src>/containment/` with dir-swap atomicity (§3.5). | resolved in-design | agent |
| OQ-P2-4 | Phase 1 OQ-4 (covering CLI): resolved — `covering` subcommand (§4.1); `run` still self-heals via `ensure_covering`. | resolved in-design | agent |
| OQ-P2-5 | Tiles relocate to `<src>/tiles/` per spec §2. Resolved 2026-07-07: confirmed — config.yaml and Ansible serving-path changes land in the same deploy; one-time manual cleanup of old `<src>/<ts>/` dirs on production. | resolved | operator |
| OQ-P2-6 | Byte-comparability definition: canonicalizes within-tile record order (Phase 1 nondeterministic); Phase 2 adds `place_id` secondary sort making future runs byte-deterministic. Resolved in-design; noted because it interprets the acceptance wording. | resolved in-design | agent |
| OQ-P2-7 | Legacy CLI removed (§4.1). Resolved 2026-07-07: operator confirms no outstanding usage of the flag-only invocation (no cron/Ansible/automated call sites; pipeline runs are manual). No sweep needed; operator on-box invocations switch to `run`/`all`. | resolved (acked) | operator |
| OQ-P2-8 | FSQ source disposition (spec §11): unchanged — design converts the FSQ import regardless; whether it runs on real data awaits the pin-or-drop decision (HD-3). | carried | operator |
| — | `COVER_MAX_ZOOM` sizing, manifest sharding, static `getRecord`: carried to Phase 4 / later, per spec §11. | carried | — |
