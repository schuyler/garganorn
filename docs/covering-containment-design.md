---
category: Design
tags: [garganorn, duckdb, pipeline, quadtree, containment, covering]
last_updated: 2026-07-06
status: draft — implementation design for Phase 1 of pipeline-restructure-design.md
---

# Covering + Containment Rewrite: Phase 1 Implementation Design

Implements §3.5 (covering stage) and §3.7 (containment rewrite) of
`docs/pipeline-restructure-design.md` against the *existing* working-DB
pipeline, per the Phase 1 execution plan (§8). Base branch `feat/quadtree`.

Out of scope (Phases 2–4): parquet conversion of import/tile_assignment/
export, orchestrator subcommands, sentinel/resume deletion, envelope format,
server removals. The Phase 0 baseline capture and the `qk_env` unit test are
prerequisites of Phase 1 acceptance and are included here (§7, §8).

## 1. Spec-to-code mapping

| Spec requirement | Code change |
|---|---|
| §3.5 covering stage, `COVER_MIN_ZOOM`/`COVER_MAX_ZOOM` params | NEW `garganorn/covering.py`: constants, `stage_covering()`, `ensure_covering()`, `bbox_to_quadkeys()` |
| §3.5 `qk_env` SQL macro (DuckDB port of `quadkey_to_bbox`) | NEW `garganorn/sql/qk_env_macro.sql` (`qk_tile_x`, `qk_tile_y`, `qk_env` macros) |
| §3.5 output `covering/<qk4>.parquet`, sorted `(tile_qk, boundary_id)` | Written by `stage_covering()` under `<output>/overture_division/covering/` |
| §3.7 containment as parquet-join over covering | REWRITE `compute_containment()` in `garganorn/stages.py`; NEW `garganorn/sql/compute_containment.sql` template |
| §3.7 output `containment/<qk4>.parquet` | Written per qk4 prefix; `place_containment` becomes a VIEW over it so the existing export SQL is unchanged |
| §9 deletion ledger row 1 (Phase 1) | DELETE `_run_containment`, `_process_tile`, old `compute_containment` body (`stages.py:89-330`) — only after parity (§7) |
| Q3 graceful degradation (no boundaries → no relations) | Preserved: `boundaries_db is None` or empty covering → empty `place_containment` |
| Orchestration (Phase-1 minimal) | MODIFY `run_pipeline()` in `garganorn/quadtree.py`: build covering after `export_boundaries_db()`; `ensure_covering()` before containment for every source with `boundaries_db` |
| Constraint docs | UPDATE `docs/design-constraints.md` P6 (describes deleted code); note SPATIAL-6 moot in `docs/pipeline-status.md` |

### Files touched

- NEW `garganorn/covering.py`
- NEW `garganorn/sql/qk_env_macro.sql`
- NEW `garganorn/sql/covering_seed.sql`, `garganorn/sql/covering_level.sql`
  (level-loop step), `garganorn/sql/compute_containment.sql`
- MODIFY `garganorn/stages.py` (rewrite `compute_containment`,
  `stage_containment`; delete `_run_containment`, `_process_tile`)
- MODIFY `garganorn/quadtree.py` (wire covering + new stage args into
  `run_pipeline`)
- MODIFY `docs/design-constraints.md`, `docs/pipeline-status.md`
- NEW `tests/test_covering.py`, `tests/test_containment_covering.py`;
  MODIFY/DELETE parts of `tests/test_phase3_containment.py` (§8.4)
- NEW `scripts/containment_parity.py` (baseline capture + diff, §7)

## 2. Covering stage (§3.5)

### 2.1 Public interface (`garganorn/covering.py`)

```python
COVER_MIN_ZOOM = 4
COVER_MAX_ZOOM = 12

def lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    """Web-mercator forward: clamp lat to ±85.05112878, return (x, y)."""

def bbox_to_quadkeys(min_lon, min_lat, max_lon, max_lat, zoom) -> list[str]:
    """Quadkeys at `zoom` whose tiles intersect the bbox.
    D7: min_lon > max_lon means antimeridian crossing; return the union
    of the two lobes [min_lon, 180] and [-180, max_lon]."""

def stage_covering(boundaries_db: str, covering_dir: str, *,
                   memory_limit: str = "48GB",
                   temp_directory: str | None = None,
                   cover_min_zoom: int = COVER_MIN_ZOOM,
                   cover_max_zoom: int = COVER_MAX_ZOOM,
                   force: bool = False) -> dict:
    """Build covering/<qk4>.parquet from boundaries.duckdb.
    Returns stats: {total, interior, edge, per_level: {z: n}}.
    Skips when fresh (see 2.5). Ephemeral in-memory connection with
    SET temp_directory, SET memory_limit, SET preserve_insertion_order=false,
    LOAD spatial; ATTACH boundaries_db READ_ONLY as bnd.

    temp_directory=None (the default) resolves to covering_dir + ".spill",
    a sibling of the output dir on the same volume; it is created at build
    start and removed after the swap (and cleaned up as a crash leftover,
    see 2.5). SET temp_directory is therefore *always* issued — the
    in-memory connection can spill regardless of whether the caller passes
    a scratch path. Callers (run_pipeline, tests) may override to point
    spill at a different volume."""

def ensure_covering(boundaries_db: str, covering_dir: str | None = None,
                    **kwargs) -> str:
    """Derive covering_dir (default: dirname(boundaries_db)/covering),
    call stage_covering (no-op when fresh), return the dir."""
```

`covering.py` imports `quadkey_to_bbox` from `stages.py` (no circular
import: `stages.py` does not import `covering.py`; `quadtree.py` imports
both).

### 2.2 `qk_env` macro (`garganorn/sql/qk_env_macro.sql`)

Three scalar macros, executed at connection setup by both covering and the
macro's unit test:

```sql
CREATE OR REPLACE MACRO qk_tile_x(qk) AS
    list_sum(list_transform(generate_series(1, length(qk)),
        i -> CASE WHEN qk[i] IN ('1','3')
                  THEN 2 ** (length(qk) - i) ELSE 0 END));
CREATE OR REPLACE MACRO qk_tile_y(qk) AS
    list_sum(list_transform(generate_series(1, length(qk)),
        i -> CASE WHEN qk[i] IN ('2','3')
                  THEN 2 ** (length(qk) - i) ELSE 0 END));
CREATE OR REPLACE MACRO qk_env(qk) AS
    ST_MakeEnvelope(
        qk_tile_x(qk) / (2 ** length(qk)) * 360 - 180,
        degrees(atan((exp(t) - exp(-t)) / 2))  -- t = pi()*(1 - 2*(qk_tile_y(qk)+1)/2**length(qk)); expanded inline
        , ... );
```

(The final macro is written out with the sinh expansion inlined for both
lat_min and lat_max; DuckDB scalar macros are single expressions, hence the
helper macros. `sinh` is written as `(exp(t)-exp(-t))/2` so only `atan`,
`exp`, `pi`, `degrees` are required, per spec.)

**Implementer warning — the y/y+1 asymmetry.** Tile y increases
*southward*, so the envelope's north edge (`lat_max`, the `ymax` argument
of `ST_MakeEnvelope`) uses `t` computed from `qk_tile_y(qk)`, while the
south edge (`lat_min`, the `ymin` argument) uses `qk_tile_y(qk) + 1` — as
in the snippet's inline comment. Writing both expansions from the same
`t`, or attaching `+1` to the wrong edge, is the single likeliest
transcription error in this macro; it is exactly what the 10,000-quadkey
agreement test (§7.1 item 1) exists to catch, and a swapped edge fails it
on every quadkey.

`quadkey_to_bbox()` (`stages.py:455`) is the reference implementation.
Required test: agreement over 10,000 random quadkeys of mixed lengths
(1–17) to 1e-9 on all four envelope coordinates (spec §3.5; a Phase 0
deliverable — this test lands first).

### 2.3 Algorithm (level-by-level, set-based)

All SQL below runs in the covering stage's ephemeral connection.

**Seed (z4).** Deviation from spec mechanics, same result: the spec says
"insert seed pairs via Arrow", but pyarrow is not a project dependency.
Instead, Python inserts the 256 z4 tiles (quadkey + bbox computed with
`quadkey_to_bbox()`, trivially via `executemany`) into a temp table
`z4_tiles(qk, xmin, ymin, xmax, ymax)`, and one SQL join produces the seeds
— D7 handled in the join condition:

```sql
CREATE TEMP TABLE l_current AS
SELECT b.id AS boundary_id, b.admin_level AS level, t.qk AS tile_qk,
       ST_Intersection(b.geometry, qk_env(t.qk)) AS geom
FROM bnd.places b
JOIN z4_tiles t
  ON b.min_latitude <= t.ymax AND b.max_latitude >= t.ymin
 AND (CASE WHEN b.min_longitude <= b.max_longitude
           THEN b.min_longitude <= t.xmax AND b.max_longitude >= t.xmin
           ELSE b.min_longitude <= t.xmax OR  b.max_longitude >= t.xmin
      END)                                  -- D7: two lobes
WHERE ST_Intersects(b.geometry, qk_env(t.qk));
```

1M boundaries × 256 tiles is a bounded blockwise join; no R-tree needed
(D1 is irrelevant here — the join is by bbox arithmetic, and
`ST_Intersects` refines after the cheap filter). Rows with degenerate
clips are dropped immediately (see filter below).

**Level loop (z = cover_min_zoom .. cover_max_zoom).** Each iteration
materializes the containment flag once (ST_Contains on complex geometries
is the dominant cost; do not evaluate it twice):

```sql
-- covering_level.sql, parameters: ${z} (current), children only when z < max
CREATE TEMP TABLE l_flagged AS
SELECT *, ST_Contains(geom, qk_env(tile_qk)) AS is_interior
FROM l_current;

INSERT INTO covering_out           -- interior emission at every z in [4, 12]
SELECT tile_qk, boundary_id, level, 'interior' FROM l_flagged WHERE is_interior;

-- z < COVER_MAX_ZOOM: expand non-interior rows ×4, re-clip to child envelope
CREATE TEMP TABLE l_next AS
SELECT boundary_id, level, tile_qk || d.d AS tile_qk,
       ST_Intersection(geom, qk_env(tile_qk || d.d)) AS geom
FROM l_flagged, (VALUES ('0'),('1'),('2'),('3')) d(d)
WHERE NOT is_interior;
-- then: DELETE degenerate rows (or filter in the CTAS):
--   keep only ST_IsValid(geom) AND ST_Area(geom) > 0
DROP TABLE l_current; ALTER ... -- (rename l_next -> l_current); DROP l_flagged;
```

**Terminal (z = COVER_MAX_ZOOM = 12).** Same flagging query; `is_interior`
rows → `'interior'`, remaining rows → `'edge'`. This resolves a spec
ambiguity (§6, OQ-1): §3.5 step 3 says "surviving rows → edge", but the
parameters paragraph says "interior tiles appear at any zoom in [4, 12]".
We run the interior test at z12 too — correct either way (an interior tile
misclassified as edge only costs per-point `ST_Contains` at containment
time), but interior-at-12 is faster downstream and matches the schema
comment.

**Clipping correctness.** Geometries are clipped to the tile's *own*
envelope at expansion time (`ST_Intersection(geom, qk_env(child))`), one
level tighter than the spec's "parent envelope" wording. Both are valid:
for `tile_env ⊆ clip_env`, `ST_Contains(geom ∩ clip_env, tile_env) ⟺
ST_Contains(geom, tile_env)`. This is stated as a code comment and tested
(§8.2). The `ST_IsValid AND ST_Area > 0` filter mirrors the old
`_run_containment` step-0 filter (`stages.py:129`): a boundary that merely
touches a tile along an edge can contain no point-in-polygon match, so
dropping it is parity-safe.

**Output write.** `covering_out(tile_qk VARCHAR, boundary_id VARCHAR,
level INTEGER, kind VARCHAR)` accumulates in a temp table. A Python loop
over `SELECT DISTINCT left(tile_qk, 4)` writes one file per prefix:

```sql
COPY (SELECT tile_qk, boundary_id, level, kind
      FROM covering_out WHERE left(tile_qk, 4) = '${prefix}'
      ORDER BY tile_qk, boundary_id)                 -- D2 sort invariant
TO '${tmp_dir}/${prefix}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

Per-file `COPY ... ORDER BY` (not `PARTITION_BY`) because partitioned COPY
does not guarantee per-partition sort order and the spec names flat files
`covering/<qk4>.parquet`.

Per-level row counts, kind totals, and per-boundary p50/p99/max are logged
(feeds §10 validation item 1 without rework).

### 2.4 Output schema

```
covering/<qk4>.parquet     one file per z4 prefix present
  tile_qk      VARCHAR     length in [COVER_MIN_ZOOM, COVER_MAX_ZOOM]
  boundary_id  VARCHAR     bnd.places.id
  level        INTEGER     copied from boundaries.duckdb (see below)
  kind         VARCHAR     'interior' | 'edge' (edge only at COVER_MAX_ZOOM)
  sort: (tile_qk, boundary_id)
covering/_meta.json        written last; parameters + stats + generated_at
```

**`level` in Phase 1 = `admin_level` values (superseded, Phase 2b).** §3.5
says `level` is the atgeo containment level per §3.4's subtype mapping, but
§3.4 (division import rework) was Phase 2, and at the time this design was
written `boundaries.duckdb` still carried `admin_level` (also still read by
`boundaries.py` for the server path, so its schema could not change then).
The covering stage copied `admin_level` into the column named `level`.
Relation ordering parity was preserved (old code ordered by `admin_level
ASC`; DuckDB default NULLS LAST applies to both — localities could carry
NULL admin_level). **This is now false as of Phase 2b**
(`docs/phase2b-design.md` Part A): `level` is the atgeo containment
vocabulary keyed on Overture `subtype`, not `admin_level` — see
`docs/atgeo-appview-sdk-design.md` §1.7. `admin_level` no longer exists in
the exported schema; ordering is `ORDER BY level ASC` (total by
construction, no NULLS-last handling needed). See OQ-2.

### 2.5 Freshness and atomicity

- Build everything under `covering_dir + ".tmp"`, then swap. Precise
  sequence:
  1. **Build start**: unconditionally `rmtree` any pre-existing
     `covering_dir + ".tmp"`, `covering_dir + ".old"`, and
     `covering_dir + ".spill"` (crash leftovers from a previous run —
     `ignore_errors`-free removal of stale state before any work), then
     create `.tmp` (and `.spill`, §2.1) fresh.
  2. **Swap**: if `covering_dir` exists, rename it to
     `covering_dir + ".old"`; the rename is guarded on existence because
     a first-ever build has nothing to move aside. Rename `.tmp` into
     place, then `rmtree` `.old` (if created) and `.spill`.
  A crash therefore leaves at most: the old covering plus a stale `.tmp`
  (clobbered at next build start), or — if the crash lands between the
  two renames — a `.old` and no `covering_dir`. In the latter case the
  freshness gate fails (no `_meta.json` at `covering_dir`) and the next
  build's step-1 cleanup removes the `.old`; the covering is rebuilt from
  `boundaries.duckdb`, which is never touched by the swap.
- Freshness gate: `_is_output_fresh(covering_dir/_meta.json,
  [boundaries_db])` (reuses `stages.py:41`). `_meta.json` is written last
  inside the tmp dir, so a partially built covering is never fresh. It
  also records `cover_min_zoom`/`cover_max_zoom`; a parameter change with
  a fresh mtime is detected by comparing recorded params and forces a
  rebuild.
- No sentinel entry: covering is an artifact-graph stage with its own
  mtime freshness, not a working-DB stage. (`STAGE_ORDER` unchanged.)

## 3. Containment rewrite (§3.7)

### 3.1 Signatures

```python
# stages.py — replaces the old body entirely
def compute_containment(con, boundaries_db, pk_expr, lon_expr, lat_expr,
                        collection_prefix="org.atgeo.places.overture.division",
                        covering_dir=None, containment_dir=None):
    """Write containment/<qk4>.parquet and create the place_containment
    VIEW over them. Empty place_containment table when boundaries_db is
    None, covering_dir is absent/empty, or no prefix produced rows (Q3)."""

def stage_containment(con, source, pk_expr, lon_expr, lat_expr,
                      boundaries_db, t0,
                      covering_dir=None, containment_dir=None):
```

`max_boundaries` / `max_zoom` parameters are deleted with the recursion
(their signature tests go too, §8.4). `collection_prefix` keeps its name,
default, and rkey format (`'{prefix}:' || boundary_id`).

### 3.2 Execution

1. `LOAD spatial`; `ATTACH boundaries_db READ_ONLY AS bnd` (geometry lookup
   for the edge arm only — no spatial index in the join path; SPATIAL-6
   moot).
2. Materialize a slim, qk17-sorted projection once (Phase-1 substitute for
   the qk17-sorted `places.parquet` that Phase 2 will provide):

   ```sql
   CREATE TEMP TABLE places_slim AS
   SELECT ${pk_expr} AS place_id, p.qk17, ${lon_expr} AS lon, ${lat_expr} AS lat
   FROM places p
   WHERE p.qk17 IS NOT NULL AND length(p.qk17) = 17 AND p.qk17 ~ '^[0-3]{17}$'
   ORDER BY p.qk17;
   ```

   One sort instead of up-to-256 full-table scans; the prefix loop then
   prunes via zonemaps (D2). The qk17 validity filter matches
   `compute_tile_assignments.sql`; invalid-qk17 places never reach export
   anyway (inner join on `tile_assignments`).
3. Prefix loop (Python) over `SELECT DISTINCT left(qk17, 4) FROM
   places_slim ORDER BY 1`. Prefixes with no `covering/<qk4>.parquet` file
   are skipped (no boundary overlaps that cell — covering tiles all have
   length ≥ 4 and every covering tile matching a place shares its z4
   prefix, so the partitioning is exact).
4. Per prefix, one `COPY` from `compute_containment.sql`
   (`string.Template`-style, interior arms generated by the Python driver
   for L in `COVER_MIN_ZOOM..COVER_MAX_ZOOM`):

   ```sql
   COPY (
   WITH p AS (
       SELECT place_id, qk17, lon, lat FROM places_slim
       WHERE left(qk17, 4) = '${prefix}'
   ),
   cov AS (SELECT * FROM read_parquet('${covering_file}')),
   interior AS (
       -- one arm per L in 4..12, UNION ALL; equi-joins on fixed-length prefix
       SELECT p.place_id, c.boundary_id, c.level
       FROM p JOIN cov c
         ON c.kind = 'interior' AND len(c.tile_qk) = ${L}
        AND left(p.qk17, ${L}) = c.tile_qk
   ),
   edge AS (
       SELECT p.place_id, c.boundary_id, c.level
       FROM p
       JOIN cov c ON c.kind = 'edge' AND left(p.qk17, ${max_zoom}) = c.tile_qk
       JOIN bnd.places b ON b.id = c.boundary_id
       WHERE p.lat BETWEEN b.min_latitude AND b.max_latitude
         AND (CASE WHEN b.min_longitude <= b.max_longitude
                   THEN p.lon BETWEEN b.min_longitude AND b.max_longitude
                   ELSE p.lon >= b.min_longitude OR p.lon <= b.max_longitude
              END)                                   -- D7
         AND ST_Contains(b.geometry, ST_Point(p.lon, p.lat))
   ),
   matches AS (SELECT * FROM interior UNION ALL SELECT * FROM edge)
   SELECT ta.tile_qk, m.place_id,
          to_json({within: list({rkey: '${collection_prefix}:' || m.boundary_id}
                                ORDER BY m.level ASC)})::VARCHAR AS relations_json
   FROM matches m
   JOIN tile_assignments ta ON ta.place_id = m.place_id
   GROUP BY ta.tile_qk, m.place_id
   ORDER BY ta.tile_qk, m.place_id
   ) TO '${output_tmp}' (FORMAT PARQUET, COMPRESSION ZSTD);
   ```

   Then `os.rename(output_tmp, containment_dir/<qk4>.parquet)`. The
   `list()` is bounded per prefix (D6: groups ≤ places in one z4 cell;
   state per group ≤ containing-boundary count). For a given boundary, the
   interior and edge tile sets are disjoint by construction (interior
   tiles are removed from the working set before expansion, so no covering
   tile descends from another of the same boundary) — a place matches each
   boundary at most once; no DISTINCT needed (tested, §8.3).
5. Compatibility view for the unchanged export SQL:

   ```sql
   CREATE OR REPLACE VIEW place_containment AS
   SELECT place_id, relations_json
   FROM read_parquet(${written_file_list});
   ```

   The explicit file list (not a glob) avoids `read_parquet` failing on an
   empty directory. If no files were written, fall back to the current
   empty-table path (`CREATE TABLE place_containment (place_id VARCHAR,
   relations_json VARCHAR)`), preserving Q3 exactly.

### 3.3 Output schema and location

```
<tile_dir>/containment/<qk4>.parquet
  tile_qk        VARCHAR   place's export tile (from tile_assignments)
  place_id       VARCHAR
  relations_json VARCHAR   {"within":[{"rkey": ...}, ...]} ordered by level ASC
  sort: (tile_qk, place_id)
```

**Phase-1 location: under the timestamped run dir** (`tile_dir`), not
`<src>/containment/` as in the final artifact graph. Rationale: in Phase 1
the `places` table lives in the per-run working DB, so the containment
artifact is only coherent with its own run; placing it in `tile_dir` ties
its lifecycle to the existing keep-2 cleanup and sentinel resume. Phase 2
relocates it when `places.parquet` gives it stable inputs. Flagged as OQ-3.

## 4. Orchestration wiring (`quadtree.py`, minimal Phase-1 diff)

- `run_pipeline()`, `overture_division` branch: after
  `export_boundaries_db(db_path, source_dir, t0)`, call
  `stage_covering(os.path.join(source_dir, "boundaries.duckdb"),
  os.path.join(source_dir, "covering"), memory_limit=memory_limit)`. Not
  sentinel-tracked (own freshness; rerun after crash is a rebuild, which
  atomic swap makes safe). `temp_directory` is not passed: `run_pipeline`
  gains no new parameter, and `stage_covering` defaults spill to
  `covering_dir + ".spill"` (§2.1), so the in-memory connection can always
  spill next to the output. This satisfies §8's D4 requirement without
  CLI or signature changes; a caller needing spill on a different volume
  passes `temp_directory` explicitly (Phase 2's `covering` subcommand
  exposes it as a flag).
- `run_pipeline()`, all sources: when `boundaries_db` is not None, call
  `covering_dir = ensure_covering(boundaries_db,
  memory_limit=memory_limit)` before the containment stage, then
  `stage_containment(..., covering_dir=covering_dir,
  containment_dir=os.path.join(tile_dir, "containment"))`. For a stale or
  missing covering this self-heals without CLI changes; when fresh it is a
  stat call. The `covering` CLI subcommand arrives with Phase 2's
  orchestrator (§4 of the spec).
- `_collect_input_files()` gains nothing: covering is derived from
  `boundaries.duckdb`, which is already in the freshness input list.

## 5. Data flow summary

```
boundaries.duckdb (id, geometry, admin_level, min/max lat/lon; R-tree; existing)
    └─ stage_covering ──► overture_division/covering/<qk4>.parquet  (+ _meta.json)
places (working-DB table) ─┐
tile_assignments (table) ──┼─ compute_containment ──► <tile_dir>/containment/<qk4>.parquet
covering/<qk4>.parquet ────┤                                  │
boundaries.duckdb (edge arm geometry) ─┘                      ▼
                                    place_containment VIEW ──► existing *_export_tiles.sql (unchanged)
```

## 6. Correctness parity with current containment

### Equivalence argument

- Old phase 1 (boundary contains whole leaf tile → CROSS JOIN all places
  in tile) ≡ new interior arm: a place whose qk17 prefix matches an
  interior covering tile is inside that tile, which is inside the
  boundary. The covering emits interior tiles exactly where the old code's
  `ST_Contains(clipped_geom, tile_env)` succeeded — same predicate, same
  clipping trick, different tile decomposition (which cannot change
  point-set membership).
- Old phase 2 (per-point `ST_Contains` on clipped boundaries intersecting
  the leaf tile, minus phase-1 boundaries) ≡ new edge arm, with two
  deliberate differences listed below. The covering's z12 edge band is a
  strictly tighter candidate set than the old per-tile `ST_Intersects`
  prefilter, and candidate narrowing never changes which
  `(place, boundary)` pairs pass the final `ST_Contains`.

### Expected differences (all SPATIAL-7 edge classes)

1. **Points exactly on old leaf-tile edges.** Old phase 2 tested
   `ST_Contains` against geometry clipped to the leaf tile; a point on the
   clip edge failed even when inside the true boundary. New edge arm uses
   the full geometry — the new result is correct where they differ, and
   the diff set can only grow (never lose) pairs.
2. **Degenerate/invalid clip handling.** Both old and new drop clipped
   geometries failing `ST_IsValid`/`ST_Area > 0`, but at different tile
   decompositions; disagreements are confined to zero-area touch cases.
3. **Tie ordering inside `within`.** Both order by level/admin_level ASC;
   ties are nondeterministic in both. Parity compares `(place_id, rkey)`
   *pair sets*, not JSON strings.

### Verification mechanics (Phase 0/1 acceptance)

1. **Baseline (Phase 0).** `scripts/containment_parity.py capture`: run the
   current branch on the SF extent (`-122.5137 37.7099 -122.3785 37.8101`)
   for `overture_place` + `overture_division` with containment; extract
   sorted CSV of `(place_id, rkey)` pairs (from `place_containment` /
   tile files), per-tile record counts, tile file set, wall time per
   stage. Store under `test_work/` or the scratchpad, path recorded in the
   PR. (Data and runtime environment: production box per
   `knowledge/server_infrastructure.md`, or a local cached extract if one
   exists — implementer confirms which.)
2. **Diff (Phase 1 gate).** `scripts/containment_parity.py diff`: pair-set
   comparison. Acceptance per spec §8: identical except logged, eyeballed
   diffs that are demonstrably in classes 1–2 above (script prints each
   diff with the point's distance to the nearest boundary/tile edge).
3. **In-suite oracle** (independent of the old code, survives its
   deletion): for fixture boundaries and a grid + random sample of places,
   assert new `compute_containment` pair set == brute-force
   `ST_Contains(boundary, point)` over all boundaries (§8.3). This is the
   miniature of validation-plan item 2.
4. Old containment code is deleted **only after** step 2 passes; the
   deletion commit is separate from the implementation commit.

## 7. Test strategy (red/green order)

Framework: pytest (`pytest tests/`, ~780 tests; full suite must pass at
each gate). Fixtures follow `test_phase3_containment.py`'s
`_create_division_db` pattern (synthetic WKT boundaries; add nested,
antimeridian-adjacent, and locality-with-NULL-admin_level cases).

### 7.0 Phase-0 gate: DuckDB version pin and construct smoke test — CLOSED

The SQL in this design depends on behaviors that are not guaranteed
across all DuckDB versions: scalar `generate_series` returning a list,
1-based string subscripts (`qk[i]`), the `**` power operator,
`list_transform` with a lambda inside a scalar macro, and struct
`to_json` with `list(... ORDER BY ...)`. These must be verified against
the exact DuckDB version(s) in use *before* any of the SQL is authored —
not discovered late via the macro test.

**Gate executed 2026-07-06.** The repo carries two virtualenvs
(`pyproject.toml` declares `duckdb` unpinned):

| Environment | DuckDB | Result |
|---|---|---|
| `.venv/` (active dev) | **1.5.1** | all constructs pass |
| `venv/` (legacy) | 1.2.1 | all constructs pass |

Smoke-test one-liners and observed outputs (both versions identical):

- `SELECT generate_series(1, 4)` → `[1, 2, 3, 4]` (list, as required)
- `SELECT s[1], s[3] FROM (SELECT '0123' AS s)` → `('0', '2')` (1-based)
- `SELECT 2 ** 10` → `1024.0` — note `**` returns DOUBLE; exact for
  integer exponents < 53 bits, so quadkey lengths ≤ 17 are safe, but
  `qk_tile_x('132')` returns `6.0` not `6` (tests compare numerically)
- The full `qk_tile_x` macro from §2.2 (`list_transform` + lambda +
  `generate_series` + subscripts + `**` inside a scalar macro) creates
  and evaluates correctly: `qk_tile_x('132')` = `6.0`
- `SELECT to_json({within: list({rkey: 'p:' || id} ORDER BY lvl ASC)})`
  over `VALUES ('b',2),('a',1)` → `{"within":[{"rkey":"p:a"},{"rkey":"p:b"}]}`
- `LOAD spatial; SELECT ST_AsText(ST_MakeEnvelope(-1,-1,1,1))` →
  `POLYGON ((-1 -1, -1 1, 1 1, 1 -1, -1 -1))`

**Pinned version: 1.5.1** (the active `.venv`); the 1.2.1 pass shows the
constructs are stable across the full locally observed range. Remaining
action before the *global* run only: confirm the production box's DuckDB
version and, if it falls outside [1.2.1, 1.5.1], re-run these one-liners
there (see OQ-6, now resolved).

### 7.1 `tests/test_covering.py` — written first, red

1. **`qk_env` macro vs `quadkey_to_bbox`**: 10,000 random quadkeys, lengths
   1–17, all four coordinates within 1e-9. (Phase 0 item; lands before
   everything else. Red until the macro file exists.)
2. **`bbox_to_quadkeys` / `lonlat_to_tile`**: known bboxes → expected z4
   sets; whole world → 256 tiles; latitude clamping at ±85.05…;
   antimeridian bbox (`min_lon > max_lon`) → union of two lobes and
   nothing in the gap (D7).
3. **`stage_covering` schema/shape**: output files named `<qk4>.parquet`;
   columns `(tile_qk, boundary_id, level, kind)`; `len(tile_qk)` within
   `[cover_min_zoom, cover_max_zoom]`; `kind='edge'` only at max zoom;
   rows sorted `(tile_qk, boundary_id)`; `level` equals the boundary's
   `admin_level`; `_meta.json` present with params.
4. **Semantic invariants** (checked against original geometries with a
   scratch spatial connection): every interior tile's envelope is
   contained by its boundary; every edge tile's envelope intersects it;
   no covering tile is a descendant (prefix-extension) of an interior
   tile of the same boundary.
5. **Point classification property**: for sampled points, "qk17 prefix
   hits an interior tile, or hits an edge tile and full-geometry
   `ST_Contains` passes" ⟺ direct `ST_Contains(boundary, point)`. Include
   points near boundary edges and tile corners.
6. **Zoom parameters**: `cover_min_zoom`/`cover_max_zoom` overrides
   respected (use small values, e.g. 4..7, to keep tests fast).
7. **Freshness/atomicity**: second call is a no-op; touching
   `boundaries.duckdb` triggers rebuild; `force=True` rebuilds; changed
   zoom params with fresh mtimes rebuild; interrupted build (tmp dir left
   behind) recovers; crash-between-renames case (leftover `.old` present,
   `covering_dir` absent) recovers — next build removes the `.old` at
   start and produces a fresh, correct covering (§2.5 step 1).

### 7.2 `tests/test_containment_covering.py` — written second, red

1. **Ports of surviving behavior tests** from `test_phase3_containment.py`:
   rkey-only relations, division prefix, expected boundary set for the SF
   point, `collection_prefix` kwarg with the same default.
2. **Ordering**: `within` ordered by level ASC; NULL levels last.
3. **Brute-force oracle parity** (§6 step 3), including places in multiple
   qk4 prefixes and nested boundaries (no duplicate rkeys in `within`).
4. **Artifacts**: `containment/<qk4>.parquet` written with schema
   `(tile_qk, place_id, relations_json)` sorted `(tile_qk, place_id)`;
   `tile_qk` agrees with `tile_assignments`.
5. **Degradation (Q3)**: `boundaries_db=None` → empty `place_containment`,
   export runs; covering dir missing/empty → same; prefix without covering
   file skipped; place outside every boundary absent from output.
6. **Export integration**: mini end-to-end `run_pipeline` (pattern from
   existing pipeline tests) producing tile JSON whose `relations` match —
   proves the `place_containment` view is join-compatible with the
   unchanged export SQL, and that covering is built/ensured by
   orchestration.
7. **Edge-arm antimeridian branch (D7)**: synthetic boundary row with
   `min_longitude > max_longitude` (e.g. lobes `[170, 180]` and
   `[-180, -170]`) whose geometry matches, plus places with points in the
   west lobe, in the east lobe, and in the gap between them.

   *What the tests pin:*

   - **Lobe-inclusion tests** pin the D7 OR-logic in the edge arm WHERE
     clause.  A buggy AND condition (`p.lon >= b.min_longitude AND p.lon <=
     b.max_longitude`) evaluates to false for a lobe point at lon=175 (since
     `175 <= -170` is false), wrongly dropping the place.  These tests are
     the behaviorally observable check of the D7 predicate.

   - **Gap assertions** are end-to-end sanity checks (gap place receives
     `gap_boundary`'s rkey; never `ami_boundary`'s), NOT a test of the D7
     CASE's exclusion branch.  The exclusion branch is structurally
     unreachable for gap points: the covering seed SQL uses the same D7
     condition on the boundary bbox (§2.3), so gap tiles are never seeded
     for an antimeridian boundary and no `(gap_place, ami_boundary)` row
     can enter the edge arm join regardless of the CASE predicate.  Even
     where reachable (edge tiles at the lobe boundary), the exclusion side
     is masked by `ST_Contains`: any point in the gap that failed the CASE
     would have lon ∉ lobe geometry interior, making `ST_Contains` false
     anyway.  The CASE's exclusion branch therefore affects performance only
     (candidate pruning), not correctness, and no test can observe it as a
     behavioral difference in output.

   This is the containment-time counterpart of the §7.1 item-2
   `bbox_to_quadkeys` D7 test; both D7 code paths get a red test.

### 7.3 Modified/deleted tests (same change set as the code deletion)

- `test_phase3_containment.py::TestComputeContainmentAdaptive` — deleted
  (asserts `max_boundaries`/`max_zoom` signature of the removed
  recursion). Its two behavior tests (`test_prefix_length_correctness`,
  `test_subdivision_produces_same_results`) are superseded by 7.2.3.
- Any `test_stages.py`/`test_pipeline.py` assertions on the old
  containment internals — audit with `grep -l compute_containment
  tests/`, port or delete per the same rule: behavior tests are ported,
  implementation-shape tests die with the implementation.
- `TestBoundaryLookupCollection` etc. (server-side `boundaries.py`) are
  untouched — that path still reads `boundaries.duckdb` directly.

## 8. Performance considerations

- **No R-tree in any new join path** (D1 satisfied by avoidance): covering
  seeds by bbox arithmetic against 256 tiles; containment joins covering
  by quadkey prefix and `bnd.places` by id. The R-tree remains for the
  server's `boundaries.py` lookups only.
- **D2 sorts**: every parquet written carries explicit `ORDER BY`;
  `places_slim` is CTAS-sorted by qk17 so the per-prefix filter prunes by
  zonemap instead of rescanning (Phase 2's qk17-sorted `places.parquet`
  makes this permanent).
- **D4/D6**: covering level-loop CTAS and the containment `COPY` contain
  only joins/sorts plus the one bounded per-prefix `list()`; everything
  spills. Covering's connection sets `memory_limit` (default 48GB,
  configurable) and `temp_directory` — in-memory connections cannot spill
  without it, which is why `stage_covering` always resolves one: the
  `temp_directory` parameter defaults to `covering_dir + ".spill"` (§2.1),
  so the §4 orchestration (which passes only `memory_limit`) still gets a
  spillable connection. `SET preserve_insertion_order = false` everywhere.
- **ST_Contains evaluated once per (boundary, tile) per level** via the
  `is_interior` flag materialization; the clipped `geom` column shrinks as
  tiles shrink (the same vertex-count optimization as the old per-tile
  code, now amortized once per division release instead of per run).
- **Covering size**: rows grow with total boundary perimeter ÷ z12 tile
  edge; spec sanity bound 10⁷–10⁸ rows total. Per-level counts logged;
  wildly larger numbers on the SF/global runs mean revisiting
  `COVER_MAX_ZOOM` (spec §11).
- **Edge-arm geometry cost (watch item)**: the new edge arm runs
  `ST_Contains` against *full* boundary geometries (correct, and the
  reason parity class 1 improves), where the old code tested tile-clipped
  ones. Candidates are limited to the z12 edge band, but a
  100k-vertex country polygon per point-test could dominate — note that
  DuckDB spatial does *not* cache prepared geometries across join rows,
  so the full polygon is re-processed for every candidate point; there is
  no amortization to hope for. Measure on the SF baseline (§6).
  Contingency, only if measured slow: per-prefix temp table of edge
  geometries clipped to their z12 tile envelope (restores the vertex
  bound). **Adopting the contingency is a parity-affecting change, not a
  silent swap**: clipping reintroduces parity class 1 (§6) at z12 tile
  edges — points exactly on a z12 clip edge fail `ST_Contains` against
  the clipped geometry while passing against the full one — so the §6
  step-2 parity gate must be re-run (and its diffs re-eyeballed against
  classes 1–2) before the contingency lands. Not implemented
  speculatively.
- **Working-DB attach churn**: one ATTACH per containment stage, one per
  covering build — no per-tile statements, no temp-table thrash; the old
  code issued 3 SQL statements per leaf tile across thousands of tiles.

## 9. Open questions / flagged deviations

- **OQ-1 (spec ambiguity, resolved in-design)**: §3.5 step 3 vs the
  parameters paragraph on interior emission at z12. This design runs the
  interior test at z12 (interior at any zoom in [4,12]; edge only at 12).
  Either reading is correct; ours is cheaper at containment time. Confirm.
- **OQ-2 (RESOLVED, Phase 2b)**: covering `level` carried `admin_level`
  values until §3.4 (Phase 2) introduced the atgeo level vocabulary;
  `boundaries.duckdb` schema was unchanged in Phase 1 because
  `boundaries.py` (server) still read `admin_level`. Phase 2b
  (`docs/phase2b-design.md` Part A) completes this: `admin_level` is
  dropped from the exported schema entirely, `level` is derived from
  Overture `subtype` via the atgeo vocabulary (`docs/atgeo-appview-sdk-design.md`
  §1.7, stride-5 renumbered), and `boundaries.py` orders `ORDER BY level
  ASC, id ASC`.
- **OQ-3**: Phase-1 containment artifact lives under the timestamped run
  dir, moving to `<src>/containment/` in Phase 2. Confirm.
- **OQ-4**: covering is built inside `run_pipeline` (division branch) and
  self-healed by `ensure_covering` for other sources; no standalone CLI
  until Phase 2's subcommands. Confirm this is acceptable operationally.
- **OQ-5 (deviation)**: seed step uses a 256-row z4 tile table + SQL join
  instead of the spec's "Python seeds via Arrow" (pyarrow is not a
  dependency). Same seed set, D7 preserved. Confirm.
- **OQ-6 (RESOLVED 2026-07-06)**: DuckDB version skew. Verification is a
  Phase-0 gate, not a late canary — see §7.0 for the executed smoke tests
  and results. Local versions confirmed: `.venv` 1.5.1 (pinned dev
  version), legacy `venv` 1.2.1; every construct the design's SQL relies
  on passes on both. The macro test (7.1 item 1) remains as a regression
  guard, not as the discovery mechanism. Residual action: confirm the
  production DuckDB version before the global run and re-run the §7.0
  one-liners there if it is outside [1.2.1, 1.5.1].
- **OQ-7**: antimeridian-crossing boundaries currently store a
  world-spanning bbox (`ST_XMin/ST_XMax`), never `min > max`, so the D7
  branches in seed and edge arms are latent until §3.4 encodes lobes.
  They cost nothing and are tested with synthetic `min > max` rows.
- **OQ-8 (RESOLVED/MOOT 2026-07-06)**: Baseline capture dropped — containment
  never worked on the current production server, so there is no valid baseline
  to capture and no parity gate to run. Correctness rests on the test suite:
  brute-force oracle parity tests in `tests/test_containment_covering.py`.
  See Phase 0/1 amendments in §8 of `docs/pipeline-restructure-design.md`.
