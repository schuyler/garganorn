# Pipeline Status

Pipeline development is complete. This document captures what to know when
testing and operating the pipeline: known limitations, explicit tradeoffs,
things to watch, and fixes still wanted.

The full audit was conducted 2026-04-14. All critical and important findings
were fixed in commit 24d57b9. What remains is documented here.

The pipeline was later restructured (2026-07, see
`pipeline-implementation-decisions.md`) to pass data between stages as
file-based parquet artifacts instead of a shared working DuckDB. Three
operational changes worth knowing:

- **Subcommands.** `python -m garganorn.quadtree` now takes a subcommand — `run`
  for one source, `all --config` for everything, plus `density`, `idf`, and
  `covering` for the shared artifacts. The old flag-only invocation is gone.
- **Layout and caching.** Tiles moved to `<output>/<source>/tiles/<timestamp>/`
  with a `<source>/tiles/current` symlink. Each stage writes a `.parquet`
  artifact (`places`, `tile_assignments`, `containment/`) guarded by a
  `.meta.json` sidecar and skips itself when that artifact is still fresh;
  `--force` rebuilds.
- **Crash recovery.** There is no sentinel table or resume state anymore. After
  a `kill -9`, re-running finds any half-written stage stale (via its `.tmp` or
  meta) and rebuilds it from the start. Serving is never affected mid-run —
  `current` only swaps once a run's `manifest.json` lands. (`stage_idf` was
  missing `.tmp`+`finalize_artifact` at Phase 2 merge; fixed in `b2aae95`.)

OQ-P2-5 (serving-path migration) is merged (`fd0ff8d`). The tile route
is `/tiles/<slug>/<path>` (`__main__.py`), and `config.yaml` uses the Phase 2
`<source>/tiles/current` layout with per-collection `slug` and `cache_ttl`.

Phase 2b (record envelope + level vocabulary) is merged (`af3c9dd`,
2026-07-10). Tiles are served in the atgeo v1 envelope
(`{atgeo:1, collection, attribution, generated_at, records:[{uri, cid, value}]}`).

**Tiles are in production.** The Ansible infra wiring (`atgeo-server-config`)
is deployed, and the first real CONUS tile build (foursquare, overture-place,
osm; overture_division is pipeline-only, not served) went live on
`places.atgeo.org` on 2026-08-03. `getCoverage` (nsid `org.atgeo.getCoverage`)
and the `/tiles/<slug>/<path>` route are both serving verified traffic for all
three collections.

---

## Fixes Still Wanted

### Antimeridian-spanning features dropped (DATA-4)

Features crossing the ±180° meridian are silently dropped by bbox validation.
This affects Pacific data (Russia east, Fiji, Antarctica). Fix requires
detecting `xmin > xmax` as antimeridian crossing and using OR logic in bbox
filters. Not architecturally hard, just needs doing.

**Impact**: Missing places in affected regions. Test with Pacific data.

### Debug print statements in production (EXPORT-11)

`print()` calls remain in production code paths. Replace with `logging` or
remove.

**Impact**: Noise in server logs. Quick fix.

### `stage_export` doesn't honor `temp_directory` (EXPORT-14)

`stage_export` (`garganorn/stages.py`) hardcodes its DuckDB spill directory as
`<run_dir>.spill` instead of accepting a `temp_directory` param like every
other stage (covering/idf/density all do). Minor connection-discipline
inconsistency, not a correctness bug.

**Impact**: Can't redirect export's spill I/O to a different disk without
redirecting the whole run directory.

---

## Known Limitations

### Density is approximate near division edges (SPATIAL-3)

Division locality density uses a bbox-overlap join between z15 density tiles
and locality bounding boxes. Tiles that barely touch a locality's bbox are
included in the density average, causing slight over-estimation near edges.

This is an explicit tradeoff: the alternative (centroid-based join) was tried
first and failed because small localities may not contain any tile centroid,
producing zero density scores. Weighted intersection area would be more
accurate but requires expensive `ST_Intersection` per tile per locality.

**Impact**: Negligible. Density is one component of a composite importance
score. A few percent noise on density translates to small ranking effects.

### No R-tree on phase-2 containment temp table (SPATIAL-6)

The containment pipeline materializes boundary candidates to a DuckDB temp
table (`tile_boundaries`). DuckDB temp tables cannot have R-tree indexes.
Phase 2 per-point `ST_Contains` runs as a sequential scan against these
candidates.

This is mitigated by the pre-filter + clip optimization (commit history on
feat/quadtree), which narrows candidates from ~1M boundaries to ~2K per tile
via `ST_Intersects` with R-tree in a top-level WHERE clause, then clips
geometries to the tile envelope to reduce vertex counts. The quadtree
subdivision further limits candidates per leaf tile.

**Impact**: Watch per-tile timing during end-to-end pipeline runs. If dense
tiles (many small localities) are slow, this is where to look. The fix would
be using a persistent table with R-tree instead of a temp table.

### `strip_json_nulls` vulnerable to special key characters (EXPORT-4)

The custom `strip_json_nulls()` function may fail on JSON keys containing
`{`, `}`, `"`, or `,`. No known failures in production.

**Impact**: Waiting on DuckDB native `json_strip_nulls()` (PR #21748). Add
validation for special characters if it becomes an issue.

### Non-Latin script search returns empty results (EXPORT-1/12)

The trigram search system generates trigrams from Latin-script text. Queries
in CJK, Arabic, Hebrew, Cyrillic, Thai, etc. produce no trigrams and return
empty results. A guard was added to prevent the database error that occurred
previously.

If `searchRecords` is removed (see below), this limitation disappears entirely.

---

## Observability Gaps

These don't affect correctness but make it harder to diagnose problems.
Consider adding logging next time you touch the relevant import paths.

### Negative population values silently accepted (DATA-2)

Overture divisions may have negative population values. The import clamps them
to 0 for scoring, but doesn't log how many are affected.

### Maritime divisions dropped by `is_land=true` filter (DATA-5)

The Overture division import filters `is_land=true`, excluding bays, straits,
seas. This may be intentional. No logging on how many divisions are dropped.

### Overture `categories.primary` NULL rate unknown (DATA-6)

No logging on how many places lack `categories.primary`. Affects IDF
computation since unnesting NULL category arrays silently drops the row
(design-constraints.md D5).

### OSM way node reference resolution quality unknown (DATA-11)

No logging on how many OSM way node references fail to resolve during centroid
computation. QuackOSM handles this internally.

---

## Cosmetic Items

These don't affect functionality. Fix opportunistically or not at all.

- **SCORE-7**: Importance can exceed 100 with non-default norm constants
- **SCORE-8**: Corrupted density/IDF values can produce negative importance
- **SCORE-9**: IDF `ln(N/0)` theoretically possible (trigram filtering prevents it)
- **SPATIAL-7**: Points exactly on boundary edges assigned arbitrarily
- **SPATIAL-8**: `ST_Union_Agg` may cause memory pressure during export
- **DATA-7**: Overture names struct may contain empty arrays/objects instead of NULL
- **DATA-9**: OSM nodes at exact coordinate boundaries (±90°, ±180°) may produce invalid bboxes
- **DATA-10**: Empty/whitespace-only names not filtered during import
- **DATA-12**: No explicit coordinate range validation (overlaps SPATIAL-1, already fixed)
- **EXPORT-5**: Coordinate precision DECIMAL(10,6) is ~0.1m, may not match GeoJSON standard
- **EXPORT-9**: Variant deduplication allows same name with different metadata
- **EXPORT-10**: Linear scan for record lookup within tiles (tiles are small)
- **EXPORT-13**: Coordinate range validation in tile assignment (overlaps SPATIAL-1)
- **SPATIAL-9**: No test asserts the division-import Hilbert sort order (`ST_Hilbert` in `stages.py`) is actually applied; the admin-level/level-vocab filter itself is tested (`test_phase3_containment.py`), the sort isn't.

---

## Remaining Restructure Work

Phases 1, 2, and 2b of the pipeline restructure are merged and deployed (see
`pipeline-restructure-design.md` for the phase plan). What's left:

- **Phase 3 — server removals**: `searchRecords`/`getCoverage` removal, the
  trigram/JW/search machinery, `name_index`. Zero-user cleanup, no urgency.
- **Phase 4 — global validation**: the full validation plan against
  worldwide data. The 2026-08-03 production deploy is a CONUS-bbox build,
  not global — Phase 4 is still open. Tentative sequencing (2026-07-09):
  Phase 4 before Phase 3.
- **OQ-P2-8**: FSQ source disposition (pin release or drop) — undecided;
  the importer works either way.
- **Deferred, no urgency**: `COVER_MAX_ZOOM` sizing, manifest sharding,
  static `getRecord`.

---

## SearchRecords and the Trigram Pipeline

The `searchRecords` endpoint and its supporting infrastructure (trigram index,
Jaro-Winkler scoring, `_strip_accents`, `name_index` table) are under
evaluation for removal. If removed:

- Non-Latin name search limitation disappears
- SCORE-6/7/8/9 become moot
- ~400+ lines of `database.py` removed
- `name_index` table removed from schema
- Trigram generation removed from import

Clients would need an alternative search mechanism. Decision pending.

---

## Design Constraints Reference

See `design-constraints.md` for the full reference of DuckDB behaviors,
pipeline invariants, and normalization constants. Key points:

- R-tree indexes only activate in top-level WHERE clauses (D1)
- Zone maps require sorted columns in parquet (D2)
- CTAS is fast, UPDATE/ALTER TABLE is slow (D3)
- `unnest()` on NULL arrays silently drops the row (D5)
- All spatial indexing uses quadkeys at z17 (P1)
- Containment is a precomputed covering joined by quadkey prefix, not per-tile recursion (P6)

## Approaches Explored and Discarded

See `explored-and-discarded.md` for the full list. Notable: S2 spatial
indexing, pipeline framework, QuackOSM for OSM import, separate IDF build
stage, Double Metaphone phonetic index, ART index for trigrams, WoF venue
import.
