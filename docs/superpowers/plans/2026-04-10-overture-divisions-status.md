# Overture Divisions Migration — Status Update

**Date**: 2026-04-10
**Branch**: `feat/quadtree`
**Plan doc**: `docs/superpowers/plans/2026-04-10-overture-divisions.md`
**Design files**: `docs/superpowers/plans/2026-04-10-phase2-design.md`, `docs/superpowers/plans/2026-04-10-phase3-design.md`, `docs/superpowers/plans/2026-04-10-phase4-design.md`

## Phase 1: Schema and Rename — COMPLETE ✓

**Committed as `9e41913`** on `feat/quadtree`.

Changes:
- Removed `level` property from `#relation` in `garganorn/lexicon/place.json`
- Updated rkey description to generic format, `within` description to "ordered broadest to most specific"
- Renamed `org.atgeo.places.overture` → `org.atgeo.places.overture.place` in 8 files (9 references)
- New test file: `tests/test_phase1_schema_rename.py` (6 tests)
- 611 tests pass (baseline was 605 + 6 new)

## Phase 2: Division Import Pipeline — COMPLETE ✓

**Committed as `23966c7`** on `feat/quadtree`.

Files created:
- `garganorn/sql/overture_division_import.sql` — two-parquet-path CTEs (division + division_area), ST_Union_Agg, bbox materialization, importance=0, empty variants
- `garganorn/sql/overture_division_export_tiles.sql` — bbox locations (north/south/east/west), attributes, no geometry

Files modified:
- `garganorn/quadtree.py` — SOURCE_PK, ATTRIBUTION, _coord_exprs, argparse choices, --division-parquet/--division-area-parquet args, importance/variants skip guard, boundary DB export (Hilbert sort, R-tree, atomic write)

Tests: `tests/test_overture_division.py` (12 tests)

**Known test cleanup** (cosmetic, not blocking):
- `test_overture_division.py`: fragile source-code inspection heuristic in `test_variants_skipped`, duplicate `test_overture_division_registered_in_source_pk`

## Phase 3: Containment Restructure — COMPLETE ✓

**Committed as `be85739`** on `feat/quadtree`.

Files modified:
- `garganorn/boundaries.py` — BoundaryLookup.COLLECTION → `org.atgeo.places.overture.division`, containment() returns rkey-only, queries `places` table
- `garganorn/quadtree.py` — compute_containment() restructured: `collection_prefix` parameter, `bnd` alias, `bnd.places` table, `id`/`geometry`/`admin_level` columns, rkey-only output, `DETACH bnd`
- `tests/conftest.py` — DIVISION_BOUNDARIES data, _create_division_db(), division_db_path fixture, boundary_lookup uses division DB
- `tests/test_boundaries.py` — name/level assertions removed, rkey prefix updated
- `tests/test_export.py` — _SF_WITHIN_JSON updated, assertions updated, wof_path→division_path renames, comment updates
- `tests/test_coord_exprs_bug.py` — _make_wof_db→_make_division_db, division schema, wof_path→division_path
- `tests/test_server.py` — mock updated to rkey-only division format

Tests: `tests/test_phase3_containment.py` (7 tests), 631 passed

## Phase 4: Division Database Class and Server Integration — COMPLETE ✓

**Committed as `8157507`** on `feat/quadtree`.

Files modified:
- `garganorn/boundaries.py` — `OvertureDivision(Database)` class added, `get_record()` only (no search); `connect()` loads spatial extension; `process_record()` parses `names` STRUCT at query time
- `garganorn/config.py` — `"overture_division": OvertureDivision` replaces `"wof": WhosOnFirst`
- `garganorn/quadtree.py` — boundary DB export enriched with names, subtype, country, region, wikidata, population, importance, variants columns
- `garganorn/__init__.py` — `OvertureDivision` exported, `WhosOnFirst` removed
- `tests/conftest.py` — WoF fixtures removed, `division_db` fixture added, `_create_division_db()` enriched with full schema and named-column INSERT
- `tests/test_boundaries.py` — `TestOvertureDivisionGetRecord` (9 tests) added, `TestWhosOnFirstGetRecord` removed
- `tests/test_config.py` — `test_overture_division_type_creates_overture_division` added, WoF test removed
- `test_config.yaml`, `test_config_missing_boundaries.yaml` — paths updated to `boundaries.duckdb`
- `tests/test_export.py` — stale WoF references cleaned up

Key implementation decisions:
- `connect()` loads spatial extension — required to open files with GEOMETRY columns even without ST_* function calls
- `process_record()` parses `names` STRUCT at query time — ignores pre-computed `variants` column (single source of truth)
- Bbox-only locations (no geo point) — divisions are areas, containment naturally skipped by server.py

Tests: 632 passed, 0 failed, 1 xfailed (baseline was 631)

## Key Design Decisions

- **Two parquet paths** for division import (not single glob) — division and division_area have different schemas
- **`ST_Union_Agg()`** for multi-area geometry aggregation
- **Bbox location fields**: `north`/`south`/`east`/`west` per `garganorn/lexicon/bbox.json`
- **Hilbert ordering** on boundary DB geometry for zone map effectiveness
- **R-tree only in top-level WHERE** — DuckDB 1.5 doesn't use spatial indexing on join conditions
- **WoF fixtures kept** through Phase 3, removed in Phase 4
- **Boundary DB enrichment** — add record-serving columns so OvertureDivision can serve full records as fallback
- **Names struct at query time** — single source of truth for variants, ignore pre-computed column

## Workflow Task Status

### Phase 1 (all complete)
- Design → review → gate (2 fix loops) → PASSED
- Baseline: 605 passed
- Red TDD (6 tests) → review → gate → PASSED
- Green TDD → review → gate → PASSED
- Final test: 611 passed → PASSED
- Requirements → Documentation → Doc review → Doc gate → PASSED
- **Committed as `9e41913`**

### Phase 2 (all complete)
- Design → review → gate (1 fix loop) → PASSED
- Baseline: 611 passed → PASSED
- Red TDD (12 tests) → review → gate → PASSED
- Green TDD → review → gate → PASSED
- Final test gate → PASSED
- Requirements gate → PASSED
- Documentation → Doc review → Doc gate → PASSED
- **Committed as `23966c7`**

### Phase 3 (all complete)
- Design → review → gate (1 fix loop) → PASSED
- Baseline: 611 passed → PASSED
- Red TDD (7 tests) → review → gate → PASSED
- Green TDD → review → gate → PASSED (3 fix loops: stale WoF refs in tests)
- Final test: 631 passed → PASSED
- Requirements → Documentation → Doc review → Doc gate → PASSED
- **Committed as `be85739`**

### Phase 4 (all complete)
- Design → review → gate → PASSED (3 fix loops: spatial extension, column order, fixture note)
- Baseline: 631 passed → PASSED
- Red TDD (10 tests) → review → gate → PASSED (2 fix loops: fixture infra, skip→fail)
- Green TDD → review → gate → PASSED (1 fix loop: stale WoF refs in tests)
- Final test: 632 passed → PASSED
- Requirements → Documentation → Doc review → Doc gate → PASSED
- **Committed as `8157507`**

## Test Count

- Baseline (before all phases): 605 passed, 1 xfailed
- After Phase 1 (`9e41913`): 611 passed, 1 xfailed
- After Phase 2 (`23966c7`): 623 passed, 1 xfailed
- After Phase 3 (`be85739`): 631 passed, 1 xfailed
- After Phase 4 (`8157507`): **632 passed, 1 xfailed** (net +27 tests across all phases)

## Status

All 4 phases complete and committed on `feat/quadtree`. Branch is ahead of `origin/feat/quadtree` by 5 commits (`9e41913` through `8157507`). Ready for PR to main when end-to-end integration is verified.
