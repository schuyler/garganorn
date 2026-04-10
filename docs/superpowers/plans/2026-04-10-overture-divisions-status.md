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

## Phase 2: Division Import Pipeline — PENDING DOCS + CONFIRM

**Implementation complete, not yet committed.** Final test gate PASSED. Requirements gate PASSED. Documentation in progress.

Files created:
- `garganorn/sql/overture_division_import.sql` — two-parquet-path CTEs (division + division_area), ST_Union_Agg, bbox materialization, importance=0, empty variants
- `garganorn/sql/overture_division_export_tiles.sql` — bbox locations (north/south/east/west), attributes, no geometry

Files modified:
- `garganorn/quadtree.py` — SOURCE_PK, ATTRIBUTION, _coord_exprs, argparse choices, --division-parquet/--division-area-parquet args, importance/variants skip guard, boundary DB export (Hilbert sort, R-tree, atomic write)

Tests: `tests/test_overture_division.py` (12 tests)

**Remaining**: Documentation (in progress) → Doc review → Doc gate → Confirm before merge

**Known test cleanup needed** (cosmetic, not blocking):
- `test_overture_division.py`: fragile source-code inspection heuristic in `test_variants_skipped`, duplicate `test_overture_division_registered_in_source_pk`

## Phase 3: Containment Restructure — GREEN FIX #2 DONE, PENDING GATE + REMAINING

**Implementation complete. All 631 tests pass (0 failures).** Green fix #2 applied (test_server.py mock, stale variable names, stale comments). Pending re-review gate, then final test gate and remaining pipeline.

Files modified:
- `garganorn/boundaries.py` — BoundaryLookup.COLLECTION → `org.atgeo.places.overture.division`, containment() returns rkey-only, queries `places` table
- `garganorn/quadtree.py` — compute_containment() restructured: `collection_prefix` parameter, `bnd` alias, `bnd.places` table, `id`/`geometry`/`admin_level` columns, rkey-only output, `DETACH bnd`
- `tests/conftest.py` — DIVISION_BOUNDARIES data, _create_division_db(), division_db_path fixture, boundary_lookup uses division DB
- `tests/test_boundaries.py` — name/level assertions removed, rkey prefix updated
- `tests/test_export.py` — _SF_WITHIN_JSON updated, assertions updated, wof_path→division_path renames, comment updates
- `tests/test_coord_exprs_bug.py` — _make_wof_db→_make_division_db, division schema, wof_path→division_path
- `tests/test_server.py` — mock updated to rkey-only division format

Tests: `tests/test_phase3_containment.py` (7 tests)

**Remaining**: Green gate #2 (pending re-review #2 results already in) → Final test gate → Requirements gate → Documentation → Doc review → Doc gate → Confirm

### Green fix history
- Fix #1: Updated 11 failing tests (test_coord_exprs_bug.py 2, test_export.py 9) — fixtures to division schema, assertions to rkey-only
- Re-review #1: FAIL — test_server.py mock not updated, stale wof_path/wof_conn variable names
- Fix #2: Updated test_server.py mock, renamed stale variables, updated stale comments
- Re-review #2: FAIL — remaining stale wof_path in test_coord_exprs_bug.py (2nd test), stale heading/docstring in test_export.py
- Fix #3 (applied directly): Renamed wof_path→division_path in test_coord_exprs_bug.py, updated heading and docstring in test_export.py

## Phase 4: Division Database Class and Server Integration — DESIGN IN REVIEW

**Design complete at `~/.claude/plans/phase4-design.md`.** Design review found 2 critical (missing __init__.py and test_config.py in removal scope) and 4 important issues. Fix #1 applied. Re-review #1 found 2 more issues (shared fixture note needed). Fix #2 applied (shared fixture callout added to design). Pending final design gate.

Key design decisions:
- `OvertureDivision(Database)` class in `boundaries.py` — get_record() only, no search
- Boundary DB enriched with record-serving columns (names, subtype, etc.)
- Names struct parsed at query time (single source of truth, ignore pre-computed variants column)
- WhosOnFirst removed (class, config, fixtures, tests)
- `garganorn/__init__.py` updated (WhosOnFirst→OvertureDivision)
- `tests/test_config.py` updated (wof test→overture_division test)
- `test_config.yaml` / `test_config_missing_boundaries.yaml` paths updated
- Shared `_create_division_db()` fixture preserved for both boundary_lookup and division_db

Files to modify: `garganorn/boundaries.py`, `garganorn/config.py`, `garganorn/quadtree.py`, `garganorn/__init__.py`, `tests/conftest.py`, `tests/test_boundaries.py`, `tests/test_config.py`, `test_config.yaml`, `test_config_missing_boundaries.yaml`

**Remaining**: Design gate #1 → Baseline test gate (depends on Phase 3 final test) → Red TDD → review → gate → Green TDD → review → gate → Final test → Requirements → Documentation → Doc review → Doc gate → Confirm

## Key Design Decisions

- **Two parquet paths** for division import (not single glob) — division and division_area have different schemas
- **`ST_Union_Agg()`** for multi-area geometry aggregation
- **Bbox location fields**: `north`/`south`/`east`/`west` per `garganorn/lexicon/bbox.json`
- **Hilbert ordering** on boundary DB geometry for zone map effectiveness
- **R-tree only in top-level WHERE** — DuckDB 1.5 doesn't use spatial indexing on join conditions
- **WoF fixtures kept** alongside division fixtures in conftest.py through Phase 3 (WoF removal in Phase 4)
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

### Phase 2
- Design → review → gate (1 fix loop) → PASSED
- Baseline: 611 passed → PASSED
- Red TDD (12 tests) → review → gate → PASSED
- Green TDD → review → gate → PASSED
- Final test gate → PASSED (12/12 Phase 2 tests pass, 11 failures are Phase 3)
- Requirements gate → PASSED (all 14 requirements met)
- **IN PROGRESS**: Documentation (task #3) → Doc review (#4) → Doc gate (#5) → Confirm (#6)
- Known cleanup: fragile test heuristic + duplicate test in test_overture_division.py

### Phase 3
- Design → review → gate (1 fix loop) → PASSED
- Baseline: 611 passed
- Red TDD (7 tests) → review → gate → PASSED
- Green TDD: implementation done → review → gate FAILED (11 test failures)
- Green fix #1: 11 tests fixed → re-review #1 → gate FAILED (test_server.py mock)
- Green fix #2: test_server.py + stale names → re-review #2 → gate FAILED (remaining stale refs)
- Green fix #3: applied directly (stale wof_path, heading, docstring) — **needs re-review**
- **NEXT**: Green gate (needs re-review of fix #3) → Final test (#10) → Requirements (#11) → Doc (#12-14) → Confirm (#15)

### Phase 4
- Design → review → gate FAILED (missing __init__.py, test_config.py)
- Design fix #1 → re-review #1 → gate FAILED (shared fixture note)
- Design fix #2 applied — **needs re-review**
- **NEXT**: Design gate #1 (#34) → Baseline test (#19) → Red TDD (#20) → ... → Confirm (#31)

## Test Count

- Baseline (before all phases): 605 passed, 1 xfailed
- After Phase 1 commit: 611 passed, 1 xfailed
- Current (Phase 2+3 uncommitted): **631 passed, 0 failed, 1 xfailed**
- Target: 640+ passed, 0 failed (611 baseline + ~30 new tests across phases 2-4)

## Files to Resume With

- Plan doc: `docs/superpowers/plans/2026-04-10-overture-divisions.md`
- Phase 2 design: `~/.claude/plans/phase2-design.md`
- Phase 3 design: `~/.claude/plans/phase3-design.md`
- Phase 4 design: `~/.claude/plans/phase4-design.md`
- This status file: `docs/superpowers/plans/2026-04-10-overture-divisions-status.md`

## Active Task List (for resuming)

Phase 2: Documentation (#3 in progress) → Doc review (#4) → Doc gate (#5) → Confirm (#6)
Phase 3: Green gate needs re-review of fix #3 → Final test (#10) → Requirements (#11) → Doc (#12-14) → Confirm (#15)
Phase 4: Design gate needs re-review of fix #2 → Baseline test (#19) → full pipeline through Confirm (#31)

Phase 3 requirements (#11) can run in parallel with Phase 3 final test (#10) once green gate passes.
Phase 2 docs and Phase 3 green gate are independent and can proceed in parallel.
