# Phase 2 — Acceptance Plan and Follow-ups

Phase 2 (parquet artifacts + subcommand orchestrator) is implemented and the
full test suite is green. Phase 2 baseline: 916 passed, 1 xfailed, 0 failed
(`venv/bin/pytest`, DuckDB 1.2.1). With OQ-P2-5 (merged, `fd0ff8d`): 931 passed,
1 xfailed, 0 failed. Design reference: `phase2-artifacts-design.md` (§9
acceptance, §7.8 parity harness, §10 open questions).

This file is the checklist for taking Phase 2 from "tests pass" to "merged and
deployed."

---

## Acceptance results — PASSED (recorded 2026-07-09)

On-box acceptance is **complete**; both §9.1 and §9.2 passed. Phase 2 is
accepted. Note: Phase 2 (`69767f6`) was already squash-merged to `main` before
this run, so this was retroactive verification, not a merge gate.

**Environment**
- Host `garganorn-1`; interpreter `/opt/garganorn/venv/bin/python` (DuckDB
  **1.4.4** — the box, vs 1.2.1 in the local test env; §9.1 compares both
  commits on the same 1.4.4, so the comparison is internally consistent).
- Baseline commit `5328e4e` (Phase 1) vs candidate `69767f6` (Phase 2), run from
  a fresh clone `~/garganorn-accept`.
- **Pipeline entrypoint is `python -m garganorn.quadtree`** — `python -m
  garganorn` launches the Flask server, not the pipeline. Phase 1 uses a flat
  CLI (`--source …`); Phase 2 uses the `run` subcommand.

**Data**
- Overture release **`2026-06-17.0`** (places + `division` + `division_area`).
  The release originally cached on the box (`2026-03-18.0`) had aged off
  Overture's S3, so a current release was downloaded. This does not affect the
  refactor-parity verdict: §9.1 compares *identical inputs on both commits*.

**§9.1 — byte-comparability: ZERO diffs, both sources**
- `overture_division`: 33 records / 2 tiles. `overture_place`: 54,104 places,
  with containment (place tiles contained within division boundaries built in
  the same run). `tile_parity.py diff` = `OK: no differences` for both.
- **idf/density omitted on both commits** (empty tables → importance from
  idf/density = 0, symmetrically). This is forced: `5328e4e`'s `--idf-parquet`
  is a build-and-exit trigger, so a Phase 1 place *run* cannot consume a
  prebuilt idf. Sound because the scoring SQL is byte-identical between the two
  commits (`git diff 5328e4e 69767f6 --` over `overture_place_import.sql`,
  `overture_place_idf.sql`, `overture_division_import.sql`, `density_extract.sql`
  is empty), and for the SF bbox tiles stay under `max_per_tile`. Caveat:
  idf/density-driven ranking is therefore not exercised by this run.

**§9.2 — `kill -9` crash recovery: PASS**
- `run overture_place` (SF bbox), `kill -9` landed mid-import (log showed
  `import: starting` without `import: done`). The killed run left an **empty**
  `overture_place/` — no `places.parquet`, no `.meta.json`, no `.tmp` — i.e. no
  partial/fresh-looking artifact.
- Rerun of the identical command with **no manual cleanup and no `--force`**
  rebuilt from import (`import: starting` → `import: done`, did not skip-as-fresh)
  and completed. `tile_parity.py diff` against a never-killed control =
  `OK: no differences`.

Reproduce: `~/garganorn-accept` on the box, scripts `~/accept_91.sh` and
`~/accept_92.sh`.

---

## Prerequisite: the parity harness — DONE

`scripts/tile_parity.py` **now exists** (committed in `69767f6`, with
`tests/test_tile_parity.py`) and matches the spec below; it was used for the
acceptance above. This section is retained for reference. Original spec:

- CLI: `capture <tiles_dir> <out_dir>` and `diff <ref_dir> <tiles_dir>`.
- Canonical form per tile: gunzip → parse → sort `records` by `rkey` →
  `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`.
  (Within-tile sort is needed because Phase 1's `ORDER BY ta.tile_qk` leaves tie
  order unspecified; Phase 2 is deterministic via the `place_id` secondary sort.)
- `manifest.json` compared with `generated_at` removed.
- `manifest.duckdb` compared as `SELECT rkey, tile_qk ORDER BY rkey, tile_qk`.
- `diff` exits nonzero on any difference — zero tolerance (identical containment
  code on both sides; the Phase 1 SPATIAL-7 allowance does not apply here).
- Unit-test the canonicalizer: pairs differing only in record order / key order /
  manifest `generated_at` compare equal; any value difference is reported with
  tile + rkey.

The in-suite `kill -9` harness (`tests/crash_harness.py`) already exists and its
tests pass.

---

## Acceptance procedure

Run 9.1 and 9.2 in the same on-box session. Record the commit and data release
used.

### 9.1 — Byte-comparability vs Phase 1 (§9.1)

Confirms the Phase 2 refactor is a no-op on tile payloads (envelope unchanged;
`generated_at` lives only in manifest metadata).

1. **Capture the Phase 1 baseline.** At commit `5328e4e` (before merging Phase 2),
   run the current pipeline on the SF test bbox
   `-122.5137 37.7099 -122.3785 37.8101` for `overture_place` +
   `overture_division` with containment. `scripts/tile_parity.py capture` the
   tile dir. (Either cherry-pick the harness onto the `5328e4e` checkout, or run
   the harness from the Phase 2 worktree against a `5328e4e` data run — either
   way, record the commit + data release.)
2. **Diff after Phase 2.** Same inputs, via the `run` subcommand, then
   `tile_parity.py diff`. **Zero differences required** for both sources' tile
   sets, canonical tile bytes, manifest quadkey lists, and `manifest.duckdb` row
   sets.

### 9.2 — `kill -9` acceptance (§9.2)

Confirms crash recovery without sentinel/resume/manual cleanup.

- In-suite (already passing): the §7.7 subprocess test (crash at
  `import:mid-copy`) plus the state matrix.
- On the box (same session as 9.1):
  1. Start `run` for `overture_place`.
  2. `kill -9` it during the import COPY (visible in the logs).
  3. Rerun the identical command — no manual cleanup between kill and rerun.
  4. Assert: the rerun rebuilds from import (log inspection), completes, and
     `tile_parity.py diff` against a never-killed control run is clean.

### Sign-off — PASSED 2026-07-09

Acceptance passes when 9.1 shows zero diffs for both sources and 9.2's rerun is
clean against its control. **Both conditions met** (see "Acceptance results" at
the top). Phase 2 (`69767f6`) is already merged to `main`. OQ-P2-5 (serving-path
migration) below was originally scoped as a deploy blocker; examination (2026-07-09)
found it is **not** blocking — prod serves no tiles today, so there is no
regression to gate. It splits into a standalone garganorn code change (merged as
`fd0ff8d`) and a deferred infra follow-up.

---

## Follow-ups

**Next up (ordered):**
1. ~~**idf crash-safety**~~ — **implemented, pending merge.** `stage_idf` now
   writes via `.tmp` + `finalize_artifact` + `artifact_fresh({})`, mirroring
   `stage_density_extract`. Tests added: `TestStageIdfMetaSidecar` (5
   crash-safety tests) + 3 of the 5 `TestStageIdfMtimeCaching` tests updated to
   the meta-driven contract. Suite: 936 passed / 1 xfailed / 0 failed.
2. **Phase 2b** — OQ-P2-1 envelope amendment + OQ-P2-2 level vocabulary. The next
   feature batch; unblocked now that Phase 2 is merged. OQ-P2-2 has an on-box
   `SELECT DISTINCT subtype` precondition to check first.
3. **OQ-P2-5 infra follow-up** — Ansible `tiles:` wiring + the `config.yaml.j2`
   `type: overture` bug. Deferred until prod actually serves tiles; not blocking.

### OQ-P2-5 — serving-path migration — merged (`fd0ff8d`, 2026-07-09)

Full plan: `docs/oq-p2-5-serving-path-design.md`. Tests green: 931 passed,
1 xfailed. Landed on `main` at `fd0ff8d` via the red/green/review pipeline.

**What is implemented:** `garganorn/__main__.py` now serves tiles via
`/tiles/<slug>/<path:tile_path>`. The slug→tiles_dir map is built from config at
startup; unknown slug, missing file, or path traversal returns 404.
`Cache-Control: public, max-age=<cache_ttl>` (no `immutable`) is set when
`cache_ttl` is configured. `create_app` raises `ValueError` if a collection's
`base_url` doesn't end with `/<slug>`. Global `serve_dir` is removed.
`config.yaml` was updated to the new schema: per-collection `slug`, `cache_ttl:
86400`, `base_url` ending in `/<slug>`, and `manifest`/`tiles_dir` using the
`<source>/tiles/current` Phase 2 layout.

**Still deferred (infra follow-up — when we serve tiles in prod):** add a
`tiles:` block to `roles/garganorn/templates/config.yaml.j2` and an on-demand
`tiles.yml` Ansible task. Also fix the latent pre-existing bug there:
`config.yaml.j2` emits `type: overture` but `config.py` `DATABASE_TYPES` only
has `overture_place`, so enabling `garganorn_source_overture` crashes boot.
One-time manual cleanup of old `<source>/<timestamp>/` run dirs on production
when infra lands.

### Correctness — schedule soon

- **idf crash-safety (§3.1) — fixed, implemented pending merge.** Bug: `stage_idf`
  wrote `idf.parquet` in-place via `COPY` and gated freshness on `_is_output_fresh`
  (no `.meta.json`). A `kill -9` mid-COPY left a partial `idf.parquet` whose mtime
  read as fresh, so the next run silently reused corrupt IDF scores. Density was
  migrated to `.tmp` + `finalize_artifact` in Phase 2; idf was missed.
  Fix: `stage_idf` now writes via `.tmp` + `finalize_artifact` + `artifact_fresh({})`
  mirroring `stage_density_extract`. Tests added: `TestStageIdfMetaSidecar` (5
  crash-safety tests) + 3 of the 5 `TestStageIdfMtimeCaching` tests updated to the
  meta-driven contract. Suite after fix: 936 passed / 1 xfailed / 0 failed.

### Phase 2b (follow immediately after Phase 2 merges; not gated on anything)

- **OQ-P2-1 — envelope amendment.** Adopt the §3.8 record envelope (`atgeo: 1`,
  per-tile `generated_at`, `{uri, cid, value}` wrapping) and manifest §1.3 fields.
  Deferred from Phase 2 specifically to keep the byte-comparability acceptance a
  clean no-op. Ships in Phase 2b.
- **OQ-P2-2 — level vocabulary.** `boundaries.duckdb` keeps `admin_level` through
  Phase 2; the level-vocabulary change lands in the same Phase 2b change set. Open
  precondition: an on-box `SELECT DISTINCT subtype` verification.

### Lower priority / opportunistic

- **`stage_export` spill location.** Export derives its own spill dir
  (`<run_dir>.spill`) instead of honoring a configured `temp_directory`. Every
  other stage threads `temp_directory` (fixed for covering/idf/density this
  session). Consider giving `stage_export` a `temp_directory` param so spill can
  target the operator's scratch NVMe. Minor §3 connection-discipline
  inconsistency, not a correctness bug.
- **Pre-existing test-coverage gaps (predate Phase 2, not regressions).** No test
  asserts the `boundaries.duckdb` `admin_level` filter correctness
  (`BETWEEN 0 AND 2 OR subtype = 'locality'`) or the Hilbert sort order. Add
  assertions next time `stage_division_import` is touched.

### Carried to Phase 4 / later (per spec §11)

- OQ-P2-8: FSQ source disposition (pin-or-drop, HD-3) — the importer is converted
  either way; whether it runs on real data awaits the decision.
- `COVER_MAX_ZOOM` sizing, manifest sharding, static `getRecord`.
