# Cleanup punch list

What is left to do to bring the code, the lexicons, and the documentation into
line with the decisions the project has actually made.

Items carry enough context to act on and no more. Design rationale belongs
elsewhere — why the gazetteer serves no search endpoint is in
`tile-privacy-design.md`, what the tile format is is in `atgeo-spec.md`.
Restating that reasoning here is how the last planning document grew into a
schedule that reality diverged from.

**Delete this file when every box is checked.**

---

## P1 — Run-stamped tile URLs

No re-export needed; the run directories already exist on disk. Land before P6.

- [x] Tile URLs embed the run version; `getCoverage` returns stamped URLs
- [x] Tiles serve `Cache-Control: public, max-age=604800, immutable`
- [x] `tile_dirs[slug]` roots at `{source}/tiles/` so prior runs stay servable
- [x] Keep the `{source}/tiles/` nesting — it keeps `places.parquet`,
      `containment/`, and `boundaries.duckdb` out of the web-served tree
- [x] Retention stays count-based, keep-two-runs; no time-based retention and
      no startup assertion
- [x] Rename `pipeline.output` from `tiles` to `data`, giving
      `data/{source}/tiles/{stamp}/`, in `config.yaml.example` and the README.
      The spec's serving-layout section needs no edit — it writes the path as
      `{output_dir}`
- [x] Move `{garganorn_home}/tiles` to `{garganorn_home}/data` on the server
      and rebase the Ansible role defaults and template onto `data/`. The
      top-level `boundaries:` key is unaffected by this move — it points at
      `db/wof-boundaries.duckdb`, outside the tiles tree

**Accept:** two consecutive runs produce disjoint URL sets; the prior run's
URLs still resolve after a new build; a request resolving to `places.parquet`
returns 404.

## P2 — Ansible and deploy

Land before P6's rebuild, or the rebuild will not take effect.

- [x] Tile build task notifies `restart garganorn`
- [x] Ansible handlers are not tag-filtered; they fire whenever notified,
      independent of `--tags`. No `tags: always` needed — the actual gap is
      the previous bullet, the tile pipeline task's missing
      `notify: restart garganorn`
- [x] Replace the "has never been timed on the box" comment on the `async`
      kill-ceiling in `defaults/main.yml` (currently 43200) with the measured
      ~90 minutes. The value is a kill-ceiling, not a build cadence, and must
      not be lowered — doing so would kill planet-scale builds. There is no
      such value in `host_vars/garganorn-1.yml`
- [x] Reconcile `garganorn_bbox` and `garganorn_tile_bbox`. They are separate
      variables with different defaults — `garganorn_bbox` (empty default)
      drives the import stage in `import.yml`; `garganorn_tile_bbox` (CONUS
      default in `defaults/main.yml`) drives the pipeline `bbox` in
      `config.yaml.j2`. Setting only the first renders a CONUS tile build while
      appearing to be configured for planet. Either give them one name or make
      the tile variable default to the import one
- [x] Remove the Foursquare find/assert block and `garganorn_source_fsq`.
      `garganorn_source_wof` is confirmed dead too (garganorn's own pipeline
      builds `boundaries.duckdb` from Overture Division, not WoF; no code
      reads a `boundaries:` config key) and was removed alongside it, along
      with the wholly-dead `databases:` config key in `config.yaml.j2`
      (garganorn's `config.py` hasn't read it since P4)
- [x] `config.yaml.j2` updated for P1's versioned `base_url` and P6's
      attribution keys
- [x] Nothing in the role populates the source cache. `tiles.yml` assumes
      Overture and OSM parquet are already present, so a build against an
      empty or partial cache produces empty collections. Either make
      `download-overture.sh` a role task or make it a prerequisite the role
      checks. Pairs with the glob preflight in P8
- [x] `{{ garganorn_home }}/data` is created only as a side effect of the
      temp-directory task in `tiles.yml`, so `--tags app,service` alone leaves
      it missing. Make it an explicit task

**Accept:** a `--tags tiles` run leaves the service restarted, and
`getCoverage` reflects the new run with no manual step. Provisioning a fresh
host and running `--tags tiles` produces a populated tileset with no manual
step at all.

## P3 — Lexicon conformance

- [x] `getRecord.json` output `uri`: `format: at-uri` → `format: uri`
- [x] Declare `source`/`license` (required) and `importance` (optional) in
      `getRecord`'s output schema
- [x] `place.json`: add `$type` and `importance` (not `collection` — see
      below)
- [x] `place.json`: delete the `#ref` def — it exists only for the check-in
      write path, which is out of scope (see below)
- [x] Delete the local `hthree.json` lexicon file; it duplicates
      `community.lexicon.location.hthree`, already bundled via lexrpc. Keep
      both union references to it in `place.json`
- [x] Delete `fsq.json` (already absent — nothing to do)
- [x] Add a test asserting `getRecord`'s output validates against
      `getRecord.json`'s schema, so an `at-uri`-style regression fails the
      suite
- [ ] The tile-payload header shape (`collection`, `source`, `license`,
      `generated_at`, from `envelope.py:build_tile_payload`) has no lexicon
      or schema covering it. `collection` was mistakenly folded into the
      `place.json` item above during scoping — it's a tile-payload field,
      never a record field, so `place.json` is the wrong place for it. Real
      gap, deferred; not a spec edit

`published_at` stays declared though nothing produces it — a deliberate
exception, not an oversight. `same_as` and `relation.name` stay declared for
the same reason: future work on identifying conflations between datasets,
with room to expand to other place relationships. `getRecord.json`'s `cid`
parameter and output field also stay declared but unimplemented: a CID is a
pure content hash, so version selection by `cid` is buildable later (at
minimum across the two tile runs retention already keeps) without needing
garganorn records to live in a real PDS/MST. `_query` stays in responses and
stays undocumented.

**Accept:** live XRPC `getRecord` returns 200; the schema test fails if the
`at-uri` regression is reintroduced.

## P4 — searchRecords and the serving-DB layer

- [x] Delete `searchRecords`: method, registration, lexicon, tests
- [x] Delete `listRecords`: method and lexicon
- [x] Delete the trigram, Jaro-Winkler, `_strip_accents`, and `name_index`
      machinery from `database.py` and from the import SQL
- [x] Delete the serving half of `database.py` — `nearest()`,
      `process_record`, `Database.get_record` — keeping what `stages.py:31` and
      `quadtree.py:9` import
- [x] Delete `garganorn/boundaries.py` and the serving-time relations
      computation at `server.py:88-110`, which overwrites the tile's
      precomputed `relations.within`
- [x] Remove `databases:` from config, `DATABASE_TYPES` from `config.py`, and
      `self.db` from `Server`
- [x] Remove `design-constraints.md` entries Q1 and Q2, and the
      `JW_THRESHOLD`, `JW_TOKEN_ALPHA`, and `IMPORTANCE_FLOOR_K` rows from its
      constants table

`boundaries.duckdb`, the pipeline artifact, is unaffected — only the
serving-time `BoundaryLookup` class goes.

**Accept:** full suite green; `getRecord` returns the tile's own `relations`
unmodified; no imports of removed symbols remain.

## P5 — Foursquare removal

- [x] Delete `FoursquareOSP`, `sql/foursquare_import.sql`,
      `sql/foursquare_export_tiles.sql`, `scripts/import-fsq-extract.sh`, the
      `fsq` branch of `scripts/build-density.sh`, `docs/foursquare.md`, and the
      foursquare config entries. (`scripts/build-density.sh` does not exist in
      this repo — nothing to remove there.)

## P6 — Envelope change

Forces a full re-export. Land P1 and P2 first.

- [x] Remove the `atgeo` version key from tile payloads, `manifest.json`,
      `manifest.duckdb` metadata, and the test assertions pinning the five-key
      envelope
- [x] Replace `attribution` with sibling keys `source` and `license`
- [ ] Both changes ship in one re-export

## P7 — Documentation

- [ ] Schuyler reviews `atgeo-spec.md` intensively, including re-deriving the
      `importance` formula now that its citation (`foursquare_import.sql:45-48`)
      is dead. No piecemeal edits to it until he has
- [ ] State the user-safety principle once, prominently, and reference it
      rather than re-deriving it at each error condition. Blocked on the spec
      review above
- [x] Rewrite the README: no searchRecords, no Foursquare, worked examples
      actually run against overture/osm, `getCoverage` + tile fetch as the
      interface, remove the dead `docs/s2_duckdb_design.md` link, and state
      which collections are served and that the server has no users
- [x] Delete `atgeo-execution-plan.md`
- [x] Dissolve `pipeline-status.md`: durable tradeoffs to `design-constraints.md`,
      open items here, the rest deleted
- [x] Promote `tile-privacy-design.md` out of "Feature Specs" in `index.md`
- [x] `getCoverage.json`'s description carries the reason there is no `q`
      parameter
- [x] Record the licensing posture in the repo — folded into
      `design-constraints.md` ("Licensing Posture") rather than a standalone
      doc; the attribution mechanism itself is already stated in
      `atgeo-spec.md`
- [x] Delete `pipeline-restructure-design.md`
- [ ] Write reference material for the pipeline artifacts — what each stage
      writes, the schemas, the sort orders — from the code rather than from
      the deleted design document
- [ ] Repoint or remove the ~212 `§N` cross-references in `garganorn/` (18)
      and `tests/` (194) across 28 files. They point into `atgeo-spec.md` and
      the deleted `pipeline-restructure-design.md`, neither of which has
      numbered sections any more
- [ ] Find every place in the code that references a non-authoritative design
      document and remove the reference
- [ ] Nothing verifies the `file.py:line` citations in `docs/`. `atgeo-spec.md`
      claims they "fail visibly when a statement stops being true," but two were
      found stale by inspection. Either check them in CI or stop claiming they
      self-verify
- [ ] Delete `lexicon-discovery-plan.md`, after correcting its
      `explored-and-discarded.md` entry, which says "not yet adopted" but the
      lexicon-schema serving in `getRecord` shipped and the `listRecords` half
      is being reverted
- [ ] Strip the dead `Explored in` pointers from
      `explored-and-discarded.md` and drop the field; the summaries are
      self-contained
- [ ] Assess `compute-containment-oom-fix.md` — likely a completed-work
      artifact in the same category
- [ ] Reconcile `atgeo-appview-sdk-design.md` §1.2 and §1.3 with the shipped
      envelope and manifest: no `atgeo` version field or version-rejection
      rule, `source`/`license` in place of `attribution`, a `generated_at`-only
      manifest that is not served over HTTP, and stamped tile URLs
- [ ] `name-variants-design.md` (listed as a live implementation spec in
      `index.md`) is stale beyond a quick patch: it names both
      `scripts/import-osm.sh` and `scripts/import-overture-extract.sh` (§3.1,
      §3.2), both deleted in the P8 dead-script sweep, and still describes the
      removed `name_index` table (`is_variant`, `norm_name`, trigram columns)
      and FSQ as a source. Needs a full reconciliation pass, not a rename of
      the two script references

`atgeo-appview-sdk-design.md` and `org.atgeo.tiles.service.json` stay, framed
as aspirational.

## P8 — Validation and small cleanups

- [ ] Light validation of the global build: per-collection record counts
      against source parquet; tile count and total gzipped size per source;
      `getCoverage` + fetch in scattered regions with a plausibility check; and
      confirmation that every URL `getCoverage` returns resolves
- [x] Remove the compatibility shim at `tile_reader.py:41-49` — its stated
      removal condition (first re-export after the envelope deploy) is met
- [x] Remove the dead `tiles.max_per_tile` config key
- [x] Remove `print()` calls from production paths (already zero; nothing to
      do)
- [ ] Assert the division-import Hilbert sort is actually applied
      (`ST_Hilbert` in `stages.py`); nothing tests it today. See D2
- [ ] Preflight the configured source globs before a build runs. `config.yaml`
      globs `db/cache/overture/*/part-*.parquet`; when the cache holds only
      divisions the glob matches nothing and the pipeline produces an empty
      collection with no error. `download-overture.sh` verifies per file
      against the S3 manifest, so the gap is not in downloading — nothing
      checks that what the config asks for exists before building on it
- [ ] Filter empty and whitespace-only names at import; they currently reach
      tiles as blank-named records
- [ ] Sweep the suite for tests that enforce nothing — ones asserting against
      their own fixtures, or exercising code that no longer exists. Find them
      by mutating the source and seeing what stays green. Known instances:
      `tests/test_audit_spatial_processing.py`; six classes in
      `tests/test_import_pipeline.py` (`TestNodeImport`, `TestTagFiltering`,
      `TestWayCentroidResolution`, `TestOutputSchema`, `TestDensityOsmMode`,
      `TestInlineIdfOsmMode`) that reference nonexistent
      `build-density.sh`/`build-idf.sh` scripts and test hand-copied SQL
      disconnected from production code
- [ ] `tests/test_pipeline.py`'s assertion failure messages (around lines
      434, 437) say `tiles.memory_limit`/`tiles.max_per_tile`, but the test
      itself correctly exercises the live `pipeline:` config section — just
      wrong strings in the error text, not a test-correctness bug
- [ ] Evaluate once the removals above are done: should maritime divisions
      keep being dropped by the `is_land=true` filter, which excludes bays,
      straits, and seas? It is a completeness question, not a logging one
- [x] Evaluate once the removals above are done: does `IDF ln(N/0)` survive
      the trigram removal, or was it only reachable through the search path?
      It survives: `stage_idf` computes IDF from live place data via
      `GROUP BY category`, which can't produce `n_places=0`, and it feeds the
      production importance formula, wholly independent of the deleted
      search/trigram path. No code change needed.
- [ ] Strip orphaned audit finding IDs (`SCORE-n`, `SPATIAL-n`, `DATA-n`,
      `EXPORT-n`) from source comments, test names, and `docs/` as those files
      are touched — nothing defines them any more
- [x] Delete `scripts/import-osm.sh` and `scripts/import-overture-extract.sh`
      — dead. `stages.py`'s `stage_import` calls `garganorn/sql/osm_import.sql`
      and `overture_place_import.sql` directly and never shells out to
      either script. Deleted both scripts and the classes in
      `tests/test_import_pipeline.py` pinning their exact SQL contents, plus
      a second, separately-scoped test file (`tests/test_download_scripts.py`)
      found only by running the full suite. `README.md` needed no edit — it
      already didn't reference either script; `DOCKER.md` is deleted outright
      below rather than fixed
- [x] `tests/conftest.py`'s `overture_db`/`osm_db`/`osm_db_path` fixtures and
      the `_create_osm_db`/`OSM_PLACES`/`OSM_IMPORTANCE` builder are unused —
      confirmed by grep, nothing in the suite queries them since P4's serving-DB
      removal. Delete them (only `overture_db_path` is still used, by
      `test_database.py`)
- [x] Remove the outmoded docker support
- [ ] `--tags tiles` runs Ansible with `poll: 60` against a ~90-minute build,
      pinning the operator's terminal for the duration and losing the run on a
      dropped connection. This is why the role keeps getting routed around by
      ad-hoc scripts. Use `poll: 0` with a separate status-check play, or drive
      Ansible from the box

---

## Sequencing

Recommended order: **P5 → P4 → P3 → P1 → P2 → P6**, with P7 and P8 folded in
wherever they fit.

## Out of scope

Recorded so they are not re-derived as good ideas:

- The antimeridian fix. Known edge case, to be tested and fixed later.
- Per-source score derivation. Unnecessary once the density artifact is
  treated as a Produced Work.
- Building the AppView or the client SDKs.
- The check-in write path, and any lexicon supporting it.
- A bbox-only `searchRecords`, or any other server-side search.
- Distributing the manifest to clients.
- Reviving Foursquare or Who's on First.
