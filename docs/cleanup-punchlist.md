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
- [x] The tile-payload header shape (`collection`, `source`, `license`,
      `generated_at`, from `envelope.py:build_tile_payload`) has no lexicon
      or schema covering it. `collection` was mistakenly folded into the
      `place.json` item above during scoping — it's a tile-payload field,
      never a record field, so `place.json` is the wrong place for it.
      Shipped: `garganorn/lexicon/tilePayload.json` (`org.atgeo.tilePayload`),
      validated against `build_tile_payload`'s actual output in
      `tests/test_envelope.py`

`published_at` stays declared though nothing produces it — a deliberate
exception, not an oversight. `same_as` stays declared for the same reason:
future work on identifying conflations between datasets, with room to expand
to other place relationships. `relation.name` no longer belongs in that list
— clients need division names inline, and producing it is scoped in P10. `getRecord.json`'s `cid`
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
      `process_record`, `Database.get_record` — keeping what `stages.py` and
      `quadtree.py` import
- [x] Delete `garganorn/boundaries.py` and the serving-time relations
      computation at `server.py`, which overwrites the tile's
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
      `importance` formula now that its citation (`foursquare_import.sql`)
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
- [x] Repoint or remove the ~212 `§N` cross-references in `garganorn/` (18)
      and `tests/` (194) across 28 files. They point into `atgeo-spec.md` and
      the deleted `pipeline-restructure-design.md`, neither of which has
      numbered sections any more. Shipped across two passes: ~197 refs in
      `garganorn/`+`tests/` (tranche 3, `a495fa9`), then the three remaining
      doc-level ones — a false `wof-boundaries-design.md` pointer in
      `explored-and-discarded.md`, a dead provenance comment in
      `test_containment_covering.py`, and a `(spec §4)` citation in
      `pipeline-implementation-decisions.md`, all resolved by deletion (none
      load-bearing). The `atgeo-appview-sdk-design.md` remainder went with
      that document's overhaul below; what's left lives in
      `name-variants-design.md`, tracked as its own item
- [ ] Find every place in the code that references a non-authoritative design
      document and remove the reference
- [x] Nothing verifies the `file.py:line` citations in `docs/`. `atgeo-spec.md`
      claims they "fail visibly when a statement stops being true," but two were
      found stale by inspection. Either check them in CI or stop claiming they
      self-verify. Moot: the `atgeo-spec.md` redraft (`a540ae9`) already
      dropped both the citations and the self-verifying claim (Schuyler's
      gated requirement was "no `file.py:line` citations"). Ordinary citation
      rot remains in `design-constraints.md`/`explored-and-discarded.md`,
      but neither claims to self-verify
- [x] Delete `lexicon-discovery-plan.md`, after correcting its
      `explored-and-discarded.md` entry, which says "not yet adopted" but the
      lexicon-schema serving in `getRecord` shipped and the `listRecords` half
      is being reverted. Corrected: the entry now says "partially adopted"
      — the DID document route and `getRecord`'s lexicon-schema serving both
      shipped as ambient AT Protocol plumbing; DNS TXT discovery and
      `listRecords` did not. Deleted the plan doc and its `index.md` entry
- [x] Strip the dead `Explored in` pointers from
      `explored-and-discarded.md` and drop the field; the summaries are
      self-contained
- [x] Assess `compute-containment-oom-fix.md` — likely a completed-work
      artifact in the same category. Confirmed and dissolved: its root-cause
      lesson was already captured as D6 in `design-constraints.md`; a second,
      distinct DuckDB gotcha from the doc's "Second OOM" section was missing
      and added as D10; the still-open "data-quality half" (overlap
      detection) is now tracked in P8 below. The rest — narrative process,
      validation steps, file/line inventory — was deleted with the doc
- [x] `atgeo-appview-sdk-design.md` needs a full overhaul, not a §1.2/§1.3
      patch (envelope/manifest reconciliation — no `atgeo` version field or
      version-rejection rule, `source`/`license` in place of `attribution`,
      a `generated_at`-only manifest not served over HTTP, stamped tile
      URLs — was the original narrower scope). Done by dissolving it. The
      document braided three things together and only the sidecar was still
      live: the SDK half became `atgeo-client-sdk.md`, the sidecar half
      became `atgeo-appview-design.md`, and the protocol restatement was
      deleted because `atgeo-spec.md` is now the normative wire document and
      the restatement contradicted it in every particular listed above. Also
      absorbed elsewhere and dropped: the containment vocabulary (in the
      spec), the `https`-versus-`at://` provenance argument (in the spec),
      the read-path precision rule (in `tile-privacy-design.md` and the SDK
      doc), and the conformance-vector plan (in the SDK doc, minus the two
      files that client-side quadkey removal retired). Section numbering is
      gone from both successors, so there are no `§N` targets left to rot.
      One substantive change rather than a trim, flagged in the new document
      as unreviewed: the sidecar must serve `getCoverage` rather than a
      manifest, or no ATGeo client could talk to it
- [ ] `name-variants-design.md` (listed as a live implementation spec in
      `index.md`) is stale beyond a quick patch: it names both
      `scripts/import-osm.sh` and `scripts/import-overture-extract.sh` (§3.1,
      §3.2), both deleted in the P8 dead-script sweep, and still describes the
      removed `name_index` table (`is_variant`, `norm_name`, trigram columns)
      and FSQ as a source. Needs a full reconciliation pass, not a rename of
      the two script references

`atgeo-appview-design.md`, `atgeo-client-sdk.md`, and
`org.atgeo.tiles.service.json` stay, framed as aspirational.

## P8 — Validation and small cleanups

- [x] Light validation of the global build: per-collection record counts
      against source parquet; tile count and total gzipped size per source;
      `getCoverage` + fetch in scattered regions with a plausibility check; and
      confirmation that every URL `getCoverage` returns resolves. Run
      2026-08-08 against garganorn-1's live build (`overture_place`
      20260808T051317, `osm` 20260808T072257). `overture_place` output
      (74,223,561) matched source parquet exactly; `osm` output (27,295,425)
      sat below the ~28.5M named-node upper bound, explained by the
      category-tag whitelist, not an unexplained cliff. 222,008 tiles/13.7GB
      gzipped (overture_place), 78,203 tiles/1.5GB gzipped (osm) — plausible.
      4 scattered regions (Lisbon, NYC, rural Mongolia, outback Australia)
      showed the adaptive quadtree correctly sizing tiles to density and
      `BboxTooLarge` correctly guarding dense Overture/NYC; sample tile
      content carried the live P6 envelope and populated
      `relations.within`. 100/100 sampled `getCoverage` URLs resolved 200.
      No findings
- [x] Remove the compatibility shim at `tile_reader.py` — its stated
      removal condition (first re-export after the envelope deploy) is met
- [x] Remove the dead `tiles.max_per_tile` config key
- [x] Remove `print()` calls from production paths (already zero; nothing to
      do)
- [x] Assert the division-import Hilbert sort is actually applied
      (`ST_Hilbert` in `stages.py`); nothing tests it today. See D2.
      Shipped: `test_boundaries_duckdb_places_hilbert_sorted` in
      `tests/test_overture_division.py`, mutation-tested (fails when the
      `ORDER BY ST_Hilbert(...)` clause is removed)
- [x] Preflight the configured source globs before a build runs. `config.yaml`
      globs `db/cache/overture/*/part-*.parquet`; when the cache holds only
      divisions the glob matches nothing and the pipeline produces an empty
      collection with no error. `download-overture.sh` verifies per file
      against the S3 manifest, so the gap is not in downloading — nothing
      checks that what the config asks for exists before building on it.
      Shipped: `_resolve_glob_paths(..., required=True)` in
      `garganorn/stages.py` now raises `RuntimeError` on an empty configured
      glob at all four stages that consume a primary source glob —
      `stage_import`, `stage_division_import`, `stage_density_extract`, and
      `stage_idf` — naming the pattern in the error
- [x] Filter empty and whitespace-only names at import; they currently reach
      tiles as blank-named records. The requirement is no blank-appearing
      name reaching a tile, not literally just empty-string/whitespace — a
      NULL name is the same symptom. Shipped for both sources: OSM already
      excluded NULL and now also excludes empty/whitespace; Overture had no
      name filter at all before this and now excludes NULL, empty, and
      whitespace-only names in the same clause
      (`garganorn/sql/overture_place_import.sql`). Fixture callers that used
      NULL name as a don't-care placeholder (`write_minimal_overture_parquet`
      in `tests/quadtree_helpers.py`, `ov003`/`ov004`/`ov005` in
      `tests/conftest.py`) were given real placeholder names instead
- [ ] Sweep the suite for tests that enforce nothing — ones asserting against
      their own fixtures, or exercising code that no longer exists. Find them
      by mutating the source and seeing what stays green. Known instances:
      `tests/test_audit_spatial_processing.py`; six classes in
      `tests/test_import_pipeline.py` (`TestNodeImport`, `TestTagFiltering`,
      `TestWayCentroidResolution`, `TestOutputSchema`, `TestDensityOsmMode`,
      `TestInlineIdfOsmMode`) that reference nonexistent
      `build-density.sh`/`build-idf.sh` scripts and test hand-copied SQL
      disconnected from production code
- [x] `tests/test_pipeline.py`'s assertion failure messages (around lines
      434, 437) say `tiles.memory_limit`/`tiles.max_per_tile`, but the test
      itself correctly exercises the live `pipeline:` config section — just
      wrong strings in the error text, not a test-correctness bug. Fixed to
      say `pipeline.memory_limit`/`pipeline.max_per_tile`
- [ ] Evaluate once the removals above are done: should maritime divisions
      keep being dropped by the `is_land=true` filter, which excludes bays,
      straits, and seas? It is a completeness question, not a logging one
- [x] ~~Detect overlapping same-level boundary polygons~~ — superseded.
      This was scoped as a data-quality flag, but Schuyler clarified this
      project reports source data as-is; overlapping same-level boundaries
      that genuinely disagree are not a bug to detect or fix (see
      `feedback_data_quality_not_our_job` in Claude's memory). The
      performance angle of the same phenomenon was investigated separately
      and closed as a non-issue for the `compute_containment` slow cells in
      `performance-improvements.md` P9.2; P9.3 tracks the one real byproduct
      (literal duplicate boundary records) as its own unscoped item
- [x] Evaluate once the removals above are done: does `IDF ln(N/0)` survive
      the trigram removal, or was it only reachable through the search path?
      It survives: `stage_idf` computes IDF from live place data via
      `GROUP BY category`, which can't produce `n_places=0`, and it feeds the
      production importance formula, wholly independent of the deleted
      search/trigram path. No code change needed.
- [x] Strip orphaned audit finding IDs (`SCORE-n`, `SPATIAL-n`, `DATA-n`,
      `EXPORT-n`) from source comments, test names, and `docs/` as those files
      are touched — nothing defines them any more. Stripped from
      `tests/test_audit_*.py`, `test_export11.py`, `test_antimeridian_bbox.py`,
      `test_tile_assignment.py`, and `garganorn/stages.py`/`garganorn/sql/*.sql`
      comments; substantive descriptions kept, only the dead ID citations
      removed. `docs/` had no occurrences
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
- [x] `--tags tiles` runs Ansible with `poll: 60` against a ~90-minute build,
      pinning the operator's terminal for the duration and losing the run on a
      dropped connection. This is why the role keeps getting routed around by
      ad-hoc scripts. Use `poll: 0` with a separate status-check play, or drive
      Ansible from the box. Shipped in `atgeo-server-config`:
      `roles/garganorn/tasks/tiles.yml` gained a `pgrep`-based guard that
      skips launching a new build if one's already running, keeping
      `poll: 60`/`notify: restart garganorn` unchanged; `run-tile-build.sh`
      detaches the whole `ansible-playbook` invocation via nohup so a
      dropped connection doesn't kill it, and tails the remote log to report
      one of three honest outcomes (launched / already running / status
      unknown) instead of always claiming success

## P10 — Divisions: tile assignment and containment names

Both items exist because a client asking "where am I" gets an answer it
cannot use. They are coupled: containment names fix the common case, correct
tile assignment fixes the collection. Forces a re-export.

- [ ] **Divisions are assigned to tiles by bounding-box midpoint.**
      `sql/overture_division_import.sql` comments "qk17 placed at the
      geometry centroid" and then computes the midpoint of the bbox, which
      is not the centroid and for anything crescent-shaped, multipart, or
      with overseas territories is not inside the division at all — Norway,
      Chile, Indonesia. The deeper error is indexing an area by a point:
      a client's bbox over San Francisco gets whichever division midpoints
      happen to land in it, missing the division the user is standing in if
      its midpoint is just outside, and missing every division larger than
      the bbox unconditionally. Correct rule: assign each division to the
      **deepest tile wholly containing its bbox**, which is the longest
      common quadkey prefix of the bbox corners — no geometry needed, the
      bbox columns are already in the table. Unique, so "one record, one
      tile" and dedup-free concatenation both survive, and `getCoverage`
      then returns the whole containment stack for a region at a cost of
      about one extra tile per zoom level. Fix the lying comment either way
- [ ] Two decisions the assignment rule forces, to settle before
      implementing. **First**, it collides with adaptive assignment:
      `compute_tile_assignments.sql` picks the coarsest tile holding
      ≤ `max_per_tile` records, a record-count rule, and if that is allowed
      to split a shallow tile deeper it breaks containment again. For the
      division collection the extent rule has to win. **Second**,
      straddling: a division whose bbox crosses a quadkey boundary is pushed
      shallow, and a small one sitting on the prime meridian lands at z0.
      Probably survivable — few records, and coarse tiles stay small — but
      it needs a stated answer (accept, clamp, or let those divisions appear
      in up to four tiles and give up dedup-free concatenation for this
      collection alone) rather than being discovered in the data
- [ ] **Produce `relation.name`.** `relations.within` is a list of opaque
      `org.atgeo.places.overture.division:{id}` rkeys, so rendering "San
      Francisco, California" costs one `getRecord` per division per result —
      an N+1 that tells the server exactly which records a user is looking
      at, which is the surveillance surface `getCoverage` exists to avoid.
      The field is already declared in `place.json`. It is built in
      `sql/compute_containment.sql`; `bnd.places` already carries
      `names` and `level` alongside `id`, so this is a projection of
      `(id, names)` into a temp table joined once at the final SELECT
      (materialize before joining — see D10). Gzip absorbs the size: a tile
      is 1,000 records in one small area repeating a handful of names
- [ ] Decide whether to carry `level` in the same object. `atgeo-spec.md`
      says "the level itself is not carried; the ordering is the
      information," which is true for rendering the whole string and useless
      for a client that wants just the city. One integer, and the object is
      already being touched. Spec edit if yes
- [ ] Note in `atgeo-spec.md` that containment stops at locality.
      `stage_division_import` builds `bnd.places` with `WHERE level <= 50`, so
      `relations.within` never contains a borough, macrohood, neighborhood,
      or microhood — those exist in the division tileset but in no place
      record's containment. Currently undocumented, and it is the difference
      between "San Francisco, California, United States" (available) and
      "Hayes Valley" (not)

**Accept:** a bbox over a city returns that city's own division record and
its ancestors, not a scatter of neighbours whose midpoints happened to land
nearby; a place record renders its full containment string with no further
network calls.

---

## Sequencing

Recommended order: **P5 → P4 → P3 → P1 → P2 → P6**, with P7 and P8 folded in
wherever they fit. P10 is independent of all of them and forces its own
re-export.

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
