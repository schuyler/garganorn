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

- [ ] Tile URLs embed the run version; `getCoverage` returns stamped URLs
- [ ] Tiles serve `Cache-Control: public, max-age=<ttl>, immutable`
- [ ] `tile_dirs[slug]` roots at `{source}/tiles/` so prior runs stay servable
- [ ] Keep the `{source}/tiles/` nesting — it keeps `places.parquet`,
      `containment/`, and `boundaries.duckdb` out of the web-served tree
- [ ] Retention becomes time-based (`stages.py:1709-1719` currently keeps two
      runs); assert `retention >= max-age` at startup
- [ ] Rename `pipeline.output` from `tiles` to `data`, giving
      `data/{source}/tiles/{stamp}/`. Requires moving `{garganorn_home}/tiles`
      on the server, and touches `config.yaml.example`, the Ansible role
      defaults and template, the README, and the spec's serving-layout section

**Accept:** two consecutive runs produce disjoint URL sets; the prior run's
URLs still resolve after a new build; a request resolving to `places.parquet`
returns 404; startup fails when `max-age` exceeds the retention window.

## P2 — Ansible and deploy

Land before P6's rebuild, or the rebuild will not take effect.

- [ ] Tile build task notifies `restart garganorn`
- [ ] Confirm the handler fires under `--tags tiles` — handlers are tag-filtered
      and may need `tags: always`
- [ ] Build timer to 14400 in both `tiles.yml` and `host_vars/garganorn-1.yml`;
      replace the "has never been timed on the box" comment with the measured
      ~90 minutes
- [ ] Remove the Foursquare find/assert block and `garganorn_source_fsq`; check
      whether `garganorn_source_wof` is referenced before removing it
- [ ] `config.yaml.j2` updated for P1's versioned `base_url` and P6's
      attribution keys

**Accept:** a `--tags tiles` run leaves the service restarted, and
`getCoverage` reflects the new run with no manual step.

## P3 — Lexicon conformance

- [ ] `getRecord.json` output `uri`: `format: at-uri` → `format: uri`
- [ ] Delete the `cid` parameter from `getRecord.json`
- [ ] Declare `attribution` (required) and `importance` (optional) in
      `getRecord`'s output schema
- [ ] `place.json`: add `$type` and `importance`
- [ ] `place.json`: delete `same_as`, `relation.name`, and the `#ref` def
- [ ] Delete `hthree.json` and its two union references in `place.json`
- [ ] Delete `fsq.json`
- [ ] Conformance harness validating real tile-file records against the
      lexicons, and driving the XRPC surface through `lexrpc.Server.call()`
      rather than calling bound methods

`published_at` stays declared though nothing produces it — a deliberate
exception, not an oversight. `_query` stays in responses and stays
undocumented.

**Accept:** live XRPC `getRecord` returns 200; the harness fails if the
`at-uri` regression is reintroduced.

## P4 — searchRecords and the serving-DB layer

- [ ] Delete `searchRecords`: method, registration, lexicon, tests
- [ ] Delete `listRecords`: method and lexicon
- [ ] Delete the trigram, Jaro-Winkler, `_strip_accents`, and `name_index`
      machinery from `database.py` and from the import SQL
- [ ] Delete the serving half of `database.py` — `nearest()`,
      `process_record`, `Database.get_record` — keeping what `stages.py:31` and
      `quadtree.py:9` import
- [ ] Delete `garganorn/boundaries.py` and the serving-time relations
      computation at `server.py:88-110`, which overwrites the tile's
      precomputed `relations.within`
- [ ] Remove `databases:` from config, `DATABASE_TYPES` from `config.py`, and
      `self.db` from `Server`
- [ ] Remove `design-constraints.md` entries Q1 and Q2, and the
      `JW_THRESHOLD`, `JW_TOKEN_ALPHA`, and `IMPORTANCE_FLOOR_K` rows from its
      constants table

`boundaries.duckdb`, the pipeline artifact, is unaffected — only the
serving-time `BoundaryLookup` class goes.

**Accept:** full suite green; `getRecord` returns the tile's own `relations`
unmodified; no imports of removed symbols remain.

## P5 — Foursquare removal

- [ ] Delete `FoursquareOSP`, `sql/foursquare_import.sql`,
      `sql/foursquare_export_tiles.sql`, `scripts/import-fsq-extract.sh`, the
      `fsq` branch of `scripts/build-density.sh`, `docs/foursquare.md`, and the
      foursquare config entries
- [ ] Re-anchor the spec's `importance` formula citation, which points at
      `foursquare_import.sql:45-48`
- [ ] Verify the Overture and OSM importance formulas match whatever the spec
      then claims

## P6 — Envelope change

Forces a full re-export. Land P1 and P2 first.

- [ ] Remove the `atgeo` version key from tile payloads, `manifest.json`,
      `manifest.duckdb` metadata, and the test assertions pinning the five-key
      envelope
- [ ] Replace `attribution` with sibling keys `source` and `license`
- [ ] Both changes ship in one re-export

## P7 — Documentation

- [ ] Schuyler reviews `atgeo-spec.md` intensively. No piecemeal edits to it
      until he has
- [ ] State the user-safety principle once, prominently, and reference it
      rather than re-deriving it at each error condition. Blocked on the spec
      review above
- [ ] Rewrite the README: no searchRecords, no Foursquare, worked examples
      actually run against overture/osm, `getCoverage` + tile fetch as the
      interface, remove the dead `docs/s2_duckdb_design.md` link, and state
      which collections are served and that the server has no users
- [x] Delete `atgeo-execution-plan.md`
- [x] Dissolve `pipeline-status.md`: durable tradeoffs to `design-constraints.md`,
      open items here, the rest deleted
- [x] Promote `tile-privacy-design.md` out of "Feature Specs" in `index.md`
- [ ] `getCoverage.json`'s description carries the reason there is no `q`
      parameter
- [ ] Record the licensing posture in the repo: the OSM tileset is an ODbL
      Derivative Database, the density artifact is a Produced Work, and
      attribution is a source link plus a license link. Currently written down
      nowhere in the repo
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

`atgeo-appview-sdk-design.md` and `org.atgeo.tiles.service.json` stay, framed
as aspirational.

## P8 — Validation and small cleanups

- [ ] Light validation of the global build: per-collection record counts
      against source parquet; tile count and total gzipped size per source;
      `getCoverage` + fetch in scattered regions with a plausibility check; and
      confirmation that every URL `getCoverage` returns resolves
- [ ] Remove the compatibility shim at `tile_reader.py:41-49` — its stated
      removal condition (first re-export after the envelope deploy) is met
- [ ] Remove the dead `tiles.max_per_tile` config key
- [ ] Remove `print()` calls from production paths
- [ ] Assert the division-import Hilbert sort is actually applied
      (`ST_Hilbert` in `stages.py`); nothing tests it today. See D2
- [ ] Filter empty and whitespace-only names at import; they currently reach
      tiles as blank-named records
- [ ] Sweep the suite for tests that enforce nothing — ones asserting against
      their own fixtures, or exercising code that no longer exists. Find them
      by mutating the source and seeing what stays green. Known instance:
      `tests/test_audit_spatial_processing.py`
- [ ] Evaluate once the removals above are done: should maritime divisions
      keep being dropped by the `is_land=true` filter, which excludes bays,
      straits, and seas? It is a completeness question, not a logging one
- [ ] Evaluate once the removals above are done: does `IDF ln(N/0)` survive
      the trigram removal, or was it only reachable through the search path?
- [ ] Strip orphaned audit finding IDs (`SCORE-n`, `SPATIAL-n`, `DATA-n`,
      `EXPORT-n`) from source comments, test names, and `docs/` as those files
      are touched — nothing defines them any more

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
