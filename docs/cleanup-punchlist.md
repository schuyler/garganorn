# Cleanup punch list

What is left to do to bring the code, the lexicons, and the documentation into
line with the decisions the project has actually made. One line per item, each
either checked or not.

Rationale is deliberately absent. Why the gazetteer serves no search endpoint
lives in `tile-privacy-design.md`; what the tile format is lives in
`atgeo-spec.md`. This file only records what remains.

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

- [ ] State the user-safety principle once, prominently, and reference it
      rather than re-deriving it at each error condition
- [ ] Rewrite the README: no searchRecords, no Foursquare, worked examples
      actually run against overture/osm, `getCoverage` + tile fetch as the
      interface, remove the dead `docs/s2_duckdb_design.md` link
- [ ] Delete `atgeo-execution-plan.md`
- [ ] Correct `pipeline-status.md`: Phase 3 no longer claims `getCoverage`
      removal, the searchRecords "decision pending" section is resolved, and
      OQ-P2-8 is answered
- [ ] Promote `tile-privacy-design.md` out of "Feature Specs" in `index.md`
- [ ] `getCoverage.json`'s description carries the reason there is no `q`
      parameter

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
- [ ] Remove `print()` calls from production paths (EXPORT-11)

---

## Sequencing

`P1 → P2 → P6` is the only hard ordering. P3, P4, P5, P7 are largely
file-disjoint and can run in parallel, except that P3's `place.json` work
wants P4 landed first so the two emitters have stopped disagreeing.

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
