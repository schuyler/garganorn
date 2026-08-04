---
category: Design
tags: [atgeo, garganorn, execution, agents, coordination]
last_updated: 2026-08-03
confidence: plan; workstream estimates are judgment, not measurement
status: garganorn-side workstreams (pipeline restructure Phases 1/2/2b,
  OQ-P2-5 serving-path) are merged and deployed as of 2026-08-03. The atgeo
  protocol spec workstream produced docs/atgeo-spec.md (committed
  `f788f6c`, 2026-07-11, 1.0-draft, Schuyler review pending). AppView/SDK
  workstreams (docs/atgeo-appview-sdk-design.md) remain largely unbuilt.
---

# atgeo Ensemble Execution Plan

The coordination layer for executing two designs — the Garganorn pipeline
restructure and the atgeo protocol/AppView/SDK design — with an ensemble
of coding agents working in parallel. Those documents say *what* and
*why*; this one says *who does what in what order*, what each workstream
delivers, and which decisions no agent is permitted to make.

The coordination mechanism is deliberate: **frozen artifacts, not shared
context.** Agents drift worse than humans, so every cross-workstream
dependency is a file with a version — the protocol spec, the conformance
corpus, the envelope — never "what another agent decided." A workstream
that needs something not yet frozen is blocked, not improvising.

## Workstream graph

```
WS-1 protocol freeze ──────┬──> WS-3 TS SDK + web demo ──> corpus v1 ──┬──> WS-5 Swift SDK
                           │                                           └──> WS-6 Kotlin SDK
                           ├──> WS-4 AppView
WS-2 pipeline restructure ─┘         (WS-2 covering/*.parquet ──> WS-4 §2.x containment, optional)
                           
WS-7 conflation/licensing design ──> WS-2 conflation stage (later phase)
                                 └─> WS-3 API freeze (collection default)
WS-8 write path ── blocked on HD-1 (check-in lexicon)
WS-9 site updates ── after WS-2 Phase 3 and WS-3 ship
```

Parallelism: WS-2 and WS-3 can start the moment WS-1 lands; WS-4 needs
only the envelope from WS-1; WS-5/6 need corpus v1 from WS-3. WS-7 is a
design task runnable any time but gates parts of WS-2 and WS-3 as noted.

## Workstreams

### WS-1: Protocol freeze v1

Deliverables: the atgeo design's §1 encoded as a standalone versioned
spec file in a new `atgeo-spec` repo; `org.atgeo.tiles.service.json`
finalized from the existing draft; the conformance repo
(`atgeo-conformance`) scaffolded with `quadkey.json`, `snap.json`,
`coverage.json`, `envelope.json` vectors generated from the Python
reference (`quadkey_to_bbox` and inverse tile math in Garganorn's
`stages.py`) plus hand-written adversarial cases (antimeridian, poles,
grid-boundary snapping, max-length quadkeys, unknown envelope fields,
wrong major version).

Acceptance: vectors round-trip against the Python reference in CI;
spec file review sign-off (HD-4). Size: small — days, one agent.

### WS-2: Pipeline restructure

Executes `pipeline-restructure-design.md` as written, Phases 0–4, on the
production box. The envelope adoption (§3.8 amendment) lands in Phase 2
and consumes WS-1's frozen envelope. The level-vocabulary mapping (§3.4)
consumes atgeo-spec.md §7 and must verify the Overture subtype set against
current parquet, amending the table upward rather than guessing.

Acceptance: as specified per phase in that document. The Phase 4 global
validation additionally reports the two numbers other workstreams are
waiting on: gzipped manifest size per source (feeds the §7.2/§1.3 shard
decision) and division tileset total size (feeds WS-3's two-tier
prefetch strategy).

### WS-3: TypeScript SDK + web demo

Deliverables: `@atgeo/client` per atgeo §3 (read path first), the web
demo app, and generation of the `normalize.json` and `ranking.json`
vectors as the implementation stabilizes — these become corpus v1, the
freeze that unblocks WS-5/6. Repo layout: standard npm package, ESM,
CI running the corpus, `corpus_version` export, bundle-size check
(≤ 15 kB gzipped read path) as a CI gate, not a hope.

Blocked on: WS-1. Partially gated by WS-7: whether `collection` is an
optional filter or required choice in `nearby()`. Until WS-7 lands, the
agent implements the conservative interim (single default collection,
`collection` optional) and marks the decision point in code.

Acceptance: corpus green; demo runs against live places.atgeo.org data;
the five-line integration in the README is literally the demo's core.

### WS-4: AppView

Deliverables: the sidecar per atgeo §2, including §2.8 abuse config and
the rebuild-from-CAR test with fixture repos. Deployed as a Dokploy
service against a single PDS with one configured collection.

Blocked on: WS-1 (envelope). The containment enrichment (consuming
`covering/*.parquet`) is optional scope pending its own open decision
(edge-tile point-in-polygon without a spatial engine); v1 may ship
interior-only or no containment — agent implements the hook, human
decides the depth (HD-6).

Acceptance: create/update/delete/account-deletion events reflected in
tiles within one flush interval; byte-identical tiles after rebuild;
per-DID cap demonstrably drops over-cap writes.

### WS-5 / WS-6: Swift and Kotlin SDKs

Deliverables per atgeo §3.5: SwiftPM and Maven packages, demo apps,
corpus in CI. Fully parallel with each other; each agent's inputs are
the spec, corpus v1, and the TS reference as *reading material* — not as
code to transliterate; the corpus is the contract, idiom is local.

Blocked on: corpus v1 (WS-3).

Acceptance: corpus green; demo app runs on device/simulator; a native
developer reviewing the API surface finds nothing "translated" (HD-4
spot check).

### WS-7: Conflation and licensing design

A design document of the same grade as the other two, covering: v1
collection strategy (curated default vs. multi-source with `same_as`
dedup), the pipeline conflation stage that populates `same_as` (GERS and
wikidata IDs as anchors, name+proximity+category as fallback), and
license isolation — per-source score derivation (or density as an
independently licensed artifact), attribution surfacing as an SDK
*requirement*, ODbL share-alike posture stated as design constraints with
the explicit caveat that license conclusions need human/legal review
(HD-5), not agent judgment.

### WS-8: Write path

`composeLocation` + `checkin` across all three SDKs, using the atgeo ref
shape. Blocked on HD-1. Do not start speculatively; a wrong record shape
here is an ecosystem-visible mistake.

### WS-9: Site updates

Punch list for atgeo.org, executed alongside the changes that motivate
each item:

1. Roadmap: Spatial AppView section — remove the `searchRecords` query
   interface framing; describe tile-serving AppView. (With WS-4.)
2. Roadmap: H3-query privacy item — superseded by client-side search
   over tiles; H3 keeps the write-path representation role. (With WS-3.)
3. API page: remove `searchRecords`/`getCoverage`; document the manifest
   + tile fetch pattern and point at the SDK. (Same change set as WS-2
   Phase 3 — the design doc requires this coupling.)
4. Lexicon page: reference atgeo-spec.md §7 as normative for `within`
   levels. (With WS-1.)
5. Usage page: replace API-first integration guidance with the five-line
   SDK integration. (With WS-3 release.)
6. Note FSQ dataset staleness and its disposition per HD-3.

## Human decision register

Agents must treat these as external inputs. An agent that finds itself
needing one of these answers stops and surfaces the blockage.

| ID | Decision | Owner | Blocks |
|----|----------|-------|--------|
| HD-1 | Check-in record lexicon (draft `org.atgeo.checkin` vs. track lexicon.community) | ATGeo working group | WS-8, demo check-in buttons |
| HD-2 | Default public instance: operator, bandwidth budget, governance | ATCF | WS-3 zero-config constructor target |
| HD-3 | FSQ source disposition: pin last release vs. drop | Schuyler | WS-2 scheduling for FSQ; WS-9 item 6 |
| HD-4 | Protocol freeze sign-off; SDK API-surface reviews | Schuyler | WS-1 completion; WS-3/5/6 release |
| HD-5 | ODbL/licensing conclusions in WS-7 | Human (legal review as needed) | WS-7 finalization |
| HD-6 | AppView containment depth (none / interior-only / full) | Schuyler | WS-4 optional scope |
| HD-7 | H3 default write resolution (res 8 proposed) | Community sanity check | WS-8 defaults |

## Agent operating rules

1. **Corpus is law.** A vector failure is a build failure. No SDK ships
   behavior the corpus doesn't test if that behavior is spec-adjacent —
   propose a vector addition instead.
2. **No private improvements.** Ranking, normalization, and coverage
   semantics change only via spec + corpus version bump. "Better" results
   in one SDK are a bug in three.
3. **Frozen inputs only.** Consume the versioned spec and corpus, not
   design-conversation context. If the spec is ambiguous, the fix is a
   spec patch (through HD-4), not a local interpretation.
4. **Fail loudly on unmapped data.** The level-mapping rule generalizes:
   unknown subtypes, unknown envelope major versions, unmapped source
   fields — stop and surface, never default silently. (Unknown *extra*
   JSON fields are the one deliberate exception: ignore and preserve.)
5. **Deletion requires parity first.** Inherited from the pipeline doc's
   phase structure: no old code path is removed until its replacement
   matches it on the recorded baseline.
6. **Decisions in the register are not yours.** Surface, don't resolve.

## Sequencing summary

Week-zero parallel starts: WS-1 (short), WS-7 (design), and WS-2 Phase 0
(baseline capture needs nothing from WS-1). WS-2 proper and WS-3 start on
WS-1 landing; WS-4 shortly after; WS-5/6 on corpus v1; WS-8 whenever HD-1
resolves; WS-9 items ride their coupled workstreams. The critical path to
a developer-visible product is WS-1 → WS-3 → demo, and that is by design:
adoption artifacts ship before infrastructure completeness.
