# Garganorn Documentation Index

## Pipeline

| Document | Description |
|----------|-------------|
| [pipeline-status.md](pipeline-status.md) | Current operational state: known limitations, tradeoffs, fixes wanted, observability gaps |
| [design-constraints.md](design-constraints.md) | DuckDB behaviors, pipeline invariants, normalization constants |
| [pipeline-implementation-decisions.md](pipeline-implementation-decisions.md) | Condensed design decisions behind the shipped pipeline (covering/containment, artifact restructure, envelope, level vocabulary, serving-path) |
| [explored-and-discarded.md](explored-and-discarded.md) | Approaches investigated and not adopted |

## atgeo Protocol

| Document | Description |
|----------|-------------|
| [atgeo-spec.md](atgeo-spec.md) | Normative tile/manifest/envelope protocol spec (1.0-draft, Schuyler review pending) |
| [atgeo-appview-sdk-design.md](atgeo-appview-sdk-design.md) | Spatial AppView + client SDK design (mostly unbuilt) |
| [org.atgeo.tiles.service.json](org.atgeo.tiles.service.json) | Lexicon schema for `org.atgeo.tiles.service` records |

## Pipeline Restructure (in progress)

Master plan for the parquet-artifact pipeline restructure. Phases 1, 2, and
2b are merged and deployed (decisions condensed in
`pipeline-implementation-decisions.md` above); Phase 3 (server removals) and
Phase 4 (global validation) are not started — see `pipeline-status.md` for
what's left.

| Document | Description |
|----------|-------------|
| [pipeline-restructure-design.md](pipeline-restructure-design.md) | Master execution spec for the restructure (all phases) |
| [atgeo-execution-plan.md](atgeo-execution-plan.md) | Multi-agent coordination plan across garganorn + atgeo protocol workstreams |

## Data Sources

| Document | Description |
|----------|-------------|
| [foursquare.md](foursquare.md) | Foursquare Open Source Places schema and data model |
| [overture.md](overture.md) | Overture Maps Places schema and data model |

## Feature Specs

| Document | Description |
|----------|-------------|
| [name-variants-design.md](name-variants-design.md) | Multilingual and variant name storage/retrieval |
| [tile-privacy-design.md](tile-privacy-design.md) | Tile-based query privacy and user safety design |
| [wikimedia-importance-evaluation.md](wikimedia-importance-evaluation.md) | Wikipedia page-rank as notability signal (planned) |
| [lexicon-discovery-plan.md](lexicon-discovery-plan.md) | AT Protocol lexicon discovery via DID/WebFinger (evaluated, not adopted — see explored-and-discarded.md) |

## Reference Queries

| Document | Description |
|----------|-------------|
| [overture_place_wikidata_exploration.sql](overture_place_wikidata_exploration.sql) | Wikidata coverage in Overture Places (for Wikimedia importance) |
