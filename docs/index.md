# Garganorn Documentation Index

## Start here

| Document | Description |
|----------|-------------|
| [tile-privacy-design.md](tile-privacy-design.md) | Why there is no server-side search. The constraint the rest of the architecture is derived from. |

## Pipeline

| Document | Description |
|----------|-------------|
| [pipeline-artifacts.md](pipeline-artifacts.md) | What each pipeline stage writes: artifact format, schema, sort order, and why |
| [design-constraints.md](design-constraints.md) | DuckDB behaviors, pipeline invariants, tradeoffs, normalization constants |
| [pipeline-implementation-decisions.md](pipeline-implementation-decisions.md) | Condensed design decisions behind the shipped pipeline (covering/containment, artifact restructure, envelope, level vocabulary, serving-path) |
| [explored-and-discarded.md](explored-and-discarded.md) | Approaches investigated and not adopted |
| [performance-improvements.md](performance-improvements.md) | Proposed performance work on already-correct code, including correct division tile assignment and containment names (planned, not started) |
| [known-data-quality-issues.md](known-data-quality-issues.md) | Source-data characteristics investigated and deliberately left unfixed |

## atgeo Protocol

| Document | Description |
|----------|-------------|
| [atgeo-spec.md](atgeo-spec.md) | Tile format, record envelope, containment levels, and the XRPC methods a client can call |
| [atgeo-client-sdk.md](atgeo-client-sdk.md) | Client SDK design: two methods, `searchPlaces` and `getPlace` (unbuilt) |
| [atgeo-appview-design.md](atgeo-appview-design.md) | Firehose sidecar serving live location records as tiles (unbuilt) |
| [org.atgeo.tiles.service.json](org.atgeo.tiles.service.json) | Lexicon schema for `org.atgeo.tiles.service` records |

## Data Sources

| Document | Description |
|----------|-------------|
| [overture.md](overture.md) | Overture Maps Places schema and data model |

## Feature Specs

| Document | Description |
|----------|-------------|
| [wikimedia-importance-evaluation.md](wikimedia-importance-evaluation.md) | Wikipedia page-rank as notability signal (planned) |
| [planned-features.md](planned-features.md) | New feature ideas not yet designed or scoped |

## Reference Queries

| Document | Description |
|----------|-------------|
| [overture_place_wikidata_exploration.sql](overture_place_wikidata_exploration.sql) | Wikidata coverage in Overture Places (for Wikimedia importance) |
