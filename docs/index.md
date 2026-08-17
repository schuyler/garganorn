# Garganorn Documentation Index

## Start here

| Document | Description |
|----------|-------------|
| [tile-privacy-design.md](tile-privacy-design.md) | Why there is no server-side search. The constraint the rest of the architecture is derived from. |

## Pipeline

| Document | Description |
|----------|-------------|
| [pipeline-artifacts.md](pipeline-artifacts.md) | What each pipeline stage writes: artifact format, schema, sort order, and why |
| [design-constraints.md](design-constraints.md) | Pipeline invariants and architectural rules garganorn's own code must satisfy |
| [gotchas.md](gotchas.md) | Behaviors of third-party tools (DuckDB, GeoParquet, Overture, osmium, pytest) and geographic-data edge cases (antimeridian) that cost real time to learn and aren't discoverable from their documentation |
| [explored-and-discarded.md](explored-and-discarded.md) | Approaches investigated and not adopted |
| [known-data-quality-issues.md](known-data-quality-issues.md) | Source-data characteristics investigated and left unfixed, settled won't-fixes and open dispositions alike |

## atgeo Protocol

| Document | Description |
|----------|-------------|
| [atgeo-spec.md](atgeo-spec.md) | Tile format, record envelope, containment levels, and the XRPC methods a client can call (pending overhaul — not normative) |
| [atgeo-client-sdk.md](atgeo-client-sdk.md) | Client SDK design: two methods, `searchPlaces` and `getPlace` (unbuilt; pending overhaul — not normative) |
| [atgeo-appview-design.md](atgeo-appview-design.md) | Firehose sidecar serving live location records as tiles (unbuilt; pending overhaul — not normative) |
| [org.atgeo.tiles.service.json](org.atgeo.tiles.service.json) | Lexicon schema for `org.atgeo.tiles.service` records |

## Data Sources

| Document | Description |
|----------|-------------|
| [overture.md](overture.md) | Overture Maps Places and Divisions schemas and data model |

## Feature Specs

| Document | Description |
|----------|-------------|
| [wikimedia-importance-evaluation.md](wikimedia-importance-evaluation.md) | Wikipedia page-rank as notability signal (planned) |
| [planned-features.md](planned-features.md) | New feature ideas not yet designed or scoped |

## Reference Queries

| Document | Description |
|----------|-------------|
| [overture_place_wikidata_exploration.sql](overture_place_wikidata_exploration.sql) | Wikidata coverage in Overture Places (for Wikimedia importance) |
