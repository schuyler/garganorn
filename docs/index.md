# Garganorn Documentation Index

## Start here

| Document | Description |
|----------|-------------|
| [tile-privacy-design.md](tile-privacy-design.md) | Why there is no server-side search. The constraint the rest of the architecture is derived from. |

## Pipeline

| Document | Description |
|----------|-------------|
| [pipeline-artifacts.md](pipeline-artifacts.md) | What each pipeline stage writes: artifact format, schema, sort order, and why |
| [design-constraints.md](design-constraints.md) | Non-trivial implications of the design requirements and the implementation choices together — consequences neither states on its own |
| [gotchas.md](gotchas.md) | Behaviors of third-party tools (DuckDB, GeoParquet, Overture, osmium, pytest) and geographic-data edge cases (antimeridian) that cost real time to learn and aren't discoverable from their documentation |
| [explored-and-discarded.md](explored-and-discarded.md) | Approaches investigated and not adopted |
| [known-data-quality-issues.md](known-data-quality-issues.md) | Source-data characteristics investigated and left unfixed, settled won't-fixes and open dispositions alike |

## atgeo Protocol

| Document | Description |
|----------|-------------|
| [atgeo-spec.md](atgeo-spec.md) | The gazetteer interface: place identity (`urn:atgeo:` origins), tilesets, tile format, place records, importance, containment levels, the XRPC methods and plain-HTTP routes, error statuses, caching and CORS. Normative for anything on the wire |
| [atgeo-client-sdk.md](atgeo-client-sdk.md) | Client SDK design rationale and port-relevant behavior: two calls, `searchPlaces` and `getPlace` (describes the shipped TypeScript SDK; not normative for the wire) |
| [atgeo-appview-design.md](atgeo-appview-design.md) | Firehose sidecar serving live location records as tiles (unbuilt; pending overhaul — not normative) |
| [lexicon-publication.md](lexicon-publication.md) | Runbook: the `_lexicon.atgeo.org` TXT record and publishing the `org.atgeo.*` lexicons with `goat` |

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
