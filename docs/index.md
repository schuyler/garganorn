# Garganorn Documentation Index

## Pipeline

| Document | Description |
|----------|-------------|
| [pipeline-status.md](pipeline-status.md) | Known limitations, tradeoffs, fixes wanted, observability gaps |
| [design-constraints.md](design-constraints.md) | DuckDB behaviors, pipeline invariants, normalization constants |
| [explored-and-discarded.md](explored-and-discarded.md) | Approaches investigated and not adopted |
| [wof-boundary-complexity.md](wof-boundary-complexity.md) | WoF boundary vertex counts and performance implications |

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
| [lexicon-discovery-plan.md](lexicon-discovery-plan.md) | AT Protocol lexicon discovery via DID/WebFinger |
| [integration-test-design.md](integration-test-design.md) | Quadtree tile export integration test strategy |

## Reference Queries

| Document | Description |
|----------|-------------|
| [overture_place_wikidata_exploration.sql](overture_place_wikidata_exploration.sql) | Wikidata coverage in Overture Places (for Wikimedia importance) |

## Baselines

| Document | Description |
|----------|-------------|
| [benchmark_baseline_2026-03-25.txt](benchmark_baseline_2026-03-25.txt) | Query performance baseline from garganorn-1 |
