# Explored and Discarded

Brief summaries of approaches that were investigated and not adopted for the Garganorn gazetteer project.

## S2 Spatial Indexing

**Summary**: S2 cell IDs were used for spatial indexing and density computation. The design used Google's S2 geometry library via DuckDB's `geography` community extension, with `s2_cellfromlonlat()` and `s2_cell_parent()` functions for hierarchical spatial keys at levels 6-14.

**Why discarded**: S2 was eliminated entirely from the pipeline in favor of quadkeys (Bing tile system). The switch to `ST_QuadKey()` simplified the spatial indexing approach and removed the dependency on the `geography` extension for core operations.

**Successor**: Quadkey-based spatial indexing using `ST_QuadKey()` at z17 for places, z15 for density tiles. See `design-constraints.md`, "All spatial indexing uses quadkeys" for the current invariant.

## Pipeline Framework

**Summary**: A DAG-based pipeline framework (`pipeline.py`, ~1250 lines) was developed with stage metadata tables, caching, and checkpointing. The framework tracked stage completion in a database table and supported skip-if-up-to-date behavior.

**Why discarded**: The framework was never integrated into production and remained pure overhead. The real orchestrator was `run_pipeline()` in `quadtree.py`. The framework added complexity without delivering value.

**Successor**: Simple stage functions in `stages.py` with mtime-based caching. No database state tracking, no DAG metadata—just function calls and file modification time checks.

## QuackOSM for OSM Import

**Summary**: QuackOSM was used to convert OSM PBF files to GeoParquet, resolving full way and relation geometries (polygons, multipolygons). This took hours for the planet file and required ~10x the PBF size in temp disk (~850 GB for 85 GB planet).

**Why discarded**: Garganorn only needs point coordinates for gazetteer search, not full polygon geometry. QuackOSM's geometry resolution was wasted work that immediately got reduced to points via `ST_PointOnSurface()`.

**Successor**: osm-pbf-parquet (Rust tool) converts PBF to Hive-partitioned Parquet in ~30 minutes with raw nodes/ways/relations. Centroids computed in SQL via `avg(lat), avg(lon)` of constituent nodes. Acceptable accuracy tradeoff for gazetteer use case.

## Separate IDF Build Stage

**Summary**: Category IDF (Inverse Document Frequency) was originally computed in a separate `build-idf.sh` script that ran before place import, reading from the Stage 1 GeoParquet and producing an IDF lookup table.

**Why discarded**: Added unnecessary pipeline complexity and an extra file to manage. IDF can be computed inline during importance scoring directly from the places table.

**Successor**: `quadtree.py`'s `idf` subcommand — a separate, freshness-gated CLI stage producing `<source>/idf.parquet`, passed back via `--idf-parquet` (the same shape as the build stage this section describes as discarded). The resulting `idf_scores` temp table is injected via `${idf_cte}` and dropped at the end of the source's import SQL.

## Double Metaphone Phonetic Index

**Summary**: Phonetic indexing using Double Metaphone codes via the `splink_udfs` community extension. Tokens were phonetically encoded for fuzzy name matching.

**Why discarded**: Character trigrams provide better cross-word matching (e.g., "pizza hut" generates `za `, `a h` trigrams) and simpler implementation. Full-string Jaro-Winkler scoring handles ranking effectively without token-level phonetic encoding.

**Successor**: Trigram retrieval with full-string Jaro-Winkler scoring — itself since removed with the rest of the Jaro-Winkler machinery when server-side search was deleted (see "Token-Level Scoring with Length Penalty" below).

## Token-Level Scoring with Length Penalty

**Summary**: Token-level Jaro-Winkler scoring split query and name into words, matched tokens greedily by highest JW score, and averaged match scores. Included length penalties to prefer equal-length matches.

**Why discarded**: Full-string JW scoring is simpler and performs as well. The token-level blend (JW_TOKEN_ALPHA = 0.5) is still used, but the implementation is streamlined. Length penalties added complexity without measurable quality improvement.

**Successor**: Simplified 50/50 blend of full-name JW and token-level JW (JW_TOKEN_ALPHA) — itself since removed with the rest of the Jaro-Winkler machinery when server-side search was deleted.

## ART Index for Trigram Column

**Summary**: Proposed using an Adaptive Radix Tree (ART) index on the `name_index.trigram` column to accelerate trigram lookups.

**Why discarded**: DuckDB's zonemaps on sorted columns are sufficient. `name_index` is `ORDER BY trigram`, so DuckDB's min/max statistics per row group identify exact row groups containing each trigram. ART indexes on 616M rows are not buffer-managed and would consume unbounded RAM. The queries that most need optimization (multi-trigram) exceed the selectivity threshold where DuckDB would use the index.

**Successor**: Sorted `name_index` with zone map pruning — itself removed with the rest of the trigram/Jaro-Winkler machinery when server-side search was deleted.

## External Density Parquet Attachment

**Summary**: Density was computed as a standalone artifact (`cell_counts.parquet`) from a global places dataset and attached read-only during import. Separate annual build process.

**Why discarded**: Added operational complexity—tracking a separate file, ensuring version compatibility, manual rebuild schedule. Density computation is fast enough to run inline during import.

**Successor**: `quadtree.py`'s standalone `density` subcommand writes `shared/density_tiles.parquet`, an external Parquet artifact — not database state; `all` runs it first, before `idf`, `overture_division`, and the remaining sources. See `stages.py:stage_density_extract()`.

## Lexicon Discovery via DID/WebFinger

**Summary**: Publishing `org.atgeo.*` lexicons via AT Protocol's standard lexicon resolution mechanism: DNS TXT lookup, DID resolution, and `com.atproto.repo.getRecord` for lexicon schema collection.

**Status**: Partially adopted, not the full mechanism. The DID document route (`/.well-known/did.json`) and `getRecord` serving `com.atproto.lexicon.schema` records both shipped — they're ambient AT Protocol identity plumbing needed regardless of discovery. The DNS TXT record, `listRecords` for the lexicon-schema collection, and `goat`-based verification were not adopted; `listRecords` was implemented once and then removed entirely (unrelated to discovery — see the searchRecords/listRecords removal). Garganorn's actual lexicon-serving interface is `/<nsid>` paths. Standard discovery via DNS/goat tooling is not widely used and hasn't been revisited.

## Structured Place Name Model

**Summary**: Replacing flat `#name` array with Overture-style structured naming: `primary` string, `common` multilingual map, `rules` variant array. Distinguishes official, alternate, short, colloquial variants with explicit type field.

**Why discarded**: Over-engineering for a gazetteer. The current flat `variants` array with optional `type` and `language` fields is sufficient. ATProto working group discussions indicated this was too complex for v1.

**Successor**: Current `variants` array in `place.json` lexicon (optional `type`, `language` fields per variant).

## `same_as` String Array

**Summary**: `same_as` as an array of bare `record-key` strings for cross-dataset identity links.

**Why discarded**: Self-describing references are better. Consumers receiving a place record have no idea what dataset those IDs refer to without parsing or guessing.

**Successor**: `relations.same_as` array of structured `relationEntry` dicts with `collection` + `id` pairs. (Note: As of 2026-04-14, this may not yet be implemented; see the lexicon proposal for details.)


## WoF (WhosOnFirst) Venue Import

**Summary**: Importing WoF's ~20M venue records from 274 GitHub GeoJSON repos. Venues data originates from SimpleGeo's 2011 "Public Spaces Collection" and is fundamentally stale.

**Why discarded**: WoF venue data overlaps with Foursquare OSP and Overture Places but adds limited value. The data is stale (2011 origin), unevenly covered (heavily US-skewed), and has no bulk download format. WoF admin boundaries are more useful for gazetteering.

**Successor**: WoF admin boundaries only. Venues deferred pending multi-source deduplication strategy.
