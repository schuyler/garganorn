# Known data quality issues

Characteristics of the source data (Overture, OSM) that were investigated
and deliberately left unfixed. Garganorn reports source data as-is; it
does not adjudicate or correct disagreements between source records. This
document exists so that stance doesn't have to be re-derived each time a
scan of the data turns up something that looks like a bug.

## Overlapping same-level administrative boundaries

If two same-level administrative boundaries genuinely overlap according
to the source, that's legitimate content to report — not a bug to detect,
flag, or reject. A place correctly reported as being within two
overlapping same-level regions is the correct answer, not a wrong one.
Garganorn's job is to faithfully report what the source says, not
adjudicate whose claim is right.

Measured directly against the 2026-08-08 production build on `garganorn-1`
(`overture_division/covering/*.parquet`, `boundaries.duckdb`; 617,734
boundaries, 1,106,134 edge-arm tiles at the containment test's terminal
zoom): distribution of distinct same-level boundaries per tile — mean 3.9,
median 3, p90=8, p99=16, p999=31, max=490. High-overlap hotspots are a
disjoint set from the slow-cell list in `performance-improvements.md`'s
containment-performance section, and are cheap per-boundary despite the
high count: Taipei/New Taiwan (490
boundaries in one tile — Taiwan's fine-grained 里/li subdivisions, 5–810
vertices each), Guangxi and Shandong China, Java Indonesia, Delhi India,
Zambia's Copperbelt.

## Duplicate boundary records

A global exact-bbox self-join (same admin subtype, identical
`min/max_lat/lon`) found 711 pairs of boundaries with `ST_Equals`-identical
geometry stored under two different boundary IDs (and often two name
variants) — e.g. Tunis governorate stored as both `ولاية تونس` and `تونس`
under separate IDs. Concentrated in Thailand (205 pairs), Japan (153),
Russia (141), China (37); ~0.2% of all boundaries. A separate, smaller
pattern in Indonesia: 22 `Kel. <name>` / `<name>` pairs (Kelurahan-prefix
variants of the same feature) with IoU 0.92–0.98, not exact matches.

Won't-fix, exact and fuzzy pairs alike: collapsing even byte-identical
duplicates is still a decision about which of two source records is the
"real" one, which is the same adjudication this project stays out of for
disagreeing boundaries generally.
