# Known data quality issues

Characteristics of the source data (Overture, OSM) that were investigated
and left unfixed. Garganorn reports source data as-is; it does not
adjudicate or correct disagreements between source records. This document
exists so that stance doesn't have to be re-derived each time a scan of
the data turns up something that looks like a bug. Most entries are
settled won't-fixes; where a disposition is still open, the section says
so.

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
disjoint set from the cells known to be slow in containment, and are cheap
per-boundary despite the high count: Taipei/New Taiwan (490 boundaries in
one tile — Taiwan's fine-grained 里/li subdivisions, 5–810
vertices each), Guangxi and Shandong China, Java Indonesia, Delhi India,
Zambia's Copperbelt.

## Divisions referenced from very many tiles

`stage_division_tile_references` reads a division's tile references
straight off its covering leaves, so a division with sprawling geometry is
referenced from every tile it overlaps and nothing bounds or reports the
count. Measured against the 2026-08-15 build on `garganorn-1`
(`overture_division/tiles/current/manifest.duckdb`): 1,211,209 references
over 3,319 distinct tiles, with a single division reaching 505 of them.
Earlier Discovery cases were Antarctica at 71 cells at z4 and a level-50
division at 27 cells at z7 against a level-50 median of 1.

The disposition here is not settled. Antarctica's spread is the source's
correct answer and bounding it would be wrong; a level-50 division at 27
cells is more likely a defective geometry; and no measurement so far
separates the two. The global totals above don't either — they say nothing
about which records make up the tail.

## One `division_area` row per division

The Overture schema permits a division to have many `division_area` rows,
and non-contiguous territory is the obvious reason to expect them. In
`2026-07-22.0` it does not happen: 1,071,108 land areas cover 1,071,108
divisions, one apiece. Non-contiguity is carried inside the geometry
instead — 38,019 of those areas are MULTIPOLYGON rather than POLYGON. The
same scan puts `division` at 4,655,003 rows against `division_area`'s
1,071,108, so the inner join between them drops ~3.6M divisions that carry
no land geometry at all.

The consequence is that `overture_division_import.sql`'s `ST_Union_Agg`
has nothing to merge — measured as a spatial no-op on this release, though
it does normalize ring order and so re-serializes those 38,019
multipolygons. It is kept deliberately: 30.8s against a 97-minute build is
cheap insurance, and a release that does emit two rows for one division
must still yield one geometry.

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
