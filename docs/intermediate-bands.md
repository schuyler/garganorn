# Intermediate overview bands

Sampled tile levels between the regular tiles and the z1–z5 overview band, so a city-sized view gets thousands of its most notable places instead of a couple of hundred arbitrary ones. Not built.

**Status: proposed, awaiting approval** in [#36](https://github.com/schuyler/garganorn/issues/36#issuecomment-6456426154). The proposal there folds a notability signal ([#46](https://github.com/schuyler/garganorn/issues/46)) into `importance` and adds the overview pyramid in the same rebuild. [The proposal](#the-proposal-one-rebuild-for-notability-and-the-pyramid) below summarizes it. The measurements that led to it come first.

Measured 2026-10-26 against places.atgeo.org's global builds, with `max_tiles` at 75.

## The gap

A `getTiles` answer is either everything in the box (the regular tiles, if they fit the budget) or the overview band: the top 10,000 records of the whole tileset, by importance, at z1–z5. Nothing in between. A view that needs 76 tiles gets the same planet-wide sample as a view of a continent.

City views fall into that gap. For a 506 by 470 pixel map of Overture places, a z11 view needs 99 to 655 regular tiles across 36 big cities, and z12 needs 38 to 308 (Beijing and Shanghai excepted, where both sources are thin) (full table in [atgeo-client-sdk.md](atgeo-client-sdk.md#how-much-this-costs)). Over budget, the answer is the overview band, and the overview band holds very little of any one city: the z11 San Francisco view has 59,695 Overture places and 214 of them are in the band. OSM: 16,115 and 58.

Raising the budget only moves the gap. Each zoom level out roughly doubles the tiles a view needs, so a city-wide view needs a budget in the hundreds, and every tile costs a client about 1 MB of JSON.

## The catch: the sample is only as good as the ranking

The sampling rank can't be today's importance. For places, importance is `60 * density + 40 * idf`: busy neighbourhood times rare category. That ranks the rare businesses of downtown above the city's landmarks. The top of the z11 San Francisco view, for Overture:

> Shorty Goldstein's (85), Crow & Rose, Tenant Lawyers (84), LinkedIn Profile & Resume Writing Services (83), Aqua Gig (82), Voice Automated (82)…

Coit Tower is 7,994th, Oracle Park 8,218th, the Exploratorium 42,538th of 59,695. OSM is the same story: Ellison Enterprises Corp tops it, and the Exploratorium is 14,129th of 16,115.

So I simulated the bands on that view: fetch every record, group by cell, keep each cell's top K. Then count how many of six landmarks (Coit Tower, Oracle Park, Exploratorium, Ferry Building, Palace of Fine Arts, Golden Gate Park) have at least one record in the sample whose name contains theirs. All six are in both full datasets. Records come in under 7 × K because quiet cells hold fewer than K.

| Source | Sample | Tiles | Records | Landmarks |
|---|---|---|---|---|
| Overture | today's z1–z5 band | 1 | 214 | 0 of 6 |
| Overture | z12, K=500, by importance | 7 | 3,007 | 2 of 6 |
| Overture | z12, K=1000, by importance | 7 | 5,554 | 3 of 6 |
| OSM | today's z1–z5 band | 1 | 58 | 0 of 6 |
| OSM | z12, K=500, by importance | 7 | 2,629 | 1 of 6 |
| OSM | z12, K=1000, by importance | 7 | 4,429 | 4 of 6 |
| OSM | z12, K=250, Wikidata/Wikipedia tag first | 7 | 1,379 | 5 of 6 |
| OSM | z12, K=500, Wikidata/Wikipedia tag first | 7 | 2,629 | 6 of 6 |

A crude notability signal, "has a `wikidata` or `wikipedia` tag" (1,098 of the 16,115 OSM records in view), is enough to make the sample work. Overture places carry no per-place Wikidata ID in our tiles, so they need another way in. [wikimedia-importance-evaluation.md](wikimedia-importance-evaluation.md) names "no notability signal" as the problem and finds no direct join path. The candidate is conflation with OSM; see [Notability goes into `importance`](#notability-goes-into-importance).

A notability signal pays off on its own too: the z1–z5 band is cut by the same importance, so it improves today's overview answers as well.

Generic searches stay zoom-in territory. A notable-places sample holds a few coffee shops, not the 357 Overture places with "coffee" in the name in that San Francisco view. That's what `overview: true` is for: the SDKs pass it through, and the app decides what to do. See [Generic searches, meanwhile](#generic-searches-meanwhile).

## The proposal: one rebuild for notability and the pyramid

Ranking happens at build time. `importance` is written into every tile record, and the z1–z5 band and every sampled level are top-N cuts taken during the build. So a notability signal and the sampled levels each need a rebuild, and they should share one. Building the levels on today's importance would bake in the wrong sample.

### Notability goes into `importance`

A rank used only by the sampled levels looks spec-free, but it isn't. The spec defines the z1–z5 band as the records with the highest `importance`, so cutting that band by another rank is a spec change anyway, and leaving the band on today's rank leaves it at 0 of 6 landmarks. `importance` is also how the SDKs' default `fused` sort ranks a search with no text, and its tiebreak between equal (rounded) text scores, so those answers lead with landmarks too, with no SDK change. The spec change is to the text of its "Importance" section, plus the overview-band and `getTiles` wording the pyramid design lists (including the "never mixes the two bands" sentence). There's no lexicon or wire change: it's still an integer from 0 to 100.

It has to be lexicographic, not a bonus. In a dense downtown cell, density saturates and rare-category businesses score 82 to 85, so a bonus or a `max(base, X)` floor leaves a landmark below them unless X is at least about 85. At that point it's a lexicographic split anyway. The simulation that worked ordered by "tagged first, then importance". For OSM:

    notable:      importance = 71 + round(29 · min(pr / P, 1))
    not notable:  importance = round(0.70 · (60 · min(density/10, 1) + 40 · min(idf/18, 1)))

Here `pr` is the Nominatim Wikipedia page rank (0 to 1) for the record's QID, or 0 when the file doesn't have it, and `P` is set by measurement. Every notable record outranks every non-notable one, and the non-notable records keep their order. Squeezing 0–100 into 0–70 creates ties, broken by id as now.

What counts as notable:

- **OSM:** a bare `wikidata` tag, or a `wikipedia` tag (`lang:Title`) resolved to a QID through the page-rank file's `(language, title)` columns. `brand:wikidata`, `operator:wikidata`, `subject:wikidata` and the like don't count. A QID carried by more than a few elements is treated as a brand that mappers put in the wrong key, and doesn't count either; the threshold is to be measured.
- **Overture places:** nothing usable today. `brand.wikidata` measures the brand's fame, not the place's, so it's excluded. The candidate is conflation: borrow the QID of a notable OSM record with a matching name (at or above the SDKs' Jaro-Winkler threshold) within about 150 m. It ships in the rebuild only if a client-side prototype on San Francisco and a few of the 36 cities shows about 90% precision on a hand-checked sample. Otherwise Overture places keep today's formula, and the docs say their samples rank by busy area and rare category.
- **Overture divisions:** unchanged. The z1–z5 band already carries every country, region and dependency, and population orders localities sensibly.

### The levels: an overview pyramid

The design is in [#36](https://github.com/schuyler/garganorn/issues/36#issuecomment-6412338067). It replaces this document's original sketch, which put a band tile on every cell at z8, z10 and z12:

- **Dense tiles only.** At each level L, only an L-tile holding more than `max_per_tile` places is sampled, to its top K by importance. Everywhere else the regular tiles at zoom L or shallower are already complete, so the level-L tiling of an area is those tiles plus the sampled dense ones. That's a partition of the area: every point falls in at most one tile, and every place in a complete tile is there, while a sampled tile keeps only its top K.
- **Serving.** `getTiles` answers from the deepest built level whose tile count fits the budget, with `overview: true`. It falls back to the z1–z5 band as now. A view whose regular tiles fit answers exactly as today.
- **Namespace.** Sampled tiles live in `<run>/p/…` and get a `band` column in the tile assignments and in `record_tiles`, because a z10 sampled tile and a z10 regular tile share a quadkey. Records are byte-identical to their regular copies, so client dedupe is unaffected. Old runs keep loading.
- **Levels: even zooms, 6 to 16. K = 500.** With notable records sorted first, landing a level coarser costs little, and K = 500 found all six San Francisco landmarks for OSM at z12. A 75-tile answer made entirely of sampled tiles holds about 37,500 records, about what a typical full regular answer costs now (~6 MB on the wire, for Overture). An answer that mixes in complete regular tiles is bounded the same way a regular answer is.
- **Per source:** on for Overture places and OSM, off for divisions, which have no dense tile deeper than z13.

### Cost

- **Overture places, measured:** the even-zoom dense-tile counts (565 + 3,044 + 10,881 + 16,099 + 12,407 + 1,465 = 44,461 tiles) at K = 500 come to 22.2M records. That's 0.25× the regular band's 88.4M named places (Overture 2026-10-21.0), about 4.2 GB gzipped at 190 B a record. The extra storage is linear in K, since every sampled tile holds more than 1000 places.
- **OSM:** not measured. Count dense tiles per even level from `places.parquet` before the rebuild.
- **Page-rank lookup:** a few hundred MB of Parquet, on the build side only.
- **Rebuild time, estimated, not measured:** the last forced planet build took about 3h23m. Six group-by-and-window passes per source, about 25% more Overture export, and the page-rank join should add 15 to 30%, so about 4 to 4.4 hours. Conflation, if it ships, is the unknown term. Time the stages on a regional extract first.

### Re-measure against the 36-city set before building

1. Which level answers z11, z12 and z13 views (506 by 470 pixels, budget 75), with how many tiles and how many records in view.
2. Landmark recall: per city, the share of the top 10 OSM records in view by page rank that land in the K = 500 sample at the answering level.
3. The new OSM z1–z5 band: it should still hold the 36 cities' own city nodes, and no one country's tagging habits should dominate it.
4. OSM storage, and the brand-guard threshold.
5. Conflation precision, if attempted.

After the rebuild, repeat [#46](https://github.com/schuyler/garganorn/issues/46)'s check: the six San Francisco landmarks are in the z1–z5 band, or in the top few hundred of the view.

### Not in this change

A budget counted in records rather than tiles. Records are the real cost and tiles only a proxy, but it changes what `maxTiles` means in the spec. Measure it later.

## Generic searches, meanwhile

A notable-places sample is the wrong answer for "coffee". That search needs complete data over a smaller area. The atgeo.org map demo does that on the client today, with no server change. When the view is over budget, it asks `getTiles` about successively halved boxes around the map centre, snapped to the 0.01° grid, with at most four probes. It then searches the first box that gets regular tiles, and says it searched only the central few kilometres. Measured on the live server at z12 and z13, it finds 17 to 105 coffee places in Philadelphia, London, Tokyo and São Paulo, where the overview answer found 0 or 1.

That's an app choosing a smaller region, not the SDK covering a large one with several calls, so it keeps to the SDK's no-subdivision rule. Once the pyramid ships, `overview: true` stops meaning "planet-wide sample". The demo will then need to decide when a regional notable sample beats a smaller complete box.

## How these numbers were made

A script against the public server, not the pipeline. For a 506 by 470 pixel view (the atgeo.org demo map), it computes the bbox at a given zoom, splits it into sub-boxes until each fits the budget, unions their `getTiles` answers (the set an unbudgeted server would return), then fetches the tiles. The band simulation groups the fetched records by the quadkey of their point at the band zoom and keeps the top K. It's a client-side approximation: it uses the records whose point is inside the view, and it ignores divisions.
