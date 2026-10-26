# Intermediate overview bands

A proposal to put sampled tile bands at z8, z10 and z12, between the regular tiles and the z1–z5 overview band, so a city-sized view gets a few thousand of its most notable places instead of a couple of hundred arbitrary ones. Not built. It depends on importance knowing what's notable, which it doesn't yet.

Measured 2026-10-26 against places.atgeo.org's global builds, with `max_tiles` at 75.

## The gap

A `getTiles` answer is either everything in the box (the regular tiles, if they fit the budget) or the overview band: the top 10,000 records of the whole tileset, by importance, at z1–z5. Nothing in between. A view that needs 76 tiles gets the same planet-wide sample as a view of a continent.

City views fall into that gap. For a 506 by 470 pixel map of Overture places, a z11 view needs 99 to 655 regular tiles across 36 big cities, and z12 needs 38 to 308 (Beijing and Shanghai excepted, where both sources are thin) (full table in [atgeo-client-sdk.md](atgeo-client-sdk.md#how-much-this-costs)). Over budget, the answer is the overview band, and the overview band holds very little of any one city: the z11 San Francisco view has 59,695 Overture places and 214 of them are in the band. OSM: 16,115 and 58.

Raising the budget only moves the gap. Each zoom level out roughly doubles the tiles a view needs, so a city-wide view needs a budget in the hundreds, and every tile costs a client about 1 MB of JSON.

## The idea

Build more overview bands, at z8, z10 and z12. A band tile holds the top K records (K around 500) of its cell, by some sampling rank. Records go in by their point, as in the regular tiles; divisions also go into every band tile their geometry overlaps, as in the regular tiles and the z1–z5 band.

`getTiles` tries the regular tiles first, as now. Over budget, it tries the z12 band, then z10, then z8, then z1–z5, and answers with the first that fits, with `"overview": true`. The z1–z5 band stays the uncapped last resort.

Because the sample is per cell, its density follows the data's: a busy cell keeps its top 500 and a quiet cell keeps everything it has. A band answer at the budget costs about what a regular answer at the budget costs, since band tiles hold about as many records as regular ones (regular tiles average about 500 records).

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

A crude notability signal, "has a `wikidata` or `wikipedia` tag" (1,098 of the 16,115 OSM records in view), is enough to make the sample work. Overture places carry no per-place Wikidata ID in our tiles, so they need a real notability score: that's [wikimedia-importance-evaluation.md](wikimedia-importance-evaluation.md), which already names "no notability signal" as the problem.

That work comes first, and it pays off on its own: the z1–z5 band is cut by the same importance, so a notability signal improves today's overview answers with no wire or pipeline change. The bands are the second step.

Generic searches stay zoom-in territory. A notable-places sample holds a few coffee shops, not the 357 Overture places with "coffee" in the name in that San Francisco view. That's what `overview: true` is for: the SDKs pass it through, and the atgeo.org demo turns it into "zoom in for more".

## What changes

**The wire: nothing required.** Tile URLs are opaque, a band answer is an overview answer, and a record in a band tile is byte-identical to its regular copy, so the deduplication rule covers it. Clients and SDKs work unchanged. The tileset descriptor's `overview.records` counts the z1–z5 band today; whether it should describe the other bands (an additive `overview.bands: [{zoom, records}]`, say) is an open question. The spec's "zooms 1 through 5" and "the overview answer is uncapped" sentences need rewording: band answers are capped by the budget, and only the z1–z5 band isn't.

**Pipeline: the band tiles need a namespace of their own.** Quadkey length is what marks the z1–z5 band today (`TileManifest`, `stage_export`'s `qk[:6]` layout), and that only works because z1–z5 can't collide with the regular z6–z17 tiles. A z10 band tile and a z10 regular tile have the same quadkey. So:

- a band column on the tile assignments and on `manifest.duckdb`'s `record_tiles`, and a band directory in the run (`<run>/band/<qk[:6]>/<qk>.json.gz`, say), served at an opaque URL of its own;
- `_union_tile_assignments` can't rely on disjoint zooms any more. A place can be in regular tile `qk` and band tile `qk`, so every join on `(place_id, tile_qk)` (containment, export) has to key on the band too. Get that wrong and the join fans out or a copy gets a different `relations.within`. See design-constraints.md, "A record may be referenced by more than one tile";
- a band stage per source, modelled on `stage_summary_tile_assignment` (and `stage_summary_division_tile_references` for divisions), but cut per cell, not globally.

**Server:** `TileManifest` grows a lookup per band, and `get_tiles` walks the bands finest first. Each level is a bbox intersection over that band's quadkeys, the same as `regular_tiles` today.

## Open questions

- **Storage.** Unmeasured. Each band adds at most one copy of each record, and in practice `min(K, n)` per cell. Sparse cells copy everything, so the overhead is worst where the data is thinnest. Measure it by grouping `places.parquet` by `left(qk17, L)` for each band zoom L and summing `least(count(*), K)`.
- **Levels and K.** z8/z10/z12 with K=500 rests on one city. Try it against the 36-city set before settling.
- **Rank.** Fold notability into `importance`, or give the bands their own rank and leave importance alone? Importance is on the wire and comparable across one dataset's tilesets, so changing it is a spec matter. A band-only rank isn't.
- **Per-request or per-cell levels.** Picking one band for the whole answer is simple. Mixing per cell (regular tiles where a cell is sparse, band tiles where it's dense) would hand back complete data in more places, at the cost of a fiddlier `get_tiles`.

## How these numbers were made

A script against the public server, not the pipeline. For a 506 by 470 pixel view (the atgeo.org demo map), it computes the bbox at a given zoom, splits it into sub-boxes until each fits the budget, unions their `getTiles` answers (the set an unbudgeted server would return), then fetches the tiles. The band simulation groups the fetched records by the quadkey of their point at the band zoom and keeps the top K. It's a client-side approximation: it uses the records whose point is inside the view, and it ignores divisions.
