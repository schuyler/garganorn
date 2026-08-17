# Wikimedia Importance as a Data Source

Evaluated 2026-04-11. Not yet implemented.

## Problem

Garganorn's importance formula (`60 * density + 40 * idf` for places; division
scoring differs — see `design-constraints.md`, "Importance scoring varies by
entity type") has no notability signal. Garganorn doesn't serve text queries
today — it registers only `getRecord` and `getCoverage` — but if it did, there
would be nothing to distinguish the Golden Gate Bridge from a nearby dentist's
office with similar density: both would rank equally.

## Nominatim's Approach

Nominatim publishes preprocessed Wikipedia page-rank data:

- **Primary importance** (`wikimedia-importance.csv.gz`, 307MB, ~19M rows):
  Maps Wikipedia articles and redirects to importance scores (0–1 float)
  via Wikidata QIDs. Every row has a `wikidata_id` column.
- **Secondary importance** (spatial raster): Area-level notability used as a
  tiebreaker. Conceptually similar to garganorn's density signal.

Docs:
- https://nominatim.org/release-docs/latest/customize/Importance/
- https://nominatim.org/release-docs/latest/customize/Ranking/

## The Data File

`wikimedia-importance.csv.gz` from `nominatim.org/data/`:

| Column | Type | Example |
|--------|------|---------|
| language | string | `en` |
| type | `a` (article) or `r` (redirect) | `a` |
| title | string | `Golden_Gate_Bridge` |
| importance | float 0–1 | `0.5410855989806941` |
| wikidata_id | QID | `Q44440` |

~10.3M article rows, ~8.7M redirect rows. Redirects share the same QID and
importance as their target article. For joining, collapse to one row per QID
(max importance).

## Wikidata Coverage in Garganorn's Sources

Investigated against Overture 2026-03-18.0 parquet on 2026-04-11.

| Source | Wikidata QID? | How | Coverage |
|--------|--------------|-----|----------|
| OSM | Yes | `tags['wikidata']` | Not yet measured; common on landmarks, parks, transit, universities, museums |
| Overture Places | Partial | `brand.wikidata` | 1.3M of 72.8M places (1.8%); chains/brands only |
| Overture Divisions | Yes | Dedicated `wikidata` VARCHAR column | Not yet measured |
| Foursquare | No | — | — |

### Overture Places `sources[]` — investigated, not useful

The `sources[]` array contains dataset + record_id per contributing source.
The distinct dataset values in the 2026-03-18.0 release:

| dataset | count |
|---------|-------|
| Overture | 72,783,221 |
| meta | 58,843,409 |
| Foursquare | 6,503,660 |
| Microsoft | 5,492,871 |
| AllThePlaces | 1,651,532 |
| DAC | 153,642 |
| PinMeTo | 130,234 |
| RenderSEO | 4,685 |
| Krick | 3,188 |

No OSM and no Wikidata as a source dataset. Despite OSM contributing to
other Overture themes (buildings, transportation, divisions), Overture Places
is assembled entirely from non-OSM sources. The `sources[]` field does not
provide a Wikidata join path.

### Overture Places `brand.wikidata` — brands only, not landmarks

Covers chain/brand places (Domino's Q839466, Avis Q791136, Ford Q44294,
TotalEnergies Q154037, etc.). Does not cover landmarks or other non-branded
notable places. A search for "Golden Gate Bridge" in Overture Places returns
41 results from meta/Microsoft/Foursquare sources, none with `brand.wikidata`.

### GERS bridge files — investigated, not useful for Wikidata

Overture's Global Entity Reference System (GERS) ships bridge files (Parquet
on S3) mapping GERS IDs to source dataset IDs. Bridge files exist for: Esri
Community Maps, geoBoundaries, Instituto Geográfico Nacional, Meta Places,
Microsoft Places, OpenStreetMap, and PinMeTo.

No Wikidata bridge file exists.

### Wikidata P13219 (GERS ID property) — defined but empty

Wikidata has a property P13219 for storing GERS IDs on entities. As of
2026-04-11, exactly **2** Wikidata entities have this property populated.
Not a viable join path.

Source: https://www.wikidata.org/wiki/Property:P13219

### Summary of join paths

For connecting garganorn places to Wikipedia importance scores:

1. **OSM → Wikidata**: Direct. OSM `wikidata` tags are the primary path for
   landmarks, monuments, parks, transit stations, etc. This is the only path
   that covers the Golden Gate Bridge class of query.

2. **Overture Places → Wikidata**: Only via `brand.wikidata` (1.8% of places).
   Useful for chain disambiguation but not for landmark notability.

3. **Overture Divisions → Wikidata**: Direct. Useful for boosting
   administrative boundaries (cities, regions, countries).

4. **Foursquare → Wikidata**: No path.

## Why It Matters

The current importance formula measures "busy area" (density) and "unusual
category" (IDF). Neither measures "famous." If garganorn served text-only
queries without a bounding box, the system would need a signal that says
"Golden Gate Bridge" is a globally-known landmark, not just another POI in a
dense area.

Wikipedia importance directly measures notability — how much attention a
place receives. Places with Wikidata QIDs skew toward exactly the class that
needs this signal: landmarks, monuments, transit stations, universities,
museums, government buildings, and well-known commercial brands.

## Integration Considerations

- The wikimedia CSV could be collapsed to a parquet lookup table (one row per
  QID, max importance) and joined during the importance stage.
- Places with a Wikidata match could receive a notability bonus that
  supplements or partially replaces the density/IDF formula.
- Places without Wikidata IDs keep the current formula unchanged.
- ~10M unique QIDs — small enough for a simple lookup join.
- The importance float (0–1) would need to be scaled to garganorn's 0–100
  integer range.
- The strongest impact would be on the OSM data source, where wikidata tags
  are common on notable places. Overture Places would see a smaller benefit
  limited to branded chains.
