# OSM Import Design

## Goal

Add OpenStreetMap as a third data source in Garganorn, alongside Foursquare
OSP and Overture Maps. The primary deployment is global; the pipeline should
also support regional extracts for smaller instances.

## Data Model

### `places` table

```sql
CREATE TABLE places (
    osm_type     VARCHAR,    -- 'n', 'w', 'r' (single char)
    osm_id       BIGINT,     -- OSM feature ID
    name         VARCHAR,    -- tags['name'], extracted for indexing
    latitude     DOUBLE,     -- node lat, or centroid of way/relation
    longitude    DOUBLE,     -- node lon, or centroid of way/relation
    geom         GEOMETRY,   -- point geometry for spatial index
    tags         VARCHAR[],  -- retained tags as 'key=value' strings
    importance   INTEGER     -- computed 0-100 score (60% density + 40% IDF)
);
```

The `rkey` for records is `{osm_type}{osm_id}` (e.g., `n240109189`,
`w50637691`, `r12345`). Since `osm_type` is stored as a single char,
no indexing is needed.

Tags are stored as a flat array of `key=value` strings. The primary category
(used for IDF) is determined at import time by tag priority order and stored
as `tags[1]`. All other retained tags follow.

### `name_index` table

Same structure as FSQ and Overture: trigrams with denormalized name, lat/lon,
and importance. Join key is a synthetic `rkey` column matching the places
table format.

```sql
CREATE TABLE name_index (
    trigram    VARCHAR,
    rkey      VARCHAR,    -- '{osm_type}{osm_id}'
    name      VARCHAR,
    latitude  VARCHAR,
    longitude VARCHAR,
    importance INTEGER
);
```

### Spatial index

R-tree on `geom`, same as existing sources.

```sql
CREATE INDEX places_rtree ON places USING RTREE (geom);
```

## Import Pipeline

### Overview

```
Geofabrik PBF ──► QuackOSM (Stage 1) ──► GeoParquet ──► DuckDB SQL (Stage 2) ──► osm.duckdb
                  broad tag filter         intermediate    precise tag filter
                  geometry resolution       file            bbox filter (if regional)
                                                           centroid computation
                                                           density/IDF/importance
                                                           name_index build
```

### Stage 1: QuackOSM — PBF to GeoParquet

QuackOSM reads the PBF, resolves way/relation geometries (the hard part),
and applies a broad tag filter. It outputs a GeoParquet with three columns:
`feature_id` (string like `node/12345`), `tags` (MAP(VARCHAR, VARCHAR)),
`geometry` (WKB).

```python
import quackosm

quackosm.convert_pbf_to_parquet(
    pbf_path=pbf_path,
    tags_filter={
        "amenity": True,
        "shop": True,
        "tourism": True,
        "leisure": True,
        "office": True,
        "craft": True,
        "healthcare": True,
        "historic": True,
        "natural": True,
        "man_made": True,
        "aeroway": True,
        "railway": True,
        "public_transport": True,
        "place": True,
    },
    keep_all_tags=True,
    explode_tags=False,
    result_file_path=output_parquet,
)
```

QuackOSM requires ~10x the PBF file size in temporary disk space during
processing. For the 85 GB planet file, that's ~850 GB. For a 4 GB regional
extract, ~40 GB.

QuackOSM is a build-time dependency only (not a runtime dependency). It
pulls in geopandas, numpy, shapely, pyarrow, etc.

### Stage 2: DuckDB SQL — Precise Filtering and Import

A SQL script (sourced by the shell import script) applies the curated tag
filtering, computes centroids, assigns primary categories, and builds the
final tables. This is where all the domain logic lives.

#### Tag filtering

Per-key inclusion/exclusion with name requirements. The full tag mapping
is documented in `docs/osm_tag_mapping.md`.

```sql
-- Conceptual structure of the tag filter CTE
WITH filtered AS (
    SELECT
        left(split_part(feature_id, '/', 1), 1) AS osm_type,
        split_part(feature_id, '/', 2)::BIGINT AS osm_id,
        tags['name'][1] AS name,
        geometry,
        tags,
        CASE
            -- Primary category by priority order
            WHEN tags['amenity'][1] IS NOT NULL THEN 'amenity=' || tags['amenity'][1]
            WHEN tags['shop'][1] IS NOT NULL THEN 'shop=' || tags['shop'][1]
            WHEN tags['tourism'][1] IS NOT NULL THEN 'tourism=' || tags['tourism'][1]
            WHEN tags['leisure'][1] IS NOT NULL THEN 'leisure=' || tags['leisure'][1]
            WHEN tags['office'][1] IS NOT NULL THEN 'office=' || tags['office'][1]
            WHEN tags['craft'][1] IS NOT NULL THEN 'craft=' || tags['craft'][1]
            WHEN tags['healthcare'][1] IS NOT NULL THEN 'healthcare=' || tags['healthcare'][1]
            WHEN tags['historic'][1] IS NOT NULL THEN 'historic=' || tags['historic'][1]
            WHEN tags['natural'][1] IS NOT NULL THEN 'natural=' || tags['natural'][1]
            WHEN tags['man_made'][1] IS NOT NULL THEN 'man_made=' || tags['man_made'][1]
            WHEN tags['aeroway'][1] IS NOT NULL THEN 'aeroway=' || tags['aeroway'][1]
            WHEN tags['railway'][1] IS NOT NULL THEN 'railway=' || tags['railway'][1]
            WHEN tags['public_transport'][1] IS NOT NULL THEN 'public_transport=' || tags['public_transport'][1]
            WHEN tags['place'][1] IS NOT NULL THEN 'place=' || tags['place'][1]
        END AS primary_category
    FROM read_parquet('stage1_output.parquet')
    WHERE
        -- Amenity: all except infrastructure, require name
        (tags['amenity'][1] IS NOT NULL
         AND tags['amenity'][1] NOT IN (
             'parking', 'parking_space', 'bench', 'waste_basket',
             'bicycle_parking', 'shelter', 'recycling', 'toilets',
             'post_box', 'drinking_water', 'vending_machine',
             'waste_disposal', 'hunting_stand', 'parking_entrance',
             'grit_bin', 'give_box', 'bbq'
         )
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Shop: all except yes/vacant, require name
        (tags['shop'][1] IS NOT NULL
         AND tags['shop'][1] NOT IN ('yes', 'vacant')
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Tourism: all, require name
        (tags['tourism'][1] IS NOT NULL
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Leisure: curated list, require name
        (tags['leisure'][1] IN (
             'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
             'golf_course', 'stadium', 'sports_hall', 'marina',
             'nature_reserve', 'garden', 'playground', 'dog_park',
             'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
             'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
             'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
             'escape_game', 'hackerspace'
         )
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Office: all except yes, require name
        (tags['office'][1] IS NOT NULL
         AND tags['office'][1] != 'yes'
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Craft: all except yes, require name
        (tags['craft'][1] IS NOT NULL
         AND tags['craft'][1] != 'yes'
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Healthcare: all except yes, require name
        (tags['healthcare'][1] IS NOT NULL
         AND tags['healthcare'][1] != 'yes'
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Historic: curated list, require name
        (tags['historic'][1] IN (
             'castle', 'monument', 'memorial', 'archaeological_site',
             'ruins', 'fort', 'manor', 'church', 'city_gate',
             'building', 'mine', 'wreck'
         )
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Natural: curated list, require name
        (tags['natural'][1] IN (
             'peak', 'beach', 'spring', 'bay', 'cave_entrance',
             'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
             'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock'
         )
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Man-made: curated list, require name
        (tags['man_made'][1] IN (
             'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
             'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
             'beacon'
         )
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Aeroway: airports and terminals only, require name
        (tags['aeroway'][1] IN ('aerodrome', 'terminal', 'heliport')
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Railway: stations only, require name
        (tags['railway'][1] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Public transport: stations only, require name
        (tags['public_transport'][1] = 'station'
         AND tags['name'][1] IS NOT NULL)
        OR
        -- Place: populated places, require name
        (tags['place'][1] IN (
             'city', 'town', 'village', 'hamlet', 'suburb',
             'neighbourhood', 'quarter', 'island', 'square'
         )
         AND tags['name'][1] IS NOT NULL)
)
```

Note: The `tags` column from QuackOSM is a `MAP(VARCHAR, VARCHAR)`. The
`[1]` index is DuckDB's syntax for accessing the first (and typically only)
value for a key in a MAP.

#### Geometry handling

Nodes produce POINT geometry directly. Ways and relations produce POLYGON
or MULTIPOLYGON from QuackOSM. Point computation happens in stage 2
using `ST_PointOnSurface()`, which guarantees the resulting point lies
within the feature boundary (unlike `ST_Centroid()`, which can fall
outside concave polygons — e.g., a crescent-shaped park):

```sql
ST_Y(ST_PointOnSurface(geometry))::DECIMAL(10,6) AS latitude,
ST_X(ST_PointOnSurface(geometry))::DECIMAL(10,6) AS longitude,
ST_PointOnSurface(geometry) AS geom
```

For nodes, `ST_PointOnSurface(POINT)` is a no-op — it returns the
point itself.

#### Tags array construction

Convert from QuackOSM's MAP to our VARCHAR[] format. The primary category
goes first, followed by other retained tags:

```sql
-- Build tags array: primary category first, then supplementary tags
list_prepend(
    primary_category,
    list_filter(
        map_entries(tags)
            .transform(e -> e.key || '=' || e.value),
        x -> x != primary_category
           AND split_part(x, '=', 1) IN (
               'cuisine', 'sport', 'religion', 'denomination',
               'opening_hours', 'phone', 'website', 'wikidata',
               'wheelchair', 'internet_access',
               'addr:street', 'addr:housenumber', 'addr:city',
               'addr:postcode', 'addr:country'
           )
    )
) AS tags
```

The set of retained supplementary tags is configurable. Tags not in this
list are discarded at import time.

#### Bbox filtering (regional extracts)

If bbox arguments are provided to the import script, add a spatial filter:

```sql
WHERE ST_X(ST_Centroid(geometry)) BETWEEN $xmin AND $xmax
  AND ST_Y(ST_Centroid(geometry)) BETWEEN $ymin AND $ymax
```

For global imports, this clause is omitted.

### Density and IDF

Both `build-density.sh` and `build-idf.sh` need an `osm` mode.

**Density**: Same S2 cell aggregation. Reads lat/lon from the stage 1
GeoParquet (or the built DuckDB). Uses `ST_X(ST_PointOnSurface(geometry))`
and `ST_Y(ST_PointOnSurface(geometry))` for coordinates (consistent with
the geometry handling decision above).

**IDF**: Uses the primary category string (`amenity=restaurant`, etc.).
The IDF query extracts the primary category using the same CASE priority
logic, or reads it from the `tags[1]` position in the final places table.

### Importance scoring

Same formula as FSQ and Overture: `60% density + 40% IDF`, normalized to
an integer 0-100.

## Database Class

```python
class OpenStreetMap(Database):
    collection = "community.lexicon.location.org.openstreetmap.places"

    def record_columns(self):
        return """
            osm_type || osm_id::VARCHAR AS rkey,
            name,
            latitude::DECIMAL(10,6)::VARCHAR AS latitude,
            longitude::DECIMAL(10,6)::VARCHAR AS longitude,
            tags
        """

    def search_columns(self):
        return """
            osm_type || osm_id::VARCHAR AS rkey,
            name,
            latitude::DECIMAL(10,6)::VARCHAR AS latitude,
            longitude::DECIMAL(10,6)::VARCHAR AS longitude
        """

    def query_record(self):
        # rkey is e.g. 'n240109189'
        # Parse osm_type from first char, osm_id from remainder
        columns = self.record_columns()
        return f"""
            SELECT {columns}
            FROM places
            WHERE osm_type = left($rkey, 1)
              AND osm_id = substr($rkey, 2)::BIGINT
        """

    # _query_trigram_text, _query_trigram_spatial, query_nearest:
    # Same Jaccard similarity pattern as FSQ/Overture.
    # Join key between places and name_index is rkey.

    def process_record(self, result):
        tags = result.pop("tags", []) or []
        tag_dict = {}
        for tag in tags:
            k, _, v = tag.partition("=")
            tag_dict[k] = v

        locations = [
            {
                "$type": "community.lexicon.location.geo",
                "latitude": result.pop("latitude"),
                "longitude": result.pop("longitude"),
            }
        ]

        # Build address from addr:* tags
        address_data = {}
        addr_map = [
            ("addr:country", "country"),
            ("addr:postcode", "postalCode"),
            ("addr:city", "locality"),
            ("addr:street", "street"),
        ]
        for tag_key, dest_key in addr_map:
            if tag_dict.get(tag_key):
                address_data[dest_key] = tag_dict.pop(tag_key)
        # Prepend housenumber to street if present
        if tag_dict.get("addr:housenumber") and address_data.get("street"):
            address_data["street"] = (
                tag_dict.pop("addr:housenumber") + " " + address_data["street"]
            )
        if address_data.get("country"):
            locations.append({
                "$type": "community.lexicon.location.address",
                **address_data
            })

        return {
            "$type": "community.lexicon.location.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations": locations,
            "names": [
                {"text": result.pop("name"), "priority": 0}
            ],
            "attributes": tag_dict,
        }
```

## Import Script Interface

```
scripts/import-osm.sh <pbf_path> [xmin ymin xmax ymax]
```

- `pbf_path`: Local path to a PBF file (planet or regional extract)
- Bbox arguments are optional; omit for global import
- Requires: Python 3.10+ with QuackOSM installed, DuckDB CLI
- Outputs: `db/osm.duckdb`

The script:
1. Runs QuackOSM (stage 1) to produce intermediate GeoParquet
2. Runs DuckDB SQL (stage 2) for precise filtering, centroid computation,
   places table creation, spatial index
3. Auto-builds density and IDF if not present
4. Computes importance scores
5. Builds name_index with trigrams
6. Analyzes and finalizes

## Wiring

- Add `"osm": OpenStreetMap` to `DATABASE_TYPES` in `config.py`
- Export `OpenStreetMap` from `__init__.py`
- Register in `__main__.py`
- Add `osm` mode to `build-density.sh` and `build-idf.sh`

## Layercake Settlements (Optional Supplement)

The Layercake project (OpenStreetMap US) publishes a weekly global
GeoParquet of `place=*` features at:

    https://data.openstreetmap.us/layercake/settlements.parquet

This is directly readable by DuckDB. It could serve as an alternative to
extracting `place=*` features from PBF, particularly for keeping populated
places fresher than the monthly/quarterly PBF re-import cycle. However,
for simplicity, the initial implementation should extract everything from
a single PBF source.

## Licensing

OSM data is licensed under ODbL 1.0. Serving OSM-derived data through
Garganorn's API creates a Derivative Database. Requirements:

- Attribution: "Map data from OpenStreetMap contributors"
- Share-alike: The derivative database (osm.duckdb) must be made available
  under ODbL upon request
- The API responses themselves are "Produced Works" and can be served under
  any terms

Since Garganorn is open source and the import pipeline is reproducible,
the share-alike requirement is straightforward to satisfy.

## Dependencies

### Build-time (import pipeline only)
- `quackosm >= 0.17.0` (Python 3.10+)
- DuckDB CLI with spatial and geography extensions

### Runtime (server)
- No new dependencies. The `OpenStreetMap` class uses the same
  DuckDB + spatial extension stack as FSQ and Overture.

## Testing

- New pytest fixtures creating a test DuckDB with OSM-shaped data
  (mix of nodes and way-centroids, with tags arrays)
- Test tag filtering edge cases: multi-tagged features, excluded amenity
  values, missing names, place=* without name
- Test `process_record` tag-to-lexicon mapping (address assembly,
  attribute extraction)
- Test search queries: spatial-only, text-only, combined
- Test rkey parsing (osm_type + osm_id round-trip)

## Resolved Decisions

1. **osm_type storage**: Single-char VARCHAR (`'n'`, `'w'`, `'r'`).
   Matches rkey format, more compact.

2. **Relation handling**: Use `ST_PointOnSurface()` instead of
   `ST_Centroid()` to guarantee the point lies within the feature
   boundary.

## Open Questions

1. ~~**QuackOSM MAP access syntax**~~: **Resolved.** QuackOSM outputs
   `tags` as `MAP(VARCHAR, VARCHAR)` (scalar values, not lists). The
   `[1]` subscript extracted the first *character* of the string, not a
   list element. All `[1]` subscripts removed. Use `tags['key']` directly.
   Also: DuckDB 1.4.x does not support `map_entries(tags).transform(...)`;
   use `list_transform(map_entries(tags), ...)` instead.

2. **Deduplication**: Features with multiple qualifying tags (e.g.,
   `amenity=hospital` + `healthcare=hospital`) should produce one row,
   not two. The CASE-based primary category assignment handles this
   naturally since each feature is evaluated once.

3. **Name variants**: OSM has `name:en`, `name:fr`, etc. Deferred for
   initial implementation. The lexicon schema supports it for later.
