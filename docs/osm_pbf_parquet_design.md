# Replacing QuackOSM with osm-pbf-parquet

## Motivation

The current OSM import pipeline (`scripts/import-osm.sh`) uses QuackOSM to
convert PBF to GeoParquet in Stage 1. QuackOSM resolves full way and
relation geometries (polygons, multipolygons), which takes hours for the
planet file and requires ~10x the PBF size in temp disk (~850 GB for the
85 GB planet).

Garganorn only needs point coordinates for its gazetteer. Full polygon
geometry is wasted work — we immediately reduce it to a point via
`ST_PointOnSurface(geometry)` in Stage 2.

[osm-pbf-parquet](https://github.com/OvertureMaps/osm-pbf-parquet) is a
Rust tool that converts PBF to Hive-partitioned Parquet in ~30 minutes for
the full planet. It does not resolve geometries — it outputs raw nodes
(with lat/lon), ways (with node reference arrays), and relations (with
member arrays). We compute centroids ourselves in SQL.

**Trade-off**: We lose `ST_PointOnSurface()` (which guarantees the point
lies within the feature boundary) and instead use `avg(lat), avg(lon)` of
constituent nodes for way centroids. For concave polygons (e.g.,
crescent-shaped parks), this centroid can fall outside the feature. This is
acceptable for gazetteer purposes — the error is small and the use case is
search, not spatial analysis.

## osm-pbf-parquet Output Schema

The tool outputs Hive-partitioned Parquet under a single directory:

```
parquet/
  type=node/
    node_0000.zstd.parquet
    node_0001.zstd.parquet
    ...
  type=way/
    way_0000.zstd.parquet
    way_0001.zstd.parquet
    ...
  type=relation/
    relation_0000.zstd.parquet
    ...
```

### Columns

All partitions share a common set of columns:

| Column      | Type                                              | Notes                   |
|-------------|---------------------------------------------------|-------------------------|
| `id`        | BIGINT                                            | OSM element ID          |
| `tags`      | MAP(VARCHAR, VARCHAR)                             | Key-value tag pairs     |
| `lat`       | DOUBLE                                            | Nodes only (NULL for ways/relations) |
| `lon`       | DOUBLE                                            | Nodes only (NULL for ways/relations) |
| `nds`       | ARRAY(STRUCT(ref BIGINT))                         | Ways only — ordered node references  |
| `members`   | ARRAY(STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)) | Relations only         |
| `changeset` | BIGINT                                            | Not used by Garganorn   |
| `timestamp` | BIGINT                                            | Not used by Garganorn   |
| `uid`       | BIGINT                                            | Not used by Garganorn   |
| `user`      | VARCHAR                                           | Not used by Garganorn   |
| `version`   | INT                                               | Not used by Garganorn   |
| `visible`   | BOOLEAN                                           | Not used by Garganorn   |

Key differences from QuackOSM output:
- No `feature_id` string (e.g., `"node/12345"`). Instead, `id` BIGINT + Hive partition `type`.
- No `geometry` column. Nodes have `lat`/`lon`; ways have `nds` (node refs).
- `tags` MAP is compatible — same `MAP(VARCHAR, VARCHAR)` type.

## Installation

osm-pbf-parquet is distributed as a pre-compiled Linux x86_64 binary.
Download from GitHub releases:

```bash
curl -L https://github.com/OvertureMaps/osm-pbf-parquet/releases/latest/download/osm-pbf-parquet-x86_64-linux.tar.gz \
  | tar xz
chmod +x osm-pbf-parquet
```

The binary has no runtime dependencies. It can be placed anywhere on PATH
or referenced directly. The import script should check for it the same way
it checks for `duckdb`.

## New Stage 1: PBF to Parquet

### Invocation

```bash
osm-pbf-parquet --input "$pbf_path" --output "$cache_dir/parquet"
```

### Cache directory structure change

**Before** (QuackOSM):
```
db/cache/osm/
  osm_places.geoparquet       # single file, ~50 GB
```

**After** (osm-pbf-parquet):
```
db/cache/osm/parquet/
  type=node/
    node_0000.zstd.parquet
    ...
  type=way/
    way_0000.zstd.parquet
    ...
  type=relation/
    relation_0000.zstd.parquet
    ...
```

### Caching

The existing cache-or-skip pattern remains. Check for the output directory
instead of a single file:

```bash
parquet_dir="${cache_dir}/parquet"
if [ -d "$parquet_dir" ] && [ -n "$(ls -A "$parquet_dir"/type=node/ 2>/dev/null)" ]; then
    echo "Using cached Parquet: $parquet_dir"
else
    echo "Converting PBF to Parquet with osm-pbf-parquet..."
    osm-pbf-parquet --input "$pbf_path" --output "$parquet_dir"
    if [ $? -ne 0 ]; then
        echo "osm-pbf-parquet conversion failed."
        exit 1
    fi
fi
```

## New Stage 2: DuckDB SQL

Stage 2 splits into two queries — one for nodes, one for ways — that both
INSERT INTO the same `places` table. The tag filtering CASE/WHEN logic is
identical in both; the difference is how coordinates are obtained.

### Table creation

```sql
CREATE TABLE places (
    osm_type         VARCHAR,
    osm_id           BIGINT,
    name             VARCHAR,
    latitude         DOUBLE,
    longitude        DOUBLE,
    geom             GEOMETRY,
    primary_category VARCHAR,
    tags             MAP(VARCHAR, VARCHAR),
    bbox             STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
    importance       INTEGER DEFAULT 0
);
```

No schema change from the current design.

### Node import

Reads directly from `type=node/` partition. Uses `lat`/`lon` columns
instead of `ST_PointOnSurface(geometry)`.

```sql
INSERT INTO places
WITH filtered AS (
    SELECT
        'n' AS osm_type,
        id AS osm_id,
        tags['name'] AS name,
        lat AS latitude,
        lon AS longitude,
        tags,
        CASE
            WHEN tags['amenity'] IS NOT NULL THEN 'amenity=' || tags['amenity']
            WHEN tags['shop'] IS NOT NULL THEN 'shop=' || tags['shop']
            WHEN tags['tourism'] IS NOT NULL THEN 'tourism=' || tags['tourism']
            WHEN tags['leisure'] IS NOT NULL THEN 'leisure=' || tags['leisure']
            WHEN tags['office'] IS NOT NULL THEN 'office=' || tags['office']
            WHEN tags['craft'] IS NOT NULL THEN 'craft=' || tags['craft']
            WHEN tags['healthcare'] IS NOT NULL THEN 'healthcare=' || tags['healthcare']
            WHEN tags['historic'] IS NOT NULL THEN 'historic=' || tags['historic']
            WHEN tags['natural'] IS NOT NULL THEN 'natural=' || tags['natural']
            WHEN tags['man_made'] IS NOT NULL THEN 'man_made=' || tags['man_made']
            WHEN tags['aeroway'] IS NOT NULL THEN 'aeroway=' || tags['aeroway']
            WHEN tags['railway'] IS NOT NULL THEN 'railway=' || tags['railway']
            WHEN tags['public_transport'] IS NOT NULL
                THEN 'public_transport=' || tags['public_transport']
            WHEN tags['place'] IS NOT NULL THEN 'place=' || tags['place']
        END AS primary_category
    FROM read_parquet('${parquet_dir}/type=node/*.parquet')
    WHERE lat IS NOT NULL AND lon IS NOT NULL
      AND (
        -- [same tag filter clauses as current import-osm.sh]
        (tags['amenity'] IS NOT NULL
         AND tags['amenity'] NOT IN (
             'parking', 'parking_space', 'bench', 'waste_basket',
             'bicycle_parking', 'shelter', 'recycling', 'toilets',
             'post_box', 'drinking_water', 'vending_machine',
             'waste_disposal', 'hunting_stand', 'parking_entrance',
             'grit_bin', 'give_box', 'bbq')
         AND tags['name'] IS NOT NULL)
        OR (tags['shop'] IS NOT NULL
            AND tags['shop'] NOT IN ('yes', 'vacant')
            AND tags['name'] IS NOT NULL)
        OR (tags['tourism'] IS NOT NULL AND tags['name'] IS NOT NULL)
        OR (tags['leisure'] IN (
                'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
                'golf_course', 'stadium', 'sports_hall', 'marina',
                'nature_reserve', 'garden', 'playground', 'dog_park',
                'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
                'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
                'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
                'escape_game', 'hackerspace')
            AND tags['name'] IS NOT NULL)
        OR (tags['office'] IS NOT NULL AND tags['office'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['craft'] IS NOT NULL AND tags['craft'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['healthcare'] IS NOT NULL AND tags['healthcare'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['historic'] IN (
                'castle', 'monument', 'memorial', 'archaeological_site',
                'ruins', 'fort', 'manor', 'church', 'city_gate',
                'building', 'mine', 'wreck')
            AND tags['name'] IS NOT NULL)
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock')
            AND tags['name'] IS NOT NULL)
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon')
            AND tags['name'] IS NOT NULL)
        OR tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
        OR tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL)
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square')
            AND tags['name'] IS NOT NULL)
      )
)
SELECT
    osm_type,
    osm_id,
    name,
    latitude,
    longitude,
    ST_Point(longitude, latitude) AS geom,
    primary_category,
    map_from_entries(
        list_filter(
            map_entries(tags),
            e -> e.key != split_part(primary_category, '=', 1)
               AND e.key IN (
                   'cuisine', 'sport', 'religion', 'denomination',
                   'opening_hours', 'phone', 'website', 'wikidata',
                   'wheelchair', 'internet_access',
                   'addr:street', 'addr:housenumber', 'addr:city',
                   'addr:postcode', 'addr:country')
        )
    ) AS tags,
    {'xmin': longitude - 0.0001,
     'ymin': latitude - 0.0001,
     'xmax': longitude + 0.0001,
     'ymax': latitude + 0.0001} AS bbox,
    0 AS importance
FROM filtered
WHERE primary_category IS NOT NULL
  AND name IS NOT NULL;
```

Key changes from current Stage 2:
- `read_parquet('...type=node/*.parquet')` instead of the single GeoParquet.
- `osm_type` is the literal `'n'` (no string parsing of `feature_id`).
- `lat`/`lon` used directly. `ST_Point(longitude, latitude)` for the `geom`
  column (needed for the R-tree index). No spatial extension needed for
  coordinate extraction.

### Way centroid resolution

This is the main design challenge. The `type=way/` partition has no
coordinates — only `nds`, an array of structs containing node reference IDs.
We must join against the `type=node/` partition to resolve coordinates.

**Approach: filter-then-join**

The full planet has ~900M ways, of which only ~2-5M match our tag filters.
The node partition has ~9B rows. A naive join is expensive. Instead:

1. Filter ways by tags first (produces the small set of qualifying ways).
2. UNNEST the `nds` arrays to collect all referenced node IDs.
3. SEMI JOIN against the node partition to fetch only the needed node
   coordinates.
4. Compute `avg(lat), avg(lon)` per way.

```sql
INSERT INTO places
WITH qualifying_ways AS (
    -- Step 1: Filter ways by tags (same filter as nodes, ~2-5M rows)
    SELECT
        id AS osm_id,
        tags['name'] AS name,
        nds,
        tags,
        CASE
            WHEN tags['amenity'] IS NOT NULL THEN 'amenity=' || tags['amenity']
            WHEN tags['shop'] IS NOT NULL THEN 'shop=' || tags['shop']
            WHEN tags['tourism'] IS NOT NULL THEN 'tourism=' || tags['tourism']
            WHEN tags['leisure'] IS NOT NULL THEN 'leisure=' || tags['leisure']
            WHEN tags['office'] IS NOT NULL THEN 'office=' || tags['office']
            WHEN tags['craft'] IS NOT NULL THEN 'craft=' || tags['craft']
            WHEN tags['healthcare'] IS NOT NULL THEN 'healthcare=' || tags['healthcare']
            WHEN tags['historic'] IS NOT NULL THEN 'historic=' || tags['historic']
            WHEN tags['natural'] IS NOT NULL THEN 'natural=' || tags['natural']
            WHEN tags['man_made'] IS NOT NULL THEN 'man_made=' || tags['man_made']
            WHEN tags['aeroway'] IS NOT NULL THEN 'aeroway=' || tags['aeroway']
            WHEN tags['railway'] IS NOT NULL THEN 'railway=' || tags['railway']
            WHEN tags['public_transport'] IS NOT NULL
                THEN 'public_transport=' || tags['public_transport']
            WHEN tags['place'] IS NOT NULL THEN 'place=' || tags['place']
        END AS primary_category
    FROM read_parquet('${parquet_dir}/type=way/*.parquet')
    WHERE (
        -- [same tag filter clauses]
        (tags['amenity'] IS NOT NULL
         AND tags['amenity'] NOT IN (
             'parking', 'parking_space', 'bench', 'waste_basket',
             'bicycle_parking', 'shelter', 'recycling', 'toilets',
             'post_box', 'drinking_water', 'vending_machine',
             'waste_disposal', 'hunting_stand', 'parking_entrance',
             'grit_bin', 'give_box', 'bbq')
         AND tags['name'] IS NOT NULL)
        OR (tags['shop'] IS NOT NULL
            AND tags['shop'] NOT IN ('yes', 'vacant')
            AND tags['name'] IS NOT NULL)
        OR (tags['tourism'] IS NOT NULL AND tags['name'] IS NOT NULL)
        OR (tags['leisure'] IN (
                'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
                'golf_course', 'stadium', 'sports_hall', 'marina',
                'nature_reserve', 'garden', 'playground', 'dog_park',
                'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
                'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
                'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
                'escape_game', 'hackerspace')
            AND tags['name'] IS NOT NULL)
        OR (tags['office'] IS NOT NULL AND tags['office'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['craft'] IS NOT NULL AND tags['craft'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['healthcare'] IS NOT NULL AND tags['healthcare'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['historic'] IN (
                'castle', 'monument', 'memorial', 'archaeological_site',
                'ruins', 'fort', 'manor', 'church', 'city_gate',
                'building', 'mine', 'wreck')
            AND tags['name'] IS NOT NULL)
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock')
            AND tags['name'] IS NOT NULL)
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon')
            AND tags['name'] IS NOT NULL)
        OR tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
        OR tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL)
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square')
            AND tags['name'] IS NOT NULL)
    )
),
-- Step 2: Unnest node references from qualifying ways
way_node_refs AS (
    SELECT
        osm_id,
        UNNEST(nds).ref AS node_ref
    FROM qualifying_ways
),
-- Step 3: Collect the distinct set of needed node IDs
needed_node_ids AS (
    SELECT DISTINCT node_ref AS id
    FROM way_node_refs
),
-- Step 4: Fetch coordinates for only the needed nodes
node_coords AS (
    SELECT n.id, n.lat, n.lon
    FROM read_parquet('${parquet_dir}/type=node/*.parquet') n
    SEMI JOIN needed_node_ids nn ON n.id = nn.id
    WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
),
-- Step 5: Compute average centroid per way
way_centroids AS (
    SELECT
        wnr.osm_id,
        avg(nc.lat) AS latitude,
        avg(nc.lon) AS longitude
    FROM way_node_refs wnr
    JOIN node_coords nc ON wnr.node_ref = nc.id
    GROUP BY wnr.osm_id
)
SELECT
    'w' AS osm_type,
    qw.osm_id,
    qw.name,
    wc.latitude,
    wc.longitude,
    ST_Point(wc.longitude, wc.latitude) AS geom,
    qw.primary_category,
    map_from_entries(
        list_filter(
            map_entries(qw.tags),
            e -> e.key != split_part(qw.primary_category, '=', 1)
               AND e.key IN (
                   'cuisine', 'sport', 'religion', 'denomination',
                   'opening_hours', 'phone', 'website', 'wikidata',
                   'wheelchair', 'internet_access',
                   'addr:street', 'addr:housenumber', 'addr:city',
                   'addr:postcode', 'addr:country')
        )
    ) AS tags,
    {'xmin': wc.longitude - 0.0001,
     'ymin': wc.latitude - 0.0001,
     'xmax': wc.longitude + 0.0001,
     'ymax': wc.latitude + 0.0001} AS bbox,
    0 AS importance
FROM qualifying_ways qw
JOIN way_centroids wc ON qw.osm_id = wc.osm_id
WHERE qw.primary_category IS NOT NULL
  AND qw.name IS NOT NULL;
```

**Performance considerations**:

- `qualifying_ways` produces ~2-5M rows from ~900M ways (heavy filter,
  fast scan).
- After UNNEST, `way_node_refs` has ~50-100M rows (average ~20 nodes per
  way).
- `needed_node_ids` (DISTINCT) reduces this to ~30-80M unique node IDs.
- The SEMI JOIN against `type=node/` (~9B rows) is the expensive step, but
  DuckDB's hash join on BIGINT is efficient. This fetches ~30-80M node
  coordinates out of ~9B — roughly 1% of nodes.
- The final GROUP BY aggregation is straightforward.

The SEMI JOIN requires DuckDB to scan the entire ~9B-row node partition
(~28-50 GB compressed Parquet). Expect this step to take **30-90 minutes**
depending on I/O throughput. This is acceptable if the overall pipeline
time stays well under QuackOSM's multi-hour baseline.

For memory-constrained environments, the qualifying_ways CTE could be
materialized to a temp table first.

**Edge case: unresolvable nodes.** Ways whose node references are entirely
absent from the node partition (e.g., referencing deleted nodes) produce
no `way_centroids` row and are dropped by the INNER JOIN. This is
intentional — such ways have no computable location.

### Bbox filtering

If bbox arguments are provided, add a coordinate filter to the final
SELECT in both node and way queries:

```sql
-- Append to node query WHERE clause:
AND longitude BETWEEN ${xmin} AND ${xmax}
AND latitude BETWEEN ${ymin} AND ${ymax}

-- Append to way centroid query WHERE clause:
AND wc.longitude BETWEEN ${xmin} AND ${xmax}
AND wc.latitude BETWEEN ${ymin} AND ${ymax}
```

## Schema Mapping

| QuackOSM (current)                        | osm-pbf-parquet (new)                | Garganorn places column |
|-------------------------------------------|--------------------------------------|------------------------|
| `left(split_part(feature_id, '/', 1), 1)` | `'n'` literal / `'w'` literal       | `osm_type`             |
| `split_part(feature_id, '/', 2)::BIGINT`  | `id`                                 | `osm_id`               |
| `tags['name']`                            | `tags['name']`                       | `name`                 |
| `ST_Y(ST_PointOnSurface(geometry))`       | `lat` (nodes) / `avg(lat)` (ways)   | `latitude`             |
| `ST_X(ST_PointOnSurface(geometry))`       | `lon` (nodes) / `avg(lon)` (ways)   | `longitude`            |
| `ST_PointOnSurface(geometry)`             | `ST_Point(lon, lat)` / `ST_Point(avg_lon, avg_lat)` | `geom`  |
| `tags` MAP                                | `tags` MAP                           | `tags` (filtered)      |
| CASE/WHEN on tags                         | CASE/WHEN on tags (identical)        | `primary_category`     |

## Pipeline Ordering Change

### Previous order (osm-data-source branch, superseded)

```
Stage 1: QuackOSM → GeoParquet
  ↓
build-density.sh osm <geoparquet>    ← reads raw GeoParquet
build-idf.sh osm <geoparquet>       ← reads raw GeoParquet
  ↓
Stage 2: DuckDB SQL → places table → importance scoring → name_index
```

Density and IDF ran before Stage 2 because they read from the Stage 1
GeoParquet. This approach is superseded — see "New order" below.

### New order

```
Stage 1: osm-pbf-parquet → Hive Parquet
  ↓
Stage 2: DuckDB SQL → places table (nodes + way centroids)
  ↓
build-density.sh osm <osm.duckdb>   ← reads from places table
  ↓
Importance scoring (IDF computed inline from places) → name_index
```

Density moves AFTER the places table is built, because:

1. The Hive Parquet from osm-pbf-parquet has no resolved coordinates for
   ways. The places table does.
2. Reading from the already-built places table is simpler — coordinates
   and primary_category are already computed.

Category IDF is computed inline during importance scoring directly from the
places table, so no separate build step is needed.

### build-density.sh changes (OSM mode)

Replace the GeoParquet reader with a DuckDB query against the places table:

```sql
-- Current (reads GeoParquet with ST_PointOnSurface):
INSERT INTO cell_counts
SELECT 14 AS level,
    s2_cell_parent(
        s2_cellfromlonlat(
            ST_X(ST_PointOnSurface(geometry)),
            ST_Y(ST_PointOnSurface(geometry))
        ), 14) AS cell_id,
    count(*) AS pt_count
FROM read_parquet('osm_places.geoparquet')
WHERE geometry IS NOT NULL
GROUP BY cell_id;

-- New (reads places table from osm.duckdb via ATTACH):
ATTACH '${osm_db_path}' AS osm_import (READ_ONLY);
INSERT INTO cell_counts
SELECT 14 AS level,
    s2_cell_parent(
        s2_cellfromlonlat(longitude, latitude), 14
    ) AS cell_id,
    count(*) AS pt_count
FROM osm_import.places
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
GROUP BY cell_id;
```

The `source_arg` for OSM mode changes from a GeoParquet file path to the
osm.duckdb database path. The script uses DuckDB's `ATTACH` to read from
it read-only. Only the base-level INSERT (level 14) changes; the cascade
loop that builds levels 13 down to 6 is unchanged.

**Execution model**: the density build runs as a separate `duckdb` CLI
invocation from bash, after the main import DuckDB session has closed. This
avoids write-lock conflicts on the tmp database file. Category IDF is
computed inline within the main importance scoring session.

### Category IDF (OSM mode)

Category IDF is now computed inline during importance scoring in
`import-osm.sh`, directly from the places table. No separate `build-idf.sh`
invocation is needed. The inline query reads `primary_category` from the
places table — the same column already computed during Stage 2 — eliminating
the duplicated CASE/WHEN category derivation logic that `build-idf.sh` previously required.

## Spatial Extension

The current pipeline requires `INSTALL spatial; LOAD spatial;` for
`ST_PointOnSurface()`. The new pipeline still needs spatial for:
- `ST_Point(lon, lat)` to create the `geom` column (needed for R-tree
  index).

So the spatial extension remains a dependency, but the geometry processing
is much lighter — just point construction, no polygon centroid computation.

## Dependencies

### Removed
- QuackOSM (Python package with geopandas, numpy, shapely, pyarrow)

### Added
- osm-pbf-parquet (single static Rust binary, no runtime dependencies)

### Unchanged
- DuckDB CLI with spatial and geography extensions

## What's Deferred

### Relations

Relations account for ~1% of qualifying features in OSM. They are
structurally more complex — a relation's members can be nodes, ways, or
other relations, and resolving their geometry requires recursive lookups.

For a future iteration, the approach would be:
1. Filter relations by tags (same filter).
2. For each qualifying relation, resolve member coordinates:
   - Node members: direct lat/lon lookup.
   - Way members: resolve via the way→node chain (same as way centroids).
   - Relation members: recursive (rare, defer further).
3. Compute avg(lat), avg(lon) across all resolved member coordinates.

This can be added as a third INSERT INTO places query without changing the
rest of the pipeline.

### Centroid accuracy improvements

If centroid accuracy becomes a concern for specific feature types (large
concave polygons), a future iteration could:
- Use a weighted centroid (area-weighted for closed ways).
- Post-process specific categories with a point-in-polygon check.

Neither is expected to be necessary for gazetteer search.

## import-osm.sh Structure (Revised)

```bash
#!/bin/bash
# ... (duckdb and osm-pbf-parquet checks, argument parsing, bbox validation)

# ─── Stage 1: Convert PBF to Parquet ─────────────────────────────────────────
parquet_dir="${cache_dir}/parquet"
if [ -d "$parquet_dir" ] && [ -n "$(ls -A "$parquet_dir"/type=node/ 2>/dev/null)" ]; then
    echo "Using cached Parquet: $parquet_dir"
else
    osm-pbf-parquet --input "$pbf_path" --output "$parquet_dir"
fi

# ─── Stage 2: Build places table ─────────────────────────────────────────────
# Node import query (INSERT INTO places ... FROM type=node/)
# Way centroid query (INSERT INTO places ... FROM type=way/ JOIN type=node/)
# DELETE FROM places WHERE geom IS NULL
# CREATE INDEX places_rtree ON places USING RTREE (geom)

# ─── Build density from places table ─────────────────────────────────────────
# build-density.sh osm "$output_dir/osm.duckdb.tmp"

# ─── Importance scoring ──────────────────────────────────────────────────────
# Same as current EXCEPT: drop the "ALTER TABLE places ADD COLUMN importance"
# statement — the column is already declared in CREATE TABLE.
# The UPDATE ... SET importance = ... logic is unchanged.

# ─── Name index ──────────────────────────────────────────────────────────────
# (same as current — trigram generation from places.name)

# ─── Finalize ─────────────────────────────────────────────────────────────────
# mv osm.duckdb.tmp osm.duckdb
```

Note: the density build script reads from `osm.duckdb.tmp` (the in-progress
database) rather than a separate parquet source. The tmp file is the DuckDB
database being constructed — it has the places table populated but not yet
finalized. The build script runs as a separate DuckDB invocation that ATTACHes
the tmp database read-only. Category IDF is computed inline within the main
importance scoring DuckDB session, so no ATTACH is needed for it.

## Test Fixture Changes

The test fixtures in `tests/conftest.py` create OSM test data directly in
DuckDB and do not depend on the import pipeline. No changes needed to test
fixtures — the places table schema is unchanged.

Integration testing of the import pipeline itself (if added) would need
a small PBF fixture and the osm-pbf-parquet binary, or mock the Stage 1
output by creating Hive-partitioned parquet files directly.
