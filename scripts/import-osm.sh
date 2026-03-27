#!/bin/bash

if ! command -v duckdb &> /dev/null; then
    echo "duckdb not installed. Please install it first."
    echo "To install duckdb, you can use:"
    echo "  curl https://install.duckdb.org/ | sh"
    echo "or follow the instructions at https://duckdb.org/docs/installation/."
    echo
    echo "Be sure to add it to your path afterwards."
    exit 1
fi

if ! command -v osm-pbf-parquet &> /dev/null; then
    echo "osm-pbf-parquet not installed. Please install it first."
    echo "To install osm-pbf-parquet, download the binary from:"
    echo "  https://github.com/OvertureMaps/osm-pbf-parquet/releases"
    echo "and place it on your PATH."
    exit 1
fi

usage() {
    echo
    echo "Usage: $0 <pbf_path> [options]"
    echo
    echo "  pbf_path: path to the OSM PBF file"
    echo
    echo "  Options:"
    echo "    --output-dir <dir>    output directory (default: scripts/../db)"
    echo "    --cache-dir <dir>     cache directory (default: scripts/../db/cache/osm)"
    echo "    --xmin <lon>          bounding box west edge"
    echo "    --xmax <lon>          bounding box east edge"
    echo "    --ymin <lat>          bounding box south edge"
    echo "    --ymax <lat>          bounding box north edge"
    echo
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

pbf_path="$1"
shift

if [ ! -f "$pbf_path" ]; then
    echo "PBF file not found: $pbf_path"
    exit 1
fi

script_dir="$(dirname "$(realpath "$0")")"
output_dir="$(realpath "${script_dir}/../db")"
cache_dir="$(realpath -m "${script_dir}/../db/cache/osm")"
xmin=""
xmax=""
ymin=""
ymax=""

while [ $# -gt 0 ]; do
    case "$1" in
        --output-dir) output_dir="$2"; shift 2 ;;
        --cache-dir)  cache_dir="$2";  shift 2 ;;
        --xmin)       xmin="$2";       shift 2 ;;
        --xmax)       xmax="$2";       shift 2 ;;
        --ymin)       ymin="$2";       shift 2 ;;
        --ymax)       ymax="$2";       shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# Validate bbox: all four must be provided together or not at all
bbox_count=0
[ -n "$xmin" ] && bbox_count=$((bbox_count + 1))
[ -n "$xmax" ] && bbox_count=$((bbox_count + 1))
[ -n "$ymin" ] && bbox_count=$((bbox_count + 1))
[ -n "$ymax" ] && bbox_count=$((bbox_count + 1))

if [ "$bbox_count" -gt 0 ] && [ "$bbox_count" -lt 4 ]; then
    echo "Bbox error: --xmin, --xmax, --ymin, --ymax must all be provided together."
    exit 1
fi

mkdir -p "$output_dir"
mkdir -p "$cache_dir"

output_db="${output_dir}/osm.duckdb"
output_db_tmp="${output_dir}/osm.duckdb.tmp"

# ─── Stage 1: Convert PBF to Parquet ──────────────────────────────────────────

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

node_parquet="${parquet_dir}/type=node/*.parquet"
way_parquet="${parquet_dir}/type=way/*.parquet"

# ─── Stage 2: Build places table ──────────────────────────────────────────────

echo
echo "Building places table..."

rm -f "$output_db_tmp"

# Construct optional bbox WHERE clauses
node_bbox_filter=""
way_bbox_filter=""
if [ "$bbox_count" -eq 4 ]; then
    node_bbox_filter="AND longitude BETWEEN ${xmin} AND ${xmax}
AND latitude BETWEEN ${ymin} AND ${ymax}"
    way_bbox_filter="AND wc.longitude BETWEEN ${xmin} AND ${xmax}
AND wc.latitude BETWEEN ${ymin} AND ${ymax}"
fi

duckdb -bail "$output_db_tmp" <<EOF
INSTALL spatial;
LOAD spatial;

CREATE TABLE places (
    osm_type         VARCHAR,
    osm_id           BIGINT,
    rkey             VARCHAR,
    name             VARCHAR,
    latitude         DOUBLE,
    longitude        DOUBLE,
    geom             GEOMETRY,
    primary_category VARCHAR,
    tags             MAP(VARCHAR, VARCHAR),
    bbox             STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
    importance       INTEGER DEFAULT 0
);

.print "Importing nodes..."
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
    FROM read_parquet('${node_parquet}')
    WHERE lat IS NOT NULL AND lon IS NOT NULL
      AND (
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
    osm_type || osm_id::VARCHAR AS rkey,
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
  AND name IS NOT NULL
  ${node_bbox_filter};

.print "Importing way centroids..."
INSERT INTO places
WITH qualifying_ways AS (
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
    FROM read_parquet('${way_parquet}')
    WHERE (
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
way_node_refs AS (
    SELECT
        osm_id,
        UNNEST(nds).ref AS node_ref
    FROM qualifying_ways
),
needed_node_ids AS (
    SELECT DISTINCT node_ref AS id
    FROM way_node_refs
),
node_coords AS (
    SELECT n.id, n.lat, n.lon
    FROM read_parquet('${node_parquet}') n
    SEMI JOIN needed_node_ids nn ON n.id = nn.id
    WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
),
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
    'w' || qw.osm_id::VARCHAR AS rkey,
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
  AND qw.name IS NOT NULL
  ${way_bbox_filter};

.print "Cleaning up null geom rows..."
DELETE FROM places WHERE geom IS NULL;

.print "Building R-tree index..."
CREATE INDEX places_rtree ON places USING RTREE (geom);
CREATE INDEX idx_rkey ON places(rkey);
EOF

if [ $? -ne 0 ]; then
    echo "Stage 2 DuckDB import failed."
    rm -f "$output_db_tmp"
    exit 1
fi

# ─── Build density and IDF from places table ──────────────────────────────────

"${script_dir}/build-density.sh" osm "$output_db_tmp"
if [ $? -ne 0 ]; then
    echo "build-density.sh failed."
    rm -f "$output_db_tmp"
    exit 1
fi

"${script_dir}/build-idf.sh" osm "$output_db_tmp"
if [ $? -ne 0 ]; then
    echo "build-idf.sh failed."
    rm -f "$output_db_tmp"
    exit 1
fi

density_parquet="${output_dir}/density-osm.parquet"
idf_parquet="${output_dir}/category_idf-osm.parquet"

# ─── Importance scoring ───────────────────────────────────────────────────────

echo
echo "Computing importance scores..."

duckdb -bail "$output_db_tmp" <<ENDSQL
INSTALL geography FROM community;
LOAD geography;

CREATE TEMP TABLE t_density AS
SELECT * FROM read_parquet('${density_parquet}') WHERE level = 12;

CREATE TEMP TABLE t_idf AS
SELECT * FROM read_parquet('${idf_parquet}');

CREATE TEMP TABLE place_density AS
SELECT
    p.rkey,
    coalesce(ln(1 + c.pt_count), 0) AS density_score
FROM places p
LEFT JOIN t_density c
    ON c.cell_id = s2_cell_parent(s2_cellfromlonlat(p.longitude, p.latitude), 12)::UBIGINT;

CREATE TEMP TABLE place_idf AS
SELECT
    p.rkey,
    coalesce(max(idf.idf_score), 0) AS idf_score
FROM places p
LEFT JOIN t_idf idf ON idf.category = p.primary_category
WHERE p.primary_category IS NOT NULL
GROUP BY p.rkey;

UPDATE places SET importance = round(
    60 * least(d.density_score / 10.0, 1.0)
  + 40 * least(coalesce(i.idf_score, 0) / 18.0, 1.0)
)::INTEGER
FROM place_density d
LEFT JOIN place_idf i USING (rkey)
WHERE places.rkey = d.rkey;

DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_density;
DROP TABLE t_idf;
ENDSQL

# ─── Name index ───────────────────────────────────────────────────────────────

echo
echo "Building name index..."

duckdb -bail "$output_db_tmp" <<'ENDSQL'
CREATE TABLE IF NOT EXISTS name_index (
    trigram          VARCHAR,
    rkey             VARCHAR,
    name             VARCHAR,
    norm_name        VARCHAR,
    importance       INTEGER
);

INSERT INTO name_index
WITH name_prep AS (
    SELECT rkey,
           name,
           lower(strip_accents(name)) AS norm_name,
           importance
    FROM places
    WHERE name IS NOT NULL AND length(name) >= 3
)
SELECT substr(np.norm_name, pos, 3) AS trigram,
       np.rkey,
       np.name,
       np.norm_name,
       np.importance
FROM name_prep np
CROSS JOIN generate_series(1, length(np.norm_name) - 2) AS gs(pos);
ENDSQL

if [ $? -ne 0 ]; then
    echo "Name index build failed."
    rm -f "$output_db_tmp"
    exit 1
fi

echo "Sorting name_index by trigram..."
duckdb -bail "$output_db_tmp" <<'ENDSQL'
SET memory_limit='16GB';
CREATE TABLE name_index_sorted AS SELECT * FROM name_index ORDER BY trigram;
DROP TABLE name_index;
ALTER TABLE name_index_sorted RENAME TO name_index;
SET memory_limit='48GB';
ENDSQL

if [ $? -ne 0 ]; then
    echo "Name index sort failed."
    rm -f "$output_db_tmp"
    exit 1
fi

echo "Analyzing..."
duckdb -bail "$output_db_tmp" -c "ANALYZE;"

if [ $? -ne 0 ]; then
    echo "ANALYZE failed."
    rm -f "$output_db_tmp"
    exit 1
fi

# ─── Finalize ─────────────────────────────────────────────────────────────────

echo
echo "Finalizing database..."
mv "$output_db_tmp" "$output_db"

echo
echo "Wrote ${output_db}"
