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

if ! command -v osmium &> /dev/null; then
    echo "osmium not installed. Please install it first."
    echo "To install osmium-tool:"
    echo "  brew install osmium-tool    # macOS"
    echo "  apt install osmium-tool     # Debian/Ubuntu"
    exit 1
fi

usage() {
    echo
    echo "Usage: $0 <pbf_path> [options]"
    echo
    echo "  pbf_path: path to the OSM PBF file"
    echo
    echo "  Options:"
    echo "    --log <path>          log output to file (tee)"
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
log_file=""

while [ $# -gt 0 ]; do
    case "$1" in
        --output-dir) output_dir="$2"; shift 2 ;;
        --cache-dir)  cache_dir="$2";  shift 2 ;;
        --xmin)       xmin="$2";       shift 2 ;;
        --xmax)       xmax="$2";       shift 2 ;;
        --ymin)       ymin="$2";       shift 2 ;;
        --ymax)       ymax="$2";       shift 2 ;;
        --log)        log_file="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

if [ -n "$log_file" ]; then
    mkdir -p "$(dirname "$log_file")"
    exec > >(tee "$log_file") 2>&1
fi

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

import_start=$SECONDS
stage_start=$SECONDS
elapsed() {
    local now=$SECONDS
    local dt=$((now - stage_start))
    stage_start=$now
    printf "  [%dm%02ds]\n" $((dt / 60)) $((dt % 60))
}

# ─── Stage 0: Filter PBF with osmium ────────────────────────────────────────

filtered_pbf="${cache_dir}/filtered.osm.pbf"

if [ -f "$filtered_pbf" ] && [ "$filtered_pbf" -nt "$pbf_path" ]; then
    echo "Using cached filtered PBF: $filtered_pbf"
else
    echo "Filtering PBF with osmium tags-filter..."
    osmium tags-filter "$pbf_path" \
        n/amenity n/shop n/tourism n/leisure n/office n/craft n/healthcare \
        n/historic n/natural n/man_made n/aeroway n/railway n/public_transport n/place \
        w/amenity w/shop w/tourism w/leisure w/office w/craft w/healthcare \
        w/historic w/natural w/man_made w/aeroway w/railway w/public_transport w/place \
        --overwrite \
        -o "$filtered_pbf"
    if [ $? -ne 0 ]; then
        echo "osmium tags-filter failed."
        exit 1
    fi
fi

pbf_input="$filtered_pbf"

elapsed

# ─── Stage 1: Convert PBF to Parquet ──────────────────────────────────────────

parquet_dir="${cache_dir}/parquet"

if [ -f "$parquet_dir/.complete" ] && [ "$parquet_dir/.complete" -nt "$pbf_input" ]; then
    echo "Using cached Parquet: $parquet_dir"
else
    echo "Converting PBF to Parquet with osm-pbf-parquet..."
    rm -rf "$parquet_dir"
    osm-pbf-parquet --input "$pbf_input" --output "$parquet_dir"
    if [ $? -ne 0 ]; then
        echo "osm-pbf-parquet conversion failed."
        exit 1
    fi
    touch "$parquet_dir/.complete"
fi

node_parquet="${parquet_dir}/type=node/*.parquet"
way_parquet="${parquet_dir}/type=way/*.parquet"
elapsed

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

elapsed

# ─── Importance scoring ───────────────────────────────────────────────────────

echo
echo "Computing importance scores..."

duckdb -bail "$output_db_tmp" <<ENDSQL
INSTALL geography FROM community;
LOAD geography;
LOAD spatial;

CREATE TEMP TABLE t_idf AS
SELECT primary_category AS category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM places
CROSS JOIN (SELECT count(*) AS total FROM places) N
WHERE primary_category IS NOT NULL
GROUP BY primary_category, N.total;

CREATE TEMP TABLE place_density AS
SELECT rkey,
       ln(1 + count(*) OVER (
           PARTITION BY s2_cell_parent(s2_cellfromlonlat(longitude, latitude), 12)
       )) AS density_score
FROM places;

CREATE TEMP TABLE place_idf AS
SELECT
    p.rkey,
    coalesce(max(idf.idf_score), 0) AS idf_score
FROM places p
LEFT JOIN t_idf idf ON idf.category = p.primary_category
WHERE p.primary_category IS NOT NULL
GROUP BY p.rkey;

CREATE TABLE places_scored AS
SELECT p.* EXCLUDE (importance),
       round(
           60 * least(coalesce(d.density_score, 0) / 10.0, 1.0)
         + 40 * least(coalesce(i.idf_score, 0) / 18.0, 1.0)
       )::INTEGER AS importance
FROM places p
LEFT JOIN place_density d USING (rkey)
LEFT JOIN place_idf i USING (rkey);
DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
CREATE INDEX places_rtree ON places USING RTREE (geom);
CREATE INDEX idx_rkey ON places(rkey);

DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_idf;
ENDSQL
if [ $? -ne 0 ]; then
    echo "Importance scoring failed."
    rm -f "$output_db_tmp"
    exit 1
fi

elapsed

# ─── Variant extraction ───────────────────────────────────────────────────────

echo
echo "Extracting name variants..."

duckdb -bail "$output_db_tmp" <<EOF
CREATE TEMP TABLE raw_variants AS
WITH tag_entries AS (
    SELECT
        'n' || id::VARCHAR AS rkey,
        unnest(map_entries(tags)) AS e
    FROM read_parquet('${node_parquet}')
    WHERE tags['name'] IS NOT NULL
    UNION ALL
    SELECT
        'w' || id::VARCHAR AS rkey,
        unnest(map_entries(tags)) AS e
    FROM read_parquet('${way_parquet}')
    WHERE tags['name'] IS NOT NULL
),
name_tags AS (
    SELECT rkey, e.key, e.value
    FROM tag_entries
    WHERE e.key LIKE 'name:%'
       OR e.key IN ('alt_name','old_name','official_name',
                    'short_name','loc_name','int_name')
),
split_values AS (
    SELECT rkey,
        trim(s.value) AS name,
        CASE
            WHEN key LIKE 'name:%' THEN 'alternate'
            WHEN key = 'alt_name' THEN 'alternate'
            WHEN key = 'old_name' THEN 'historical'
            WHEN key = 'official_name' THEN 'official'
            WHEN key = 'short_name' THEN 'short'
            WHEN key = 'loc_name' THEN 'colloquial'
            WHEN key = 'int_name' THEN 'alternate'
        END AS type,
        CASE
            WHEN key LIKE 'name:%' THEN replace(key, 'name:', '')
            ELSE NULL
        END AS language
    FROM name_tags,
         unnest(string_split(value, ';')) AS s(value)
    WHERE trim(s.value) != ''
)
SELECT rkey,
       list({'name': name, 'type': type, 'language': language}
            ORDER BY name) AS variants
FROM split_values
GROUP BY rkey;

CREATE TABLE places_with_variants AS
SELECT p.*,
       coalesce(rv.variants, []) AS variants
FROM places p
LEFT JOIN raw_variants rv USING (rkey);
DROP TABLE places;
ALTER TABLE places_with_variants RENAME TO places;
CREATE INDEX places_rtree ON places USING RTREE (geom);
CREATE INDEX idx_rkey ON places(rkey);
DROP TABLE raw_variants;
EOF

if [ $? -ne 0 ]; then
    echo "Variant extraction failed."
    rm -f "$output_db_tmp"
    exit 1
fi

elapsed

# ─── Name index ───────────────────────────────────────────────────────────────

echo
echo "Building name index..."

duckdb -bail "$output_db_tmp" <<'ENDSQL'
CREATE TABLE IF NOT EXISTS name_index (
    trigram          VARCHAR,
    rkey             VARCHAR,
    name             VARCHAR,
    norm_name        VARCHAR,
    importance       INTEGER,
    is_variant       BOOLEAN DEFAULT FALSE
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
       np.importance,
       FALSE AS is_variant
FROM name_prep np
CROSS JOIN generate_series(1, length(np.norm_name) - 2) AS gs(pos);

INSERT INTO name_index
WITH variant_names AS (
    SELECT rkey,
           v.name,
           lower(strip_accents(v.name)) AS norm_name,
           importance,
           TRUE AS is_variant
    FROM places,
         unnest(variants) AS v
    WHERE v.name IS NOT NULL AND length(v.name) >= 3
)
SELECT substr(vn.norm_name, pos, 3) AS trigram,
       vn.rkey,
       vn.name,
       vn.norm_name,
       vn.importance,
       vn.is_variant
FROM variant_names vn
CROSS JOIN generate_series(1, length(vn.norm_name) - 2) AS gs(pos);
ENDSQL

if [ $? -ne 0 ]; then
    echo "Name index build failed."
    rm -f "$output_db_tmp"
    exit 1
fi

elapsed
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

elapsed
echo "Analyzing..."
duckdb -bail "$output_db_tmp" -c "ANALYZE;"

if [ $? -ne 0 ]; then
    echo "ANALYZE failed."
    rm -f "$output_db_tmp"
    exit 1
fi

# ─── Finalize ─────────────────────────────────────────────────────────────────

elapsed
echo
echo "Finalizing database..."
mv "$output_db_tmp" "$output_db"

total=$((SECONDS - import_start))
echo
echo "Wrote ${output_db}"
printf "Total: %dm%02ds\n" $((total / 60)) $((total % 60))
