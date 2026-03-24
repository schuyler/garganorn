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

pbf_path=$1
xmin=$2
ymin=$3
xmax=$4
ymax=$5

if [ -z "$pbf_path" ]; then
    echo
    echo "Usage: $0 <pbf_path> [xmin ymin xmax ymax]"
    echo
    echo "  pbf_path: Local path to a PBF file (planet or regional extract)"
    echo "  xmin ymin xmax ymax: Optional bounding box for regional import"
    echo
    exit 1
fi

if [ ! -f "$pbf_path" ]; then
    echo "PBF file not found: $pbf_path"
    exit 1
fi

# Validate bbox arguments if provided
if [ -n "$xmin" ] || [ -n "$ymin" ] || [ -n "$xmax" ] || [ -n "$ymax" ]; then
    if [ -z "$xmin" ] || [ -z "$ymin" ] || [ -z "$xmax" ] || [ -z "$ymax" ]; then
        echo "If providing a bounding box, all four coordinates (xmin ymin xmax ymax) are required."
        exit 1
    fi

    if ! [[ "$xmin" =~ ^-?[0-9]+(\.[0-9]+)?$ ]] || ! [[ "$ymin" =~ ^-?[0-9]+(\.[0-9]+)?$ ]] || \
       ! [[ "$xmax" =~ ^-?[0-9]+(\.[0-9]+)?$ ]] || ! [[ "$ymax" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
        echo "All coordinates must be valid numbers."
        exit 1
    fi

    if (( $(echo "$xmin >= $xmax" | bc -l) )) || (( $(echo "$ymin >= $ymax" | bc -l) )); then
        echo "Invalid bounding box: xmin must be less than xmax and ymin must be less than ymax."
        exit 1
    fi

    has_bbox=true
else
    has_bbox=false
fi

script_dir="$(dirname "$(realpath "$0")")"
output_dir="${script_dir}/../db"
mkdir -p "$output_dir"

# Python with QuackOSM installed (default: python3)
QUACKOSM_PYTHON="${QUACKOSM_PYTHON:-python3}"

# ─── Stage 1: Convert PBF to GeoParquet using QuackOSM ───────────────────────

cache_dir="${output_dir}/cache/osm"
mkdir -p "$cache_dir"
parquet_file="${cache_dir}/osm_places.geoparquet"

if [ -f "$parquet_file" ]; then
    echo "Using cached GeoParquet: $parquet_file"
else
    echo "Converting PBF to GeoParquet with QuackOSM..."
    ${QUACKOSM_PYTHON} -c "
import quackosm
quackosm.convert_pbf_to_parquet(
    pbf_path='${pbf_path}',
    tags_filter={
        'amenity': True,
        'shop': True,
        'tourism': True,
        'leisure': True,
        'office': True,
        'craft': True,
        'healthcare': True,
        'historic': True,
        'natural': True,
        'man_made': True,
        'aeroway': True,
        'railway': True,
        'public_transport': True,
        'place': True,
    },
    keep_all_tags=True,
    explode_tags=False,
    result_file_path='${parquet_file}',
)
print('QuackOSM conversion complete.')
"
    if [ $? -ne 0 ]; then
        echo "QuackOSM conversion failed."
        exit 1
    fi
fi

# ─── Auto-build density and IDF ───────────────────────────────────────────────

density_file="${output_dir}/density-osm.parquet"
idf_file="${output_dir}/category_idf-osm.parquet"

if [ ! -f "$density_file" ]; then
    echo "Building density table..."
    "${script_dir}/build-density.sh" osm "$parquet_file"
fi

if [ ! -f "$idf_file" ]; then
    echo "Building IDF table..."
    "${script_dir}/build-idf.sh" osm "$parquet_file"
fi

has_density=false
has_idf=false
[ -f "$density_file" ] && has_density=true
[ -f "$idf_file" ] && has_idf=true

# ─── Stage 2: DuckDB SQL ─────────────────────────────────────────────────────

# Remove any existing temp file
rm -f "${output_dir}/osm.duckdb.tmp"

sql_file="${output_dir}/import-osm.sql"

cat > "${sql_file}" <<EOF
.print "Initializing spatial extension..."
INSTALL spatial;
LOAD spatial;

.print "Building places table..."
CREATE TABLE places AS
WITH filtered AS (
    SELECT
        left(split_part(feature_id, '/', 1), 1) AS osm_type,
        split_part(feature_id, '/', 2)::BIGINT AS osm_id,
        tags['name'][1] AS name,
        geometry,
        tags,
        CASE
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
    FROM read_parquet('${parquet_file}')
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
        -- Aeroway: airports and terminals only
        (tags['aeroway'][1] IN ('aerodrome', 'terminal', 'heliport'))
        OR
        -- Railway: stations only
        (tags['railway'][1] IN ('station', 'halt', 'tram_stop', 'subway_entrance'))
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
SELECT
    osm_type,
    osm_id,
    name,
    ST_Y(ST_PointOnSurface(geometry)) AS latitude,
    ST_X(ST_PointOnSurface(geometry)) AS longitude,
    ST_PointOnSurface(geometry) AS geom,
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
                   'addr:postcode', 'addr:country'
               )
        )
    ) AS tags,
    {'xmin': ST_X(ST_PointOnSurface(geometry)) - 0.0001,
     'ymin': ST_Y(ST_PointOnSurface(geometry)) - 0.0001,
     'xmax': ST_X(ST_PointOnSurface(geometry)) + 0.0001,
     'ymax': ST_Y(ST_PointOnSurface(geometry)) + 0.0001
    } AS bbox
FROM filtered
WHERE primary_category IS NOT NULL
  AND name IS NOT NULL
EOF

# Append bbox filter if provided
if [ "$has_bbox" = "true" ]; then
    cat >> "${sql_file}" <<EOF
    AND ST_X(ST_PointOnSurface(geometry)) BETWEEN ${xmin} AND ${xmax}
    AND ST_Y(ST_PointOnSurface(geometry)) BETWEEN ${ymin} AND ${ymax}
EOF
fi

cat >> "${sql_file}" <<EOF
;

.print "Removing null geometry rows..."
DELETE FROM places WHERE geom IS NULL;

.print "Creating spatial index..."
CREATE INDEX places_rtree ON places USING RTREE (geom);
EOF

# Importance scoring
if [ "$has_density" = "true" ] && [ "$has_idf" = "true" ]; then
    cat >> "${sql_file}" <<EOF
.print "Loading geography extension for importance scoring..."
INSTALL geography FROM community;
LOAD geography;
.print "Computing importance scores (density + IDF)..."
ALTER TABLE places ADD COLUMN importance INTEGER DEFAULT 0;
CREATE TEMP TABLE t_density AS SELECT * FROM read_parquet('${density_file}') WHERE level = 12;
CREATE TEMP TABLE t_idf AS SELECT * FROM read_parquet('${idf_file}');
CREATE TEMP TABLE place_density AS
SELECT
    p.osm_type,
    p.osm_id,
    coalesce(ln(1 + c.pt_count), 0) AS density_score
FROM places p
LEFT JOIN t_density c
    ON c.cell_id = s2_cell_parent(
        s2_cellfromlonlat(p.longitude, p.latitude), 12
    )::UBIGINT;
CREATE TEMP TABLE place_idf AS
SELECT
    p.osm_type,
    p.osm_id,
    coalesce(idf.idf_score, 0) AS idf_score
FROM places p
LEFT JOIN t_idf idf ON idf.category = p.primary_category;
UPDATE places SET importance = round(
    60 * least(d.density_score / 10.0, 1.0)
  + 40 * least(coalesce(i.idf_score, 0) / 18.0, 1.0)
)::INTEGER
FROM place_density d
LEFT JOIN place_idf i USING (osm_type, osm_id)
WHERE places.osm_type = d.osm_type AND places.osm_id = d.osm_id;
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_density;
DROP TABLE t_idf;
EOF
elif [ "$has_density" = "true" ]; then
    cat >> "${sql_file}" <<EOF
.print "Loading geography extension for importance scoring..."
INSTALL geography FROM community;
LOAD geography;
.print "Computing importance scores (density only)..."
ALTER TABLE places ADD COLUMN importance INTEGER DEFAULT 0;
UPDATE places AS p SET importance = (
    SELECT CAST(
        LEAST(
            60 * least(coalesce(ln(1 + c.pt_count), 0) / 10.0, 1.0),
            100
        ) AS INTEGER
    )
    FROM (SELECT 1) dummy
    LEFT JOIN read_parquet('${density_file}') c
        ON c.level = 12
        AND c.cell_id = s2_cell_parent(
            s2_cellfromlonlat(p.longitude, p.latitude), 12
        )::UBIGINT
);
EOF
elif [ "$has_idf" = "true" ]; then
    cat >> "${sql_file}" <<EOF
.print "Computing importance scores (IDF only)..."
ALTER TABLE places ADD COLUMN importance INTEGER DEFAULT 0;
CREATE TEMP TABLE t_idf AS SELECT * FROM read_parquet('${idf_file}');
UPDATE places SET importance = round(
    40 * least(coalesce(idf.idf_score, 0) / 18.0, 1.0)
)::INTEGER
FROM t_idf idf
WHERE idf.category = places.primary_category;
DROP TABLE t_idf;
EOF
else
    cat >> "${sql_file}" <<EOF
.print "No density or IDF data available; importance will be 0."
ALTER TABLE places ADD COLUMN importance INTEGER DEFAULT 0;
EOF
fi

cat >> "${sql_file}" <<EOF
.print "Creating name index..."
CREATE TABLE name_index AS
SELECT
    substr(lower(strip_accents(name)), i, 3) AS trigram,
    osm_type || osm_id::VARCHAR AS rkey,
    name,
    latitude::DECIMAL(10,6)::VARCHAR AS latitude,
    longitude::DECIMAL(10,6)::VARCHAR AS longitude,
    importance
FROM places,
     generate_series(1, greatest(length(lower(strip_accents(name))) - 2, 0)) AS t(i)
WHERE name IS NOT NULL AND length(name) >= 3
ORDER BY trigram;

.print "Analyzing..."
ANALYZE;
EOF

# Run the import script
echo
time duckdb -bail "${output_dir}/osm.duckdb.tmp" -c ".read ${sql_file}"

if [ $? -ne 0 ]; then
    echo "Failed to import data into DuckDB."
    rm -f "${sql_file}"
    exit 1
fi

# Promote temp file to final database
mv "${output_dir}/osm.duckdb.tmp" "${output_dir}/osm.duckdb"
rm -f "${sql_file}"

echo
duckdb "${output_dir}/osm.duckdb" -c "SELECT count(*) FROM places;"
