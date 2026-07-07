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

# Extract --log and --cache-dir options before positional parsing
log_file=""
cache_dir=""
remaining_args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --log) log_file="$2"; shift 2 ;;
        --cache-dir) cache_dir="$2"; shift 2 ;;
        *) remaining_args+=("$1"); shift ;;
    esac
done
set -- "${remaining_args[@]}"

if [ -n "$log_file" ]; then
    mkdir -p "$(dirname "$log_file")"
    exec > >(tee "$log_file") 2>&1
fi

xmin=$1
ymin=$2
xmax=$3
ymax=$4

if [ -z "$xmin" ] || [ -z "$ymin" ] || [ -z "$xmax" ] || [ -z "$ymax" ]; then
    echo
    echo "Usage: $0 [--log <path>] [--cache-dir <path>] <xmin> <ymin> <xmax> <ymax>"
    echo
    exit 1
fi

# Check that xmin is numerically less than xmax and ymin less than ymax
if ! [[ "$xmin" =~ ^-?[0-9]+(\.[0-9]+)?$ ]] || ! [[ "$ymin" =~ ^-?[0-9]+(\.[0-9]+)?$ ]] || \
   ! [[ "$xmax" =~ ^-?[0-9]+(\.[0-9]+)?$ ]] || ! [[ "$ymax" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
    echo "All coordinates must be valid numbers."
    exit 1
fi

if (( $(echo "$xmin >= $xmax" | bc -l) )) || (( $(echo "$ymin >= $ymax" | bc -l) )); then
    echo "Invalid bounding box: xmin must be less than xmax and ymin must be less than ymax."
    exit 1
fi

# Create the db/ directory in the parent folder relative to this script
# This will be the output directory for the DuckDB database
script_dir="$(dirname "$(realpath "$0")")"
output_dir="${script_dir}/../db"
mkdir -p "$output_dir"

# If --cache-dir is not provided, discover the latest release from S3
if [ -z "$cache_dir" ]; then
    # Find the latest release from https://fsq-os-places-us-east-1.s3.amazonaws.com/ using curl
    # The release values are in the format: "<Key>release/dt=2025-03-06/</Key>"
    # The XML file does not contain newlines
    # We want something POSIX compliant so it will run on both MacOS and Linux

    # Fetch the XML and extract the latest release date
    latest_release=$(curl -s "https://fsq-os-places-us-east-1.s3.amazonaws.com/" |
      grep -o "<Key>release/dt=[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}/</Key>" |
      sed 's/<Key>release\/dt=\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\)\/<\/Key>/\1/g' |
      sort -r |
      head -1)

    if [ -z "$latest_release" ]; then
        echo "No releases found."
        exit 1
    fi

    echo "Using latest release: $latest_release"

    # Construct the source data URL using the latest release date
    source_data="https://fsq-os-places-us-east-1.s3.amazonaws.com/release/dt=${latest_release}/places/parquet/places-00000.zstd.parquet"

    # Download and cache parquet files locally
    cache_dir="${output_dir}/cache/fsq/${latest_release}"
    mkdir -p "$cache_dir"
fi

# Verify cache exists and is complete
if [ ! -d "$cache_dir" ] || [ -z "$(ls "$cache_dir"/*.parquet 2>/dev/null)" ]; then
    echo "Cache missing: $cache_dir"
    echo "Run download-fsq.sh first to populate the cache."
    exit 1
fi

# Verify all 100 files are present
cached_count=$(find "$cache_dir" -maxdepth 1 -name '*.parquet' -type f | wc -l | tr -d ' ')
if [ "$cached_count" -ne 100 ]; then
    echo "Incomplete FSQ cache: found $cached_count files, expected 100 in $cache_dir"
    exit 1
fi

# Remove any existing temp file
rm -f "${output_dir}/fsq-osp.duckdb.tmp"

# Initialize the spatial extension and the places table
cat > "${output_dir}/import.sql" <<EOF
.print "Initializing..."
SET memory_limit='48GB';
install spatial;
load spatial;
.headers off
.mode list
create table places as select * EXCLUDE (geom), geom::GEOMETRY as geom from '${cache_dir}/places-00000.zstd.parquet' limit 0;
EOF

# Load the data from each parquet file into the places table
for i in $(seq 0 99); do
    source_file="${cache_dir}/places-$(printf '%05d' $i).zstd.parquet"
    cat <<EOF
SELECT printf('[%s] Importing ${i} / 100', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
insert into places select * EXCLUDE (geom), geom::GEOMETRY as geom from '${source_file}'
    where bbox.xmin >= ${xmin} and bbox.xmax <= ${xmax}
    and bbox.ymin >= ${ymin} and bbox.ymax <= ${ymax}
    and date_refreshed > '2020-03-15'
    and date_closed is null
    and longitude != 0 and latitude != 0 and geom is not null;
EOF
done >> "${output_dir}/import.sql"

# Create spatial index
cat >> "${output_dir}/import.sql" <<EOF
SELECT printf('[%s] Creating spatial index...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
create index idx_fsq_place_id on places(fsq_place_id);
EOF

# Compute importance as normalized 0-100 integer score
# 60% window-function density (S2 level 12 cell count) + 40% category IDF
cat >> "${output_dir}/import.sql" <<EOF
SELECT printf('[%s] Loading geography extension for importance scoring...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
install geography from community;
load geography;
SELECT printf('[%s] Computing importance scores...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
CREATE TEMP TABLE t_idf AS
SELECT
    category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM (
    SELECT unnest(fsq_category_ids) AS category
    FROM places
    WHERE fsq_category_ids IS NOT NULL
) cats
CROSS JOIN (SELECT count(*) AS total FROM places) N
GROUP BY category, N.total;
CREATE TEMP TABLE place_density AS
SELECT fsq_place_id,
       ln(1 + count(*) OVER (
           PARTITION BY s2_cell_parent(s2_cellfromlonlat(longitude, latitude), 12)
       )) AS density_score
FROM places;
CREATE TEMP TABLE place_idf AS
SELECT
    p.fsq_place_id,
    coalesce(max(idf.idf_score), 0) AS idf_score
FROM places p,
    unnest(p.fsq_category_ids) AS t(category)
LEFT JOIN t_idf idf ON idf.category = t.category
WHERE p.fsq_category_ids IS NOT NULL
GROUP BY p.fsq_place_id;
CREATE TABLE places_scored AS
SELECT p.*,
       round(
           60 * least(coalesce(d.density_score, 0) / 10.0, 1.0)
         + 40 * least(coalesce(i.idf_score, 0) / 18.0, 1.0)
       )::INTEGER AS importance
FROM places p
LEFT JOIN place_density d USING (fsq_place_id)
LEFT JOIN place_idf i USING (fsq_place_id);
DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
create index idx_fsq_place_id on places(fsq_place_id);
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_idf;
EOF

# Add empty variants column (FSQ has no variant source data)
cat >> "${output_dir}/import.sql" <<'EOF'
SELECT printf('[%s] Adding variants column...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
ALTER TABLE places ADD COLUMN variants
    STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [];
EOF

# Build name_index with trigrams (reads importance directly from places)
cat >> "${output_dir}/import.sql" <<EOF
SELECT printf('[%s] Creating name index...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
create table name_index as
with name_prep as (
    select
        fsq_place_id,
        name,
        lower(strip_accents(name)) as norm_name,
        coalesce(importance, 0) as importance
    from places
    where name is not null and length(name) > 0
),
trigrams as (
    select
        substr(np.norm_name, pos, 3) as trigram,
        np.fsq_place_id,
        np.name,
        np.norm_name,
        np.importance,
        false as is_variant
    from name_prep np
    cross join generate_series(1, length(np.norm_name) - 2) as gs(pos)
    where length(np.norm_name) >= 3
)
select
    trigram,
    fsq_place_id,
    name,
    norm_name,
    importance,
    is_variant
from trigrams
order by trigram;
EOF

cat >> "${output_dir}/import.sql" <<EOF
SELECT printf('[%s] Analyzing...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
analyze;
EOF

# Run the import script
echo
time duckdb -bail "${output_dir}/fsq-osp.duckdb.tmp" -c ".read ${output_dir}/import.sql"

if [ $? -ne 0 ]; then
    echo "Failed to import data into DuckDB."
    rm -f "${output_dir}/import.sql"
    exit 1
fi

# Copy over any existing database
mv "${output_dir}/fsq-osp.duckdb.tmp" "${output_dir}/fsq-osp.duckdb"
rm -f "${output_dir}/import.sql"

# Clean up old release caches
# for old_cache in "${output_dir}/cache/fsq/"*/; do
#     if [ "$(basename "$old_cache")" != "${latest_release}" ]; then
#         rm -rf "$old_cache"
#     fi
# done

echo
duckdb "${output_dir}/fsq-osp.duckdb" -c "select count(*) from places;"
