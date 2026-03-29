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

xmin=$1
ymin=$2
xmax=$3
ymax=$4

if [ -z "$xmin" ] || [ -z "$ymin" ] || [ -z "$xmax" ] || [ -z "$ymax" ]; then
    echo
    echo "Usage: $0 <xmin> <ymin> <xmax> <ymax>"
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

# Create the db/ directory in the parent folder relative to this script
# This will be the output directory for the DuckDB database
script_dir="$(dirname "$(realpath "$0")")"
output_dir="${script_dir}/../db"
mkdir -p "$output_dir"

# Download and cache parquet files locally
cache_dir="${output_dir}/cache/fsq/${latest_release}"
mkdir -p "$cache_dir"

cached_count=0
for i in $(seq 0 99); do
    filename="places-$(printf '%05d' $i).zstd.parquet"
    dest="${cache_dir}/${filename}"
    if [ -f "$dest" ]; then
        cached_count=$((cached_count + 1))
    fi
done

dl_count=0
for i in $(seq 0 99); do
    filename="places-$(printf '%05d' $i).zstd.parquet"
    dest="${cache_dir}/${filename}"
    if [ -f "$dest" ]; then
        continue
    fi
    dl_count=$((dl_count + 1))
    echo "Downloading $dl_count / $((100 - cached_count)) (cached: $cached_count)"
    url=$(echo "${source_data}" | sed "s/places-00000.zstd.parquet/${filename}/")
    if ! curl -sf -o "${dest}.tmp" "$url"; then
        echo "Failed to download ${url}"
        rm -f "${dest}.tmp"
        exit 1
    fi
    mv "${dest}.tmp" "$dest"
done

# Detect or auto-build category IDF file
idf_file="${output_dir}/category_idf-fsq.parquet"
if [ ! -f "$idf_file" ]; then
    echo "Building FSQ IDF table..."
    "${script_dir}/build-idf.sh" fsq "${cache_dir}" || { echo "Failed to build IDF table."; exit 1; }
    if [ ! -f "$idf_file" ]; then
        echo "IDF file not found after build: ${idf_file}"
        exit 1
    fi
fi

# Remove any existing temp file
rm -f "${output_dir}/fsq-osp.duckdb.tmp"

# Initialize the spatial extension and the places table
cat > "${output_dir}/import.sql" <<EOF
.print "Initializing..."
SET memory_limit='48GB';
install spatial;
load spatial;
create table places as select * EXCLUDE (geom), geom::GEOMETRY as geom from '${cache_dir}/places-00000.zstd.parquet' limit 0;
EOF

# Load the data from each parquet file into the places table
for i in $(seq 0 99); do
    source_file="${cache_dir}/places-$(printf '%05d' $i).zstd.parquet"
    cat <<EOF
.print "Importing ${i} / 100"
insert into places select * EXCLUDE (geom), geom::GEOMETRY as geom from '${source_file}'
    where bbox.xmin >= ${xmin} and bbox.xmax <= ${xmax}
    and bbox.ymin >= ${ymin} and bbox.ymax <= ${ymax}
    and date_refreshed > '2020-03-15'
    and date_closed is null;
EOF
done >> "${output_dir}/import.sql"

# Clean up the places table by removing any rows with invalid longitude or latitude
# and then create the spatial index
cat >> "${output_dir}/import.sql" <<EOF
.print "Cleaning up..."
delete from places where longitude = 0 or latitude = 0 or geom is null;
.print "Creating spatial index..."
create index places_rtree on places using rtree (geom);
create index idx_fsq_place_id on places(fsq_place_id);
EOF

# Compute importance as normalized 0-100 integer score
# 60% window-function density (S2 level 12 cell count) + 40% category IDF
cat >> "${output_dir}/import.sql" <<EOF
.print "Loading geography extension for importance scoring..."
install geography from community;
load geography;
.print "Computing importance scores..."
CREATE TEMP TABLE t_idf AS SELECT * FROM read_parquet('${idf_file}');
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
create index places_rtree on places using rtree (geom);
create index idx_fsq_place_id on places(fsq_place_id);
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_idf;
EOF

# Add empty variants column (FSQ has no variant source data)
cat >> "${output_dir}/import.sql" <<'EOF'
.print "Adding variants column..."
ALTER TABLE places ADD COLUMN variants
    STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [];
EOF

# Build name_index with trigrams (reads importance directly from places)
cat >> "${output_dir}/import.sql" <<EOF
.print "Creating name index..."
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
.print "Analyzing..."
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
