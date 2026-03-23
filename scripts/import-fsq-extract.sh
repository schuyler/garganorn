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
output_dir="$(dirname "$(realpath "$0")")/../db"
mkdir -p "$output_dir"

# Detect density file
density_file="${output_dir}/density.parquet"
if [ -f "$(realpath "${density_file}" 2>/dev/null || echo "")" ]; then
    density_file="$(realpath "${density_file}")"
    has_density=true
else
    has_density=false
fi

# Detect category IDF file
idf_file="${output_dir}/category_idf.parquet"
if [ -f "$(realpath "${idf_file}" 2>/dev/null || echo "")" ]; then
    idf_file="$(realpath "${idf_file}")"
    has_idf=true
else
    has_idf=false
fi

# Set IDF SQL fragments
# idf_cte: a CTE fragment WITHOUT the leading WITH keyword, WITH trailing comma.
# It will be composed as: WITH tokens AS (...), <idf_cte> codes AS (...), ...
# When has_idf=false, idf_cte is empty string (no extra CTE).
if [ "$has_idf" = true ]; then
    idf_cte="place_idf AS (
    SELECT
        p.fsq_place_id,
        max(idf.idf_score) AS max_idf
    FROM places p,
        unnest(p.fsq_category_ids) AS t(category)
    LEFT JOIN read_parquet('${idf_file}') idf
        ON idf.collection = 'foursquare'
        AND idf.category = t.category
    WHERE p.fsq_category_ids IS NOT NULL
    GROUP BY p.fsq_place_id
),"
    idf_join="LEFT JOIN place_idf pi USING (fsq_place_id)"
    idf_score="coalesce(pi.max_idf, 0)"
else
    idf_cte=""
    idf_join=""
    idf_score="0"
fi

# Remove any existing temp file
rm -f "${output_dir}/fsq-osp.duckdb.tmp"

# Initialize the spatial extension and the places table
cat > "${output_dir}/import.sql" <<EOF
.print "Initializing..."
install spatial;
load spatial;
create table places as select * from '${source_data}' limit 0;
EOF

# Load the data from each parquet file into the places table
for i in $(seq 0 99); do
    source_file=$(echo "${source_data}" | sed "s/places-00000.zstd.parquet/places-$(printf '%05d' $i).zstd.parquet/")
    cat <<EOF
.print "Importing ${i} / 100"
insert into places select * from '${source_file}'
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
EOF

# Build name_index with phonetic codes; branch on density file availability
if [ "$has_density" = true ]; then
    cat >> "${output_dir}/import.sql" <<EOF
.print "Loading extensions for phonetic index + density..."
install geography from community;
load geography;
install splink_udfs from community;
load splink_udfs;
.print "Creating name index (with density)..."
create table name_index as
with tokens as (
    select
        unnest(string_split(lower(strip_accents(name)), ' ')) as token,
        fsq_place_id,
        name,
        latitude::decimal(10,6)::varchar as latitude,
        longitude::decimal(10,6)::varchar as longitude,
        latitude as lat_raw,
        longitude as lon_raw,
        address, locality, postcode, region, country
    from places
    where name is not null and length(name) > 0
),
${idf_cte}
codes as (
    select
        unnest(double_metaphone(t.token)) as dm_code,
        t.token,
        t.fsq_place_id,
        t.name,
        t.latitude,
        t.longitude,
        t.address,
        t.locality,
        t.postcode,
        t.region,
        t.country,
        coalesce(ln(1 + c.pt_count), 0) + ${idf_score} as importance
    from tokens t
    ${idf_join}
    left join read_parquet('${density_file}') c
        on c.level = 12
        and c.cell_id = s2_cell_parent(
            s2_cellfromlonlat(t.lon_raw, t.lat_raw), 12
        )
    where length(t.token) > 1
),
filtered_codes as (
    select * from codes
    where dm_code is not null and dm_code != ''
),
place_code_counts as (
    select fsq_place_id, count(distinct dm_code) as n_place_codes
    from filtered_codes
    group by fsq_place_id
)
select
    fc.dm_code,
    fc.token,
    fc.fsq_place_id,
    fc.name,
    fc.latitude,
    fc.longitude,
    fc.address,
    fc.locality,
    fc.postcode,
    fc.region,
    fc.country,
    fc.importance,
    pc.n_place_codes
from filtered_codes fc
join place_code_counts pc using (fsq_place_id)
order by fc.dm_code;
EOF
else
    cat >> "${output_dir}/import.sql" <<EOF
.print "Loading splink_udfs extension for phonetic indexing..."
install splink_udfs from community;
load splink_udfs;
.print "Creating name index (no density)..."
create table name_index as
with tokens as (
    select
        unnest(string_split(lower(strip_accents(name)), ' ')) as token,
        fsq_place_id,
        name,
        latitude::decimal(10,6)::varchar as latitude,
        longitude::decimal(10,6)::varchar as longitude,
        address, locality, postcode, region, country
    from places
    where name is not null and length(name) > 0
),
${idf_cte}
codes as (
    select
        unnest(double_metaphone(t.token)) as dm_code,
        t.token,
        t.fsq_place_id,
        t.name,
        t.latitude,
        t.longitude,
        t.address,
        t.locality,
        t.postcode,
        t.region,
        t.country,
        ${idf_score} as importance
    from tokens t
    ${idf_join}
    where length(t.token) > 1
),
filtered_codes as (
    select * from codes
    where dm_code is not null and dm_code != ''
),
place_code_counts as (
    select fsq_place_id, count(distinct dm_code) as n_place_codes
    from filtered_codes
    group by fsq_place_id
)
select
    fc.dm_code,
    fc.token,
    fc.fsq_place_id,
    fc.name,
    fc.latitude,
    fc.longitude,
    fc.address,
    fc.locality,
    fc.postcode,
    fc.region,
    fc.country,
    fc.importance,
    pc.n_place_codes
from filtered_codes fc
join place_code_counts pc using (fsq_place_id)
order by fc.dm_code;
EOF
fi

cat >> "${output_dir}/import.sql" <<EOF
.print "Analyzing..."
analyze;
EOF

# Run the import script
echo
time duckdb -bail "${output_dir}/fsq-osp.duckdb.tmp" < "${output_dir}/import.sql"

if [ $? -ne 0 ]; then
    echo "Failed to import data into DuckDB."
    rm -f "${output_dir}/import.sql"
    exit 1
fi

# Copy over any existing database
mv "${output_dir}/fsq-osp.duckdb.tmp" "${output_dir}/fsq-osp.duckdb"
rm -f "${output_dir}/import.sql"

echo
echo "select count(*) from places;" | duckdb "${output_dir}/fsq-osp.duckdb"
