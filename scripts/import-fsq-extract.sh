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
    and bbox.ymin >= ${ymin} and bbox.ymax <= ${ymax};
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

# Run the import script
echo
time duckdb -bail "${output_dir}/fsq-osp.duckdb.tmp" < "${output_dir}/import.sql"

if [ $? -ne 0 ]; then
    echo "Failed to import data into DuckDB."
    exit 1
fi

# Copy over any existing database
mv "${output_dir}/fsq-osp.duckdb.tmp" "${output_dir}/fsq-osp.duckdb"
rm -f "${output_dir}/import.sql"

echo
echo "select count(*) from places;" | duckdb "${output_dir}/fsq-osp.duckdb"
