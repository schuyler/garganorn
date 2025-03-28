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

# Define database filename
db_filename="overture-maps.duckdb"

xmin=$1
ymin=$2
xmax=$3
ymax=$4
release=$5

if [ -z "$xmin" ] || [ -z "$ymin" ] || [ -z "$xmax" ] || [ -z "$ymax" ]; then
    echo 
    echo "Usage: $0 <xmin> <ymin> <xmax> <ymax> [release]"
    echo "  release: Optional Overture release date (format: YYYY-MM-DD.N)"
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

# Use provided release if specified, otherwise use default
if [ -n "$release" ]; then
    latest_release=$release
    echo "Using specified Overture release: $latest_release"
else
    # Use a known recent release as default
    latest_release="2025-03-19.1"
    echo "Using default Overture release: $latest_release (specify a release as 5th parameter to override)"
fi

# Create the db/ directory in the parent folder relative to this script
output_dir="$(dirname "$(realpath "$0")")/../db"
mkdir -p "$output_dir"

# Remove any existing temp file
rm -f "${output_dir}/${db_filename}.tmp"

# Get the list of available parquet files for places
echo "Finding available place parquet files..."
parquet_files=$(curl -s "https://overturemaps-us-west-2.s3.amazonaws.com/?list-type=2&prefix=release/${latest_release}/theme=places/type=place/" |
  grep -o ">[^<]*part-[0-9]*-[^<]*.parquet<" |
  sed 's/>\(.*\)</\1/g' |
  sort)

# For debugging if needed
if [ -z "$parquet_files" ]; then
  echo "No parquet files found. Please check the release date and URL format."
  echo "Showing sample of XML response:"
  curl -s "https://overturemaps-us-west-2.s3.amazonaws.com/?list-type=2&prefix=release/${latest_release}/theme=places/type=place/" | head -50
  exit 1
fi

# Take the first file to initialize the structure
first_file=$(echo "$parquet_files" | head -1)
source_base="https://overturemaps-us-west-2.s3.amazonaws.com"
first_file_url="${source_base}/${first_file}"

# Initialize the spatial extension and the places table
cat > "${output_dir}/import-overture.sql" <<EOF
.print "Initializing..."
install spatial;
load spatial;

-- Create the places table
create table places as select * from '${first_file_url}' limit 0;
EOF

# Load the data from each parquet file into the places table
file_count=$(echo "$parquet_files" | wc -l)
file_number=0

echo "$parquet_files" | while read -r file; do
  file_number=$((file_number + 1))
  file_url="${source_base}/${file}"
  
  cat <<EOF
.print "Importing file ${file_number}/${file_count}: ${file}"
insert into places select * from '${file_url}'
    where bbox.xmin >= ${xmin} 
      and bbox.xmax <= ${xmax}
      and bbox.ymin >= ${ymin}
      and bbox.ymax <= ${ymax};
EOF
done >> "${output_dir}/import-overture.sql"

# Clean up and create spatial index
cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Cleaning up..."
delete from places where geometry is null;

.print "Creating spatial index..."
create index places_rtree on places using rtree (geometry);
EOF

# Run the import script
echo
time duckdb -bail "${output_dir}/${db_filename}.tmp" < "${output_dir}/import-overture.sql"

if [ $? -ne 0 ]; then
    echo "Failed to import data into DuckDB."
    exit 1
fi

# Copy over any existing database
mv "${output_dir}/${db_filename}.tmp" "${output_dir}/${db_filename}"
rm -f "${output_dir}/import-overture.sql"

echo
echo "select count(*) from places;" | duckdb "${output_dir}/${db_filename}"
