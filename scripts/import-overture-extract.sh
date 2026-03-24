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
script_dir="$(dirname "$(realpath "$0")")"
output_dir="${script_dir}/../db"
mkdir -p "$output_dir"

# Detect or auto-build density file
density_file="${output_dir}/density-overture.parquet"
if [ ! -f "$density_file" ]; then
    echo "Building Overture density table..."
    "${script_dir}/build-density.sh" overture "${latest_release}"
fi

# Detect or auto-build category IDF file
idf_file="${output_dir}/category_idf-overture.parquet"
if [ ! -f "$idf_file" ]; then
    echo "Building Overture IDF table..."
    "${script_dir}/build-idf.sh" overture "${latest_release}"
fi

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

# Compute importance as normalized 0-100 integer score
# 60% density (S2 level 12 cell count) + 40% category IDF
cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Loading geography extension for importance scoring..."
install geography from community;
load geography;
.print "Computing importance scores..."
ALTER TABLE places ADD COLUMN importance INTEGER DEFAULT 0;
CREATE TEMP TABLE t_density AS SELECT * FROM read_parquet('${density_file}') WHERE level = 12;
CREATE TEMP TABLE t_idf AS SELECT * FROM read_parquet('${idf_file}');
CREATE TEMP TABLE place_density AS
SELECT
    p.id,
    coalesce(ln(1 + c.pt_count), 0) AS density_score
FROM places p
LEFT JOIN t_density c
    ON c.cell_id = s2_cell_parent(
        s2_cellfromlonlat(
            st_x(st_centroid(p.geometry)),
            st_y(st_centroid(p.geometry))
        ), 12
    );
CREATE TEMP TABLE place_idf AS
SELECT
    p.id,
    coalesce(idf.idf_score, 0) AS idf_score
FROM places p
LEFT JOIN t_idf idf ON idf.category = p.categories.primary;
UPDATE places SET importance = round(
    60 * least(d.density_score / 10.0, 1.0)
  + 40 * least(coalesce(i.idf_score, 0) / 18.0, 1.0)
)::INTEGER
FROM place_density d
LEFT JOIN place_idf i USING (id)
WHERE places.id = d.id;
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_density;
DROP TABLE t_idf;
EOF

# Build name_index with trigrams (reads importance directly from places)
cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Creating name index..."
create table name_index as
with name_prep as (
    select
        id,
        names.primary as name,
        lower(strip_accents(names.primary)) as norm_name,
        st_y(st_centroid(geometry))::decimal(10,6)::varchar as latitude,
        st_x(st_centroid(geometry))::decimal(10,6)::varchar as longitude,
        coalesce(importance, 0) as importance
    from places
    where names.primary is not null and length(names.primary) > 0
),
trigrams as (
    select distinct
        substr(np.norm_name, pos, 3) as trigram,
        np.id,
        np.name,
        np.latitude,
        np.longitude,
        np.importance
    from name_prep np
    cross join generate_series(1, length(np.norm_name) - 2) as gs(pos)
    where length(np.norm_name) >= 3
)
select trigram, id, name, latitude, longitude, importance
from trigrams
order by trigram;
EOF

cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Analyzing..."
analyze;
EOF

# Run the import script
echo
time duckdb -bail "${output_dir}/${db_filename}.tmp" -c ".read ${output_dir}/import-overture.sql"

if [ $? -ne 0 ]; then
    echo "Failed to import data into DuckDB."
    rm -f "${output_dir}/import-overture.sql"
    exit 1
fi

# Copy over any existing database
mv "${output_dir}/${db_filename}.tmp" "${output_dir}/${db_filename}"
rm -f "${output_dir}/import-overture.sql"

echo
duckdb "${output_dir}/${db_filename}" -c "select count(*) from places;"
