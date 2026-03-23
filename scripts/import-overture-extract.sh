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

# Set IDF SQL fragments for Overture
# Same CTE pattern as FSQ: fragment WITHOUT leading WITH, WITH trailing comma.
if [ "$has_idf" = true ]; then
    idf_cte_ov="place_idf AS (
    SELECT
        sub.id,
        coalesce(idf.idf_score, 0) AS max_idf
    FROM (
        SELECT id, categories.primary AS categories_primary
        FROM places
        WHERE categories.primary IS NOT NULL
    ) sub
    LEFT JOIN read_parquet('${idf_file}') idf
        ON idf.collection = 'overture'
        AND idf.category = sub.categories_primary
),"
    idf_join_ov="LEFT JOIN place_idf pi USING (id)"
    idf_score_ov="coalesce(pi.max_idf, 0)"
else
    idf_cte_ov=""
    idf_join_ov=""
    idf_score_ov="0"
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

# Build name_index with trigrams; branch on density file availability
if [ "$has_density" = true ]; then
    cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Loading geography extension for density..."
install geography from community;
load geography;
.print "Creating name index (with density)..."
create table name_index as
with name_prep as (
    select
        id,
        names.primary as name,
        lower(strip_accents(names.primary)) as norm_name,
        st_y(st_centroid(geometry))::decimal(10,6)::varchar as latitude,
        st_x(st_centroid(geometry))::decimal(10,6)::varchar as longitude,
        st_y(st_centroid(geometry)) as lat_num,
        st_x(st_centroid(geometry)) as lon_num
    from places
    where names.primary is not null and length(names.primary) > 0
),
${idf_cte_ov}
place_importance as (
    select
        np.id,
        coalesce(ln(1 + c.pt_count), 0) + ${idf_score_ov} as importance
    from name_prep np
    ${idf_join_ov}
    left join read_parquet('${density_file}') c
        on c.level = 12
        and c.cell_id = s2_cell_parent(
            s2_cellfromlonlat(np.lon_num, np.lat_num), 12
        )
),
trigrams as (
    select distinct
        substr(np.norm_name, pos, 3) as trigram,
        np.id,
        np.name,
        np.latitude,
        np.longitude,
        coalesce(pi.importance, 0) as importance
    from name_prep np
    left join place_importance pi using (id)
    cross join generate_series(1, length(np.norm_name) - 2) as gs(pos)
    where length(np.norm_name) >= 3
)
select trigram, id, name, latitude, longitude, importance
from trigrams
order by trigram;
EOF
else
    cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Creating name index (no density)..."
create table name_index as
with name_prep as (
    select
        id,
        names.primary as name,
        lower(strip_accents(names.primary)) as norm_name,
        st_y(st_centroid(geometry))::decimal(10,6)::varchar as latitude,
        st_x(st_centroid(geometry))::decimal(10,6)::varchar as longitude
    from places
    where names.primary is not null and length(names.primary) > 0
),
${idf_cte_ov}
trigrams as (
    select distinct
        substr(np.norm_name, pos, 3) as trigram,
        np.id,
        np.name,
        np.latitude,
        np.longitude,
        ${idf_score_ov} as importance
    from name_prep np
    ${idf_join_ov}
    cross join generate_series(1, length(np.norm_name) - 2) as gs(pos)
    where length(np.norm_name) >= 3
)
select trigram, id, name, latitude, longitude, importance
from trigrams
order by trigram;
EOF
fi

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
