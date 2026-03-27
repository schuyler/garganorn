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
    # Auto-discover latest Overture release from S3
    echo "Auto-discovering latest Overture release..."
    latest_release=$(curl -s "https://overturemaps-us-west-2.s3.amazonaws.com/?list-type=2&prefix=release/&delimiter=/" |
      grep -o '<Prefix>release/[0-9][^<]*/</Prefix>' |
      sed 's/<Prefix>release\/\(.*\)\/<\/Prefix>/\1/' |
      sort -r |
      head -1)
    if [ -z "$latest_release" ]; then
        echo "No Overture releases found on S3."
        exit 1
    fi
    echo "Using latest Overture release: $latest_release"
fi

# Create the db/ directory in the parent folder relative to this script
script_dir="$(dirname "$(realpath "$0")")"
output_dir="${script_dir}/../db"
mkdir -p "$output_dir"

# Download and cache parquet files locally
cache_dir="${output_dir}/cache/overture/${latest_release}"
mkdir -p "$cache_dir"

# Get the list of available parquet files for places
echo "Finding available place parquet files..."
source_base="https://overturemaps-us-west-2.s3.amazonaws.com"
parquet_files=$(curl -s "${source_base}/?list-type=2&prefix=release/${latest_release}/theme=places/type=place/" |
  grep -o ">[^<]*part-[0-9]*-[^<]*.parquet<" |
  sed 's/>\(.*\)</\1/g' |
  sort)

if [ -z "$parquet_files" ]; then
  echo "No parquet files found. Please check the release date and URL format."
  echo "Showing sample of XML response:"
  curl -s "${source_base}/?list-type=2&prefix=release/${latest_release}/theme=places/type=place/" | head -50
  exit 1
fi

# Count already-cached files
file_count=$(echo "$parquet_files" | wc -l | tr -d ' ')
cached_count=0
while IFS= read -r file; do
    filename=$(basename "$file")
    if [ -f "${cache_dir}/${filename}" ]; then
        cached_count=$((cached_count + 1))
    fi
done <<< "$parquet_files"

# Download missing files
dl_count=0
while IFS= read -r file; do
    filename=$(basename "$file")
    dest="${cache_dir}/${filename}"
    if [ -f "$dest" ]; then
        continue
    fi
    dl_count=$((dl_count + 1))
    echo "Downloading ${dl_count} / $((file_count - cached_count)) (cached: ${cached_count}): ${filename}"
    if ! curl -sf -o "${dest}.tmp" "${source_base}/${file}"; then
        echo "Failed to download ${source_base}/${file}"
        rm -f "${dest}.tmp"
        exit 1
    fi
    mv "${dest}.tmp" "$dest"
done <<< "$parquet_files"

# Detect or auto-build density file
density_file="${output_dir}/density-overture.parquet"
if [ ! -f "$density_file" ]; then
    echo "Building Overture density table..."
    "${script_dir}/build-density.sh" overture "${cache_dir}" || { echo "Failed to build density table."; exit 1; }
    if [ ! -f "$density_file" ]; then
        echo "Density file not found after build: ${density_file}"
        exit 1
    fi
fi

# Detect or auto-build category IDF file
idf_file="${output_dir}/category_idf-overture.parquet"
if [ ! -f "$idf_file" ]; then
    echo "Building Overture IDF table..."
    "${script_dir}/build-idf.sh" overture "${cache_dir}" || { echo "Failed to build IDF table."; exit 1; }
    if [ ! -f "$idf_file" ]; then
        echo "IDF file not found after build: ${idf_file}"
        exit 1
    fi
fi

# Remove any existing temp file
rm -f "${output_dir}/${db_filename}.tmp"

# Take the first cached file to initialize the structure
first_file="${cache_dir}/$(basename "$(echo "$parquet_files" | head -1)")"

# Initialize the spatial extension and the places table
cat > "${output_dir}/import-overture.sql" <<EOF
.print "Initializing..."
SET memory_limit='48GB';
install spatial;
load spatial;

-- Create the places table
create table places as select * from '${first_file}' limit 0;
EOF

# Load the data from each parquet file into the places table
file_number=0

while IFS= read -r file; do
  file_number=$((file_number + 1))
  local_file="${cache_dir}/$(basename "$file")"

  cat <<EOF
.print "Importing file ${file_number}/${file_count}: $(basename "$file")"
insert into places select * from '${local_file}'
    where bbox.xmin >= ${xmin}
      and bbox.xmax <= ${xmax}
      and bbox.ymin >= ${ymin}
      and bbox.ymax <= ${ymax};
EOF
done <<< "$parquet_files" >> "${output_dir}/import-overture.sql"

# Clean up and create spatial index
cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Cleaning up..."
delete from places where geometry is null;

.print "Creating spatial index..."
create index places_rtree on places using rtree (geometry);
create index idx_id on places(id);
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
            (p.bbox.xmin + p.bbox.xmax) / 2.0,
            (p.bbox.ymin + p.bbox.ymax) / 2.0
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

# Build name_index with trigrams in batches to avoid OOM on large datasets
cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Creating name index (batched)..."
CREATE TABLE name_index (trigram VARCHAR, id VARCHAR, name VARCHAR, norm_name VARCHAR, importance INTEGER);
EOF

for batch_start in $(seq 0 5000000 80000000); do
    batch_end=$((batch_start + 5000000))
    cat >> "${output_dir}/import-overture.sql" <<EOF
.print "  name_index batch rowid ${batch_start}–${batch_end}..."
INSERT INTO name_index
SELECT
    substr(np.norm_name, pos, 3) AS trigram,
    np.id,
    np.name,
    np.norm_name,
    np.importance
FROM (
    SELECT
        id,
        names.primary AS name,
        lower(strip_accents(names.primary)) AS norm_name,
        coalesce(importance, 0) AS importance
    FROM places
    WHERE names.primary IS NOT NULL
      AND length(names.primary) >= 3
      AND rowid >= ${batch_start}
      AND rowid < ${batch_end}
) np
CROSS JOIN generate_series(1, length(np.norm_name) - 2) AS gs(pos);
EOF
done

cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Sorting name index by trigram..."
SET memory_limit='16GB';
CREATE TABLE name_index_sorted AS SELECT * FROM name_index ORDER BY trigram;
SET memory_limit='48GB';
DROP TABLE name_index;
ALTER TABLE name_index_sorted RENAME TO name_index;
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
