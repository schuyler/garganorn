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

# Extract --log option before positional parsing
log_file=""
remaining_args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --log) log_file="$2"; shift 2 ;;
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
release=$5

if [ -z "$xmin" ] || [ -z "$ymin" ] || [ -z "$xmax" ] || [ -z "$ymax" ]; then
    echo
    echo "Usage: $0 [--log <path>] <xmin> <ymin> <xmax> <ymax> [release]"
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

create index idx_id on places(id);
EOF

# Compute importance as normalized 0-100 integer score
# 60% window-function density (S2 level 12 cell count) + 40% category IDF
cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Loading geography extension for importance scoring..."
install geography from community;
load geography;
.print "Computing importance scores..."
CREATE TEMP TABLE t_idf AS
SELECT
    categories.primary AS category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM places
CROSS JOIN (
    SELECT count(*) AS total FROM places
    WHERE categories.primary IS NOT NULL
) N
WHERE categories.primary IS NOT NULL
GROUP BY categories.primary, N.total;
CREATE TEMP TABLE place_density AS
SELECT id,
       ln(1 + count(*) OVER (
           PARTITION BY s2_cell_parent(
               s2_cellfromlonlat(
                   (bbox.xmin + bbox.xmax) / 2.0,
                   (bbox.ymin + bbox.ymax) / 2.0
               ), 12)
       )) AS density_score
FROM places;
CREATE TEMP TABLE place_idf AS
SELECT
    p.id,
    coalesce(idf.idf_score, 0) AS idf_score
FROM places p
LEFT JOIN t_idf idf ON idf.category = p.categories.primary;
CREATE TABLE places_scored AS
SELECT p.*,
       round(
           60 * least(coalesce(d.density_score, 0) / 10.0, 1.0)
         + 40 * least(coalesce(i.idf_score, 0) / 18.0, 1.0)
       )::INTEGER AS importance
FROM places p
LEFT JOIN place_density d USING (id)
LEFT JOIN place_idf i USING (id);
DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
create index idx_id on places(id);
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_idf;
EOF

# Extract name variants
cat >> "${output_dir}/import-overture.sql" <<'EOF'
.print "Extracting name variants..."
CREATE TEMP TABLE overture_variants AS
WITH common_entries AS (
    SELECT id,
        e.key AS language,
        e."value" AS name
    FROM places,
         unnest(map_entries(names.common)) AS e
    WHERE names.common IS NOT NULL
),
rule_entries AS (
    SELECT id,
        r.language,
        r."value" AS name,
        CASE r.variant
            WHEN 'common'     THEN 'alternate'
            WHEN 'official'   THEN 'official'
            WHEN 'alternate'  THEN 'alternate'
            WHEN 'short'      THEN 'short'
            ELSE 'alternate'
        END AS type
    FROM places,
         unnest(names.rules) AS r
    WHERE names.rules IS NOT NULL
),
all_variants AS (
    SELECT id, name, 'alternate' AS type, language FROM common_entries
    UNION ALL
    SELECT id, name, type, language FROM rule_entries
)
SELECT id, list({'name': name, 'type': type, 'language': language}
                ORDER BY name) AS variants
FROM all_variants
WHERE name IS NOT NULL AND name != ''
GROUP BY id;

CREATE TABLE places_with_variants AS
SELECT p.*,
       coalesce(ov.variants, []) AS variants
FROM places p
LEFT JOIN overture_variants ov USING (id);
DROP TABLE places;
ALTER TABLE places_with_variants RENAME TO places;
create index idx_id on places(id);
DROP TABLE overture_variants;
EOF

# Build name_index with trigrams in batches to avoid OOM on large datasets
cat >> "${output_dir}/import-overture.sql" <<EOF
.print "Creating name index (batched)..."
CREATE TABLE name_index (trigram VARCHAR, id VARCHAR, name VARCHAR, norm_name VARCHAR, importance INTEGER, is_variant BOOLEAN DEFAULT FALSE);
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
    np.importance,
    FALSE AS is_variant
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

cat >> "${output_dir}/import-overture.sql" <<'EOF'
.print "Indexing variant names..."
INSERT INTO name_index
WITH variant_names AS (
    SELECT id,
           v.name,
           lower(strip_accents(v.name)) AS norm_name,
           importance,
           TRUE AS is_variant
    FROM places,
         unnest(variants) AS v
    WHERE v.name IS NOT NULL AND length(v.name) >= 3
)
SELECT substr(vn.norm_name, pos, 3) AS trigram,
       vn.id,
       vn.name,
       vn.norm_name,
       vn.importance,
       vn.is_variant
FROM variant_names vn
CROSS JOIN generate_series(1, length(vn.norm_name) - 2) AS gs(pos);
EOF

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
