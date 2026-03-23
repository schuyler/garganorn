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

if [ "$1" != "fsq" ] && [ "$1" != "overture" ]; then
    echo
    echo "Usage: $0 <source>"
    echo
    echo "  source: fsq or overture"
    echo
    exit 1
fi

output_dir="$(dirname "$(realpath "$0")")/../db"
mkdir -p "$output_dir"

if [ "$1" = "fsq" ]; then
    source_db="${output_dir}/fsq-osp.duckdb"
else
    source_db="${output_dir}/overture-maps.duckdb"
fi

if [ ! -f "$source_db" ]; then
    echo "Source database not found: ${source_db}"
    echo "Please run the appropriate import script first."
    exit 1
fi

version="$(date +%Y-%m)"
output_file="${output_dir}/density-${version}.parquet"
output_file_tmp="${output_file}.tmp"
output_symlink="${output_dir}/density.parquet"
sql_file="${output_dir}/build-density.sql"

# Generate the SQL file
cat > "${sql_file}" <<EOF
INSTALL geography FROM community;
LOAD geography;
EOF

if [ "$1" = "overture" ]; then
    cat >> "${sql_file}" <<EOF
INSTALL spatial;
LOAD spatial;
EOF
fi

cat >> "${sql_file}" <<EOF
ATTACH '${source_db}' AS src (READ_ONLY);
CREATE TABLE cell_counts (
    level    TINYINT NOT NULL,
    cell_id  UBIGINT NOT NULL,
    pt_count UBIGINT NOT NULL
);
EOF

if [ "$1" = "fsq" ]; then
    cat >> "${sql_file}" <<EOF
.print "Aggregating level 14 from FSQ..."
INSERT INTO cell_counts
SELECT
    14 AS level,
    s2_cell_parent(s2_cellfromlonlat(longitude, latitude), 14) AS cell_id,
    count(*) AS pt_count
FROM src.places
WHERE longitude != 0 AND latitude != 0
GROUP BY cell_id;
EOF
else
    cat >> "${sql_file}" <<EOF
.print "Aggregating level 14 from Overture..."
INSERT INTO cell_counts
SELECT
    14 AS level,
    s2_cell_parent(
        s2_cellfromlonlat(
            st_x(st_centroid(geometry)),
            st_y(st_centroid(geometry))
        ), 14
    ) AS cell_id,
    count(*) AS pt_count
FROM src.places
WHERE geometry IS NOT NULL
GROUP BY cell_id;
EOF
fi

for child_level in $(seq 14 -1 7); do
    parent_level=$((child_level - 1))
    cat <<EOF
.print "Cascading level ${child_level} -> ${parent_level}..."
INSERT INTO cell_counts
SELECT
    ${parent_level} AS level,
    s2_cell_parent(cell_id::S2_CELL, ${parent_level}) AS cell_id,
    sum(pt_count) AS pt_count
FROM cell_counts
WHERE level = ${child_level}
GROUP BY cell_id;
EOF
done >> "${sql_file}"

cat >> "${sql_file}" <<EOF
.print "Exporting to parquet..."
COPY (
    SELECT * FROM cell_counts ORDER BY level, cell_id
) TO '${output_file_tmp}' (FORMAT PARQUET);
EOF

rm -f "$output_file_tmp"

echo
time duckdb -bail -c ".read ${sql_file}"

if [ $? -ne 0 ]; then
    echo "Failed to build density table."
    rm -f "$output_file_tmp"
    exit 1
fi

mv "$output_file_tmp" "$output_file"
ln -sf "$(basename "${output_file}")" "${output_symlink}"
rm -f "${sql_file}"

echo
echo "Wrote ${output_file}"
echo "Symlink: ${output_symlink} -> $(basename "${output_file}")"
