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
    echo "Usage: $0 <source> [source_path_or_release]"
    echo
    echo "  source: fsq or overture"
    echo "  source_path_or_release: optional local directory path or release version string"
    echo
    exit 1
fi

source="$1"
source_arg="$2"

output_dir="$(dirname "$(realpath "$0")")/../db"
mkdir -p "$output_dir"

version="$(date +%Y-%m)"

if [ "$source" = "fsq" ]; then
    output_file="${output_dir}/density-fsq-${version}.parquet"
    output_symlink="${output_dir}/density-fsq.parquet"
else
    output_file="${output_dir}/density-overture-${version}.parquet"
    output_symlink="${output_dir}/density-overture.parquet"
fi

output_file_tmp="${output_file}.tmp"
sql_file="${output_dir}/build-density.sql"

# Determine the read_parquet(...) expression based on source and argument
if [ "$source" = "fsq" ]; then
    if [[ -d "$source_arg" ]]; then
        # Local directory path
        fsq_parquet_expr="read_parquet('${source_arg}/places-*.zstd.parquet')"
        use_httpfs=false
    else
        # Release version or auto-discover
        if [ -n "$source_arg" ]; then
            release="$source_arg"
        else
            # Auto-discover latest release from S3
            echo "Auto-discovering latest FSQ release..."
            release=$(curl -s "https://fsq-os-places-us-east-1.s3.amazonaws.com/" |
              grep -o "<Key>release/dt=[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}/</Key>" |
              sed 's/<Key>release\/dt=\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\)\/<\/Key>/\1/g' |
              sort -r |
              head -1)
            if [ -z "$release" ]; then
                echo "No FSQ releases found."
                exit 1
            fi
        fi
        echo "Using FSQ release: $release"
        # Build list of 100 S3 URLs
        url_list=""
        for i in $(seq -w 0 99); do
            url="'https://fsq-os-places-us-east-1.s3.amazonaws.com/release/dt=${release}/places/parquet/places-${i}.zstd.parquet'"
            if [ -z "$url_list" ]; then
                url_list="$url"
            else
                url_list="${url_list}, ${url}"
            fi
        done
        fsq_parquet_expr="read_parquet([${url_list}])"
        use_httpfs=true
    fi
else
    # Overture mode
    if [ -n "$source_arg" ]; then
        release="$source_arg"
    else
        release="2025-03-19.1"
    fi
    echo "Using Overture release: $release"
    overture_parquet_expr="read_parquet('https://overturemaps-us-west-2.s3.amazonaws.com/release/${release}/theme=places/type=place/*.parquet')"
    use_httpfs=true
fi

# Generate the SQL file
cat > "${sql_file}" <<EOF
INSTALL geography FROM community;
LOAD geography;
EOF

if [ "$use_httpfs" = "true" ]; then
    cat >> "${sql_file}" <<EOF
INSTALL httpfs;
LOAD httpfs;
EOF
fi

cat >> "${sql_file}" <<EOF
CREATE TABLE cell_counts (
    level    TINYINT NOT NULL,
    cell_id  UBIGINT NOT NULL,
    pt_count UBIGINT NOT NULL
);
EOF

if [ "$source" = "fsq" ]; then
    cat >> "${sql_file}" <<EOF
.print "Aggregating level 14 from FSQ..."
INSERT INTO cell_counts
SELECT
    14 AS level,
    s2_cell_parent(s2_cellfromlonlat(longitude, latitude), 14) AS cell_id,
    count(*) AS pt_count
FROM ${fsq_parquet_expr}
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
            (bbox.xmin + bbox.xmax) / 2.0,
            (bbox.ymin + bbox.ymax) / 2.0
        ), 14
    ) AS cell_id,
    count(*) AS pt_count
FROM ${overture_parquet_expr}
WHERE bbox IS NOT NULL
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
