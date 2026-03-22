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

if [ "$1" != "fsq" ] && [ "$1" != "overture" ] && [ "$1" != "all" ]; then
    echo
    echo "Usage: $0 <source>"
    echo
    echo "  source: fsq, overture, or all"
    echo
    exit 1
fi

output_dir="$(dirname "$(realpath "$0")")/../db"
mkdir -p "$output_dir"

fsq_db="${output_dir}/fsq-osp.duckdb"
overture_db="${output_dir}/overture-maps.duckdb"

if [ "$1" = "fsq" ] || [ "$1" = "all" ]; then
    if [ ! -f "$fsq_db" ]; then
        echo "Source database not found: ${fsq_db}"
        echo "Please run the appropriate import script first."
        exit 1
    fi
fi

if [ "$1" = "overture" ] || [ "$1" = "all" ]; then
    if [ ! -f "$overture_db" ]; then
        echo "Source database not found: ${overture_db}"
        echo "Please run the appropriate import script first."
        exit 1
    fi
fi

version="$(date +%Y-%m)"
output_file="${output_dir}/category_idf-${version}.parquet"
output_file_tmp="${output_file}.tmp"
output_symlink="${output_dir}/category_idf.parquet"
sql_file="${output_dir}/build-idf.sql"

# Generate the SQL file
cat > "${sql_file}" <<EOF
CREATE TABLE category_idf (
    collection VARCHAR NOT NULL,
    category   VARCHAR NOT NULL,
    n_places   UBIGINT NOT NULL,
    idf_score  DOUBLE  NOT NULL
);
EOF

if [ "$1" = "fsq" ] || [ "$1" = "all" ]; then
    cat >> "${sql_file}" <<EOF
ATTACH '${fsq_db}' AS fsq_src (READ_ONLY);
.print "Computing IDF from FSQ..."
INSERT INTO category_idf
SELECT
    'foursquare' AS collection,
    category,
    count(*) AS n_places,
    ln(N.total::double / count(*)::double) AS idf_score
FROM (
    SELECT unnest(fsq_category_ids) AS category
    FROM fsq_src.places
    WHERE fsq_category_ids IS NOT NULL
) cats
CROSS JOIN (
    SELECT count(*) AS total FROM fsq_src.places
) N
GROUP BY category, N.total;
EOF
fi

if [ "$1" = "overture" ] || [ "$1" = "all" ]; then
    cat >> "${sql_file}" <<EOF
ATTACH '${overture_db}' AS ov_src (READ_ONLY);
.print "Computing IDF from Overture..."
INSERT INTO category_idf
SELECT
    'overture' AS collection,
    categories.primary AS category,
    count(*) AS n_places,
    ln(N.total::double / count(*)::double) AS idf_score
FROM ov_src.places
CROSS JOIN (
    SELECT count(*) AS total FROM ov_src.places
    WHERE categories.primary IS NOT NULL
) N
WHERE categories.primary IS NOT NULL
GROUP BY categories.primary, N.total;
EOF
fi

cat >> "${sql_file}" <<EOF
.print "Exporting to parquet..."
COPY (
    SELECT * FROM category_idf ORDER BY collection, category
) TO '${output_file_tmp}' (FORMAT PARQUET);
EOF

rm -f "$output_file_tmp"

echo
time duckdb -bail < "${sql_file}"

if [ $? -ne 0 ]; then
    echo "Failed to build category IDF table."
    rm -f "$output_file_tmp"
    exit 1
fi

mv "$output_file_tmp" "$output_file"
ln -sf "$(basename "${output_file}")" "${output_symlink}"
rm -f "${sql_file}"

echo
echo "Wrote ${output_file}"
echo "Symlink: ${output_symlink} -> $(basename "${output_file}")"
