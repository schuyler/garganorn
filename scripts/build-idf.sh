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

if [ "$1" != "fsq" ] && [ "$1" != "overture" ] && [ "$1" != "osm" ]; then
    echo
    echo "Usage: $0 <source> [source_path_or_release]"
    echo
    echo "  source: fsq, overture, or osm"
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
    output_file="${output_dir}/category_idf-fsq-${version}.parquet"
    output_symlink="${output_dir}/category_idf-fsq.parquet"
elif [ "$source" = "overture" ]; then
    output_file="${output_dir}/category_idf-overture-${version}.parquet"
    output_symlink="${output_dir}/category_idf-overture.parquet"
else
    output_file="${output_dir}/category_idf-osm-${version}.parquet"
    output_symlink="${output_dir}/category_idf-osm.parquet"
fi

output_file_tmp="${output_file}.tmp"
sql_file="${output_dir}/build-idf.sql"

# Determine the read_parquet(...) expression based on source and argument
if [ "$source" = "fsq" ]; then
    if [[ -d "$source_arg" ]]; then
        # Local directory path
        parquet_expr="read_parquet('${source_arg}/places-*.zstd.parquet')"
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
        for i in $(seq 0 99); do
            url="'https://fsq-os-places-us-east-1.s3.amazonaws.com/release/dt=${release}/places/parquet/places-$(printf '%05d' $i).zstd.parquet'"
            if [ -z "$url_list" ]; then
                url_list="$url"
            else
                url_list="${url_list}, ${url}"
            fi
        done
        parquet_expr="read_parquet([${url_list}])"
        use_httpfs=true
    fi
elif [ "$source" = "osm" ]; then
    if [ -z "$source_arg" ]; then
        echo "Error: OSM source requires a path to the stage 1 GeoParquet file."
        echo "Usage: $0 osm <geoparquet_path>"
        exit 1
    fi
    if [ ! -f "$source_arg" ]; then
        echo "Error: GeoParquet file not found: $source_arg"
        exit 1
    fi
    parquet_expr="read_parquet('${source_arg}')"
    use_httpfs=false
else
    # Overture mode
    if [ -n "$source_arg" ]; then
        release="$source_arg"
    else
        release="2025-03-19.1"
    fi
    echo "Using Overture release: $release"
    parquet_expr="read_parquet('https://overturemaps-us-west-2.s3.amazonaws.com/release/${release}/theme=places/type=place/*.parquet')"
    use_httpfs=true
fi

# Generate the SQL file
if [ "$use_httpfs" = "true" ]; then
    cat > "${sql_file}" <<EOF
INSTALL httpfs;
LOAD httpfs;
EOF
else
    > "${sql_file}"
fi

cat >> "${sql_file}" <<EOF
CREATE TABLE category_idf (
    category   VARCHAR NOT NULL,
    n_places   UBIGINT NOT NULL,
    idf_score  DOUBLE  NOT NULL
);
EOF

if [ "$source" = "fsq" ]; then
    cat >> "${sql_file}" <<EOF
.print "Computing IDF from FSQ..."
INSERT INTO category_idf
SELECT
    category,
    count(*) AS n_places,
    ln(N.total::double / count(*)::double) AS idf_score
FROM (
    SELECT unnest(fsq_category_ids) AS category
    FROM ${parquet_expr}
    WHERE fsq_category_ids IS NOT NULL
) cats
CROSS JOIN (
    SELECT count(*) AS total FROM ${parquet_expr}
) N
GROUP BY category, N.total;
EOF
elif [ "$source" = "osm" ]; then
    cat >> "${sql_file}" <<EOF
.print "Computing IDF from OSM..."
INSERT INTO category_idf
SELECT
    category,
    count(*) AS n_places,
    ln(N.total::double / count(*)::double) AS idf_score
FROM (
    SELECT
        CASE
            WHEN tags['amenity'][1] IS NOT NULL THEN 'amenity=' || tags['amenity'][1]
            WHEN tags['shop'][1] IS NOT NULL THEN 'shop=' || tags['shop'][1]
            WHEN tags['tourism'][1] IS NOT NULL THEN 'tourism=' || tags['tourism'][1]
            WHEN tags['leisure'][1] IS NOT NULL THEN 'leisure=' || tags['leisure'][1]
            WHEN tags['office'][1] IS NOT NULL THEN 'office=' || tags['office'][1]
            WHEN tags['craft'][1] IS NOT NULL THEN 'craft=' || tags['craft'][1]
            WHEN tags['healthcare'][1] IS NOT NULL THEN 'healthcare=' || tags['healthcare'][1]
            WHEN tags['historic'][1] IS NOT NULL THEN 'historic=' || tags['historic'][1]
            WHEN tags['natural'][1] IS NOT NULL THEN 'natural=' || tags['natural'][1]
            WHEN tags['man_made'][1] IS NOT NULL THEN 'man_made=' || tags['man_made'][1]
            WHEN tags['aeroway'][1] IS NOT NULL THEN 'aeroway=' || tags['aeroway'][1]
            WHEN tags['railway'][1] IS NOT NULL THEN 'railway=' || tags['railway'][1]
            WHEN tags['public_transport'][1] IS NOT NULL THEN 'public_transport=' || tags['public_transport'][1]
            WHEN tags['place'][1] IS NOT NULL THEN 'place=' || tags['place'][1]
        END AS category
    FROM ${parquet_expr}
) cats
CROSS JOIN (
    SELECT count(*) AS total FROM ${parquet_expr}
) N
WHERE category IS NOT NULL
GROUP BY category, N.total;
EOF
else
    cat >> "${sql_file}" <<EOF
.print "Computing IDF from Overture..."
INSERT INTO category_idf
SELECT
    categories.primary AS category,
    count(*) AS n_places,
    ln(N.total::double / count(*)::double) AS idf_score
FROM ${parquet_expr}
CROSS JOIN (
    SELECT count(*) AS total FROM ${parquet_expr}
    WHERE categories.primary IS NOT NULL
) N
WHERE categories.primary IS NOT NULL
GROUP BY categories.primary, N.total;
EOF
fi

cat >> "${sql_file}" <<EOF
.print "Exporting to parquet..."
COPY (
    SELECT * FROM category_idf ORDER BY category
) TO '${output_file_tmp}' (FORMAT PARQUET);
EOF

rm -f "$output_file_tmp"

echo
time duckdb -bail -c ".read ${sql_file}"

if [ $? -ne 0 ]; then
    echo "Failed to build category IDF table."
    rm -f "$output_file_tmp"
    rm -f "${sql_file}"
    exit 1
fi

mv "$output_file_tmp" "$output_file"
ln -sf "$(basename "${output_file}")" "${output_symlink}"
rm -f "${sql_file}"

echo
echo "Wrote ${output_file}"
echo "Symlink: ${output_symlink} -> $(basename "${output_file}")"
