#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# import-wof-boundaries.sh
#
# Downloads (if needed) the Geocode Earth Who's on First admin SQLite database
# and builds a DuckDB boundary database with R-tree indexed polygon geometry.
#
# Usage: import-wof-boundaries.sh [options]
#   --source <path>      Use a pre-downloaded WoF SQLite (skip download)
#   --output-dir <dir>   Output directory (default: scripts/../db)
#   --log <path>         Log output to file (tee)
# ---------------------------------------------------------------------------

# --- Dependency checks ---

if ! command -v duckdb &> /dev/null; then
    echo "Error: duckdb not installed."
    echo "Install: curl https://install.duckdb.org/ | sh"
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo "Error: curl not found."
    exit 1
fi

# --- Parse arguments ---

source_db=""
log_file=""
script_dir="$(dirname "$(realpath "$0")")"
output_dir="${script_dir}/../db"

while [[ $# -gt 0 ]]; do
    case $1 in
        --source)
            source_db="$2"
            shift 2
            ;;
        --output-dir)
            output_dir="$2"
            shift 2
            ;;
        --log)
            log_file="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--source <sqlite_path>] [--output-dir <dir>] [--log <path>]"
            exit 1
            ;;
    esac
done

if [ -n "$log_file" ]; then
    mkdir -p "$(dirname "$log_file")"
    exec > >(tee "$log_file") 2>&1
fi

mkdir -p "$output_dir"

# --- Download WoF admin SQLite if no source provided ---

WOF_DIST_URL="https://data.geocode.earth/wof/dist/sqlite"
WOF_LATEST_FILE="whosonfirst-data-admin-latest.db.bz2"

if [ -z "$source_db" ]; then
    cache_dir="${output_dir}/cache/wof"
    mkdir -p "$cache_dir"

    db_name="${WOF_LATEST_FILE%.bz2}"
    cached_db="${cache_dir}/${db_name}"

    if [ -f "$cached_db" ]; then
        echo "Using cached: $cached_db"
    else
        bz2_path="${cache_dir}/${WOF_LATEST_FILE}"
        if [ ! -f "$bz2_path" ]; then
            echo "Downloading: ${WOF_DIST_URL}/${WOF_LATEST_FILE}"
            curl -L -o "$bz2_path" "${WOF_DIST_URL}/${WOF_LATEST_FILE}"
        fi
        echo "Decompressing..."
        bunzip2 -k "$bz2_path"
    fi

    source_db="$cached_db"
fi

if [ ! -f "$source_db" ]; then
    echo "Error: Source database not found: $source_db"
    exit 1
fi

echo "Source SQLite: $source_db"

# --- Build DuckDB boundary database ---

output_db="${output_dir}/wof-boundaries.duckdb"
output_db_tmp="${output_db}.tmp"

# Remove any previous temp file
rm -f "$output_db_tmp"

echo "Building boundary database..."

duckdb -bail "$output_db_tmp" <<EOSQL
INSTALL sqlite;
LOAD sqlite;
INSTALL spatial;
LOAD spatial;

ATTACH '${source_db}' AS wof (TYPE sqlite, READ_ONLY);

-- Placetype level mapping
CREATE TABLE placetype_levels AS
SELECT * FROM (VALUES
    ('continent', 0), ('ocean', 0),
    ('empire', 5),
    ('country', 10),
    ('dependency', 15), ('disputed', 15),
    ('macroregion', 20), ('marinearea', 20),
    ('region', 25),
    ('macrocounty', 30),
    ('county', 35),
    ('metroarea', 40),
    ('localadmin', 45),
    ('locality', 50),
    ('borough', 55),
    ('macrohood', 60),
    ('neighbourhood', 65),
    ('microhood', 70),
    ('campus', 75)
) AS t(placetype, level);

-- Stage 1: Build boundaries table from WoF SPR + GeoJSON
SELECT printf('[%s] Stage 1: Building boundaries table...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
CREATE TABLE boundaries AS
WITH staged AS (
    SELECT
        s.id AS wof_id,
        s.id::VARCHAR AS rkey,
        s.name,
        s.placetype,
        pl.level,
        s.latitude,
        s.longitude,
        ST_GeomFromGeoJSON(json_extract(g.body, '\$.geometry')) AS geom,
        s.country,
        s.min_latitude,
        s.min_longitude,
        s.max_latitude,
        s.max_longitude
    FROM wof.spr s
    JOIN wof.geojson g ON s.id = g.id AND g.is_alt != 1
    JOIN placetype_levels pl ON pl.placetype = s.placetype
    WHERE s.is_current != 0
      AND s.is_deprecated != 1
      AND s.latitude IS NOT NULL
      AND s.longitude IS NOT NULL
      AND s.name IS NOT NULL
      AND g.body IS NOT NULL
)
-- Exclude point geometries (no containment value)
SELECT * FROM staged
WHERE ST_GeometryType(geom) != 'POINT'
-- Hilbert-sort clusters spatially adjacent boundaries into the same row groups,
-- making DuckDB zone maps effective for the bbox BETWEEN pre-filter in compute_containment.
ORDER BY ST_Hilbert(geom, {'min_x': -180.0, 'min_y': -90.0, 'max_x': 180.0, 'max_y': 90.0}::BOX_2D);

SELECT printf('[%s] Stage 1 complete.', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
SELECT count(*) AS boundary_count FROM boundaries;

-- Stage 2: Denormalize multilingual names
SELECT printf('[%s] Stage 2: Adding multilingual names...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
ALTER TABLE boundaries ADD COLUMN names_json VARCHAR;

UPDATE boundaries b
SET names_json = n.names_json
FROM (
    SELECT
        nm.id,
        json_group_array(
            json_object(
                'name', nm.name,
                'language', nm.language,
                'variant', nm.privateuse
            )
        )::VARCHAR AS names_json
    FROM wof.names nm
    WHERE nm.privateuse IN ('preferred', 'variant')
      AND nm.name IS NOT NULL
      AND length(nm.name) >= 1
      AND EXISTS (SELECT 1 FROM boundaries b2 WHERE b2.wof_id = nm.id)
    GROUP BY nm.id
) n
WHERE b.wof_id = n.id;

SELECT printf('[%s] Stage 2 complete.', strftime(now(), '%Y-%m-%dT%H:%M:%S'));

-- Stage 3: Add concordances (cross-references to external datasets)
SELECT printf('[%s] Stage 3: Adding concordances...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
ALTER TABLE boundaries ADD COLUMN concordances VARCHAR;

UPDATE boundaries b
SET concordances = c.concordances_json
FROM (
    SELECT
        cc.id,
        json_group_object(cc.other_source, cc.other_id::VARCHAR)::VARCHAR AS concordances_json
    FROM wof.concordances cc
    WHERE cc.other_source IN ('wk:id', 'gn:id', 'gp:id', 'wd:id',
                               'fips:code', 'iso:id', 'unlc:id')
      AND EXISTS (SELECT 1 FROM boundaries b2 WHERE b2.wof_id = cc.id)
    GROUP BY cc.id
) c
WHERE b.wof_id = c.id;

SELECT printf('[%s] Stage 3 complete.', strftime(now(), '%Y-%m-%dT%H:%M:%S'));

-- Stage 4: Create indexes and finalize
SELECT printf('[%s] Stage 4: Creating indexes...', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
CREATE INDEX boundaries_rtree ON boundaries USING RTREE (geom);
CREATE INDEX idx_rkey ON boundaries(rkey);

DROP TABLE placetype_levels;

ANALYZE;
SELECT printf('[%s] Import complete.', strftime(now(), '%Y-%m-%dT%H:%M:%S'));
EOSQL

# Atomic swap
mv "$output_db_tmp" "$output_db"

echo "Done: $output_db"
duckdb "$output_db" -c "SELECT count(*) AS boundaries FROM boundaries;"
