#!/bin/bash
# scripts/extract-osm-parquet.sh
#
# Extract an OSM PBF file to parquet format using osmium + osm-pbf-parquet.
# Caches intermediate results in <cache_dir>/.
#
# Usage: extract-osm-parquet.sh <pbf_path> [options]
#   pbf_path              Required. Path to OSM PBF file.
#   --cache-dir <dir>     Default: <script_dir>/../db/cache/osm
#   --log <path>          Tee output to this file (standalone use only).

set -euo pipefail

# This script handles stages 0 and 1 of the OSM import pipeline: filtering a
# PBF file to place-relevant OSM tags using osmium, then converting the result
# to Parquet using osm-pbf-parquet. Both stages cache their output and skip
# processing when the cache is current. import-osm.sh calls this script
# automatically before building the DuckDB places table, but it can also be
# run standalone when only the Parquet output is needed (e.g., to feed the
# quadtree tile pipeline without running the full database import).

# ─── Dependency checks ────────────────────────────────────────────────────────

if ! command -v osmium &> /dev/null; then
    echo "osmium not installed. Please install it first."
    echo "To install osmium-tool:"
    echo "  brew install osmium-tool    # macOS"
    echo "  apt install osmium-tool     # Debian/Ubuntu"
    exit 1
fi

if ! command -v osm-pbf-parquet &> /dev/null; then
    echo "osm-pbf-parquet not installed. Please install it first."
    echo "To install osm-pbf-parquet, download the binary from:"
    echo "  https://github.com/OvertureMaps/osm-pbf-parquet/releases"
    echo "and place it on your PATH."
    exit 1
fi

# ─── Usage ────────────────────────────────────────────────────────────────────

usage() {
    echo
    echo "Usage: $0 <pbf_path> [options]"
    echo
    echo "  pbf_path              Required. Path to OSM PBF file."
    echo
    echo "  Options:"
    echo "    --cache-dir <dir>   Cache directory (default: <script_dir>/../db/cache/osm)"
    echo "    --log <path>        Tee output to this file (standalone use only)."
    echo
    exit 1
}

# ─── Argument parsing ─────────────────────────────────────────────────────────

if [ $# -lt 1 ]; then
    usage
fi

pbf_path="$1"
shift

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cache_dir="${script_dir}/../db/cache/osm"
log_file=""

while [ $# -gt 0 ]; do
    case "$1" in
        --cache-dir) cache_dir="$2"; shift 2 ;;
        --log)       log_file="$2";  shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# ─── Validation ───────────────────────────────────────────────────────────────

if [ ! -f "$pbf_path" ]; then
    echo "PBF file not found: $pbf_path"
    exit 1
fi

# ─── Log setup (must happen before any output we want captured) ───────────────

if [ -n "$log_file" ]; then
    mkdir -p "$(dirname "$log_file")"
    exec > >(tee "$log_file") 2>&1
fi

# ─── Setup ────────────────────────────────────────────────────────────────────

mkdir -p "$cache_dir"

# Resolve cache_dir to an absolute path for safety guards below
cache_dir="$(cd "$cache_dir" && pwd)"

stage_start=$SECONDS
elapsed() {
    local now=$SECONDS
    local dt=$((now - stage_start))
    stage_start=$now
    printf "  [%dm%02ds]\n" $((dt / 60)) $((dt % 60))
}

# ─── Safety guard for rm -rf ──────────────────────────────────────────────────
# Usage: safe_rm_rf <path>
# Validates that path is non-empty and starts with cache_dir before removing.
safe_rm_rf() {
    local target="$1"
    if [ -z "$target" ]; then
        echo "SAFETY: refusing rm -rf of empty path" >&2
        exit 1
    fi
    case "$target" in
        "${cache_dir}"/*)
            # target is a subdirectory of cache_dir — safe to remove
            ;;
        *)
            echo "SAFETY: refusing rm -rf '${target}' (not under cache_dir '${cache_dir}')" >&2
            exit 1
            ;;
    esac
    rm -rf "$target"
}

# ─── Stage 0: Filter PBF with osmium ─────────────────────────────────────────

filtered_pbf="${cache_dir}/filtered.osm.pbf"

if [ -f "$filtered_pbf" ] && [ ! "$pbf_path" -nt "$filtered_pbf" ]; then
    echo "Using cached filtered PBF: $filtered_pbf"
else
    echo "Filtering PBF with osmium tags-filter..."
    if ! osmium tags-filter "$pbf_path" \
        n/amenity n/shop n/tourism n/leisure n/office n/craft n/healthcare \
        n/historic n/natural n/man_made n/aeroway n/railway n/public_transport n/place \
        w/amenity w/shop w/tourism w/leisure w/office w/craft w/healthcare \
        w/historic w/natural w/man_made w/aeroway w/railway w/public_transport w/place \
        --overwrite \
        -o "$filtered_pbf"; then
        echo "osmium tags-filter failed."
        exit 1
    fi
fi

elapsed

# ─── Stage 1: Convert filtered PBF to Parquet ────────────────────────────────

parquet_dir="${cache_dir}/parquet"
parquet_tmp="${cache_dir}/parquet.tmp"

if [ -f "${parquet_dir}/.complete" ] && [ ! "$filtered_pbf" -nt "${parquet_dir}/.complete" ]; then
    echo "Using cached Parquet: $parquet_dir"
else
    echo "Converting PBF to Parquet with osm-pbf-parquet..."

    # Clean up any prior crashed run
    safe_rm_rf "$parquet_tmp"

    # Run conversion into tmp directory
    if ! osm-pbf-parquet --input "$filtered_pbf" --output "$parquet_tmp"; then
        echo "osm-pbf-parquet conversion failed."
        safe_rm_rf "$parquet_tmp"
        exit 1
    fi

    # Atomically promote tmp to final: remove old parquet/, rename tmp
    safe_rm_rf "$parquet_dir"
    mv "$parquet_tmp" "$parquet_dir"
    touch "${parquet_dir}/.complete"
fi

elapsed
