#!/bin/bash
# Downloads Geofabrik OSM PBF file with MD5 verification.
# Usage: download-osm.sh [--region <region>] [--cache-dir <dir>] [--log <path>]

set -euo pipefail

# Defaults
region="north-america"
script_dir="$(cd "$(dirname "$0")" && pwd)"
cache_dir="${script_dir}/../db/cache/osm"
log_file=""

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --region)
            region="$2"
            shift 2
            ;;
        --cache-dir)
            cache_dir="$2"
            shift 2
            ;;
        --log)
            log_file="$2"
            shift 2
            ;;
        --help)
            echo "Usage: download-osm.sh [--region <region>] [--cache-dir <dir>] [--log <path>]"
            echo ""
            echo "Downloads Geofabrik OSM PBF file with MD5 verification."
            echo ""
            echo "Arguments:"
            echo "  --region <region>    Geofabrik region slug (default: north-america)"
            echo "                       Examples: north-america, europe, north-america/us-northeast"
            echo "  --cache-dir <dir>    Cache directory (default: scripts/../db/cache/osm)"
            echo "  --log <path>         Log file path (redirect output)"
            echo "  --help               Print this help message and exit"
            echo ""
            echo "Example:"
            echo "  download-osm.sh --region north-america/us-northeast --log /tmp/osm-download.log"
            exit 0
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            echo "Run 'download-osm.sh --help' for usage." >&2
            exit 1
            ;;
    esac
done

# Set up logging if --log was provided
if [ -n "$log_file" ]; then
    exec > >(tee "$log_file") 2>&1
fi

# Construct URLs and paths
basename=$(basename "$region")  # Extract last component (e.g., "us-northeast" from "north-america/us-northeast")
pbf_filename="${basename}-latest.osm.pbf"
pbf_url="https://download.geofabrik.de/${region}-latest.osm.pbf"
md5_url="https://download.geofabrik.de/${region}-latest.osm.pbf.md5"
output_path="${cache_dir}/${pbf_filename}"
tmp_path="${output_path}.tmp"

# Create cache directory if it doesn't exist
mkdir -p "$cache_dir"

# Check if file already exists and verify MD5
if [ -f "$output_path" ]; then
    echo "Checking existing file: $output_path"

    # Try to download MD5 for verification
    if md5_temp=$(mktemp) && curl -sSf -o "$md5_temp" "$md5_url"; then
        expected_md5=$(cut -d' ' -f1 "$md5_temp")
        rm -f "$md5_temp"

        # Compute MD5 of existing file (cross-platform)
        if command -v md5sum >/dev/null 2>&1; then
            actual_md5=$(md5sum "$output_path" | cut -d' ' -f1)
        else
            actual_md5=$(md5 -q "$output_path")
        fi

        if [ "$expected_md5" = "$actual_md5" ]; then
            echo "MD5 verified. File already up to date."
            echo "$output_path"
            exit 0
        else
            echo "MD5 mismatch. Re-downloading..."
            rm -f "$output_path"
        fi
    else
        # MD5 not available, assume file is complete
        echo "$output_path"
        exit 0
    fi
fi

# Download MD5 file first
echo "Downloading MD5 from $md5_url"
if ! md5_temp=$(mktemp) || ! curl -sSf -o "$md5_temp" "$md5_url"; then
    echo "Error: Failed to download MD5 file" >&2
    exit 1
fi

expected_md5=$(cut -d' ' -f1 "$md5_temp")
rm -f "$md5_temp"

# Download PBF file
echo "Downloading $pbf_url"
if ! curl -Sf -o "$tmp_path" "$pbf_url"; then
    echo "Error: Failed to download PBF file" >&2
    rm -f "$tmp_path"
    exit 1
fi

# Verify MD5
echo "Verifying MD5 checksum..."
if command -v md5sum >/dev/null 2>&1; then
    actual_md5=$(md5sum "$tmp_path" | cut -d' ' -f1)
else
    actual_md5=$(md5 -q "$tmp_path")
fi

if [ "$expected_md5" != "$actual_md5" ]; then
    echo "Error: MD5 checksum mismatch" >&2
    echo "Expected: $expected_md5" >&2
    echo "Actual: $actual_md5" >&2
    rm -f "$tmp_path"
    exit 1
fi

# Atomic rename to final path
mv "$tmp_path" "$output_path"

echo "MD5 verified."
echo "$output_path"
