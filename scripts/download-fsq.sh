#!/bin/bash

# Usage: download-fsq.sh [--cache-dir <dir>] [--log <path>]
#
# Downloads latest Foursquare Places parquet files to db/cache/fsq/{release}/
# Auto-discovers latest release from S3. Skips existing files (idempotent).
#
# Options:
#   --cache-dir <dir>   Override default cache directory (must be absolute path)
#   --log <path>        Path to log file; output is tee'd to log file and stdout
#   --help              Print this usage message and exit

# Print usage and exit
usage() {
    echo "Usage: $0 [--cache-dir <dir>] [--log <path>]"
    echo
    echo "Downloads latest Foursquare Places parquet files to cache."
    echo "Auto-discovers latest release from S3. Skips existing files."
    echo
    echo "Options:"
    echo "  --cache-dir <dir>   Override default cache directory (must be absolute path)"
    echo "  --log <path>        Path to log file; output is tee'd to log file and stdout"
    echo "  --help              Print this usage message and exit"
    echo
    echo "Default cache directory: <script_parent>/db/cache/fsq/{release}/"
    exit 0
}

# Parse arguments
cache_dir=""
log_file=""
remaining_args=()

while [ $# -gt 0 ]; do
    case "$1" in
        --help) usage ;;
        --cache-dir)
            cache_dir="$2"
            shift 2
            ;;
        --log)
            log_file="$2"
            shift 2
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            echo "Run '$0 --help' for usage." >&2
            exit 1
            ;;
    esac
done

# Set up logging if --log is provided
if [ -n "$log_file" ]; then
    mkdir -p "$(dirname "$log_file")"
    exec > >(tee "$log_file") 2>&1
fi

# Determine script directory and default cache base
script_dir="$(dirname "$(realpath "$0")")"
cache_base="${script_dir}/../db/cache/fsq"

# Auto-discover latest FSQ release from S3
# The release values are in the format: "<Key>release/dt=2025-03-06/</Key>"
latest_release=$(curl -s "https://fsq-os-places-us-east-1.s3.amazonaws.com/" |
  grep -o "<Key>release/dt=[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}/</Key>" |
  sed 's/<Key>release\/dt=\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\)\/<\/Key>/\1/g' |
  sort -r |
  head -1)

if [ -z "$latest_release" ]; then
    echo "Error: No releases found on S3."
    exit 1
fi

# Use custom cache_dir if provided, otherwise construct from release
if [ -n "$cache_dir" ]; then
    cache_dir="${cache_dir}/${latest_release}"
else
    cache_dir="${cache_base}/${latest_release}"
fi

mkdir -p "$cache_dir"
echo "Using latest release: $latest_release"
echo "Cache directory: $cache_dir"

# Download files places-00000.zstd.parquet through places-00099.zstd.parquet
total_files=100
downloaded=0
cached_count=0

for i in $(seq 0 99); do
    filename="places-$(printf '%05d' $i).zstd.parquet"
    dest="${cache_dir}/${filename}"
    url="https://fsq-os-places-us-east-1.s3.amazonaws.com/release/dt=${latest_release}/places/parquet/${filename}"

    if [ -f "$dest" ]; then
        cached_count=$((cached_count + 1))
        continue
    fi

    downloaded=$((downloaded + 1))
    echo "Downloading ${downloaded}/${total_files} (cached: ${cached_count})"

    # Atomic download: write to .tmp, then rename on success
    if ! curl -sf -o "${dest}.tmp" "$url"; then
        echo "Error: Failed to download ${url}"
        rm -f "${dest}.tmp"
        exit 1
    fi

    mv "${dest}.tmp" "$dest"
done

echo "Done. Downloaded ${downloaded} files, ${cached_count} already cached."
