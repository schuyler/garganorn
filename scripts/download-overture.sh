#!/bin/bash

# Parse arguments
release=""
cache_dir=""
log_file=""
remaining_args=()

while [ $# -gt 0 ]; do
    case "$1" in
        --release) release="$2"; shift 2 ;;
        --cache-dir) cache_dir="$2"; shift 2 ;;
        --log) log_file="$2"; shift 2 ;;
        --help)
            echo "Usage: $0 [--release YYYY-MM-DD.N] [--cache-dir <dir>] [--log <path>]"
            echo
            echo "Downloads Overture Maps places and divisions parquet files to the local cache."
            echo
            echo "Options:"
            echo "  --release YYYY-MM-DD.N  Specific Overture release (default: auto-discover latest)"
            echo "  --cache-dir <dir>       Override default cache directory"
            echo "  --log <path>            Path to log file"
            echo "  --help                  Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Run '$0 --help' for usage information." >&2
            exit 1
            ;;
    esac
done

# Set up logging
if [ -n "$log_file" ]; then
    mkdir -p "$(dirname "$log_file")"
    exec > >(tee "$log_file") 2>&1
fi

# S3 base URL
source_base="https://overturemaps-us-west-2.s3.amazonaws.com"

# Determine release
if [ -n "$release" ]; then
    latest_release=$release
    echo "Using specified Overture release: $latest_release"
else
    echo "Auto-discovering latest Overture release..."
    latest_release=$(curl -s "${source_base}/?list-type=2&prefix=release/&delimiter=/" |
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

# Determine cache directory
if [ -n "$cache_dir" ]; then
    cache_dir="${cache_dir}/${latest_release}"
else
    # Create the db/cache directory in the parent folder relative to this script
    script_dir="$(dirname "$(realpath "$0")")"
    output_dir="${script_dir}/../db"
    cache_dir="${output_dir}/cache/overture/${latest_release}"
fi

mkdir -p "$cache_dir"

# Download places parquet files
echo "Finding available place parquet files..."
places_files=$(curl -s "${source_base}/?list-type=2&prefix=release/${latest_release}/theme=places/type=place/" |
  grep -o ">[^<]*part-[0-9]*-[^<]*.parquet<" |
  sed 's/>\(.*\)</\1/g' |
  sort)

if [ -z "$places_files" ]; then
    echo "No parquet files found for places. Please check the release date and URL format."
    exit 1
fi

# Count already-cached places files
places_file_count=$(echo "$places_files" | wc -l | tr -d ' ')
places_cached_count=0
while IFS= read -r file; do
    filename=$(basename "$file")
    if [ -f "${cache_dir}/places/${filename}" ]; then
        places_cached_count=$((places_cached_count + 1))
    fi
done <<< "$places_files"

# Download missing places files
places_dl_count=0
echo "Downloading places parquet files..."
mkdir -p "${cache_dir}/places"
while IFS= read -r file; do
    filename=$(basename "$file")
    dest="${cache_dir}/places/${filename}"
    if [ -f "$dest" ]; then
        continue
    fi
    places_dl_count=$((places_dl_count + 1))
    echo "Downloading ${places_dl_count} / $((places_file_count - places_cached_count)) (cached: ${places_cached_count}): ${filename}"
    if ! curl -sf -o "${dest}.tmp" "${source_base}/${file}"; then
        echo "Failed to download ${source_base}/${file}"
        rm -f "${dest}.tmp"
        exit 1
    fi
    mv "${dest}.tmp" "$dest"
done <<< "$places_files"

# Download division and division_area parquet files
for type_name in division division_area; do
    type="type=${type_name}"
    type_dir="${cache_dir}/${type_name}"
    mkdir -p "$type_dir"

    echo "Finding available ${type_name} parquet files..."
    divisions_files=$(curl -s "${source_base}/?list-type=2&prefix=release/${latest_release}/theme=divisions/${type}/" |
      grep -o ">[^<]*part-[0-9]*-[^<]*.parquet<" |
      sed 's/>\(.*\)</\1/g' |
      sort)

    if [ -z "$divisions_files" ]; then
        echo "No parquet files found for ${type_name}"
        continue
    fi

    # Count already-cached files
    divisions_file_count=$(echo "$divisions_files" | wc -l | tr -d ' ')
    divisions_cached_count=0
    while IFS= read -r file; do
        filename=$(basename "$file")
        if [ -f "${type_dir}/${filename}" ]; then
            divisions_cached_count=$((divisions_cached_count + 1))
        fi
    done <<< "$divisions_files"

    # Download missing files
    divisions_dl_count=0
    echo "Downloading ${type_name} parquet files..."
    while IFS= read -r file; do
        filename=$(basename "$file")
        dest="${type_dir}/${filename}"
        if [ -f "$dest" ]; then
            continue
        fi
        divisions_dl_count=$((divisions_dl_count + 1))
        echo "Downloading ${divisions_dl_count} / $((divisions_file_count - divisions_cached_count)) (cached: ${divisions_cached_count}): ${type_name}/${filename}"
        if ! curl -sf -o "${dest}.tmp" "${source_base}/${file}"; then
            echo "Failed to download ${source_base}/${file}"
            rm -f "${dest}.tmp"
            exit 1
        fi
        mv "${dest}.tmp" "$dest"
    done <<< "$divisions_files"
done

echo "Done."
