# Download Scripts and Import Cache Separation

## Motivation

The current import scripts (`import-fsq-extract.sh`, `import-overture-extract.sh`,
`import-osm.sh`) couple two concerns: downloading source data from remote locations
(S3, Geofabrik) and importing from local disk. This creates problems:

1. **No offline operation**: import scripts fail if network is unavailable or endpoints are down
2. **Network blocking**: large downloads block import script execution for extended periods
3. **No pre-staging**: users cannot pre-populate the local cache before a long import run
4. **No script reuse**: download logic cannot be invoked independently

**Rule: Never import directly from S3 parquet.** Local disk is fast and plentiful. Import
scripts must require a warm cache; they should not download. This task separates downloads
into standalone scripts and strips download logic from import scripts. Imports enforce cache
presence with clear error messages.

## Design Decisions

1. **Three download scripts, one per source**: Each script owns discovery (latest release),
   URL construction, and download sequencing for its source. Parallelizing downloads of
   different sources is straightforward; parallelizing within a source is left to callers.

2. **Idempotent downloads**: Skip files that already exist on disk. Allow resumption of
   interrupted downloads by checking file size or MD5 (OSM only).

3. **Atomic downloads**: Use `.tmp` suffix during download; rename on success. Failed
   downloads are cleaned up. Concurrent downloads to the same directory are safe.

4. **Release auto-discovery**: FSQ and Overture scripts query S3 XML listing to find the
   latest release. Users can optionally specify a release for Overture (FSQ always uses
   latest). OSM uses a Geofabrik region slug (no release versioning).

5. **Standard option format**: All scripts accept `--cache-dir` and `--log` for consistency.
   OSM also accepts `--region` for Geofabrik region selection.

6. **Progress reporting**: Scripts report `{downloaded}/{total to download} (cached: {already cached})`
   to show work in progress and cache effectiveness.

7. **Exit status**: Exit 1 on any failure (network error, checksum mismatch, permission denied).
   Success exits 0. OSM download script prints path to downloaded PBF on stdout for
   chaining with import script.

## New Scripts

### `scripts/download-fsq.sh`

Downloads latest Foursquare Places parquet files to `db/cache/fsq/{release}/`.

**Usage:**
```
download-fsq.sh [--cache-dir <dir>] [--log <path>]
```

**Arguments:**
- `--cache-dir`: Override default cache directory (`db/cache/fsq/`). Must be absolute.
- `--log`: Path to log file; output is tee'd to log file and stdout.

**Behavior:**
1. Query `https://fsq-os-places-us-east-1.s3.amazonaws.com/` XML listing for latest release
   (extract date from `<Key>release/dt=YYYY-MM-DD/</Key>`)
2. Create `${cache_dir}/${release}/` directory
3. Loop over `places-00000.zstd.parquet` to `places-00099.zstd.parquet`:
   - Check if file exists in cache (idempotent)
   - If not, download from S3 with atomic `.tmp` → final rename
   - Report progress as `{n}/{total_to_download} (cached: {cached_count})`
4. Exit 1 if any download fails; exit 0 on success

**Example:**
```bash
download-fsq.sh --log /tmp/fsq-download.log
# Output: Using latest release: 2026-04-10
#         Downloading 1 / 100 (cached: 0)
#         Downloading 2 / 100 (cached: 0)
#         ... (downloads 100 files)
#         Done.
```

### `scripts/download-overture.sh`

Downloads Overture Maps places and divisions parquet files to
`db/cache/overture/{release}/`.

**Cache layout:**
```
db/cache/overture/{release}/part-*.parquet                          # places files (flat)
db/cache/overture/{release}/divisions/{type}/part-*.parquet         # divisions files
```

Places files are stored flat in the release directory. Divisions files mirror the S3
path structure under a `divisions/` subdirectory (e.g.,
`divisions/division/part-*.parquet`, `divisions/division_area/part-*.parquet`).

The cache check in `import-overture-extract.sh` looks for `*.parquet` in the flat
`{release}/` directory, which matches only places files — divisions files are in
subdirectories and are not matched by that glob.

**Usage:**
```
download-overture.sh [--release YYYY-MM-DD.N] [--cache-dir <dir>] [--log <path>]
```

**Arguments:**
- `--release`: Specific Overture release (format: `YYYY-MM-DD.N`). If not provided,
  auto-discovers the latest release from S3 listing.
- `--cache-dir`: Override default cache directory (`db/cache/overture/`).
- `--log`: Path to log file.

**Behavior:**
1. If `--release` not provided, query S3 for latest release:
   `https://overturemaps-us-west-2.s3.amazonaws.com/?list-type=2&prefix=release/&delimiter=/`
2. Create `${cache_dir}/${release}/` directory
3. List all parquet files under `theme=places/type=place/`:
   `https://overturemaps-us-west-2.s3.amazonaws.com/?list-type=2&prefix=release/${release}/theme=places/type=place/`
   - Download all `part-*.parquet` files flat into `${cache_dir}/${release}/` (count may vary; typically 1–10)
4. List and download all parquet files under each `theme=divisions/type=*/`:
   - Query for available types (e.g., `division`, `division_area`)
   - Download all `part-*.parquet` from each type directory into `${cache_dir}/${release}/divisions/{type}/`
5. Exit 1 if any download fails; exit 0 on success

**Example:**
```bash
download-overture.sh --log /tmp/overture-download.log
# Output: Auto-discovering latest Overture release...
#         Using latest Overture release: 2026-03-28.0
#         Finding available place parquet files...
#         Downloading 1 / 5 (cached: 0): part-00000-abc123.parquet
#         Finding available divisions parquet files...
#         Downloading 1 / 12 (cached: 0): part-00000-xyz789.parquet
#         Done.
```

### `scripts/download-osm.sh`

Downloads Geofabrik OSM PBF file to `db/cache/osm/{region}-latest.osm.pbf` with MD5
verification.

**Usage:**
```
download-osm.sh [--region <region>] [--cache-dir <dir>] [--log <path>]
```

**Arguments:**
- `--region`: Geofabrik region slug (default: `north-america`). Examples:
  - `europe`
  - `north-america`
  - `north-america/us-northeast`
  - `asia/south-korea`
- `--cache-dir`: Override default cache directory (`db/cache/osm/`).
- `--log`: Path to log file.

**Behavior:**
1. Construct URL: `https://download.geofabrik.de/{region}-latest.osm.pbf`
2. Extract basename (e.g., `north-america-latest.osm.pbf`)
3. Check if file exists and is complete (see idempotency logic below)
4. Download `.osm.pbf.md5` file from same directory
5. Download `.osm.pbf` with atomic `.tmp` → final rename
6. Verify MD5 checksum; if mismatch, delete file and exit 1
7. Print path to downloaded PBF to stdout (for shell piping)
8. Exit 0 on success

**Idempotency logic:**
- If `.osm.pbf` exists: check if `.md5` file is available
  - If MD5 can be downloaded, verify existing file against it
    - Match: skip download, print path, exit 0
    - Mismatch: re-download and reverify
  - If MD5 cannot be downloaded (endpoint error): assume file is complete, print path, exit 0
- If `.osm.pbf` does not exist: download fresh

**Example:**
```bash
download-osm.sh --region north-america/us-northeast --log /tmp/osm-download.log
# Output: Downloading https://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf
#         ... (download progress if available)
#         MD5 verified.
#         /Users/sderle/code/atgeo/garganorn/db/cache/osm/us-northeast-latest.osm.pbf

# Chain with import script:
download-osm.sh --region north-america | xargs scripts/import-osm.sh
```

## Modified Scripts

### `scripts/import-fsq-extract.sh`

**New flag: `--cache-dir <path>`**

Add `--cache-dir` to the option-parsing loop (alongside `--log`). When provided,
the script skips S3 release discovery entirely and uses the given path as `cache_dir`
directly. When absent, the existing S3 discovery runs and `cache_dir` is derived from
the discovered release as before.

Updated arg parsing:
```bash
log_file=""
cache_dir=""
remaining_args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --log) log_file="$2"; shift 2 ;;
        --cache-dir) cache_dir="$2"; shift 2 ;;
        *) remaining_args+=("$1"); shift ;;
    esac
done
set -- "${remaining_args[@]}"
```

When `--cache-dir` is not provided, S3 discovery runs and sets `cache_dir`:
```bash
if [ -z "$cache_dir" ]; then
    latest_release=$(curl -s "https://fsq-os-places-us-east-1.s3.amazonaws.com/" | ...)
    if [ -z "$latest_release" ]; then
        echo "No releases found."
        exit 1
    fi
    echo "Using latest release: $latest_release"
    cache_dir="${output_dir}/cache/fsq/${latest_release}"
fi
```

Replace lines 85–110 (download loop) with cache presence check:

```bash
# Verify cache exists and is complete
if [ ! -d "$cache_dir" ] || [ -z "$(ls "$cache_dir"/*.parquet 2>/dev/null)" ]; then
    echo "Cache missing: $cache_dir"
    echo "Run download-fsq.sh first to populate the cache."
    exit 1
fi

# Count cached files (must be exactly 100 for FSQ)
cached_count=$(find "$cache_dir" -maxdepth 1 -name '*.parquet' -type f | wc -l)
if [ "$cached_count" -ne 100 ]; then
    echo "Incomplete FSQ cache: found $cached_count files, expected 100 in $cache_dir"
    echo "Run download-fsq.sh to complete the cache."
    exit 1
fi
```

**Rationale:** The 100-file count is a known invariant for FSQ releases. Checking it
catches incomplete or stale caches. The `ls` check is a quick sanity check; the count
check is strict. `--cache-dir` allows tests to inject a path without triggering S3
discovery.

### `scripts/import-overture-extract.sh`

**New flag: `--cache-dir <path>`**

Add `--cache-dir` to the option-parsing loop (alongside `--log`). When provided,
the script skips S3 release discovery entirely and uses the given path as `cache_dir`
directly. When absent, the existing S3 discovery runs and `cache_dir` is derived from
the discovered release as before.

Updated arg parsing:
```bash
log_file=""
cache_dir=""
remaining_args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --log) log_file="$2"; shift 2 ;;
        --cache-dir) cache_dir="$2"; shift 2 ;;
        *) remaining_args+=("$1"); shift ;;
    esac
done
set -- "${remaining_args[@]}"
```

When `--cache-dir` is not provided, S3 discovery runs and sets `cache_dir`:
```bash
if [ -z "$cache_dir" ]; then
    if [ -n "$release" ]; then
        latest_release=$release
        echo "Using specified Overture release: $latest_release"
    else
        echo "Auto-discovering latest Overture release..."
        latest_release=$(curl -s "https://overturemaps-us-west-2.s3.amazonaws.com/..." | ...)
        if [ -z "$latest_release" ]; then
            echo "No Overture releases found on S3."
            exit 1
        fi
        echo "Using latest Overture release: $latest_release"
    fi
    cache_dir="${output_dir}/cache/overture/${latest_release}"
fi
```

Replace lines 101–127 (download loop) with cache presence check:

```bash
# Verify cache exists and has parquet files
if [ ! -d "$cache_dir" ] || [ -z "$(ls "$cache_dir"/*.parquet 2>/dev/null)" ]; then
    echo "Cache missing: $cache_dir"
    echo "Run download-overture.sh first to populate the cache."
    exit 1
fi
```

**Rationale:** Overture has variable parquet file counts (places: 1–10, divisions: varies).
A simple existence check is sufficient; the import SQL will fail if expected columns are
missing, providing user feedback. No need for strict counting. `--cache-dir` allows tests
to inject a path without triggering S3 discovery.

## Test Plan

File: `tests/test_download_scripts.py` (pytest, subprocess-based)

### Test Cases

1. **`test_fsq_import_fails_without_cache`**
   - Run `import-fsq-extract.sh --cache-dir /tmp/nonexistent-garganorn-test <bbox>`
   - Assert exit code is 1
   - Assert combined output contains "Cache missing" and "download-fsq.sh"

2. **`test_overture_import_fails_without_cache`**
   - Run `import-overture-extract.sh --cache-dir /tmp/nonexistent-garganorn-test <bbox>`
   - Assert exit code is 1
   - Assert combined output contains "Cache missing" and "download-overture.sh"

3. **`test_fsq_import_fails_incomplete_cache`**
   - Create 50 parquet files directly in `tmp_path` (incomplete cache)
   - Run `import-fsq-extract.sh --cache-dir <tmp_path> <bbox>`
   - Assert exit code is 1
   - Assert combined output contains "Incomplete FSQ cache" and "50"

Note: `--cache-dir` bypasses S3 discovery entirely. When provided, the script uses
the given path as `cache_dir` without querying S3 for the latest release.

4. **`test_download_fsq_usage`**
   - Run `download-fsq.sh --help`
   - Assert exit code is 0 and output contains usage message

5. **`test_download_overture_usage`**
   - Run `download-overture.sh --help`
   - Assert exit code is 0 and output contains usage information

6. **`test_download_osm_usage`**
   - Run `download-osm.sh --help`
   - Assert exit code is 0 and output contains usage information

7. **`test_download_osm_unknown_option`**
   - Run `download-osm.sh --unknown-opt value`
   - Assert exit code is 1 and stderr mentions "Unknown option"

### Mocking and Fixtures

- Mock S3/Geofabrik endpoints with `monkeypatch` or `responses` library (HTTP mocking)
- Create temporary cache directories for each test
- Stub curl calls or use a local HTTP server for testing download logic
- Tests should not hit actual endpoints; use recorded responses or fixtures

### Coverage

- Cache presence/absence checks
- Option parsing (--cache-dir, --log, --region)
- Progress reporting output format
- Exit codes (0 success, 1 failure)
- MD5 verification (OSM only) — both pass and fail cases

## Critical Paths

### Cache Warm-up Workflow

```
download-fsq.sh                    (5–10 min, 49GB, 100 files)
download-overture.sh               (2–5 min, 5–10GB)
download-osm.sh --region REGION    (2–20 min, 1–5GB, region-dependent)
import-fsq-extract.sh ... < bbox >
import-overture-extract.sh ... < bbox >
import-osm.sh < pbf_path >
```

**Timeline**: User calls download scripts once; imports can be repeated with warm
cache in seconds (no network I/O). If a new release is available, re-run download
script for that source.

### Error Recovery

- **Download interrupted**: Re-run the download script. Existing files are skipped;
  missing files are re-downloaded.
- **Incomplete cache detected by import**: User sees message "Run download-X.sh first"
  and re-runs download script.
- **Network error**: Download script exits 1. User investigates endpoint availability
  and retries.

## Implementation Notes

### Curl Option Flags

- `-s`: silent (no progress meter)
- `-f`: fail on HTTP error
- `-o`: write to file
- `-S`: show errors even in silent mode
- Atomic rename: download to `.tmp`, then `mv` on success

### S3 XML Parsing

Both FSQ and Overture use S3 list-type=2 (v2 API) to avoid pagination complexity.
Response contains flat XML with `<Key>` elements. Use `grep -o` and `sed` to extract
paths and dates. Sort descending to find latest release.

### Geofabrik Quirks

- Region slug uses forward slashes (e.g., `asia/south-korea`)
- Filename constructed from slug with `/` → `-` substitution
  (e.g., `asia/south-korea` → `south-korea-latest.osm.pbf`)
- MD5 file is human-readable: `{md5hash}  {filename}` (hash first, two spaces, then filename); use `cut -d' ' -f1` to extract hash

### Log File Handling

All scripts use the pattern from existing imports:
```bash
exec > >(tee "$log_file") 2>&1
```

This tees stdout and stderr to both the log file and console, allowing real-time
feedback and persistent logs.

## Files to Create

| File | Purpose |
|------|---------|
| `scripts/download-fsq.sh` | Download latest FSQ parquet |
| `scripts/download-overture.sh` | Download latest Overture parquet |
| `scripts/download-osm.sh` | Download Geofabrik PBF with MD5 verification |
| `tests/test_download_scripts.py` | Pytest test suite for download scripts |

## Files to Modify

| File | Change |
|------|--------|
| `scripts/import-fsq-extract.sh` | Remove lines 85–110 (download loop); add cache check |
| `scripts/import-overture-extract.sh` | Remove lines 101–127 (download loop); add cache check |

## References

- Current FSQ release discovery: `/scripts/import-fsq-extract.sh:53–68`
- Current Overture discovery and download: `/scripts/import-overture-extract.sh:58–127`
- Atomic download pattern: existing imports use `.tmp` → final rename
