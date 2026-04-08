#!/bin/bash
# tests/test_extract_osm_parquet.sh
# Shell test harness for scripts/extract-osm-parquet.sh
#
# Run: bash tests/test_extract_osm_parquet.sh
# Exit code: 0 if all tests pass, non-zero if any fail.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SCRIPT="${REPO_ROOT}/scripts/extract-osm-parquet.sh"

# ─── MANUAL REVIEW REQUIRED ──────────────────────────────────────────────────
# The implementation must guard rm -rf with a non-empty path check before
# removing the parquet directory during cache invalidation or cleanup of a
# prior crashed run.  Specifically, verify that $parquet_dir is:
#   1. Non-empty (never rm -rf "" or rm -rf "/")
#   2. A subdirectory of $cache_dir (never escape the cache root)
# This cannot be exercised solely through black-box testing; confirm by code
# review of scripts/extract-osm-parquet.sh before merging.
# ─────────────────────────────────────────────────────────────────────────────

# ─── Test harness ────────────────────────────────────────────────────────────

PASS=0
FAIL=0
FAILURES=()

pass() { echo "PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL + 1)); FAILURES+=("$1"); }

assert_output_contains() {
    local label="$1"
    local pattern="$2"
    local actual="$3"
    if echo "$actual" | grep -qF "$pattern"; then
        pass "$label"
    else
        fail "$label (expected output containing '${pattern}', got: ${actual})"
    fi
}

assert_file_exists() {
    local label="$1"
    local path="$2"
    if [ -e "$path" ]; then
        pass "$label"
    else
        fail "$label (expected file to exist: $path)"
    fi
}

# ─── Temp directory management ───────────────────────────────────────────────

TMPROOT=""
setup_tmpdir() {
    TMPROOT="$(mktemp -d)"
}
teardown_tmpdir() {
    [ -n "$TMPROOT" ] && rm -rf "$TMPROOT"
    TMPROOT=""
}

# Helper: run the script under test with a controlled PATH and capture both
# output and exit code.
#   run_script <extra_bin_dir> <args...>
# Sets globals: RUN_OUTPUT, RUN_EXIT
run_script() {
    local extra_bin_dir="$1"; shift
    # Use a constrained PATH: mock bin_dir first, then only essential system
    # directories. This prevents tests 3/4 (missing dependency checks) from
    # being fooled by system-installed osmium or osm-pbf-parquet.
    # bash, mkdir, touch, mktemp, grep, wc, tr, rm, mv, date, printf are
    # all available in /usr/bin or /bin on macOS and Linux.
    local output exit_code
    output=$(PATH="${extra_bin_dir}:/usr/bin:/bin" bash "$SCRIPT" "$@" 2>&1)
    exit_code=$?
    RUN_OUTPUT="$output"
    RUN_EXIT="$exit_code"
}

# ─── Mock tool builder ───────────────────────────────────────────────────────

# write_mock_osmium <bin_dir> <call_log> [exit_code]
#   Creates a mock osmium that:
#     - appends its invocation to call_log
#     - on success (exit_code=0): creates the -o <output_path> file
#     - exits with exit_code
write_mock_osmium() {
    local bin_dir="$1"
    local call_log="$2"
    local exit_code="${3:-0}"
    cat > "${bin_dir}/osmium" <<MOCK
#!/bin/bash
echo "osmium \$*" >> "${call_log}"
if [ "${exit_code}" -eq 0 ]; then
    output_path=""
    args=("\$@")
    for (( i=0; i<\${#args[@]}; i++ )); do
        if [ "\${args[i]}" = "-o" ]; then
            output_path="\${args[i+1]}"
            break
        fi
    done
    if [ -n "\$output_path" ]; then
        mkdir -p "\$(dirname "\$output_path")"
        touch "\$output_path"
    fi
fi
exit ${exit_code}
MOCK
    chmod +x "${bin_dir}/osmium"
}

# write_mock_osm_pbf_parquet <bin_dir> <call_log> [exit_code]
#   Creates a mock osm-pbf-parquet that:
#     - appends its invocation to call_log
#     - on success: creates output directory structure with sentinel and dummy files
#     - exits with exit_code
write_mock_osm_pbf_parquet() {
    local bin_dir="$1"
    local call_log="$2"
    local exit_code="${3:-0}"
    cat > "${bin_dir}/osm-pbf-parquet" <<MOCK
#!/bin/bash
echo "osm-pbf-parquet \$*" >> "${call_log}"
if [ "${exit_code}" -eq 0 ]; then
    output_dir=""
    args=("\$@")
    for (( i=0; i<\${#args[@]}; i++ )); do
        if [ "\${args[i]}" = "--output" ]; then
            output_dir="\${args[i+1]}"
            break
        fi
    done
    if [ -n "\$output_dir" ]; then
        mkdir -p "\${output_dir}/type=node"
        mkdir -p "\${output_dir}/type=way"
        touch "\${output_dir}/type=node/data.parquet"
        touch "\${output_dir}/type=way/data.parquet"
        touch "\${output_dir}/.complete"
    fi
fi
exit ${exit_code}
MOCK
    chmod +x "${bin_dir}/osm-pbf-parquet"
}

# ─── Tests ───────────────────────────────────────────────────────────────────

# Test 1: Missing required arg (no pbf_path) → exits non-zero with usage message
test_missing_pbf_arg() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    mkdir -p "$bin_dir"
    write_mock_osmium "$bin_dir" "$call_log"
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    run_script "$bin_dir"
    if [ "$RUN_EXIT" -eq 0 ]; then
        fail "missing-pbf-arg: expected non-zero exit, got 0"
    else
        pass "missing-pbf-arg: exits non-zero"
    fi
    assert_output_contains "missing-pbf-arg: usage in output" "Usage" "$RUN_OUTPUT"

    teardown_tmpdir
}

# Test 2: Non-existent PBF file → exits non-zero with error message
test_nonexistent_pbf() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    mkdir -p "$bin_dir"
    write_mock_osmium "$bin_dir" "$call_log"
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    run_script "$bin_dir" /nonexistent/does-not-exist.osm.pbf
    if [ "$RUN_EXIT" -eq 0 ]; then
        fail "nonexistent-pbf: expected non-zero exit, got 0"
    else
        pass "nonexistent-pbf: exits non-zero"
    fi
    assert_output_contains "nonexistent-pbf: error message in output" "not found" "$RUN_OUTPUT"

    teardown_tmpdir
}

# Test 3: Missing dependency osmium → exits non-zero with error mentioning osmium
test_missing_osmium() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    mkdir -p "$bin_dir"
    # Provide only osm-pbf-parquet, not osmium
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    local fake_pbf="${TMPROOT}/input.osm.pbf"
    touch "$fake_pbf"

    run_script "$bin_dir" "$fake_pbf"
    if [ "$RUN_EXIT" -eq 0 ]; then
        fail "missing-osmium: expected non-zero exit, got 0"
    else
        pass "missing-osmium: exits non-zero"
    fi
    assert_output_contains "missing-osmium: error message mentions osmium" "osmium" "$RUN_OUTPUT"

    teardown_tmpdir
}

# Test 4: Missing dependency osm-pbf-parquet → exits non-zero with error mentioning osm-pbf-parquet
test_missing_osm_pbf_parquet() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    mkdir -p "$bin_dir"
    # Provide only osmium, not osm-pbf-parquet
    write_mock_osmium "$bin_dir" "$call_log"

    local fake_pbf="${TMPROOT}/input.osm.pbf"
    touch "$fake_pbf"

    run_script "$bin_dir" "$fake_pbf"
    if [ "$RUN_EXIT" -eq 0 ]; then
        fail "missing-osm-pbf-parquet: expected non-zero exit, got 0"
    else
        pass "missing-osm-pbf-parquet: exits non-zero"
    fi
    assert_output_contains "missing-osm-pbf-parquet: error message mentions tool" "osm-pbf-parquet" "$RUN_OUTPUT"

    teardown_tmpdir
}

# Test 5: Successful run — verify output structure exists
test_successful_run() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    local cache_dir="${TMPROOT}/cache"
    mkdir -p "$bin_dir"
    write_mock_osmium "$bin_dir" "$call_log"
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    local fake_pbf="${TMPROOT}/input.osm.pbf"
    touch "$fake_pbf"

    run_script "$bin_dir" "$fake_pbf" --cache-dir "$cache_dir"
    if [ "$RUN_EXIT" -ne 0 ]; then
        fail "successful-run: expected exit 0, got $RUN_EXIT (output: $RUN_OUTPUT)"
    else
        pass "successful-run: exits zero"
    fi
    assert_file_exists "successful-run: filtered.osm.pbf exists"   "${cache_dir}/filtered.osm.pbf"
    assert_file_exists "successful-run: parquet/.complete exists"   "${cache_dir}/parquet/.complete"
    assert_file_exists "successful-run: parquet/type=node/ exists" "${cache_dir}/parquet/type=node"
    assert_file_exists "successful-run: parquet/type=way/ exists"  "${cache_dir}/parquet/type=way"
    # Verify parquet/ is a real directory — not a symlink or a plain file.
    if [ -d "${cache_dir}/parquet" ] && [ ! -L "${cache_dir}/parquet" ]; then
        pass "successful-run: parquet/ is a real directory (not symlink)"
    else
        fail "successful-run: parquet/ should be a real directory (not symlink or file)"
    fi

    teardown_tmpdir
}

# Test 6: Caching — second run with valid cache skips both tools
test_caching_skips_tools() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    local cache_dir="${TMPROOT}/cache"
    mkdir -p "$bin_dir"
    write_mock_osmium "$bin_dir" "$call_log"
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    local fake_pbf="${TMPROOT}/input.osm.pbf"
    touch "$fake_pbf"

    # First run — populates cache
    run_script "$bin_dir" "$fake_pbf" --cache-dir "$cache_dir"

    # Make the input PBF appear older than the cache by backdating it
    touch -t 202001010000 "$fake_pbf"
    # Ensure cache files are newer (current time is already newer than 2020)

    # Reset call log before second run
    rm -f "$call_log"

    # Second run — cache should be valid, tools should not be called
    run_script "$bin_dir" "$fake_pbf" --cache-dir "$cache_dir"
    if [ "$RUN_EXIT" -ne 0 ]; then
        fail "caching: second run should exit 0, got $RUN_EXIT (output: $RUN_OUTPUT)"
    else
        pass "caching: second run exits zero"
    fi

    local tool_calls=0
    if [ -f "$call_log" ]; then
        tool_calls=$(wc -l < "$call_log" | tr -d ' ')
    fi
    if [ "$tool_calls" -eq 0 ]; then
        pass "caching: no tools called on second run"
    else
        fail "caching: expected 0 tool calls on second run, got $tool_calls"
    fi

    teardown_tmpdir
}

# Test 7: Cache invalidation — input PBF newer than filtered.osm.pbf → osmium re-runs
test_cache_invalidation() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    local cache_dir="${TMPROOT}/cache"
    mkdir -p "$bin_dir" "$cache_dir"
    write_mock_osmium "$bin_dir" "$call_log"
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    local fake_pbf="${TMPROOT}/input.osm.pbf"

    # Create a stale filtered.osm.pbf (backdated to year 2020)
    touch "${cache_dir}/filtered.osm.pbf"
    touch -t 202001010000 "${cache_dir}/filtered.osm.pbf"

    # Input PBF is newer (current time)
    touch "$fake_pbf"

    run_script "$bin_dir" "$fake_pbf" --cache-dir "$cache_dir"

    local osmium_calls=0
    if [ -f "$call_log" ]; then
        osmium_calls=$(grep -c "^osmium " "$call_log" 2>/dev/null || echo 0)
    fi
    if [ "$osmium_calls" -ge 1 ]; then
        pass "cache-invalidation: osmium called when cache is stale"
    else
        fail "cache-invalidation: osmium should have been called (calls: $osmium_calls)"
    fi

    local parquet_calls=0
    if [ -f "$call_log" ]; then
        parquet_calls=$(grep -c "^osm-pbf-parquet " "$call_log" 2>/dev/null || echo 0)
    fi
    if [ "$parquet_calls" -ge 1 ]; then
        pass "cache-invalidation: osm-pbf-parquet called when cache is stale"
    else
        fail "cache-invalidation: osm-pbf-parquet should have been called (calls: $parquet_calls)"
    fi

    assert_file_exists "cache-invalidation: parquet/.complete exists after re-run" \
        "${cache_dir}/parquet/.complete"

    teardown_tmpdir
}

# Test 8: --log flag — output is also written to specified log file
test_log_flag() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    local cache_dir="${TMPROOT}/cache"
    local log_file="${TMPROOT}/run.log"
    mkdir -p "$bin_dir"
    write_mock_osmium "$bin_dir" "$call_log"
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    local fake_pbf="${TMPROOT}/input.osm.pbf"
    touch "$fake_pbf"

    run_script "$bin_dir" "$fake_pbf" --cache-dir "$cache_dir" --log "$log_file"

    assert_file_exists "log-flag: log file created" "$log_file"
    if [ -f "$log_file" ] && [ -s "$log_file" ]; then
        pass "log-flag: log file is non-empty"
    else
        fail "log-flag: log file should be non-empty"
    fi

    teardown_tmpdir
}

# Test 9: --cache-dir flag — output goes to the specified directory
test_cache_dir_flag() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    local custom_cache="${TMPROOT}/my-custom-cache"
    mkdir -p "$bin_dir"
    write_mock_osmium "$bin_dir" "$call_log"
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    local fake_pbf="${TMPROOT}/input.osm.pbf"
    touch "$fake_pbf"

    run_script "$bin_dir" "$fake_pbf" --cache-dir "$custom_cache"

    assert_file_exists "cache-dir-flag: filtered.osm.pbf in custom dir" "${custom_cache}/filtered.osm.pbf"
    assert_file_exists "cache-dir-flag: parquet/.complete in custom dir" "${custom_cache}/parquet/.complete"

    teardown_tmpdir
}

# Test 10: Leftover parquet.tmp/ from a prior crashed run is cleaned up
test_leftover_parquet_tmp() {
    setup_tmpdir
    local bin_dir="${TMPROOT}/bin"
    local call_log="${TMPROOT}/calls.log"
    local cache_dir="${TMPROOT}/cache"
    mkdir -p "$bin_dir"
    write_mock_osmium "$bin_dir" "$call_log"
    write_mock_osm_pbf_parquet "$bin_dir" "$call_log"

    local fake_pbf="${TMPROOT}/input.osm.pbf"
    touch "$fake_pbf"

    # Simulate a leftover parquet.tmp/ from a previous crashed run
    local leftover="${cache_dir}/parquet.tmp"
    mkdir -p "${leftover}/type=node"
    touch "${leftover}/type=node/crash-debris.parquet"
    touch "${leftover}/orphan-file"

    run_script "$bin_dir" "$fake_pbf" --cache-dir "$cache_dir"
    if [ "$RUN_EXIT" -ne 0 ]; then
        fail "leftover-parquet-tmp: expected exit 0, got $RUN_EXIT (output: $RUN_OUTPUT)"
    else
        pass "leftover-parquet-tmp: exits zero despite leftover parquet.tmp/"
    fi

    assert_file_exists "leftover-parquet-tmp: filtered.osm.pbf exists"   "${cache_dir}/filtered.osm.pbf"
    assert_file_exists "leftover-parquet-tmp: parquet/.complete exists"   "${cache_dir}/parquet/.complete"
    assert_file_exists "leftover-parquet-tmp: parquet/type=node/ exists" "${cache_dir}/parquet/type=node"
    assert_file_exists "leftover-parquet-tmp: parquet/type=way/ exists"  "${cache_dir}/parquet/type=way"

    if [ ! -e "$leftover" ]; then
        pass "leftover-parquet-tmp: parquet.tmp/ removed after successful run"
    else
        fail "leftover-parquet-tmp: parquet.tmp/ should have been removed (still exists: $leftover)"
    fi

    if [ ! -f "${cache_dir}/parquet/type=node/crash-debris.parquet" ]; then
        pass "leftover-parquet-tmp: crash debris not carried into final parquet/"
    else
        fail "leftover-parquet-tmp: crash debris from prior run must not appear in final output"
    fi

    teardown_tmpdir
}

# ─── Run all tests ────────────────────────────────────────────────────────────

echo "Running tests for scripts/extract-osm-parquet.sh"
echo "Script path: ${SCRIPT}"
echo

test_missing_pbf_arg
test_nonexistent_pbf
test_missing_osmium
test_missing_osm_pbf_parquet
test_successful_run
test_caching_skips_tools
test_cache_invalidation
test_log_flag
test_cache_dir_flag
test_leftover_parquet_tmp

echo
echo "Results: ${PASS} passed, ${FAIL} failed"

if [ "${FAIL}" -gt 0 ]; then
    echo
    echo "Failed tests:"
    for t in "${FAILURES[@]}"; do
        echo "  - $t"
    done
    exit 1
fi
exit 0
