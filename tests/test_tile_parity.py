"""Unit + integration tests for scripts/tile_parity.py."""
import gzip
import json
import os
import subprocess
import sys

import duckdb

# Import the script from scripts/ (not an installed package).
_SCRIPTS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

import tile_parity  # noqa: E402

_SCRIPT_PATH = os.path.join(_SCRIPTS, "tile_parity.py")


# ---------------------------------------------------------------------------
# Helpers to write fake tiles / manifests
# ---------------------------------------------------------------------------

def _write_tile(tiles_dir, qk, records, attribution="attr", collection="col"):
    """Write a .json.gz tile at <tiles_dir>/<qk[:6]>/<qk>.json.gz."""
    sub = os.path.join(tiles_dir, qk[:6])
    os.makedirs(sub, exist_ok=True)
    path = os.path.join(sub, qk + ".json.gz")
    obj = {"attribution": attribution, "collection": collection, "records": records}
    with gzip.open(path, "wt") as f:
        json.dump(obj, f)
    return path


def _write_manifest(d, source="foursquare", generated_at="2026-01-01T00:00:00+00:00",
                    quadkeys=("023130",)):
    path = os.path.join(d, "manifest.json")
    with open(path, "w") as f:
        json.dump({"source": source, "generated_at": generated_at,
                   "quadkeys": list(quadkeys)}, f, indent=2)
    return path


def _write_manifest_db(d, rows, source="foursquare",
                       generated_at="2026-01-01T00:00:00+00:00"):
    """rows: list of (rkey, tile_qk)."""
    path = os.path.join(d, "manifest.duckdb")
    con = duckdb.connect(path)
    try:
        con.execute("CREATE TABLE record_tiles (rkey VARCHAR, tile_qk VARCHAR)")
        con.executemany("INSERT INTO record_tiles VALUES (?, ?)", rows)
        con.execute("CREATE TABLE metadata (source VARCHAR, generated_at VARCHAR)")
        con.execute("INSERT INTO metadata VALUES (?, ?)", [source, generated_at])
    finally:
        con.close()
    return path


def _mkdir(p):
    p.mkdir(parents=True, exist_ok=True)
    return p


def _build_tiles_dir(base, tiles, manifest_qks, db_rows):
    """Build a full tiles_dir: tiles + manifest.json + manifest.duckdb.

    tiles: list of (qk, records). db_rows: list of (rkey, tile_qk).
    """
    os.makedirs(base, exist_ok=True)
    for qk, records in tiles:
        _write_tile(base, qk, records)
    _write_manifest(base, quadkeys=manifest_qks)
    _write_manifest_db(base, db_rows)
    return base


def _run_cli(*args):
    """Run tile_parity.py as a subprocess; return CompletedProcess."""
    return subprocess.run(
        [sys.executable, _SCRIPT_PATH, *args],
        capture_output=True, text=True, timeout=120,
    )


# ---------------------------------------------------------------------------
# Task 1: canonical_tile
# ---------------------------------------------------------------------------

def test_canonical_tile_equal_when_records_reordered(tmp_path):
    a = _write_tile(str(tmp_path / "a"), "023130",
                    [{"rkey": "r1", "name": "one"}, {"rkey": "r2", "name": "two"}])
    b = _write_tile(str(tmp_path / "b"), "023130",
                    [{"rkey": "r2", "name": "two"}, {"rkey": "r1", "name": "one"}])
    assert tile_parity.canonical_tile(a) == tile_parity.canonical_tile(b)


def test_canonical_tile_equal_when_keys_reordered(tmp_path):
    a = _write_tile(str(tmp_path / "a"), "023130",
                    [{"rkey": "r1", "name": "one", "cat": "x"}])
    b = _write_tile(str(tmp_path / "b"), "023130",
                    [{"cat": "x", "rkey": "r1", "name": "one"}])
    assert tile_parity.canonical_tile(a) == tile_parity.canonical_tile(b)


def test_canonical_tile_differs_on_value_change(tmp_path):
    a = _write_tile(str(tmp_path / "a"), "023130", [{"rkey": "r1", "name": "one"}])
    b = _write_tile(str(tmp_path / "b"), "023130", [{"rkey": "r1", "name": "ONE"}])
    assert tile_parity.canonical_tile(a) != tile_parity.canonical_tile(b)


def test_canonical_tile_no_records_key(tmp_path):
    # A tile with no "records" key must still canonicalize without error.
    sub = tmp_path / "no_records" / "023130"
    sub.mkdir(parents=True)
    path = str(sub / "023130.json.gz")
    with gzip.open(path, "wt") as f:
        json.dump({"attribution": "a", "collection": "c"}, f)
    out = tile_parity.canonical_tile(path)
    assert json.loads(out) == {"attribution": "a", "collection": "c"}


# ---------------------------------------------------------------------------
# Task 1: canonical_manifest
# ---------------------------------------------------------------------------

def test_canonical_manifest_strips_generated_at(tmp_path):
    da = _mkdir(tmp_path / "a")
    db = _mkdir(tmp_path / "b")
    a = _write_manifest(str(da), generated_at="2026-01-01T00:00:00+00:00")
    b = _write_manifest(str(db), generated_at="2099-12-31T23:59:59+00:00")
    assert tile_parity.canonical_manifest(a) == tile_parity.canonical_manifest(b)
    assert "generated_at" not in tile_parity.canonical_manifest(a)


def test_canonical_manifest_differs_on_quadkeys(tmp_path):
    a = _write_manifest(str(_mkdir(tmp_path / "a")), quadkeys=("023130",))
    b = _write_manifest(str(_mkdir(tmp_path / "b")), quadkeys=("023131",))
    assert tile_parity.canonical_manifest(a) != tile_parity.canonical_manifest(b)


# ---------------------------------------------------------------------------
# Task 4: end-to-end CLI (subprocess) integration
# ---------------------------------------------------------------------------

def test_cli_diff_identical_exits_zero(tmp_path):
    common = dict(
        tiles=[("023130", [{"rkey": "r1", "name": "one"}]),
               ("023131", [{"rkey": "r2", "name": "two"}])],
        manifest_qks=("023130", "023131"),
        db_rows=[("r1", "023130"), ("r2", "023131")],
    )
    src_a = str(tmp_path / "a_tiles")
    src_b = str(tmp_path / "b_tiles")
    _build_tiles_dir(src_a, **common)
    _build_tiles_dir(src_b, **common)

    cap_a = str(tmp_path / "a_cap")
    cap_b = str(tmp_path / "b_cap")
    assert _run_cli("capture", src_a, cap_a).returncode == 0
    assert _run_cli("capture", src_b, cap_b).returncode == 0

    res = _run_cli("diff", cap_a, cap_b)
    assert res.returncode == 0, f"stdout={res.stdout} stderr={res.stderr}"
    assert "OK: no differences" in res.stdout


def test_cli_diff_difference_exits_nonzero(tmp_path):
    src_a = str(tmp_path / "a_tiles")
    src_b = str(tmp_path / "b_tiles")
    _build_tiles_dir(src_a,
                     tiles=[("023130", [{"rkey": "r1", "name": "one"}])],
                     manifest_qks=("023130",), db_rows=[("r1", "023130")])
    _build_tiles_dir(src_b,
                     tiles=[("023130", [{"rkey": "r1", "name": "CHANGED"}])],
                     manifest_qks=("023130",), db_rows=[("r1", "023130")])

    cap_a = str(tmp_path / "a_cap")
    cap_b = str(tmp_path / "b_cap")
    assert _run_cli("capture", src_a, cap_a).returncode == 0
    assert _run_cli("capture", src_b, cap_b).returncode == 0

    res = _run_cli("diff", cap_a, cap_b)
    assert res.returncode == 1, f"stdout={res.stdout} stderr={res.stderr}"
    assert "023130" in res.stdout
    assert "r1" in res.stdout
