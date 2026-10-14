"""Unit + integration tests for scripts/tile_parity.py.

Tiles are `{publishedAt, attribution, records: [{value, importance}]}` and
manifests are keyed `record_tiles(key, tile_qk)`, so canonical_tile() strips
the tile-level `publishedAt` and sorts records by `value.origin`."""
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

def _record(key, name):
    return {"value": {"$type": "org.atgeo.place", "origin": f"urn:atgeo:{key}",
                      "name": name},
            "importance": 10}


def _write_tile(tiles_dir, qk, records, published_at="2026-07-09T18:00:00Z"):
    """Write a .json.gz tile at <tiles_dir>/<qk[:6]>/<qk>.json.gz."""
    sub = os.path.join(tiles_dir, qk[:6])
    os.makedirs(sub, exist_ok=True)
    path = os.path.join(sub, qk + ".json.gz")
    obj = {"publishedAt": published_at, "attribution": [], "records": records}
    with gzip.open(path, "wt") as f:
        json.dump(obj, f)
    return path


def _write_manifest(d, generated_at="2026-01-01T00:00:00+00:00", quadkeys=("023130",)):
    path = os.path.join(d, "manifest.json")
    with open(path, "w") as f:
        json.dump({"source": "overture_place", "generated_at": generated_at,
                   "quadkeys": list(quadkeys)}, f, indent=2)
    return path


def _write_manifest_db(d, rows):
    """rows: list of (key, tile_qk)."""
    path = os.path.join(d, "manifest.duckdb")
    con = duckdb.connect(path)
    try:
        con.execute("CREATE TABLE record_tiles (key VARCHAR, tile_qk VARCHAR)")
        con.executemany("INSERT INTO record_tiles VALUES (?, ?)", rows)
    finally:
        con.close()
    return path


def _mkdir(p):
    p.mkdir(parents=True, exist_ok=True)
    return p


def _build_tiles_dir(base, tiles, manifest_qks, db_rows):
    """Build a full tiles_dir: tiles + manifest.json + manifest.duckdb.

    tiles: list of (qk, records). db_rows: list of (key, tile_qk).
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
# canonical_tile
# ---------------------------------------------------------------------------

K1 = "overturemaps.org:place:r1"
K2 = "overturemaps.org:place:r2"


def test_canonical_tile_equal_when_records_reordered(tmp_path):
    a = _write_tile(str(tmp_path / "a"), "023130",
                    [_record(K1, "one"), _record(K2, "two")])
    b = _write_tile(str(tmp_path / "b"), "023130",
                    [_record(K2, "two"), _record(K1, "one")])
    assert tile_parity.canonical_tile(a) == tile_parity.canonical_tile(b)


def test_canonical_tile_equal_when_keys_reordered(tmp_path):
    a = _write_tile(str(tmp_path / "a"), "023130", [_record(K1, "one")])
    reordered = {"value": {"name": "one", "origin": f"urn:atgeo:{K1}",
                           "$type": "org.atgeo.place"},
                 "importance": 10}
    b = _write_tile(str(tmp_path / "b"), "023130", [reordered])
    assert tile_parity.canonical_tile(a) == tile_parity.canonical_tile(b)


def test_canonical_tile_differs_on_value_change(tmp_path):
    a = _write_tile(str(tmp_path / "a"), "023130", [_record(K1, "one")])
    b = _write_tile(str(tmp_path / "b"), "023130", [_record(K1, "ONE")])
    assert tile_parity.canonical_tile(a) != tile_parity.canonical_tile(b)


def test_canonical_tile_strips_published_at(tmp_path):
    """Two tiles identical except for the run-scoped publishedAt canonicalize equal."""
    a = _write_tile(str(tmp_path / "a"), "023130", [_record(K1, "one")],
                    published_at="2026-01-01T00:00:00Z")
    b = _write_tile(str(tmp_path / "b"), "023130", [_record(K1, "one")],
                    published_at="2099-12-31T23:59:59Z")
    assert tile_parity.canonical_tile(a) == tile_parity.canonical_tile(b)


def test_canonical_tile_no_records_key(tmp_path):
    # A tile with no "records" key must still canonicalize without error.
    sub = tmp_path / "no_records" / "023130"
    sub.mkdir(parents=True)
    path = str(sub / "023130.json.gz")
    with gzip.open(path, "wt") as f:
        json.dump({"attribution": []}, f)
    out = tile_parity.canonical_tile(path)
    assert json.loads(out) == {"attribution": []}


# ---------------------------------------------------------------------------
# canonical_manifest
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
# End-to-end CLI (subprocess) integration
# ---------------------------------------------------------------------------

def test_cli_diff_identical_exits_zero(tmp_path):
    """Runs differing only in publishedAt and record order diff as identical."""
    src_a = str(tmp_path / "a_tiles")
    src_b = str(tmp_path / "b_tiles")
    _write_tile(src_a, "023130", [_record(K1, "one"), _record(K2, "two")],
                published_at="2026-01-01T00:00:00Z")
    _write_tile(src_b, "023130", [_record(K2, "two"), _record(K1, "one")],
                published_at="2099-12-31T23:59:59Z")
    for src in (src_a, src_b):
        _write_manifest(src, quadkeys=("023130",))
        _write_manifest_db(src, [(K1, "023130"), (K2, "023130")])

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
                     tiles=[("023130", [_record(K1, "one")])],
                     manifest_qks=("023130",), db_rows=[(K1, "023130")])
    _build_tiles_dir(src_b,
                     tiles=[("023130", [_record(K1, "CHANGED")])],
                     manifest_qks=("023130",), db_rows=[(K1, "023130")])

    cap_a = str(tmp_path / "a_cap")
    cap_b = str(tmp_path / "b_cap")
    assert _run_cli("capture", src_a, cap_a).returncode == 0
    assert _run_cli("capture", src_b, cap_b).returncode == 0

    res = _run_cli("diff", cap_a, cap_b)
    assert res.returncode == 1, f"stdout={res.stdout} stderr={res.stderr}"
    assert "023130" in res.stdout
    assert f"urn:atgeo:{K1}" in res.stdout


def test_cli_diff_reports_manifest_row_by_key(tmp_path):
    src_a = str(tmp_path / "a_tiles")
    src_b = str(tmp_path / "b_tiles")
    for src, rows in ((src_a, [(K1, "023130")]), (src_b, [(K1, "023130"), (K2, "023130")])):
        _build_tiles_dir(src, tiles=[("023130", [_record(K1, "one")])],
                         manifest_qks=("023130",), db_rows=rows)

    cap_a = str(tmp_path / "a_cap")
    cap_b = str(tmp_path / "b_cap")
    assert _run_cli("capture", src_a, cap_a).returncode == 0
    assert _run_cli("capture", src_b, cap_b).returncode == 0

    res = _run_cli("diff", cap_a, cap_b)
    assert res.returncode == 1, f"stdout={res.stdout} stderr={res.stderr}"
    assert K2 in res.stdout
