"""Unit + integration tests for scripts/tile_parity.py.

The "New envelope" section below (§6 item 7; pipeline-implementation-decisions.md
"OQ-P2-1 — record envelope adoption")
covers the canonicalizer update required for the atgeo v1 envelope: tiles now
carry a top-level `generated_at` (not just manifest.json), and records are
`{uri, cid, value}`-wrapped so the record sort/dedup key moves from top-level
`rkey` to `value.rkey`. These tests FAIL against the current
canonical_tile()/canonical_manifest(), which only strip manifest-level
`generated_at` and sort/key records by top-level `rkey` — that is the RED
signal for this feature; the fix belongs to scripts/tile_parity.py
(production code, out of scope for this test-only change)."""
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


def _write_manifest(d, source="overture_place", generated_at="2026-01-01T00:00:00+00:00",
                    quadkeys=("023130",)):
    path = os.path.join(d, "manifest.json")
    with open(path, "w") as f:
        json.dump({"source": source, "generated_at": generated_at,
                   "quadkeys": list(quadkeys)}, f, indent=2)
    return path


def _write_manifest_db(d, rows, source="overture_place",
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


# ---------------------------------------------------------------------------
# New envelope (§6 item 7; pipeline-implementation-decisions.md
# "OQ-P2-1 — record envelope adoption")
#
# atgeo v1 tiles carry {collection, source, license, generated_at, records}
# with records wrapped as {uri, cid, value}. canonical_tile() must (a) strip
# the tile-level generated_at (not just the manifest's) and (b) sort/key
# records by value.rkey rather than top-level rkey.
# ---------------------------------------------------------------------------

def _write_envelope_tile(tiles_dir, qk, wrapped_records, source="src", license_="lic",
                         collection="col", generated_at="2026-07-09T18:00:00Z"):
    """Write a new-shape (atgeo v1) .json.gz tile.

    wrapped_records: list of {"uri": ..., "cid": None, "value": {...}} dicts.
    """
    sub = os.path.join(tiles_dir, qk[:6])
    os.makedirs(sub, exist_ok=True)
    path = os.path.join(sub, qk + ".json.gz")
    obj = {
        "collection": collection, "source": source, "license": license_,
        "generated_at": generated_at, "records": wrapped_records,
    }
    with gzip.open(path, "wt") as f:
        json.dump(obj, f)
    return path


def _wrapped(rkey, name, uri_prefix="https://places.atgeo.org/col"):
    return {"uri": f"{uri_prefix}/{rkey}", "cid": None,
            "value": {"rkey": rkey, "name": name}}


class TestCanonicalTileNewEnvelope:
    """canonical_tile() must strip tile-level generated_at and key records by value.rkey."""

    def test_canonical_tile_strips_tile_level_generated_at(self, tmp_path):
        """Two tiles identical except for tile-level generated_at canonicalize equal.

        FAILS against current canonical_tile(): it has no knowledge of a
        tile-level 'generated_at' key (only manifest.json's is stripped
        elsewhere), so the differing timestamps make the canonical strings
        differ.
        """
        a = _write_envelope_tile(str(tmp_path / "a"), "023130",
                                 [_wrapped("r1", "one")], generated_at="2026-01-01T00:00:00Z")
        b = _write_envelope_tile(str(tmp_path / "b"), "023130",
                                 [_wrapped("r1", "one")], generated_at="2099-12-31T23:59:59Z")
        assert tile_parity.canonical_tile(a) == tile_parity.canonical_tile(b), (
            "canonical_tile must strip the tile-level 'generated_at' field so two "
            "tiles differing only in run timestamp canonicalize identically "
            "(per the envelope decisions above)"
        )

    def test_canonical_tile_sorts_by_value_rkey_when_reordered(self, tmp_path):
        """Records reordered but keyed by value.rkey canonicalize equal.

        FAILS against current canonical_tile(): it sorts by r.get("rkey", ""),
        which is always "" for wrapped records (rkey lives at value.rkey), so
        the sort is a no-op and reordering is not neutralized.
        """
        a = _write_envelope_tile(str(tmp_path / "a"), "023130",
                                 [_wrapped("r1", "one"), _wrapped("r2", "two")])
        b = _write_envelope_tile(str(tmp_path / "b"), "023130",
                                 [_wrapped("r2", "two"), _wrapped("r1", "one")])
        assert tile_parity.canonical_tile(a) == tile_parity.canonical_tile(b), (
            "canonical_tile must sort wrapped records by value.rkey (§6 item 7)"
        )

    def test_canonical_tile_still_differs_on_value_change(self, tmp_path):
        """A genuine value change is still detected after the value.rkey sort-key update."""
        a = _write_envelope_tile(str(tmp_path / "a"), "023130", [_wrapped("r1", "one")])
        b = _write_envelope_tile(str(tmp_path / "b"), "023130", [_wrapped("r1", "ONE")])
        assert tile_parity.canonical_tile(a) != tile_parity.canonical_tile(b)


class TestCliDiffNewEnvelope:
    """End-to-end capture/diff round trip against new-envelope tiles."""

    def test_cli_diff_identical_new_envelope_tiles_exits_zero(self, tmp_path):
        """Two runs with identical new-envelope content but different
        tile-level generated_at and record order must diff as identical.

        FAILS against the current script for the same reasons as the unit
        tests above: tile-level generated_at is not stripped and records
        are not keyed by value.rkey.
        """
        src_a = str(tmp_path / "a_tiles")
        src_b = str(tmp_path / "b_tiles")
        _write_envelope_tile(src_a, "023130",
                             [_wrapped("r1", "one"), _wrapped("r2", "two")],
                             generated_at="2026-01-01T00:00:00Z")
        _write_envelope_tile(src_b, "023130",
                             [_wrapped("r2", "two"), _wrapped("r1", "one")],
                             generated_at="2099-12-31T23:59:59Z")
        _write_manifest(src_a, quadkeys=("023130",))
        _write_manifest(src_b, quadkeys=("023130",))
        _write_manifest_db(src_a, [("r1", "023130"), ("r2", "023130")])
        _write_manifest_db(src_b, [("r1", "023130"), ("r2", "023130")])

        cap_a = str(tmp_path / "a_cap")
        cap_b = str(tmp_path / "b_cap")
        assert _run_cli("capture", src_a, cap_a).returncode == 0
        assert _run_cli("capture", src_b, cap_b).returncode == 0

        res = _run_cli("diff", cap_a, cap_b)
        assert res.returncode == 0, f"stdout={res.stdout} stderr={res.stderr}"
        assert "OK: no differences" in res.stdout
