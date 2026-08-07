#!/usr/bin/env python3
"""Tile output byte-parity harness.

Two subcommands:
  capture <tiles_dir> <out_dir>  — snapshot a tile tree into a canonical form.
  diff    <ref_dir> <captured_dir> — compare two captures; nonzero exit on any diff.

Canonical form neutralizes record ordering, JSON key ordering, and the
non-deterministic manifest `generated_at` timestamp so that only genuine
value differences are reported. Zero-tolerance: any difference exits nonzero.
"""

import argparse
import gzip
import json
import os
import sys

import duckdb


def canonical_tile(gz_path):
    """Read a .json.gz tile, sort records by value.rkey, return canonical JSON string.

    Records are atgeo v1 {uri, cid, value}-wrapped; sorted by their
    value["rkey"] (missing rkey sorts as empty string). Tile-level
    `generated_at` is stripped -- it is a run-scoped timestamp
    (docs/pipeline-implementation-decisions.md, "OQ-P2-1 — record envelope
    adoption"), not a value difference. Keys are sorted and whitespace is
    stripped so the string is stable across runs that differ only in
    ordering.
    """
    def _sort_key(r):
        # New envelope: {uri, cid, value: {..., rkey, ...}}. Old (pre-envelope)
        # shape: flat record dict with a top-level rkey. Fall back to the
        # top-level key so captures of either shape canonicalize correctly.
        if "value" in r:
            return r["value"].get("rkey", "")
        return r.get("rkey", "")

    with gzip.open(gz_path) as f:
        obj = json.load(f)
    obj.pop("generated_at", None)
    if "records" in obj:
        obj["records"] = sorted(obj["records"], key=_sort_key)
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_manifest(manifest_path):
    """Load manifest.json and return a dict with `generated_at` removed.

    `generated_at` changes every run; stripping it prevents spurious diffs.
    """
    with open(manifest_path) as f:
        data = json.load(f)
    data = dict(data)
    data.pop("generated_at", None)
    return data


def iter_tiles(tiles_dir):
    """Yield (qk, gz_path) for every *.json.gz under tiles_dir, sorted by qk."""
    found = []
    for root, _dirs, files in os.walk(tiles_dir):
        for fname in files:
            if fname.endswith(".json.gz"):
                qk = fname[: -len(".json.gz")]
                found.append((qk, os.path.join(root, fname)))
    found.sort(key=lambda t: t[0])
    for qk, path in found:
        yield qk, path


def manifest_rows(manifest_db_path):
    """Return [(rkey, tile_qk), ...] ordered by rkey, tile_qk from manifest.duckdb."""
    con = duckdb.connect(manifest_db_path, read_only=True)
    try:
        rows = con.execute(
            "SELECT rkey, tile_qk FROM record_tiles ORDER BY rkey, tile_qk"
        ).fetchall()
    finally:
        con.close()
    return [(r[0], r[1]) for r in rows]


def capture(tiles_dir, out_dir):
    """Snapshot a tile tree into a canonical capture directory.

    Writes:
      <out_dir>/<qk>.canon        — canonical JSON string per tile.
      <out_dir>/manifest.json     — manifest with `generated_at` removed.
      <out_dir>/manifest_rows.json — sorted [rkey, tile_qk] pairs from manifest.duckdb.
    """
    os.makedirs(out_dir, exist_ok=True)

    for qk, gz_path in iter_tiles(tiles_dir):
        canon = canonical_tile(gz_path)
        with open(os.path.join(out_dir, qk + ".canon"), "w") as f:
            f.write(canon)

    manifest_path = os.path.join(tiles_dir, "manifest.json")
    if os.path.exists(manifest_path):
        stripped = canonical_manifest(manifest_path)
        with open(os.path.join(out_dir, "manifest.json"), "w") as f:
            json.dump(stripped, f, sort_keys=True, indent=2)

    db_path = os.path.join(tiles_dir, "manifest.duckdb")
    if os.path.exists(db_path):
        rows = manifest_rows(db_path)
        with open(os.path.join(out_dir, "manifest_rows.json"), "w") as f:
            json.dump(rows, f, indent=2)


def _load_canon_map(cap_dir):
    """Return {qk: canon_text} for all <qk>.canon files in a capture dir."""
    out = {}
    for fname in os.listdir(cap_dir):
        if fname.endswith(".canon"):
            qk = fname[: -len(".canon")]
            with open(os.path.join(cap_dir, fname)) as f:
                out[qk] = f.read()
    return out


def _tile_value_diffs(qk, ref_text, cap_text):
    """Return diff messages for a tile whose canonical bytes differ.

    Called only when ref and captured strings are already known to differ.
    Checks record presence/values per rkey and envelope-level metadata.
    Returns a list of human-readable difference strings (never empty on entry).
    """
    diffs = []
    try:
        ref_obj = json.loads(ref_text)
        cap_obj = json.loads(cap_text)
    except json.JSONDecodeError as e:
        return [f"tile {qk}: could not parse canonical form: {e}"]

    ref_recs = {r.get("rkey", ""): r for r in ref_obj.get("records", [])}
    cap_recs = {r.get("rkey", ""): r for r in cap_obj.get("records", [])}

    for rkey in sorted(set(ref_recs) - set(cap_recs)):
        diffs.append(f"tile {qk} rkey {rkey}: present in ref, missing in captured")
    for rkey in sorted(set(cap_recs) - set(ref_recs)):
        diffs.append(f"tile {qk} rkey {rkey}: present in captured, missing in ref")
    for rkey in sorted(set(ref_recs) & set(cap_recs)):
        if ref_recs[rkey] != cap_recs[rkey]:
            diffs.append(f"tile {qk} rkey {rkey}: record value differs")

    # Envelope-level differences (collection, source, license, etc.).
    ref_env = {k: v for k, v in ref_obj.items() if k != "records"}
    cap_env = {k: v for k, v in cap_obj.items() if k != "records"}
    if ref_env != cap_env:
        diffs.append(f"tile {qk}: envelope metadata differs (non-record fields)")

    if not diffs:
        # Contents differ but per-record comparison found nothing — surface anyway.
        diffs.append(f"tile {qk}: canonical bytes differ (no per-record cause isolated)")
    return diffs


def diff_captures(ref_dir, captured_dir):
    """Compare two capture dirs across three artifact classes.

    Checks: per-tile canonical files (*.canon), manifest.json, and
    manifest_rows.json.  Returns a list of human-readable difference strings;
    an empty list means the captures are identical.
    """
    diffs = []

    ref_map = _load_canon_map(ref_dir)
    cap_map = _load_canon_map(captured_dir)

    for qk in sorted(set(ref_map) - set(cap_map)):
        diffs.append(f"tile {qk}: present in ref, missing in captured")
    for qk in sorted(set(cap_map) - set(ref_map)):
        diffs.append(f"tile {qk}: present in captured, missing in ref")
    for qk in sorted(set(ref_map) & set(cap_map)):
        if ref_map[qk] != cap_map[qk]:
            diffs.extend(_tile_value_diffs(qk, ref_map[qk], cap_map[qk]))

    # manifest.json (already stripped of generated_at at capture time).
    ref_m = os.path.join(ref_dir, "manifest.json")
    cap_m = os.path.join(captured_dir, "manifest.json")
    if os.path.exists(ref_m) != os.path.exists(cap_m):
        diffs.append("manifest.json: present on one side only")
    elif os.path.exists(ref_m):
        with open(ref_m) as f:
            rm = json.load(f)
        with open(cap_m) as f:
            cm = json.load(f)
        if rm != cm:
            diffs.append(f"manifest.json: differs (ref={rm!r} captured={cm!r})")

    # manifest_rows.json (sorted [rkey, tile_qk] pairs).
    ref_r = os.path.join(ref_dir, "manifest_rows.json")
    cap_r = os.path.join(captured_dir, "manifest_rows.json")
    if os.path.exists(ref_r) != os.path.exists(cap_r):
        diffs.append("manifest_rows.json: present on one side only")
    elif os.path.exists(ref_r):
        with open(ref_r) as f:
            rr = {tuple(x) for x in json.load(f)}
        with open(cap_r) as f:
            cr = {tuple(x) for x in json.load(f)}
        for rkey, tile_qk in sorted(rr - cr):
            diffs.append(f"manifest row rkey {rkey} tile {tile_qk}: in ref, not captured")
        for rkey, tile_qk in sorted(cr - rr):
            diffs.append(f"manifest row rkey {rkey} tile {tile_qk}: in captured, not ref")

    return diffs


def main(argv=None):
    """Parse args and dispatch to capture or diff_captures.

    Exit codes: 0 = success / no diff, 1 = differences found, 2 = bad subcommand.
    """
    parser = argparse.ArgumentParser(
        description="Tile output byte-parity harness (capture / diff)."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_cap = sub.add_parser("capture", help="Snapshot a tile tree into a capture dir.")
    p_cap.add_argument("tiles_dir", help="Directory containing <qk[:6]>/<qk>.json.gz tiles.")
    p_cap.add_argument("out_dir", help="Destination capture directory.")

    p_diff = sub.add_parser("diff", help="Compare two capture dirs; nonzero exit on any diff.")
    p_diff.add_argument("ref_dir", help="Reference capture directory.")
    p_diff.add_argument("captured_dir", help="Capture directory to compare against ref.")

    args = parser.parse_args(argv)

    if args.cmd == "capture":
        capture(args.tiles_dir, args.out_dir)
        print(f"captured {args.tiles_dir} -> {args.out_dir}")
        return 0

    if args.cmd == "diff":
        diffs = diff_captures(args.ref_dir, args.captured_dir)
        if diffs:
            for d in diffs:
                print(d)
            print(f"DIFF: {len(diffs)} difference(s) found", file=sys.stderr)
            return 1
        print("OK: no differences")
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
