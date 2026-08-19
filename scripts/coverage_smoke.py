#!/usr/bin/env python3
"""getCoverage + tile smoke test against a live garganorn deployment.

Calls org.atgeo.getCoverage for a collection/bbox, validates the result
against its lexicon schema and checks the tiles cover the requested bbox,
then fetches one tile and validates its envelope and records.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request

import lexrpc
from lexrpc.base import ValidationError

from garganorn.server import load_lexicons
from garganorn.stages import bboxes_intersect, quadkey_to_bbox

DEFAULT_BASE_URL = "https://places.atgeo.org"


def check(fn, *args, **kwargs):
    """Call a lexrpc validation function, exiting with a concise message on failure."""
    try:
        fn(*args, **kwargs)
    except ValidationError as e:
        raise SystemExit(str(e))


def fetch_json(url):
    """GET url and return parsed JSON, requesting uncompressed bytes."""
    req = urllib.request.Request(url, headers={"Accept-Encoding": "identity"})
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{url}: HTTP {e.code}: {e.read().decode('utf-8', 'replace')}")


def quadkey_from_tile_url(url):
    """Extract the quadkey from a .../{qk[:6]}/{qk}.json.gz tile URL."""
    return url.rsplit("/", 1)[-1].removesuffix(".json.gz")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection", help="Collection NSID, e.g. org.atgeo.places.overture.place")
    parser.add_argument(
        "--bbox", required=True,
        help="xmin,ymin,xmax,ymax in WGS84 decimal degrees, <=2 decimal places",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"default: {DEFAULT_BASE_URL}")
    args = parser.parse_args(argv)

    server = lexrpc.Server(lexicons=load_lexicons())
    requested_bbox = tuple(float(p) for p in args.bbox.split(","))

    coverage_url = (
        f"{args.base_url}/xrpc/org.atgeo.getCoverage"
        f"?collection={args.collection}&bbox={args.bbox}"
    )
    coverage = fetch_json(coverage_url)
    check(server.validate, "org.atgeo.getCoverage", "output", coverage)
    tiles = coverage["tiles"]
    assert tiles, "getCoverage returned no tiles"
    for tile_url in tiles:
        qk = quadkey_from_tile_url(tile_url)
        tile_bbox = quadkey_to_bbox(qk)
        assert bboxes_intersect(tile_bbox, requested_bbox), (
            f"tile {qk} bbox {tile_bbox} does not intersect requested bbox {requested_bbox}"
        )
    print(f"getCoverage: {len(tiles)} tile(s), all intersect the requested bbox")

    tile_payload = fetch_json(tiles[0])
    tile_schema = server.defs["org.atgeo.tilePayload"]
    check(
        server._validate_schema,
        name="output", val=tile_payload, type_name="org.atgeo.tilePayload",
        lexicon="org.atgeo.tilePayload", schema=tile_schema,
    )
    records = tile_payload["records"]
    assert records, f"tile {tiles[0]} has no records"
    for record in records:
        check(server.validate, "org.atgeo.place", "record", record["value"])
    print(f"tile {tiles[0]}: {len(records)} record(s), all validate against org.atgeo.place")

    return 0


if __name__ == "__main__":
    sys.exit(main())
