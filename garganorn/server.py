"""Garganorn package for serving ATProtocol XRPC for org.atgeo."""
import json, math, logging
from importlib.resources import files
from typing import NamedTuple

import lexrpc
from lexrpc.base import XrpcError

_log = logging.getLogger(__name__)

def load_lexicons():
    """Load all lexicon JSON files from the lexicon directory."""
    lexicons = []
    lexicon_path = files("garganorn") / "lexicon"
    
    if not lexicon_path.is_dir():
        _log.warning("No lexicon directory found")
        return []
        
    for file_path in lexicon_path.iterdir():
        if not file_path.is_file() or not file_path.name.endswith(".json"):
            continue
        with file_path.open('r') as f:
            try:
                lexicon_data = json.load(f)
                lexicons.append(lexicon_data)
            except json.JSONDecodeError:
                _log.error("Failed to parse %s as JSON", file_path.name)
    
    return lexicons


class ServedTileset(NamedTuple):
    descriptor: dict
    manifest: object  # quadtree.TileManifest
    reader: object  # tile_reader.TileBackedCollection


URN_PREFIX = "urn:atgeo:"


class _XrpcServer(lexrpc.Server):
    """Drops the query parameters sent to a method whose lexicon declares none.

    lexrpc skips validating them, by design (a cache-buster, a utm_* tag), but
    still passes them on as keyword arguments, where `_`, `input` and `nsid`
    collide with its own and the handler's argument names. Dropped here, before
    the dispatch, no name can collide.
    """

    def __init__(self, lexicons):
        super().__init__(lexicons=lexicons)
        self._parameterless = {
            lex["id"] for lex in lexicons
            if lex["defs"].get("main", {}).get("type") in ("query", "procedure")
            and "parameters" not in lex["defs"]["main"]
        }

    def decode_params(self, method_nsid, params):
        if method_nsid in self._parameterless:
            return {}
        return super().decode_params(method_nsid, params)


class Server:
    def __init__(self, repo, tilesets, max_tiles, public_base):
        """tilesets: dict[slug, ServedTileset] in config order."""
        self.repo = repo
        self.public_base = public_base
        self.tilesets = tilesets
        self.max_tiles = max_tiles
        self.lexicons = load_lexicons()
        self.lexicon_map = {lex["id"]: lex for lex in self.lexicons}
        self.server = _XrpcServer(self.lexicons)
        self.server.register("org.atgeo.getTiles", self.get_tiles)
        self.server.register("org.atgeo.getPlace", self.get_place)
        self.server.register("org.atgeo.describeTileset", self.describe_tileset)
        self.server.register("org.atgeo.describeService", self.describe_service)

    def describe_service(self, _):
        return {
            "did": f"did:web:{self.repo}",
            "maxTiles": self.max_tiles,
            "tilesets": [
                {
                    "slug": slug,
                    "collections": [
                        {"dataset": c["dataset"], "collection": c["collection"]}
                        for c in ts.descriptor["collections"]
                    ],
                }
                for slug, ts in self.tilesets.items()
            ],
        }

    def describe_tileset(self, _, slug: str):
        ts = self.tilesets.get(slug)
        if ts is None:
            raise XrpcError(f"Unknown tileset: {slug}", "TilesetNotFound")
        return ts.descriptor

    def get_place(self, _, uri: str):
        """Answer from the first tileset carrying the URN's collection that holds the key."""
        key = uri.removeprefix(URN_PREFIX) if uri.startswith(URN_PREFIX) else ""
        parts = key.split(":", 2)
        if len(parts) != 3:
            raise XrpcError(f"No such place: {uri}", "PlaceNotFound")
        dataset, collection, id = parts
        for ts in self.tilesets.values():
            if not any(c["dataset"] == dataset and c["collection"] == collection
                       for c in ts.descriptor["collections"]):
                continue
            found = ts.reader.get_place(key)
            if found is not None:
                record, attribution = found
                view = {
                    "uri": uri,
                    "url": f"{self.public_base}/{dataset}/{collection}/{id}",
                }
                if "importance" in record:
                    view["importance"] = record["importance"]
                view["value"] = record["value"]
                view["attribution"] = attribution
                return view
        raise XrpcError(f"No such place: {uri}", "PlaceNotFound")

    def _parse_bbox(self, bbox_str):
        """Parse and validate bbox string 'xmin,ymin,xmax,ymax', on Earth (inclusive). Returns tuple or raises XrpcError."""
        parts = bbox_str.split(",")
        if len(parts) != 4:
            raise XrpcError("bbox must be four comma-separated numbers: xmin,ymin,xmax,ymax", "InvalidBbox")
        try:
            xmin, ymin, xmax, ymax = (float(p) for p in parts)
        except ValueError:
            raise XrpcError("bbox values must be valid numbers", "InvalidBbox")
        if any(math.isnan(v) or math.isinf(v) for v in (xmin, ymin, xmax, ymax)):
            raise XrpcError("bbox values must be finite numbers", "InvalidBbox")
        if not (-180 <= xmin <= 180 and -180 <= xmax <= 180
                and -90 <= ymin <= 90 and -90 <= ymax <= 90):
            raise XrpcError("bbox longitudes must be within [-180, 180] and latitudes within [-90, 90]", "InvalidBbox")
        if ymin >= ymax:
            raise XrpcError("bbox requires ymin < ymax", "InvalidBbox")
        # Allow antimeridian crossing (xmin > xmax) only when xmin is positive/eastern
        # and xmax is negative/western, which indicates crossing the ±180° meridian.
        # Reject xmin >= xmax in all other cases (truly invalid bboxes).
        if xmin >= xmax and not (xmin > 0 and xmax < 0):
            raise XrpcError("bbox requires xmin < xmax (unless crossing antimeridian with xmin > 0, xmax < 0)", "InvalidBbox")
        return (xmin, ymin, xmax, ymax)

    def _check_bbox_precision(self, bbox_str: str):
        """Raise XrpcError BboxTooPrecise if any bbox coordinate exceeds 2 decimal places.

        Enforces the 0.01° grid required by the tile-based privacy model: clients must
        snap bounding boxes to coarse grid boundaries so the server cannot infer precise
        user location from getTiles requests.

        Precision is checked by string inspection, not float comparison, so
        '37.770' (3 chars after '.') and '1e2' (scientific notation) are both rejected.
        """
        parts = bbox_str.split(",")
        if len(parts) != 4:
            return
        for part in parts:
            part = part.strip()
            if 'e' in part or 'E' in part:
                raise XrpcError(
                    "bbox coordinate has more than 2 decimal places; snap to 0.01° grid",
                    "BboxTooPrecise",
                )
            dot_pos = part.find('.')
            if dot_pos == -1:
                continue
            if len(part) - dot_pos - 1 > 2:
                raise XrpcError(
                    "bbox coordinate has more than 2 decimal places; snap to 0.01° grid",
                    "BboxTooPrecise",
                )

    def get_tiles(self, _, dataset: str, bbox: str, collection: str = None):
        """Return tile URLs covering bbox for the dataset (and collection, if given).

        When the combined regular tiles exceed max_tiles, answers with the
        selected tilesets' overview tiles and sets "overview": True.

        Raises BboxTooPrecise if any coordinate exceeds 2 decimal places,
        InvalidBbox if the bbox string is malformed, DatasetNotFound or
        CollectionNotFound if no tileset carries them, and BboxTooLarge if the
        bbox exceeds max_tiles and a selected tileset has no overview tiles.
        """
        self._check_bbox_precision(bbox)
        parsed_bbox = self._parse_bbox(bbox)
        in_dataset = [
            ts for ts in self.tilesets.values()
            if any(c["dataset"] == dataset for c in ts.descriptor["collections"])
        ]
        if not in_dataset:
            raise XrpcError(f"Unknown dataset: {dataset}", "DatasetNotFound")
        selected = [
            ts for ts in in_dataset
            if collection is None
            or any(c["dataset"] == dataset and c["collection"] == collection
                   for c in ts.descriptor["collections"])
        ]
        if not selected:
            raise XrpcError(f"Unknown collection: {dataset}/{collection}", "CollectionNotFound")
        tiles = [u for ts in selected for u in ts.manifest.regular_tiles(parsed_bbox)]
        if len(tiles) <= self.max_tiles:
            return {"tiles": sorted(tiles)}
        overviews = [ts.manifest.overview_tiles(parsed_bbox) for ts in selected]
        if any(o is None for o in overviews):
            raise XrpcError(
                f"bbox covers {len(tiles)} tiles; the limit is {self.max_tiles}", "BboxTooLarge",
            )
        return {"tiles": sorted(u for o in overviews for u in o), "overview": True}
