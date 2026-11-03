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
    search: object  # tile_reader.TileSearchIndex


URN_PREFIX = "urn:atgeo:"
SEARCH_LIMIT = 20
SEARCH_TOKEN_LIMIT = 12


class _XrpcServer(lexrpc.Server):
    """Settles undeclared query parameters before lexrpc's dispatch sees them.

    lexrpc passes query parameters to `Server.call(nsid, input=None, **params)`
    as keyword arguments, so a key named `input` or `nsid` collides with its
    own arguments (and `_` with the handler's) and escapes as a TypeError, a
    500. Settled here, before the dispatch, no name can collide:

    - A method whose lexicon declares no parameters drops them all. lexrpc
      skips validating them, by design (a cache-buster, a utm_* tag).
    - Any other method rejects a key its lexicon doesn't declare with a
      ValueError, which lexrpc answers as 400 InvalidRequest, the same answer
      its own validation gives every other unknown key (#29).
    """

    def __init__(self, lexicons):
        super().__init__(lexicons=lexicons)
        self._parameterless = set()
        self._declared = {}
        for lex in lexicons:
            main = lex["defs"].get("main", {})
            if main.get("type") not in ("query", "procedure"):
                continue
            if "parameters" in main:
                self._declared[lex["id"]] = main["parameters"].get("properties", {}).keys()
            else:
                self._parameterless.add(lex["id"])

    def decode_params(self, method_nsid, params):
        if method_nsid in self._parameterless:
            return {}
        params = list(params)
        declared = self._declared.get(method_nsid)
        if declared is not None:
            if unknown := {name for name, _ in params} - declared:
                raise ValueError(f"unknown parameters: {unknown}")
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
        self.server.register("org.atgeo.searchPlaces", self.search_places)
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
            if view := self._view(ts, key):
                return view
        raise XrpcError(f"No such place: {uri}", "PlaceNotFound")

    def _view(self, ts, key):
        """The placeView of a key in one tileset, or None when it holds no such record."""
        found = ts.reader.get_place(key)
        return None if found is None else self._place_view(key, *found)

    def _place_view(self, key, record, attribution):
        dataset, collection, id = key.split(":", 2)
        view = {
            "uri": URN_PREFIX + key,
            "url": f"{self.public_base}/{dataset}/{collection}/{id}",
        }
        if "importance" in record:
            view["importance"] = record["importance"]
        view["value"] = record["value"]
        view["attribution"] = attribution
        return view

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
        # and xmax is negative/western, which indicates crossing the ±180° meridian,
        # and the box has width (180 and -180 are the same meridian).
        # Reject xmin >= xmax in all other cases (truly invalid bboxes).
        if xmin >= xmax and not (xmin > 0 and xmax < 0 and xmin - xmax < 360):
            raise XrpcError("bbox requires xmin < xmax (unless crossing antimeridian with xmin > 0 > xmax and nonzero width)", "InvalidBbox")
        return (xmin, ymin, xmax, ymax)

    def _check_bbox_precision(self, bbox_str: str, decimals: int):
        """Raise XrpcError BboxTooPrecise if any bbox coordinate exceeds `decimals` decimal places.

        Enforces the grid required by the tile-based privacy model: clients must
        snap bounding boxes to coarse grid boundaries so the server cannot infer precise
        user location from getTiles (0.01°, 2 decimals) or searchPlaces (1°, 0) requests.

        Precision is checked by string inspection, not float comparison, so
        '37.770' (3 chars after '.') and '1e2' (scientific notation) are both rejected
        at 2 decimals.
        """
        grid = f"{10 ** -decimals:g}°"
        message = f"bbox coordinate has more than {decimals} decimal places; snap to {grid} grid"
        parts = bbox_str.split(",")
        if len(parts) != 4:
            return
        for part in parts:
            part = part.strip()
            if 'e' in part or 'E' in part:
                raise XrpcError(message, "BboxTooPrecise")
            dot_pos = part.find('.')
            if dot_pos == -1:
                continue
            if len(part) - dot_pos - 1 > decimals:
                raise XrpcError(message, "BboxTooPrecise")

    def _select_tilesets(self, dataset, collection):
        """The tilesets carrying the dataset (and collection, if given)."""
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
        return selected

    def get_tiles(self, _, dataset: str, bbox: str, collection: str = None):
        """Return tile URLs covering bbox for the dataset (and collection, if given).

        When the combined regular tiles exceed max_tiles, answers with the
        selected tilesets' overview tiles and sets "overview": True.

        Raises BboxTooPrecise if any coordinate exceeds 2 decimal places,
        InvalidBbox if the bbox string is malformed, DatasetNotFound or
        CollectionNotFound if no tileset carries them, and BboxTooLarge if the
        bbox exceeds max_tiles and a selected tileset has no overview tiles.
        """
        self._check_bbox_precision(bbox, 2)
        parsed_bbox = self._parse_bbox(bbox)
        selected = self._select_tilesets(dataset, collection)
        tiles = [u for ts in selected for u in ts.manifest.regular_tiles(parsed_bbox)]
        if len(tiles) <= self.max_tiles:
            return {"tiles": sorted(tiles)}
        overviews = [ts.manifest.overview_tiles(parsed_bbox) for ts in selected]
        if any(o is None for o in overviews):
            raise XrpcError(
                f"bbox covers {len(tiles)} tiles; the limit is {self.max_tiles}", "BboxTooLarge",
            )
        return {"tiles": sorted(u for o in overviews for u in o), "overview": True}

    def search_places(self, _, dataset: str, q: str, collection: str = None, bbox: str = None):
        """Return up to SEARCH_LIMIT places of the dataset (and collection, if
        given) with a name matching every word of q, most notable first.

        bbox, if given, is a whole-degree region. Raises KeywordRequired if
        none of q's first SEARCH_TOKEN_LIMIT distinct words has two characters,
        BboxTooPrecise or InvalidBbox for a bad region, and DatasetNotFound or
        CollectionNotFound as get_tiles does.
        """
        parsed_bbox = None
        if bbox is not None:
            self._check_bbox_precision(bbox, 0)
            parsed_bbox = self._parse_bbox(bbox)
        selected = self._select_tilesets(dataset, collection)
        tokens = list(dict.fromkeys(selected[0].search.tokens(q)))[:SEARCH_TOKEN_LIMIT]
        if not any(len(t) >= 2 for t in tokens):
            raise XrpcError("q needs a word of two or more characters", "KeywordRequired")
        scope = f"{dataset}:{collection}" if collection is not None else None
        hits = [
            (page_rank, importance, key, ts) for ts in selected
            for key, page_rank, importance in ts.search.search(
                tokens, parsed_bbox, scope, SEARCH_LIMIT)
        ]
        hits.sort(key=lambda h: (h[0] is None, -(h[0] or 0.0), -h[1], h[2]))
        top = hits[:SEARCH_LIMIT]
        found = {
            id(ts): ts.reader.get_places([key for _, _, key, t in top if t is ts])
            for ts in {id(t): t for *_, t in top}.values()
        }
        return {"places": [
            self._place_view(key, *found[id(ts)][key])
            for _, _, key, ts in top if key in found[id(ts)]
        ]}
