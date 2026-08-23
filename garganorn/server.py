"""Garganorn package for serving ATProtocol XRPC for org.atgeo."""
import json, math, time, logging
from importlib.resources import files

import lexrpc
from lexrpc.base import XrpcError
from garganorn import envelope
from garganorn.quadtree import BboxTooLarge

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

LEXICON_SCHEMA_COLLECTION = "com.atproto.lexicon.schema"
COLLECTION_METADATA_COLLECTION = "org.atgeo.collection"


class Server:
    nsid = "org.atgeo"
    methods = {
        "com.atproto.repo.getRecord": "get_record",
    }

    def __init__(self, repo, logger, tile_manifests=None,
                 tile_collections=None, max_coverage_tiles=50,
                 collection_metadata=None):
        self.repo = repo
        self.tile_manifests = tile_manifests or {}
        self.tile_collections = tile_collections or {}
        self.max_coverage_tiles = max_coverage_tiles
        self.collection_metadata = collection_metadata or {}
        self.lexicons = load_lexicons()
        self.lexicon_map = {lex["id"]: lex for lex in self.lexicons}
        self.server = lexrpc.Server(lexicons=self.lexicons)
        self.logger = logger
        for name, method in self.methods.items():
            """Register bound methods with the server."""
            self.server.register(name, getattr(self, method))
        self.server.register("org.atgeo.getCoverage", self.get_coverage)
        self.server.register("org.atgeo.describeService", self.describe_service)

    def record_uri(self, collection, rkey):
        return envelope.record_uri(self.repo, collection, rkey)

    def get_record(self, _, repo: str, collection: str, rkey: str):
        # Lexicon schema collection: serve from in-memory lexicon_map
        if collection == LEXICON_SCHEMA_COLLECTION:
            lexicon = self.lexicon_map.get(rkey)
            if lexicon is None:
                raise XrpcError(
                    f"Record {rkey} not found in collection {collection}",
                    "RecordNotFound",
                )
            return {
                "uri": f"at://did:web:{self.repo}/{LEXICON_SCHEMA_COLLECTION}/{rkey}",
                "value": lexicon,
            }

        # Collection metadata: served from the boot-loaded in-memory map,
        # keyed by the described collection's own NSID.
        if collection == COLLECTION_METADATA_COLLECTION:
            described = self.collection_metadata.get(rkey)
            if described is None:
                raise XrpcError(
                    f"Record {rkey} not found in collection {collection}",
                    "RecordNotFound",
                )
            return {
                "uri": self.record_uri(COLLECTION_METADATA_COLLECTION, rkey),
                "source": described["source"],
                "license": described["license"],
                "value": described,
            }

        start_time = time.perf_counter()
        source = self.tile_collections.get(collection)
        if source is None:
            raise XrpcError(f"Collection {collection} not found on server {self.repo}", "CollectionNotFound")
        record = source.get_record(repo, collection, rkey)
        if record is None:
            raise XrpcError(f"Record {rkey} not found in collection {collection}", "RecordNotFound")

        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "uri": self.record_uri(collection, record["rkey"]),
            "source": source.source_url,
            "license": source.license_url,
            **({"importance": record.pop("importance")} if "importance" in record else {}),
            "value": record,
            "_query": {
                "parameters": {
                    "repo": repo,
                    "collection": collection,
                    "rkey": rkey
                },
                "elapsed_ms": run_time
            }
        }

    def describe_service(self, _):
        """Enumerate served collections. The list is tile_manifests' keys --
        the same map get_coverage consults for CollectionNotFound (R1)."""
        collections = []
        for collection in self.tile_manifests:
            entry = {"collection": collection}
            if collection in self.collection_metadata:
                entry["metadata"] = self.record_uri(COLLECTION_METADATA_COLLECTION, collection)
            collections.append(entry)
        return {"did": f"did:web:{self.repo}", "collections": collections}

    def _parse_bbox(self, bbox_str):
        """Parse and validate bbox string 'xmin,ymin,xmax,ymax'. Returns tuple or raises XrpcError."""
        parts = bbox_str.split(",")
        if len(parts) != 4:
            raise XrpcError("bbox must be four comma-separated numbers: xmin,ymin,xmax,ymax", "InvalidBbox")
        try:
            xmin, ymin, xmax, ymax = (float(p) for p in parts)
        except ValueError:
            raise XrpcError("bbox values must be valid numbers", "InvalidBbox")
        if any(math.isnan(v) or math.isinf(v) for v in (xmin, ymin, xmax, ymax)):
            raise XrpcError("bbox values must be finite numbers", "InvalidBbox")
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
        user location from getCoverage requests.

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

    def get_coverage(self, _, collection: str, bbox: str):
        """Return tile URLs covering bbox for the given collection.

        Raises BboxTooPrecise if any coordinate exceeds 2 decimal places,
        BboxTooLarge if the bbox spans more tiles than max_coverage_tiles,
        CollectionNotFound if collection has no tile manifest, or InvalidBbox
        if the bbox string is malformed.
        """
        self._check_bbox_precision(bbox)
        parsed_bbox = self._parse_bbox(bbox)
        manifest = self.tile_manifests.get(collection)
        if manifest is None:
            raise XrpcError(f"Unknown collection: {collection}", "CollectionNotFound")
        try:
            tiles = manifest.get_tiles_for_bbox(*parsed_bbox, max_tiles=self.max_coverage_tiles)
        except BboxTooLarge as e:
            raise XrpcError(str(e), "BboxTooLarge") from e
        return {"tiles": sorted(tiles)}
