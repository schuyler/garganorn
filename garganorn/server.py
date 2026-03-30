"""Garganorn package for serving ATProtocol XRPC for org.atgeo."""
import json, math, time, logging
from importlib.resources import files

import lexrpc
from lexrpc.base import XrpcError

def load_lexicons():
    """Load all lexicon JSON files from the lexicon directory."""
    lexicons = []
    lexicon_path = files("garganorn") / "lexicon"
    
    if not lexicon_path.is_dir():
        print("Warning: No lexicon directory found")
        return []
        
    for file_path in lexicon_path.iterdir():
        if not file_path.is_file() or not file_path.name.endswith(".json"):
            continue
        with file_path.open('r') as f:
            try:
                lexicon_data = json.load(f)
                lexicons.append(lexicon_data)
                #print(f"Loaded lexicon: {lexicon_data['id']} from {file_path.name}")
            except json.JSONDecodeError:
                print(f"Error: Failed to parse {file_path.name} as JSON")
    
    return lexicons

LEXICON_SCHEMA_COLLECTION = "com.atproto.lexicon.schema"


class Server:
    nsid = "org.atgeo"
    methods = {
        f"{nsid}.searchRecords": "search_records",
        "com.atproto.repo.getRecord": "get_record",
        "com.atproto.repo.listRecords": "list_records",
    }

    def __init__(self, repo, dbs, logger, boundaries=None):
        self.repo = repo
        self.db = dict([(db.collection, db) for db in dbs])
        self.boundaries = boundaries
        self.lexicons = load_lexicons()
        self.lexicon_map = {lex["id"]: lex for lex in self.lexicons}
        self.server = lexrpc.Server(lexicons=self.lexicons)
        self.logger = logger
        for name, method in self.methods.items():
            """Register bound methods with the server."""
            #print(f"Registering {name} to {method}")
            self.server.register(name, getattr(self, method))

    def record_uri(self, collection, rkey):
        assert collection in self.db, f"Collection {collection} not found on server {self.repo}"
        return f"https://{self.repo}/{collection}/{rkey}"

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

        start_time = time.perf_counter()
        if collection not in self.db:
            raise XrpcError(f"Collection {collection} not found on server {self.repo}", "CollectionNotFound")
        record = self.db[collection].get_record(repo, collection, rkey)
        if record is None:
            raise XrpcError(f"Record {rkey} not found in collection {collection}", "RecordNotFound")

        # Compute containment relations
        relations = {}
        if self.boundaries:
            locations = record.get("locations", [])
            for loc in locations:
                if loc.get("$type") == "community.lexicon.location.geo":
                    lat = float(loc["latitude"])
                    lon = float(loc["longitude"])
                    within = self.boundaries.containment(lat, lon)
                    if within:
                        relations["within"] = within
                    break

        # Inject relations into the record value
        if relations:
            record["relations"] = relations

        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "uri": self.record_uri(collection, record["rkey"]),
            "attribution": self.db[collection].attribution,
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

    def list_records(self, _, repo: str, collection: str, limit: int = 50,
                     cursor: str = "", reverse: bool = False):
        if collection != LEXICON_SCHEMA_COLLECTION:
            raise XrpcError(
                f"Collection {collection} not found on server {self.repo}",
                "CollectionNotFound",
            )

        # Sort lexicon NSIDs for stable pagination
        nsids = sorted(self.lexicon_map.keys(), reverse=reverse)

        # Apply cursor: skip past the cursor NSID
        if cursor:
            try:
                idx = nsids.index(cursor) + 1
            except ValueError:
                idx = 0
            nsids = nsids[idx:]

        # Apply limit
        page = nsids[:limit]
        next_cursor = page[-1] if len(nsids) > limit else None

        records = [
            {
                "uri": f"at://did:web:{self.repo}/{LEXICON_SCHEMA_COLLECTION}/{nsid}",
                "value": self.lexicon_map[nsid],
            }
            for nsid in page
        ]

        result = {"records": records}
        if next_cursor:
            result["cursor"] = next_cursor
        return result

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
        if xmin >= xmax or ymin >= ymax:
            raise XrpcError("bbox requires xmin < xmax and ymin < ymax", "InvalidBbox")
        return (xmin, ymin, xmax, ymax)

    def search_records(self, _, collection: str, latitude: str = "", longitude: str = "",
                       q: str = "", limit: str = "50", bbox: str = ""):
        self.logger.info(f"Searching records in {collection} with bbox={bbox}, latitude={latitude}, longitude={longitude}, q={q}, limit={limit}")
        if collection not in self.db:
            raise XrpcError(f"Collection {collection} not found on server {self.repo}", "CollectionNotFound")
        parsed_bbox = None
        if bbox:
            parsed_bbox = self._parse_bbox(bbox)
        elif latitude and longitude:
            try:
                lat = float(latitude)
                lon = float(longitude)
            except ValueError:
                raise XrpcError("Latitude and longitude coordinates must be valid numbers", "InvalidCoordinates")
            expand_m = 5000
            expand_lat = expand_m / 111194.927
            expand_lon = expand_lat / math.cos(lat * math.pi / 180) if abs(lat) < 90 else expand_lat
            parsed_bbox = (
                max(lon - expand_lon, -180),
                max(lat - expand_lat, -90),
                min(lon + expand_lon, 180),
                min(lat + expand_lat, 90),
            )
        if parsed_bbox is None and not q:
            raise XrpcError("Either q, bbox, or latitude/longitude must be provided", "InvalidQuery")
        start_time = time.perf_counter()
        result = self.db[collection].nearest(bbox=parsed_bbox, q=q or None, limit=int(limit))
        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "records": [
                {
                    "$type": f"{self.nsid}.searchRecords#record",
                    "uri": self.record_uri(collection, r["rkey"]),
                    "attribution": self.db[collection].attribution,
                    "distance_m": r.pop("distance_m"),
                    **({"score": round(r.pop("score"), 3)} if "score" in r else {}),
                    **({"importance": r.pop("importance")} if "importance" in r else {}),
                    "value": r,
                } for r in result
            ],
            "_query": {
                "parameters": {
                    "repo": self.repo,
                    "collection": collection,
                    "bbox": bbox,
                    "q": q,
                    "latitude": latitude,
                    "longitude": longitude,
                    "limit": limit
                },
                "elapsed_ms": run_time
            }
        }
        
if __name__ == "__main__":
    import sys
    from database import OvertureMaps, FoursquareOSP

    dbs = [
        OvertureMaps("db/overture-maps.duckdb"),
        FoursquareOSP("db/fsq-osp.duckdb"),  # Uncomment if you have the Foursquare database
    ]
    gazetteer = Server("places.atgeo.org", dbs, logging.getLogger())

    collection = "org.atgeo.places.foursquare"
    nsid = f"{gazetteer.nsid}.searchRecords"
    params = gazetteer.server.decode_params(nsid, (
        ("collection", collection),
        ("latitude", "37.776145"),
        ("longitude", "-122.433898"),
        ("limit", "5")
    ))
    result = gazetteer.server.call(nsid, {}, **params) 
    output = dict(result)   
    json.dump(output, sys.stdout, indent=2)

    nsid = f"com.atproto.repo.getRecord"
    rkey = output["records"][0]["value"]["rkey"]
    params = gazetteer.server.decode_params(nsid, (
        ("repo", "places.atgeo.org"),
        ("collection", collection),
        ("rkey", rkey)
    ))
    output = gazetteer.server.call(nsid, {}, **params)
    json.dump(output, sys.stdout, indent=2)
