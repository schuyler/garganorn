"""Garganorn package for serving ATProtocol XRPC for community.lexicon.location."""
import json, time
from importlib.resources import files

import lexrpc

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

class Server:
    nsid = "community.lexicon.location"
    methods = {
        f"{nsid}.searchRecords": "search_records",
        "com.atproto.repo.getRecord": "get_record",
    }

    def __init__(self, repo, dbs):
        self.repo = repo
        self.db = dict([(db.collection, db) for db in dbs])
        self.lexicons = load_lexicons()
        self.server = lexrpc.Server(lexicons=self.lexicons)
        for name, method in self.methods.items():
            """Register bound methods with the server."""
            #print(f"Registering {name} to {method}")
            self.server.register(name, getattr(self, method))

    def record_uri(self, collection, rkey):
        assert collection in self.db, f"Collection {collection} not found on server {self.repo}"
        return f"at://{self.repo}/{collection}/{rkey}"

    def get_record(self, _, repo: str, collection: str, rkey: str):
        start_time = time.perf_counter()
        assert collection in self.db, f"Collection {collection} not found on server {self.repo}"
        record = self.db[collection].get_record(repo, collection, rkey)
        if record is None:
            return {"error": "RecordNotFound"}
        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "uri": self.record_uri(collection, record["rkey"]),
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
    
    def search_records(self, _, collection: str, latitude: str = "", longitude: str = "", q: str = "", limit: str = "50"):
        print(f"Searching records in {collection} with latitude={latitude}, longitude={longitude}, q={q}, limit={limit}")
        """Find the nearest location to a given latitude and longitude."""
        if (not latitude or not longitude) and not q:
            return {"error": "InvalidQuery"}
        lat = lon = None
        if latitude and longitude:
            try: 
                lat = float(latitude)
                lon = float(longitude)
            except ValueError:
                return {"error": "InvalidCoordinates"}
        start_time = time.perf_counter()
        assert collection in self.db, f"Collection {collection} not found on server {self.repo}"
        result = self.db[collection].nearest(lat, lon, q, limit=int(limit))
        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "records": [
                {
                    "$type": f"{self.nsid}.searchRecords#record",
                    "uri": self.record_uri(collection, r["rkey"]),
                    "distance_m": r.pop("distance_m"),
                    "value": r,
                } for r in result
            ],
            "_query": {
                "parameters": {
                    "repo": self.repo,
                    "collection": collection,
                    "latitude": latitude,
                    "longitude": longitude,
                    "limit": limit
                },
                "elapsed_ms": run_time
            }
        }

if __name__ == "__main__":
    import sys
    from database import OvertureMaps

    dbs = [
        OvertureMaps("db/overture-maps.duckdb"),
        # FoursquareOSP("db/fsq-osp.duckdb"),  # Uncomment if you have the Foursquare database
    ]
    gazetteer = Server("gazetteer.social", dbs)

    nsid = f"{gazetteer.nsid}.searchRecords"
    params = gazetteer.server.decode_params(nsid, (
        ("collection", "org.overturemaps.places"),
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
        ("repo", "gazetteer.social"),
        ("collection", "org.overturemaps.places"),
        ("rkey", rkey)
    ))
    output = gazetteer.server.call(nsid, {}, **params)
    json.dump(output, sys.stdout, indent=2)
