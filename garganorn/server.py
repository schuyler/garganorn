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
    nsid = "social.gazetteer"
    methods = {
        f"{nsid}.listNearestRecords": "nearest_records",
        "com.atproto.repo.getRecord": "get_record",
    }

    def __init__(self, repo, db):
        self.repo = repo
        self.db = db
        self.lexicons = load_lexicons()
        self.server = lexrpc.Server(lexicons=self.lexicons)
        for name, method in self.methods.items():
            """Register bound methods with the server."""
            #print(f"Registering {name} to {method}")
            self.server.register(name, getattr(self, method))

    def record_uri(self, record):
        record_id = record["attributes"]["id"]
        if not record_id:
            raise ValueError("Record ID is missing")
        return f"at://{self.repo}/{self.db.collection}/{record_id}"

    def get_record(self, _, repo: str, collection: str, rkey: str):
        start_time = time.perf_counter()
        record = self.db.get_record(repo, collection, rkey)
        if record is None:
            return {"error": "RecordNotFound"}
        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "uri": self.record_uri(record),
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
    
    def nearest_records(self, _, latitude: str, longitude: str, limit: str = "50"):
        """Find the nearest location to a given latitude and longitude."""
        try: 
            lat = float(latitude)
            lon = float(longitude)
        except ValueError:
            return {"error": "InvalidCoordinates"}
        start_time = time.perf_counter()
        result = self.db.nearest(lat, lon, limit=int(limit))
        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "records": [
                {
                    "$type": "social.gazetteer.listNearestRecords#record",
                    "uri": self.record_uri(r),
                    "distance_m": r.pop("distance_m"),
                    "value": r,
                } for r in result
            ],
            "_query": {
                "parameters": {
                    "repo": self.repo,
                    "collection": self.db.collection,
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

    db = OvertureMaps("db/overture-maps.duckdb")
    gazetteer = Server("gazetteer.social", db)

    nsid = f"{gazetteer.nsid}.listNearestRecords"
    params = gazetteer.server.decode_params(nsid, (("latitude", "37.776145"), ("longitude", "-122.433898"), ("limit", "5")))
    result = gazetteer.server.call(nsid, {}, **params) 
    output = dict(result)   
    json.dump(output, sys.stdout, indent=2)

    nsid = f"com.atproto.repo.getRecord"
    rkey = output["records"][0]["value"]["attributes"]["id"]
    params = gazetteer.server.decode_params(nsid, (
        ("repo", "gazetteer.social"),
        ("collection", "org.overturemaps.id"),
        ("rkey", rkey)
    ))
    output = gazetteer.server.call(nsid, {}, **params)
    json.dump(output, sys.stdout, indent=2)
