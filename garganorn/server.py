"""Garganorn package for serving ATProtocol XRPC for community.lexicon.location."""
import json, time
from importlib.resources import files

import lexrpc

def load_lexicons():
    """Load all lexicon JSON files from the lexicon directory."""
    lexicons = []
    lexicon_path = files("garganorn") / "lexicon"
    
    if not lexicon_path.exists():
        print("Warning: No lexicon directory found")
        return lexicons
        
    for file_path in lexicon_path.glob("*.json"):
        with open(file_path, 'r') as f:
            try:
                lexicon_data = json.load(f)
                lexicons.append(lexicon_data)
                print(f"Loaded lexicon: {lexicon_data['id']} from {file_path.name}")
            except json.JSONDecodeError:
                print(f"Error: Failed to parse {file_path.name} as JSON")
    
    return lexicons

class Server:
    nsid = "info.schuyler.gazetteer"

    def __init__(self, db):
        self.db = db
        self.lexicons = load_lexicons()
        self.server = lexrpc.Server(lexicons=self.lexicons)
        for method in ["nearest"]:
            """Register bound methods with the server."""
            print(f"Registering method: {method} with NSID: {self.qualify(method)}")
            self.server.register(self.qualify(method), getattr(self, method))

    def qualify(self, method_name: str):
        """Qualify a method name with the server's namespace."""
        return f"{self.nsid}.{method_name}"

    def nearest(self, _, latitude: str, longitude: str, limit: str = "50"):
        """Find the nearest location to a given latitude and longitude."""
        try: 
            lat = float(latitude)
            lon = float(longitude)
        except ValueError:
            return {"error": "invalid_coordinates"}
        start_time = time.perf_counter()
        result = self.db.nearest(lat, lon, limit=int(limit))
        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "locations": result,
            "parameters": {
                "latitude": latitude,
                "longitude": longitude,
                "limit": limit,
                "catalog": "default" # hardcoded for now
            },
            "elapsed_ms": run_time
        }

if __name__ == "__main__":
    import sys
    from database import OvertureMaps
    input = {}
    db = OvertureMaps("db/overture-maps.duckdb")
    gazetteer = Server(db)
    nsid = f"{gazetteer.nsid}.nearest"
    params = gazetteer.server.decode_params(nsid, (("latitude", "37.776145"), ("longitude", "-122.433898"), ("limit", "5")))
    output = gazetteer.server.call(nsid, input, **params)
    json.dump(output, sys.stdout, indent=2)