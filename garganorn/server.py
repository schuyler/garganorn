"""Garganorn package for serving ATProtocol XRPC for community.lexicon.location."""
import json, time
from importlib.resources import files

from lexrpc import Server

from database import FoursquareOSP

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

server = Server(lexicons=load_lexicons())
db =  FoursquareOSP("db/fsq-osp.duckdb")

@server.method("info.schuyler.gazetteer.nearest")
def nearest(_, latitude: str, longitude: str, limit: str = "50"):
    """Find the nearest location to a given latitude and longitude."""
    try: 
        lat = float(latitude)
        lon = float(longitude)
    except ValueError:
        return {"error": "invalid_coordinates"}
    start_time = time.perf_counter()
    result = db.nearest(lat, lon)
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
    nsid = "info.schuyler.gazetteer.nearest"
    input = {}
    params = server.decode_params(nsid, (("latitude", "37.776145"), ("longitude", "-122.433898"), ("limit", "5")))
    output = server.call(nsid, input, **params)
    json.dump(output, sys.stdout, indent=2)