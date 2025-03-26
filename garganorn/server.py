"""Garganorn package for serving ATProtocol XRPC for community.lexicon.location."""
import json
from importlib.resources import files

from .database import Database

from lexrpc import Server

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
            except json.JSONDecodeError:
                print(f"Error: Failed to parse {file_path.name} as JSON")
    
    return lexicons

server = Server(lexicons=load_lexicons())
db =  Database("db/fsq-osp.duckdb")

@server.method("info.schuyler.locations.nearest")
def nearest(_, latitude: str, longitude: str):
    """Find the nearest location to a given latitude and longitude."""
    try: 
        lat = float(latitude)
        lon = float(longitude)
    except ValueError:
        return {"error": "invalid_coordinates"}
    result = db.nearest(lat, lon)
    return {"locations": result}