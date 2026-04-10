"""Configuration loader for Garganorn."""
import yaml
from pathlib import Path
from .database import FoursquareOSP, OvertureMaps, OpenStreetMap
from .boundaries import OvertureDivision

DATABASE_TYPES = {
    "foursquare": FoursquareOSP,
    "overture": OvertureMaps,
    "osm": OpenStreetMap,
    "overture_division": OvertureDivision,
}

def load_config(path):
    """Load a YAML config file and return (repo, databases, boundaries_path, tiles_config)."""
    with open(path) as f:
        config = yaml.safe_load(f)

    repo = config.get("repo", "places.atgeo.org")
    dbs = []
    for entry in config.get("databases", []):
        db_type = entry["type"]
        db_path = entry["path"]
        cls = DATABASE_TYPES.get(db_type)
        if cls is None:
            raise ValueError(f"Unknown database type: {db_type}")
        dbs.append(cls(db_path))

    boundaries_path = config.get("boundaries")

    return repo, dbs, boundaries_path, config.get("tiles")
