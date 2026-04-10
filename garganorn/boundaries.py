"""Admin boundary lookup and record resolution."""
import json
import tempfile
from pathlib import Path

import duckdb

from .database import Database


class BoundaryLookup:
    """Point-in-polygon lookup against admin boundaries."""

    COLLECTION = "org.atgeo.places.overture.division"

    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.conn = None

    def connect(self):
        if self.conn is None:
            self.conn = duckdb.connect(str(self.db_path), read_only=True)
            try:
                self.conn.load_extension("spatial")
            except Exception:
                self.conn.install_extension("spatial")
                self.conn.load_extension("spatial")
        return self.conn

    def containment(self, lat, lon):
        """Return all admin regions containing the given point.

        Returns a list of dicts ordered by admin_level ascending (continent
        first, most specific last), each containing:
            rkey: collection-qualified rkey (org.atgeo.places.overture.division:<id>)
        """
        conn = self.connect()
        rows = conn.execute("""
            SELECT id FROM places
            WHERE ST_Contains(geometry, ST_Point($lon, $lat))
            ORDER BY admin_level ASC
        """, {"lat": lat, "lon": lon}).fetchall()
        return [{"rkey": f"{self.COLLECTION}:{r[0]}"} for r in rows]

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None


class WhosOnFirst(Database):
    """Minimal Database subclass for WoF boundary record resolution.

    Supports get_record only. No search, no name_index.
    """

    collection = "org.atgeo.places.wof"

    def connect(self):
        """Connect to boundary database (no name_index validation)."""
        if self.conn is None:
            self.conn = duckdb.connect(str(self.db_path), read_only=True)
            self.temp_dir = tempfile.mkdtemp(prefix='duckdb_temp_')
            self.conn.execute(f"SET temp_directory='{self.temp_dir}'")
        return self.conn

    def record_columns(self):
        return """
            rkey,
            name,
            latitude::decimal(10,6)::varchar AS latitude,
            longitude::decimal(10,6)::varchar AS longitude,
            placetype,
            level,
            country,
            min_latitude::decimal(10,6)::varchar AS min_latitude,
            min_longitude::decimal(10,6)::varchar AS min_longitude,
            max_latitude::decimal(10,6)::varchar AS max_latitude,
            max_longitude::decimal(10,6)::varchar AS max_longitude,
            names_json,
            concordances
        """

    def query_record(self):
        columns = self.record_columns()
        return f"""
            SELECT {columns}, 0 AS importance
            FROM boundaries
            WHERE rkey = $rkey
        """

    def process_record(self, result):
        locations = [
            {
                "$type": "community.lexicon.location.geo",
                "latitude": result.pop("latitude"),
                "longitude": result.pop("longitude"),
            }
        ]

        # Add bbox from boundary extents
        min_lat = result.pop("min_latitude", None)
        min_lon = result.pop("min_longitude", None)
        max_lat = result.pop("max_latitude", None)
        max_lon = result.pop("max_longitude", None)
        if all(v is not None for v in [min_lat, min_lon, max_lat, max_lon]):
            locations.append({
                "$type": "community.lexicon.location.bbox",
                "north": max_lat,
                "west": min_lon,
                "south": min_lat,
                "east": max_lon,
            })

        # Parse multilingual names into variants
        variants = []
        names_json = result.pop("names_json", None)
        if names_json:
            for entry in json.loads(names_json):
                variant = {"name": entry["name"]}
                if entry.get("language"):
                    variant["language"] = entry["language"]
                if entry.get("variant"):
                    variant["type"] = entry["variant"]
                variants.append(variant)

        # Build attributes
        concordances_str = result.pop("concordances", None)
        placetype = result.pop("placetype", None)
        level = result.pop("level", None)
        country = result.pop("country", None)

        attributes = {}
        if placetype:
            attributes["placetype"] = placetype
        if level is not None:
            attributes["level"] = level
        if country:
            attributes["country"] = country
        if concordances_str:
            attributes["concordances"] = json.loads(concordances_str)

        return {
            "$type": "org.atgeo.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations": locations,
            "name": result.pop("name"),
            "variants": variants,
            "attributes": attributes,
        }

    def query_nearest(self, _params, trigrams=None):
        raise NotImplementedError("WoF boundary collection does not support search")
