"""Admin boundary lookup and record resolution."""
import tempfile
from pathlib import Path

import duckdb

from .database import Database


class BoundaryLookup:
    """Point-in-polygon lookup against Overture division boundaries.

    Queries a boundaries.duckdb file produced by the overture_division pipeline
    stage. The database schema uses a `places` table with columns `id`,
    `geometry`, and `admin_level` — matching the division export schema.
    """

    # Collection used to qualify rkeys in containment output.
    # Matches the overture_division pipeline collection name.
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

        Only rkey is returned. Name, level, and other division metadata are
        available from the division tile for that rkey; clients resolve them
        from the admin tile layer rather than duplicating them in every
        containment relation.
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


class OvertureDivision(Database):
    """Minimal Database subclass for Overture division record resolution.

    Supports get_record only. No search, no name_index.
    """

    collection = "org.atgeo.places.overture.division"
    attribution = "https://docs.overturemaps.org/attribution/"

    def connect(self):
        """Connect to boundary database (no name_index validation)."""
        if self.conn is None:
            self.conn = duckdb.connect(str(self.db_path), read_only=True)
            self.temp_dir = tempfile.mkdtemp(prefix='duckdb_temp_')
            self.conn.execute(f"SET temp_directory='{self.temp_dir}'")
            # Spatial extension is required to open files containing GEOMETRY
            # columns — DuckDB cannot deserialize the type without it, even if
            # no ST_* functions are called.
            self._load_extension("spatial")
        return self.conn

    def record_columns(self):
        return """
            id AS rkey,
            names,
            subtype,
            country,
            region,
            admin_level,
            wikidata,
            population,
            min_latitude::decimal(10,6)::varchar AS min_latitude,
            min_longitude::decimal(10,6)::varchar AS min_longitude,
            max_latitude::decimal(10,6)::varchar AS max_latitude,
            max_longitude::decimal(10,6)::varchar AS max_longitude,
            variants
        """

    def query_record(self):
        columns = self.record_columns()
        return f"""
            SELECT {columns}, importance
            FROM places
            WHERE id = $rkey
        """

    def process_record(self, result):
        # Locations: bbox only (divisions are areas, not points)
        locations = []
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

        # Parse names struct into primary name + variants
        names = result.pop("names", None)
        name = None
        variants = []
        if names:
            name = names.get("primary")
            common = names.get("common")
            if common and isinstance(common, dict):
                for lang, lang_name in common.items():
                    if lang_name and lang_name != name:
                        variants.append({"name": lang_name, "language": lang})
            rules = names.get("rules")
            if rules:
                for rule in rules:
                    entry = {"name": rule["value"]}
                    if rule.get("language"):
                        entry["language"] = rule["language"]
                    if rule.get("variant"):
                        entry["type"] = rule["variant"]
                    variants.append(entry)

        # Note: pre-computed variants column is intentionally ignored to avoid
        # duplication if the import pipeline later adds variant extraction.
        # Names struct is the single source of truth for variants.
        result.pop("variants", None)

        # Build attributes
        subtype = result.pop("subtype", None)
        country = result.pop("country", None)
        region = result.pop("region", None)
        admin_level = result.pop("admin_level", None)
        wikidata = result.pop("wikidata", None)
        population = result.pop("population", None)

        attributes = {}
        if subtype:
            attributes["subtype"] = subtype
        if country:
            attributes["country"] = country
        if region:
            attributes["region"] = region
        if admin_level is not None:
            attributes["admin_level"] = admin_level
        if wikidata:
            attributes["wikidata"] = wikidata
        if population is not None and population > 0:
            attributes["population"] = population

        return {
            "$type": "org.atgeo.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations": locations,
            "name": name or "",
            "variants": variants,
            "attributes": attributes,
        }

    def query_nearest(self, _params, trigrams=None):
        raise NotImplementedError("Division collection does not support search")
