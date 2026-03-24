from pathlib import Path
from typing import TypedDict, Optional
import tempfile
import os
import shutil
import unicodedata

import math
import duckdb

DEG_TO_M = 111194.927
DEG_TO_RAD = math.pi / 180

IMPORTANCE_FLOOR_K = 1000
GLOBE_AREA_KM2 = 510_000_000


def compute_importance_floor(area_km2: float, K: float = IMPORTANCE_FLOOR_K) -> int:
    """Compute minimum importance threshold based on search area size."""
    if area_km2 <= 0:
        return 0
    return min(int(4 * math.log(1 + area_km2 / K)), 100)

# SearchParams is a type that holds parameters for spatial queries. The keys are:
# - centroid: a POINT in WKT format (e.g., "POINT(longitude latitude)")
# - xmin, ymin, xmax, ymax: bounding box coordinates
# - limit: maximum number of results to return
class SearchParams(TypedDict, total=False):
    centroid: str  # POINT in WKT format (e.g., "POINT(longitude latitude)")
    xmin: float  # bounding box minimum x coordinate
    ymin: float  # bounding box minimum y coordinate
    xmax: float  # bounding box maximum x coordinate
    ymax: float  # bounding box maximum y coordinate
    limit: int  # maximum number of results to return
    q: Optional[str]  # query string

class Database:
    """DuckDB handler for gazetteer database with spatial capabilities."""
    collection: str = "org.atgeo"

    JACCARD_THRESHOLD = 0.1
    MAX_QUERY_TRIGRAMS = 50

    @staticmethod
    def _strip_accents(s: str) -> str:
        """Remove accent marks from a string using NFD normalization."""
        return "".join(
            c for c in unicodedata.normalize("NFD", s)
            if unicodedata.category(c) != "Mn"
        )

    @staticmethod
    def _compute_trigrams(q: str) -> list:
        """
        Compute sorted, deduplicated trigrams from a query string.
        Lowercases and strips accents, then generates all 3-char substrings
        from the full string (including spaces). Caps at MAX_QUERY_TRIGRAMS.
        """
        s = Database._strip_accents(q.lower())
        trigrams = sorted(set(s[i:i+3] for i in range(len(s) - 2)))
        return trigrams[:Database.MAX_QUERY_TRIGRAMS]

    def __init__(self, db_path):
        """
        Initialize a connection to the gazetteer database.

        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = Path(db_path)
        self.conn = None
        self.temp_dir = None

    def connect(self):
        """Connect to the database and load extensions."""
        if self.conn is None:
            # Connect in read-only mode
            self.conn = duckdb.connect(str(self.db_path), read_only=True)

            # Create a temporary directory for DuckDB to use
            self.temp_dir = tempfile.mkdtemp(prefix='duckdb_temp_')

            # Configure DuckDB to use our writable temp directory
            self.conn.execute(f"SET temp_directory='{self.temp_dir}'")

            # Load extensions — try load first (works when pre-installed),
            # fall back to install+load (works in dev / writable environments).
            self._load_extension("spatial")

            # Validate required schema
            tables = self.conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'name_index'"
            ).fetchall()
            if not tables:
                raise RuntimeError(
                    f"Required table 'name_index' not found in {self.db_path}. "
                    "Run the import script to create the database with trigram indexing."
                )
            columns = self.conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'name_index' AND column_name = 'trigram'"
            ).fetchall()
            if not columns:
                raise RuntimeError(
                    f"Required column 'trigram' not found in name_index in {self.db_path}. "
                    "Re-run the import script with trigram indexing."
                )

        return self.conn

    def _load_extension(self, name: str, repository: str = None):
        """
        Load a DuckDB extension. Tries load_extension first (pre-installed),
        falls back to install_extension + load_extension (writable env).
        """
        try:
            self.conn.load_extension(name)
        except Exception:
            if repository:
                self.conn.install_extension(name, repository=repository)
            else:
                self.conn.install_extension(name)
            self.conn.load_extension(name)

    def close(self):
        """Close the database connection and clean up temp directory."""
        if self.conn:
            self.conn.close()
            self.conn = None

        # Clean up temp directory
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
            except OSError as e:
                print(f"Warning: Could not remove temp directory {self.temp_dir}: {e}")
            finally:
                self.temp_dir = None

    def __del__(self):
        """Cleanup when object is destroyed."""
        self.close()

    def execute(self, query, params=None):
        """Execute a query on the database."""
        if not self.conn:
            self.connect()
        assert self.conn is not None, "Database connection is not established."
        # DuckDB rejects parameters that are not referenced in the query.
        # Filter params to only include keys that appear as $key in the SQL.
        if params:
            import re
            referenced = set(re.findall(r'\$(\w+)', query))
            params = {k: v for k, v in params.items() if k in referenced}
        stmt = self.conn.execute(query, params)
        rows = stmt.fetchall()
        assert stmt.description is not None, "Query did not return any results."
        columns = tuple(c[0] for c in stmt.description)
        return [dict(zip(columns, row)) for row in rows]

    def query_record(self):
        raise NotImplementedError

    def process_record(self, result):
        return {
            "$type": "org.atgeo.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations":  [
                {
                    "$type": "community.lexicon.location.geo",
                    "latitude": result.pop("latitude"),
                    "longitude": result.pop("longitude"),
                }
            ],
            "names": [
                {"text": result.pop("name"), "priority": 0}
            ],
            "attributes": result
        }

    def get_record(self, _repo: str, _collection: str, rkey: str):
        records = self.execute(self.query_record(), {"rkey": rkey})
        return self.process_record(records[0]) if records else None

    def query_nearest(self, _params: SearchParams, trigrams=None):
        raise NotImplementedError

    def process_nearest(self, result):
        # Extract distance before calling process_record
        distance_m = result.pop("distance_m")
        result.pop("score", None)    # Internal scoring, not exposed in API

        # Use the standard record processing
        record = self.process_record(result)

        # Add the distance field
        record["distance_m"] = distance_m

        return record

    def nearest(self, latitude=None, longitude=None, q=None, expand_m=5000, limit=50):
        self.connect()
        params : SearchParams = { "limit": limit }
        if latitude is not None and longitude is not None:
        # Expand the bounding box around the point by roughly expand_m meters
            expand_lat = expand_m / DEG_TO_M
            expand_lon = expand_lat / math.cos(latitude * DEG_TO_RAD) if math.fabs(latitude) < 90 else expand_lat
            bbox = (max((longitude - expand_lon, -180)),
                    max((latitude - expand_lat, -90)),
                    min((longitude + expand_lon, 180)),
                    min((latitude + expand_lat, 90)))
            params.update({
                "centroid": f"POINT({longitude} {latitude})",
                "xmin": bbox[0],
                "ymin": bbox[1],
                "xmax": bbox[2],
                "ymax": bbox[3]
            })
            width_km = (bbox[2] - bbox[0]) * 111 * math.cos(math.radians(latitude))
            height_km = (bbox[3] - bbox[1]) * 111
            area_km2 = width_km * height_km
        else:
            area_km2 = GLOBE_AREA_KM2
        trigrams = None
        if q:
            params["q"] = q
            # Compute trigrams for trigram index path
            trigrams = self._compute_trigrams(q)
            for i, tri in enumerate(trigrams):
                params[f"g{i}"] = tri
            importance_floor = compute_importance_floor(area_km2)
            params["importance_floor"] = importance_floor
        print(f"Searching with params: {params}")
        result = self.execute(
            self.query_nearest(params, trigrams=trigrams), params
        )
        return [self.process_nearest(item) for item in result]


class FoursquareOSP(Database):
    collection = "org.atgeo.places.foursquare"

    def record_columns(self):
        return f"""
            fsq_place_id as rkey,
            fsq_place_id,
            name,
            latitude::decimal(10,6)::varchar as latitude,
            longitude::decimal(10,6)::varchar as longitude,
            address,
            locality,
            postcode,
            region,
            admin_region,
            post_town,
            po_box,
            country,
            date_created,
            date_refreshed,
            tel,
            website,
            email,
            facebook_id,
            instagram,
            twitter,
            fsq_category_ids,
            fsq_category_labels,
            placemaker_url
        """

    def search_columns(self):
        return """
            fsq_place_id as rkey,
            name,
            latitude::decimal(10,6)::varchar as latitude,
            longitude::decimal(10,6)::varchar as longitude,
            address,
            locality,
            postcode,
            region,
            country
        """

    def query_record(self):
        columns = self.record_columns()
        return f"""
            select
                {columns}
            from places
            where fsq_place_id = $rkey
        """

    def _query_trigram_text(self, params: SearchParams, trigrams: list) -> str:
        """
        Text-only trigram search using trigram Jaccard similarity.
        Computes intersection/union directly from name_index rows.
        """
        n_query = len(trigrams)
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        return f"""
            SELECT
                fsq_place_id AS rkey,
                name,
                latitude,
                longitude,
                address,
                locality,
                postcode,
                region,
                country,
                0 AS distance_m,
                count(DISTINCT trigram)::float
                    / (greatest(length(lower(strip_accents(name))) - 2, 1) + {n_query}
                       - count(DISTINCT trigram))::float AS score
            FROM name_index
            WHERE trigram IN ({placeholders})
              AND importance >= $importance_floor
            GROUP BY fsq_place_id, name, latitude, longitude,
                     address, locality, postcode, region, country
            HAVING count(DISTINCT trigram)::float
                / (greatest(length(lower(strip_accents(name))) - 2, 1) + {n_query}
                   - count(DISTINCT trigram))::float >= {self.JACCARD_THRESHOLD}
            ORDER BY score DESC, max(importance) DESC
            LIMIT $limit
        """

    def _query_trigram_spatial(self, params: SearchParams, trigrams: list) -> str:
        """
        Spatial + text trigram search using trigram Jaccard similarity.
        Joins places to name_index with bbox + trigram IN filters.
        No Jaccard threshold — bbox constrains the result set.
        """
        n_query = len(trigrams)
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        return f"""
            SELECT
                p.fsq_place_id AS rkey,
                p.name,
                p.latitude::decimal(10,6)::varchar AS latitude,
                p.longitude::decimal(10,6)::varchar AS longitude,
                p.address,
                p.locality,
                p.postcode,
                p.region,
                p.country,
                min(ST_Distance_Sphere(p.geom, ST_GeomFromText($centroid))::integer) AS distance_m,
                count(DISTINCT n.trigram)::float
                    / (greatest(length(lower(strip_accents(p.name))) - 2, 1) + {n_query}
                       - count(DISTINCT n.trigram))::float AS score
            FROM places p
            JOIN name_index n ON p.fsq_place_id = n.fsq_place_id
            WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
              AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
              AND n.trigram IN ({placeholders})
              AND n.importance >= $importance_floor
            GROUP BY p.fsq_place_id, p.name, p.latitude, p.longitude,
                     p.address, p.locality, p.postcode, p.region, p.country, p.geom
            ORDER BY score DESC, distance_m
            LIMIT $limit
        """

    def query_nearest(self, params: SearchParams, trigrams=None):
        assert "centroid" in params or "q" in params, "Either centroid or q must be provided"

        has_spatial = "centroid" in params
        has_text = "q" in params

        if has_text and has_spatial:
            return self._query_trigram_spatial(params, trigrams)
        elif has_text:
            return self._query_trigram_text(params, trigrams)
        else:
            # Spatial-only
            columns = self.search_columns()
            return f"""
                select
                    {columns},
                    ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer as distance_m
                from places
                where bbox.xmin > $xmin and bbox.ymin > $ymin
                  and bbox.xmax < $xmax and bbox.ymax < $ymax
                order by distance_m
                limit $limit;
            """

    def process_record(self, result):
        locations = [
            {
                "$type": "community.lexicon.location.geo",
                "latitude": result.pop("latitude"),
                "longitude": result.pop("longitude"),
            }
        ]

        # Create address lexicon object if address data is available
        address_data = {}
        address_map = [
            ("country", "country"),
            ("postcode", "postalCode"),
            ("region", "region"),
            ("locality", "locality"),
            ("address", "street"),
        ]
        for src_key, dest_key in address_map:
            if result.get(src_key):
                address_data[dest_key] = result.pop(src_key)

        # Add address to locations if we have at least the required country field
        if address_data.get("country"):
            locations.append({
                "$type": "community.lexicon.location.address",
                **address_data
            })

        return {
            "$type": "org.atgeo.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations": locations,
            "names": [
                {"text": result.pop("name"), "priority": 0}
            ],
            "attributes": result
        }

class OvertureMaps(Database):
    collection = "org.atgeo.places.overture"

    def record_columns(self):
        return f"""
            id as rkey,
            id,
            names.primary as name,
            st_y(st_centroid(geometry))::decimal(10,6)::varchar as latitude,
            st_x(st_centroid(geometry))::decimal(10,6)::varchar as longitude,
            names,
            categories,
            addresses,
            websites,
            socials,
            emails,
            phones,
            brand,
            confidence::decimal(4,3)::varchar as confidence,
            version,
            sources
        """

    def search_columns(self):
        return """
            id as rkey,
            names.primary as name,
            st_y(st_centroid(geometry))::decimal(10,6)::varchar as latitude,
            st_x(st_centroid(geometry))::decimal(10,6)::varchar as longitude,
            addresses
        """

    def query_record(self):
        columns = self.record_columns()
        return f"""
            select
                {columns}
            from places
            where id = $rkey
        """

    def _query_trigram_text(self, params: SearchParams, trigrams: list) -> str:
        """
        Text-only trigram search using trigram Jaccard similarity.
        Computes intersection/union directly from name_index rows.
        """
        n_query = len(trigrams)
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        return f"""
            SELECT
                id AS rkey,
                name,
                latitude,
                longitude,
                NULL AS addresses,
                0 AS distance_m,
                count(DISTINCT trigram)::float
                    / (greatest(length(lower(strip_accents(name))) - 2, 1) + {n_query}
                       - count(DISTINCT trigram))::float AS score
            FROM name_index
            WHERE trigram IN ({placeholders})
              AND importance >= $importance_floor
            GROUP BY id, name, latitude, longitude
            HAVING count(DISTINCT trigram)::float
                / (greatest(length(lower(strip_accents(name))) - 2, 1) + {n_query}
                   - count(DISTINCT trigram))::float >= {self.JACCARD_THRESHOLD}
            ORDER BY score DESC, max(importance) DESC
            LIMIT $limit
        """

    def _query_trigram_spatial(self, params: SearchParams, trigrams: list) -> str:
        """
        Spatial + text trigram search using trigram Jaccard similarity.
        Joins places to name_index with bbox + trigram IN filters.
        No Jaccard threshold — bbox constrains the result set.
        """
        n_query = len(trigrams)
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        return f"""
            SELECT
                p.id AS rkey,
                p.names.primary AS name,
                st_y(st_centroid(p.geometry))::decimal(10,6)::varchar AS latitude,
                st_x(st_centroid(p.geometry))::decimal(10,6)::varchar AS longitude,
                p.addresses,
                min(ST_Distance_Sphere(p.geometry, ST_GeomFromText($centroid))::integer) AS distance_m,
                count(DISTINCT n.trigram)::float
                    / (greatest(length(lower(strip_accents(p.names.primary))) - 2, 1) + {n_query}
                       - count(DISTINCT n.trigram))::float AS score
            FROM places p
            JOIN name_index n ON p.id = n.id
            WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
              AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
              AND n.trigram IN ({placeholders})
              AND n.importance >= $importance_floor
            GROUP BY p.id, p.names, p.geometry, p.addresses
            ORDER BY score DESC, distance_m
            LIMIT $limit
        """

    def query_nearest(self, params: SearchParams, trigrams=None):
        assert "centroid" in params or "q" in params, "Either centroid or q must be provided"

        has_spatial = "centroid" in params
        has_text = "q" in params

        if has_text and has_spatial:
            return self._query_trigram_spatial(params, trigrams)
        elif has_text:
            return self._query_trigram_text(params, trigrams)
        else:
            # Spatial-only
            columns = self.search_columns()
            return f"""
                select
                    {columns},
                    ST_Distance_Sphere(geometry, ST_GeomFromText($centroid))::integer as distance_m
                from places
                where bbox.xmin > $xmin and bbox.ymin > $ymin
                  and bbox.xmax < $xmax and bbox.ymax < $ymax
                order by distance_m
                limit $limit;
            """

    def process_record(self, result):
        locations = [
            {
                "$type": "community.lexicon.location.geo",
                "latitude": result.pop("latitude"),
                "longitude": result.pop("longitude"),
            }
        ]

        # Extract address information from addresses array if available
        addresses = result.get("addresses")
        if addresses and isinstance(addresses, list):
            for address in addresses:
                address_data = {}
                address_map = [
                    ("country", "country"),
                    ("postcode", "postalCode"),
                    ("locality", "locality"),
                    ("freeform", "street"),
                ]
                for src_key, dest_key in address_map:
                    if address.get(src_key):
                        address_data[dest_key] = address[src_key]

                # Handle region separately due to country prefix parsing
                if address.get("region"):
                    region = address["region"]
                    if "-" in region:
                        address_data["region"] = region.split("-", 1)[1]
                    else:
                        address_data["region"] = region

                # Add address to locations if we have at least the required country field
                if address_data.get("country"):
                    locations.append({
                        "$type": "community.lexicon.location.address",
                        **address_data
                    })

            # Remove addresses from result to avoid duplication in attributes
            result.pop("addresses")

        return {
            "$type": "org.atgeo.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations": locations,
            "names": [
                {"text": result.pop("name"), "priority": 0}
            ],
            "attributes": result
        }

if __name__ == "__main__":
    from pprint import pprint

    d = FoursquareOSP("db/fsq-osp.duckdb")
    result = d.nearest(37.776145, -122.433898)
    pprint(result)
    d.close()

    d = OvertureMaps("db/overture-maps.duckdb")
    result = d.nearest(37.776145, -122.433898)
    pprint(result)

    record = d.get_record("", "org.overturemaps.places", result[0]["rkey"])
    pprint(record)

    d.close()
