from pathlib import Path
from typing import TypedDict, Optional
import tempfile
import os
import shutil

import math
import duckdb

DEG_TO_M = 111194.927
DEG_TO_RAD = math.pi / 180

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
    collection: str = "social.gazetteer"

    JACCARD_THRESHOLD = 0.1
    MAX_QUERY_TOKENS = 7
    _JOIN_ALIASES = "abcdefg"

    @staticmethod
    def _build_name_index_join(n_tokens: int, join_key: str) -> str:
        aliases = Database._JOIN_ALIASES
        first = aliases[0]
        lines = [f"FROM name_index {first}"]
        for i in range(1, n_tokens):
            alias = aliases[i]
            lines.append(f"JOIN name_index {alias} ON {first}.{join_key} = {alias}.{join_key}")
        where_clauses = [f"{aliases[i]}.token = lower(strip_accents($t{i}))" for i in range(n_tokens)]
        lines.append("WHERE " + "\n  AND ".join(where_clauses))
        lines.append(f"ORDER BY {first}.importance DESC")
        lines.append("LIMIT $limit")
        return "\n".join(lines)

    def __init__(self, db_path):
        """
        Initialize a connection to the gazetteer database.

        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = Path(db_path)
        self.conn = None
        self.temp_dir = None
        self.has_name_index = False
        self.has_phonetic_index = False

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

            # Detect name_index table
            tables = self.conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = 'name_index'"
            ).fetchall()
            self.has_name_index = len(tables) > 0

            # Detect phonetic columns in name_index
            self.has_phonetic_index = False
            if self.has_name_index:
                columns = self.conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'name_index' AND column_name = 'dm_code'"
                ).fetchall()
                self.has_phonetic_index = len(columns) > 0

            # Load splink_udfs only if we have a phonetic index to query
            if self.has_phonetic_index:
                try:
                    self._load_extension("splink_udfs", repository="community")
                except Exception as e:
                    # If splink_udfs can't load, fall back to non-phonetic search.
                    # The name_index still has token columns for exact-match search.
                    print(f"Warning: splink_udfs extension not available ({e}). "
                          f"Phonetic search disabled, falling back to token search.")
                    self.has_phonetic_index = False

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

    def _tokenize_query(self, q: str) -> list:
        """
        Tokenize a query string the same way the import scripts do:
        lowercase, split on spaces, filter tokens with length <= 1.
        Caps at MAX_QUERY_TOKENS, preferring longest tokens.
        """
        tokens = [t for t in q.lower().split() if len(t) > 1]
        if len(tokens) > self.MAX_QUERY_TOKENS:
            tokens = sorted(tokens, key=len, reverse=True)[:self.MAX_QUERY_TOKENS]
        return tokens

    def _compute_phonetic_codes(self, q: str) -> list:
        """
        Compute phonetic codes for a query string using DuckDB's
        double_metaphone(). Returns a deduplicated list of non-empty codes.

        Uses DuckDB to guarantee identical encoding to what was used at index
        build time. Requires splink_udfs to be loaded on self.conn.
        """
        tokens = self._tokenize_query(q)
        if not tokens:
            return []

        # Build a single SQL that computes dm codes for all tokens at once.
        # Uses VALUES clause to pass tokens, then unnest double_metaphone.
        values = ", ".join(f"($t{i})" for i in range(len(tokens)))
        params = {f"t{i}": token for i, token in enumerate(tokens)}

        rows = self.conn.execute(f"""
            SELECT DISTINCT code
            FROM (
                SELECT unnest(double_metaphone(col0)) AS code
                FROM (VALUES {values}) AS t(col0)
                WHERE length(col0) > 1
            ) sub
            WHERE code IS NOT NULL AND code != ''
        """, params).fetchall()

        return [row[0] for row in rows]

    def query_record(self):
        raise NotImplementedError

    def process_record(self, result):
        return {
            "$type": "community.lexicon.location.place",
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

    def query_nearest(self, _params: SearchParams):
        raise NotImplementedError

    def process_nearest(self, result):
        # Extract distance before calling process_record
        distance_m = result.pop("distance_m")
        result.pop("jaccard", None)  # Internal scoring, not exposed in API

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
        if q:
            params["q"] = q
            # Tokenize and bind t0, t1, ... for multi-token name_index self-join.
            tokens = self._tokenize_query(q)
            for i, token in enumerate(tokens):
                params[f"t{i}"] = token
        print(f"Searching with params: {params}")
        result = self.execute(
            self.query_nearest(params), params
        )
        return [self.process_nearest(item) for item in result]


class FoursquareOSP(Database):
    collection = "community.lexicon.location.com.foursquare.places"

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

    def _query_name_index(self, params: SearchParams) -> str:
        """
        Multi-token self-join text-only search against name_index.
        Used as fallback when phonetic index is not available.
        """
        tokens = [v for k, v in sorted(
            ((k, v) for k, v in params.items() if k.startswith("t") and k[1:].isdigit()),
            key=lambda kv: int(kv[0][1:])
        )]
        if not tokens:
            return "SELECT NULL WHERE false"

        select_clause = """SELECT
        a.fsq_place_id AS rkey,
        a.name,
        a.latitude,
        a.longitude,
        a.address,
        a.locality,
        a.postcode,
        a.region,
        a.country,
        0 AS distance_m"""

        join_clause = self._build_name_index_join(len(tokens), "fsq_place_id")
        return f"{select_clause}\n{join_clause};"

    def _query_phonetic_name_index(self, params: SearchParams) -> str:
        """
        Build a text-only phonetic search query against name_index.
        Uses Jaccard similarity on double_metaphone codes.
        Mutates params to add c0, c1, ... phonetic code parameters.
        """
        codes = self._compute_phonetic_codes(params["q"])
        if not codes:
            return "SELECT NULL as rkey, NULL as name, NULL as latitude, NULL as longitude, NULL as address, NULL as locality, NULL as postcode, NULL as region, NULL as country, 0 as distance_m WHERE false"

        n_query_codes = len(codes)

        # Bind phonetic codes as parameters c0, c1, ...
        for i, code in enumerate(codes):
            params[f"c{i}"] = code

        code_placeholders = ", ".join(f"$c{i}" for i in range(len(codes)))

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
                count(DISTINCT dm_code)::float
                    / (n_place_codes + {n_query_codes}
                       - count(DISTINCT dm_code))::float AS jaccard
            FROM name_index
            WHERE dm_code IN ({code_placeholders})
            GROUP BY fsq_place_id, name, latitude, longitude,
                     address, locality, postcode, region, country,
                     n_place_codes
            HAVING count(DISTINCT dm_code)::float
                / (n_place_codes + {n_query_codes}
                   - count(DISTINCT dm_code))::float >= {self.JACCARD_THRESHOLD}
            ORDER BY jaccard DESC, max(importance) DESC
            LIMIT $limit
        """

    def _query_phonetic_spatial(self, params: SearchParams) -> str:
        """
        Build a spatial + phonetic text query.
        Uses bbox spatial filter + dm_code IN filter + Jaccard scoring.
        Mutates params to add c0, c1, ... phonetic code parameters.
        """
        codes = self._compute_phonetic_codes(params["q"])
        if not codes:
            return "SELECT NULL as rkey, NULL as name, NULL as latitude, NULL as longitude, NULL as address, NULL as locality, NULL as postcode, NULL as region, NULL as country, 0 as distance_m WHERE false"

        n_query_codes = len(codes)

        for i, code in enumerate(codes):
            params[f"c{i}"] = code

        code_placeholders = ", ".join(f"$c{i}" for i in range(len(codes)))

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
                ST_Distance_Sphere(p.geom, ST_GeomFromText($centroid))::integer AS distance_m,
                count(DISTINCT n.dm_code)::float
                    / (n.n_place_codes + {n_query_codes}
                       - count(DISTINCT n.dm_code))::float AS jaccard
            FROM places p
            JOIN name_index n ON p.fsq_place_id = n.fsq_place_id
            WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
              AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
              AND n.dm_code IN ({code_placeholders})
            GROUP BY p.fsq_place_id, p.name, p.latitude, p.longitude,
                     p.address, p.locality, p.postcode, p.region, p.country,
                     p.geom, n.n_place_codes
            HAVING count(DISTINCT n.dm_code)::float
                / (n.n_place_codes + {n_query_codes}
                   - count(DISTINCT n.dm_code))::float >= {self.JACCARD_THRESHOLD}
            ORDER BY jaccard DESC, distance_m
            LIMIT $limit
        """

    def query_nearest(self, params: SearchParams):
        assert "centroid" in params or "q" in params, "Either centroid or q must be provided for nearest search."
        columns = self.search_columns()
        if params.get("centroid"):
            distance_m = "ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer"
            spatial_filter = "bbox.xmin > $xmin and bbox.ymin > $ymin and bbox.xmax < $xmax and bbox.ymax < $ymax"
        else:
            distance_m = "0"
            spatial_filter = ""
        if params.get("q"):
            if spatial_filter:
                # Spatial + text: use phonetic spatial query if available,
                # otherwise fall back to ILIKE on the bbox-filtered set.
                if self.has_phonetic_index:
                    return self._query_phonetic_spatial(params)
                text_filter = "name ILIKE '%' || $q || '%'"
            else:
                # Text-only: use the sorted name_index table for fast
                # lookup via zone maps.
                if self.has_name_index:
                    if self.has_phonetic_index:
                        return self._query_phonetic_name_index(params)
                    return self._query_name_index(params)
                # No name_index at all: fall back to full scan ILIKE
                return f"""
                    select
                        {columns},
                        0 as distance_m
                    from places
                    where name ILIKE '%' || $q || '%'
                    order by name
                    limit $limit;
                """
        else:
            text_filter = ""
        filter_conditions = " and ".join(filter(None, (spatial_filter, text_filter)))

        return f"""
            select
                {columns},
                {distance_m} as distance_m
            from places
            where {filter_conditions}
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
            "$type": "community.lexicon.location.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations": locations,
            "names": [
                {"text": result.pop("name"), "priority": 0}
            ],
            "attributes": result
        }

class OvertureMaps(Database):
    collection = "community.lexicon.location.org.overturemaps.places"

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

    def _query_name_index(self, params: SearchParams) -> str:
        """
        Multi-token self-join text-only search against name_index.
        Used as fallback when phonetic index is not available.
        """
        tokens = [v for k, v in sorted(
            ((k, v) for k, v in params.items() if k.startswith("t") and k[1:].isdigit()),
            key=lambda kv: int(kv[0][1:])
        )]
        if not tokens:
            return "SELECT NULL WHERE false"

        select_clause = """SELECT
        a.id AS rkey,
        a.name,
        a.latitude,
        a.longitude,
        NULL AS addresses,
        0 AS distance_m"""

        join_clause = self._build_name_index_join(len(tokens), "id")
        return f"{select_clause}\n{join_clause};"

    def _query_phonetic_name_index(self, params: SearchParams) -> str:
        """
        Build a text-only phonetic search query against name_index.
        Uses Jaccard similarity on double_metaphone codes.
        Mutates params to add c0, c1, ... phonetic code parameters.
        """
        codes = self._compute_phonetic_codes(params["q"])
        if not codes:
            return "SELECT NULL as rkey, NULL as name, NULL as latitude, NULL as longitude, NULL as addresses, 0 as distance_m WHERE false"

        n_query_codes = len(codes)

        for i, code in enumerate(codes):
            params[f"c{i}"] = code

        code_placeholders = ", ".join(f"$c{i}" for i in range(len(codes)))

        return f"""
            SELECT
                id AS rkey,
                name,
                latitude,
                longitude,
                NULL AS addresses,
                0 AS distance_m,
                count(DISTINCT dm_code)::float
                    / (n_place_codes + {n_query_codes}
                       - count(DISTINCT dm_code))::float AS jaccard
            FROM name_index
            WHERE dm_code IN ({code_placeholders})
            GROUP BY id, name, latitude, longitude, n_place_codes
            HAVING count(DISTINCT dm_code)::float
                / (n_place_codes + {n_query_codes}
                   - count(DISTINCT dm_code))::float >= {self.JACCARD_THRESHOLD}
            ORDER BY jaccard DESC, max(importance) DESC
            LIMIT $limit
        """

    def _query_phonetic_spatial(self, params: SearchParams) -> str:
        """
        Build a spatial + phonetic text query.
        Uses bbox spatial filter + dm_code IN filter + Jaccard scoring.
        Mutates params to add c0, c1, ... phonetic code parameters.
        """
        codes = self._compute_phonetic_codes(params["q"])
        if not codes:
            return "SELECT NULL as rkey, NULL as name, NULL as latitude, NULL as longitude, NULL as addresses, 0 as distance_m WHERE false"

        n_query_codes = len(codes)

        for i, code in enumerate(codes):
            params[f"c{i}"] = code

        code_placeholders = ", ".join(f"$c{i}" for i in range(len(codes)))

        return f"""
            SELECT
                p.id AS rkey,
                p.names.primary AS name,
                st_y(st_centroid(p.geometry))::decimal(10,6)::varchar AS latitude,
                st_x(st_centroid(p.geometry))::decimal(10,6)::varchar AS longitude,
                p.addresses,
                ST_Distance_Sphere(p.geometry, ST_GeomFromText($centroid))::integer AS distance_m,
                count(DISTINCT n.dm_code)::float
                    / (n.n_place_codes + {n_query_codes}
                       - count(DISTINCT n.dm_code))::float AS jaccard
            FROM places p
            JOIN name_index n ON p.id = n.id
            WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
              AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
              AND n.dm_code IN ({code_placeholders})
            GROUP BY p.id, p.names, p.geometry, p.addresses, n.n_place_codes
            HAVING count(DISTINCT n.dm_code)::float
                / (n.n_place_codes + {n_query_codes}
                   - count(DISTINCT n.dm_code))::float >= {self.JACCARD_THRESHOLD}
            ORDER BY jaccard DESC, distance_m
            LIMIT $limit
        """

    def query_nearest(self, params: SearchParams):
        columns = self.search_columns()
        if params.get("centroid"):
            distance_m = "ST_Distance_Sphere(geometry, ST_GeomFromText($centroid))::integer"
            spatial_filter = "bbox.xmin > $xmin and bbox.ymin > $ymin and bbox.xmax < $xmax and bbox.ymax < $ymax"
        else:
            distance_m = "0"
            spatial_filter = ""
        if params.get("q"):
            if spatial_filter:
                # Spatial + text: use phonetic spatial query if available,
                # otherwise fall back to ILIKE on the bbox-filtered set.
                if self.has_phonetic_index:
                    return self._query_phonetic_spatial(params)
                text_filter = "names.primary ILIKE '%' || $q || '%'"
            else:
                # Text-only: use the sorted name_index for fast lookup.
                if self.has_name_index:
                    if self.has_phonetic_index:
                        return self._query_phonetic_name_index(params)
                    return self._query_name_index(params)
                # No name_index at all: fall back to full scan
                text_filter = "names.primary ILIKE '%' || $q || '%'"
        else:
            text_filter = ""
        filter_conditions = " and ".join(filter(None, (spatial_filter, text_filter)))
        return f"""
            select
                {columns},
                {distance_m} as distance_m
            from places
            where {filter_conditions}
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
            "$type": "community.lexicon.location.place",
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
