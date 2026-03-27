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
    return min(int(4 * math.log(1 + area_km2 / K)), 50)

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

    JW_THRESHOLD = 0.6
    JW_TOKEN_ALPHA = 0.5
    MAX_QUERY_TRIGRAMS = 50
    MAX_QUERY_TOKENS = 12

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
            "name": result.pop("name"),
            "variants": [],
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

    def nearest(self, bbox=None, q=None, limit=50):
        self.connect()
        params: SearchParams = {"limit": limit}
        if bbox is not None:
            xmin, ymin, xmax, ymax = bbox
            mid_lon = (xmin + xmax) / 2
            mid_lat = (ymin + ymax) / 2
            params.update({
                "centroid": f"POINT({mid_lon} {mid_lat})",
                "xmin": xmin,
                "ymin": ymin,
                "xmax": xmax,
                "ymax": ymax,
            })
            width_km = (xmax - xmin) * 111 * math.cos(math.radians(mid_lat))
            height_km = (ymax - ymin) * 111
            area_km2 = width_km * height_km
        else:
            area_km2 = GLOBE_AREA_KM2
        trigrams = None
        if q:
            norm_q = Database._strip_accents(q.lower())
            params["norm_q"] = norm_q
            trigrams = self._compute_trigrams(q)
            for i, tri in enumerate(trigrams):
                params[f"g{i}"] = tri
            importance_floor = compute_importance_floor(area_km2)
            params["importance_floor"] = importance_floor
            tokens = [t for t in norm_q.split() if t][:Database.MAX_QUERY_TOKENS]
            for i, token in enumerate(tokens):
                params[f"t{i}"] = token
        print(f"Searching with params: {params}")
        result = self.execute(
            self.query_nearest(params, trigrams=trigrams), params
        )

        if q:
            # Text/spatial+text: query returns minimal columns (rkey, name, distance_m, score).
            # Hydrate rkeys to get full records with display columns.
            if not result:
                return []
            rkeys = [row["rkey"] for row in result]
            distances = {row["rkey"]: row["distance_m"] for row in result}

            h_params = {f"h{i}": rk for i, rk in enumerate(rkeys)}
            full_rows = self.execute(self.query_hydrate(len(rkeys)), h_params)
            full_map = {}
            for row in full_rows:
                raw_rkey = row["rkey"]
                record = self.process_record(row)
                full_map[raw_rkey] = record

            # Build ordered result list preserving search ranking
            records = []
            for rkey in rkeys:
                if rkey in full_map:
                    record = full_map[rkey]
                    record["distance_m"] = distances.get(rkey, 0)
                    records.append(record)
            return records
        else:
            # Spatial-only: record_columns() already selected, no hydration needed
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

    def query_hydrate(self, count):
        columns = self.record_columns()
        placeholders = ", ".join(f"$h{i}" for i in range(count))
        return f"SELECT {columns} FROM places WHERE fsq_place_id IN ({placeholders})"

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
        Text-only trigram search using Jaro-Winkler similarity.
        Candidates are retrieved via trigram pre-filter; JW scores the outer query.
        Multi-token queries use a top-N CTE chain for token-level blending.
        """
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        token_count = sum(1 for k in params if k.startswith('t') and k[1:].isdigit())
        if token_count > 1:
            top_n = min(max(params.get("limit", 50) * 20, 200), 2000)
            token_values = ", ".join(f"($t{i})" for i in range(token_count))
            alpha = self.JW_TOKEN_ALPHA
            return f"""
                WITH candidates AS (
                    SELECT DISTINCT fsq_place_id, name, norm_name, importance
                    FROM name_index
                    WHERE trigram IN ({placeholders})
                      AND importance >= $importance_floor
                ),
                ranked AS (
                    SELECT c.*,
                        jaro_winkler_similarity($norm_q, c.norm_name) AS full_jw
                    FROM candidates c
                    ORDER BY full_jw DESC
                    LIMIT {top_n}
                ),
                name_tokens AS (
                    SELECT r.fsq_place_id, r.name,
                        unnest(list_filter(string_split(r.norm_name, ' '), x -> x != '')) AS nt
                    FROM ranked r
                ),
                token_scores AS (
                    SELECT nt.fsq_place_id, nt.name, q.qt,
                        max(jaro_winkler_similarity(q.qt, nt.nt)) AS best
                    FROM name_tokens nt
                    CROSS JOIN (VALUES {token_values}) AS q(qt)
                    GROUP BY nt.fsq_place_id, nt.name, q.qt
                ),
                token_avg AS (
                    SELECT fsq_place_id, name, avg(best) AS token_jw
                    FROM token_scores
                    GROUP BY fsq_place_id, name
                ),
                scored AS (
                    SELECT r.*,
                        {alpha} * r.full_jw + {1 - alpha} * COALESCE(t.token_jw, r.full_jw) AS score
                    FROM ranked r
                    LEFT JOIN token_avg t ON r.fsq_place_id = t.fsq_place_id AND r.name = t.name
                )
                SELECT
                    s.fsq_place_id AS rkey,
                    s.name,
                    0 AS distance_m,
                    s.score
                FROM scored s
                WHERE s.score >= {self.JW_THRESHOLD}
                ORDER BY s.score DESC, s.importance DESC
                LIMIT $limit
            """
        return f"""
            WITH candidates AS (
                SELECT DISTINCT fsq_place_id, name, norm_name, importance
                FROM name_index
                WHERE trigram IN ({placeholders})
                  AND importance >= $importance_floor
            )
            SELECT
                c.fsq_place_id AS rkey,
                c.name,
                0 AS distance_m,
                jaro_winkler_similarity($norm_q, c.norm_name) AS score
            FROM candidates c
            WHERE jaro_winkler_similarity($norm_q, c.norm_name) >= {self.JW_THRESHOLD}
            ORDER BY score DESC, c.importance DESC
            LIMIT $limit
        """

    def _query_trigram_spatial(self, params: SearchParams, trigrams: list) -> str:
        """
        Spatial + text trigram search using Jaro-Winkler similarity.
        Joins places to name_index with bbox + trigram IN filters.
        Multi-token queries use a top-N CTE chain for token-level blending.
        """
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        token_count = sum(1 for k in params if k.startswith('t') and k[1:].isdigit())
        if token_count > 1:
            top_n = min(max(params.get("limit", 50) * 20, 200), 2000)
            token_values = ", ".join(f"($t{i})" for i in range(token_count))
            alpha = self.JW_TOKEN_ALPHA
            return f"""
                WITH candidates AS (
                    SELECT DISTINCT p.fsq_place_id, p.name, n.norm_name,
                        p.geom, n.importance
                    FROM places p
                    JOIN name_index n ON p.fsq_place_id = n.fsq_place_id
                    WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
                      AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
                      AND n.trigram IN ({placeholders})
                      AND n.importance >= $importance_floor
                ),
                ranked AS (
                    SELECT c.*,
                        jaro_winkler_similarity($norm_q, c.norm_name) AS full_jw
                    FROM candidates c
                    ORDER BY full_jw DESC
                    LIMIT {top_n}
                ),
                name_tokens AS (
                    SELECT r.fsq_place_id, r.name,
                        unnest(list_filter(string_split(r.norm_name, ' '), x -> x != '')) AS nt
                    FROM ranked r
                ),
                token_scores AS (
                    SELECT nt.fsq_place_id, nt.name, q.qt,
                        max(jaro_winkler_similarity(q.qt, nt.nt)) AS best
                    FROM name_tokens nt
                    CROSS JOIN (VALUES {token_values}) AS q(qt)
                    GROUP BY nt.fsq_place_id, nt.name, q.qt
                ),
                token_avg AS (
                    SELECT fsq_place_id, name, avg(best) AS token_jw
                    FROM token_scores
                    GROUP BY fsq_place_id, name
                ),
                scored AS (
                    SELECT r.*,
                        {alpha} * r.full_jw + {1 - alpha} * COALESCE(t.token_jw, r.full_jw) AS score
                    FROM ranked r
                    LEFT JOIN token_avg t ON r.fsq_place_id = t.fsq_place_id AND r.name = t.name
                )
                SELECT
                    fsq_place_id AS rkey,
                    name,
                    ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer AS distance_m,
                    score
                FROM scored
                WHERE score >= {self.JW_THRESHOLD}
                ORDER BY score DESC, distance_m
                LIMIT $limit
            """
        return f"""
            WITH candidates AS (
                SELECT DISTINCT p.fsq_place_id, p.name, n.norm_name,
                    p.geom, n.importance
                FROM places p
                JOIN name_index n ON p.fsq_place_id = n.fsq_place_id
                WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
                  AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
                  AND n.trigram IN ({placeholders})
                  AND n.importance >= $importance_floor
            )
            SELECT
                fsq_place_id AS rkey,
                name,
                ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer AS distance_m,
                jaro_winkler_similarity($norm_q, norm_name) AS score
            FROM candidates
            WHERE score >= {self.JW_THRESHOLD}
            ORDER BY score DESC, distance_m
            LIMIT $limit
        """

    def query_nearest(self, params: SearchParams, trigrams=None):
        assert "centroid" in params or "norm_q" in params, "Either centroid or q must be provided"

        has_spatial = "centroid" in params
        has_text = "norm_q" in params

        if has_text and has_spatial:
            return self._query_trigram_spatial(params, trigrams)
        elif has_text:
            return self._query_trigram_text(params, trigrams)
        else:
            # Spatial-only
            columns = self.record_columns()
            return f"""
                select
                    {columns},
                    ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer as distance_m
                from places
                where bbox.xmin > $xmin and bbox.ymin > $ymin
                  and bbox.xmax < $xmax and bbox.ymax < $ymax
                order by importance desc, distance_m
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
            "name": result.pop("name"),
            "variants": [],
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

    def query_hydrate(self, count):
        columns = self.record_columns()
        placeholders = ", ".join(f"$h{i}" for i in range(count))
        return f"SELECT {columns} FROM places WHERE id IN ({placeholders})"

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
        Text-only trigram search using Jaro-Winkler similarity.
        Candidates are retrieved via trigram pre-filter; JW scores the outer query.
        Multi-token queries use a top-N CTE chain for token-level blending.
        """
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        token_count = sum(1 for k in params if k.startswith('t') and k[1:].isdigit())
        if token_count > 1:
            top_n = min(max(params.get("limit", 50) * 20, 200), 2000)
            token_values = ", ".join(f"($t{i})" for i in range(token_count))
            alpha = self.JW_TOKEN_ALPHA
            return f"""
                WITH candidates AS (
                    SELECT DISTINCT id, name, norm_name, importance
                    FROM name_index
                    WHERE trigram IN ({placeholders})
                      AND importance >= $importance_floor
                ),
                ranked AS (
                    SELECT c.*,
                        jaro_winkler_similarity($norm_q, c.norm_name) AS full_jw
                    FROM candidates c
                    ORDER BY full_jw DESC
                    LIMIT {top_n}
                ),
                name_tokens AS (
                    SELECT r.id, r.name,
                        unnest(list_filter(string_split(r.norm_name, ' '), x -> x != '')) AS nt
                    FROM ranked r
                ),
                token_scores AS (
                    SELECT nt.id, nt.name, q.qt,
                        max(jaro_winkler_similarity(q.qt, nt.nt)) AS best
                    FROM name_tokens nt
                    CROSS JOIN (VALUES {token_values}) AS q(qt)
                    GROUP BY nt.id, nt.name, q.qt
                ),
                token_avg AS (
                    SELECT id, name, avg(best) AS token_jw
                    FROM token_scores
                    GROUP BY id, name
                ),
                scored AS (
                    SELECT r.*,
                        {alpha} * r.full_jw + {1 - alpha} * COALESCE(t.token_jw, r.full_jw) AS score
                    FROM ranked r
                    LEFT JOIN token_avg t ON r.id = t.id AND r.name = t.name
                )
                SELECT
                    s.id AS rkey,
                    s.name,
                    0 AS distance_m,
                    s.score
                FROM scored s
                WHERE s.score >= {self.JW_THRESHOLD}
                ORDER BY s.score DESC, s.importance DESC
                LIMIT $limit
            """
        return f"""
            WITH candidates AS (
                SELECT DISTINCT id, name, norm_name, importance
                FROM name_index
                WHERE trigram IN ({placeholders})
                  AND importance >= $importance_floor
            )
            SELECT
                c.id AS rkey,
                c.name,
                0 AS distance_m,
                jaro_winkler_similarity($norm_q, c.norm_name) AS score
            FROM candidates c
            WHERE jaro_winkler_similarity($norm_q, c.norm_name) >= {self.JW_THRESHOLD}
            ORDER BY score DESC, c.importance DESC
            LIMIT $limit
        """

    def _query_trigram_spatial(self, params: SearchParams, trigrams: list) -> str:
        """
        Spatial + text trigram search using Jaro-Winkler similarity.
        Joins places to name_index with bbox + trigram IN filters.
        JW threshold applied to filter weak matches.
        Multi-token queries use a top-N CTE chain for token-level blending.
        """
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        token_count = sum(1 for k in params if k.startswith('t') and k[1:].isdigit())
        if token_count > 1:
            top_n = min(max(params.get("limit", 50) * 20, 200), 2000)
            token_values = ", ".join(f"($t{i})" for i in range(token_count))
            alpha = self.JW_TOKEN_ALPHA
            return f"""
                WITH candidates AS (
                    SELECT DISTINCT p.id, p.names.primary AS name, n.norm_name,
                        p.geometry, n.importance
                    FROM places p
                    JOIN name_index n ON p.id = n.id
                    WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
                      AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
                      AND n.trigram IN ({placeholders})
                      AND n.importance >= $importance_floor
                ),
                ranked AS (
                    SELECT c.*,
                        jaro_winkler_similarity($norm_q, c.norm_name) AS full_jw
                    FROM candidates c
                    ORDER BY full_jw DESC
                    LIMIT {top_n}
                ),
                name_tokens AS (
                    SELECT r.id, r.name,
                        unnest(list_filter(string_split(r.norm_name, ' '), x -> x != '')) AS nt
                    FROM ranked r
                ),
                token_scores AS (
                    SELECT nt.id, nt.name, q.qt,
                        max(jaro_winkler_similarity(q.qt, nt.nt)) AS best
                    FROM name_tokens nt
                    CROSS JOIN (VALUES {token_values}) AS q(qt)
                    GROUP BY nt.id, nt.name, q.qt
                ),
                token_avg AS (
                    SELECT id, name, avg(best) AS token_jw
                    FROM token_scores
                    GROUP BY id, name
                ),
                scored AS (
                    SELECT r.*,
                        {alpha} * r.full_jw + {1 - alpha} * COALESCE(t.token_jw, r.full_jw) AS score
                    FROM ranked r
                    LEFT JOIN token_avg t ON r.id = t.id AND r.name = t.name
                )
                SELECT
                    id AS rkey,
                    name,
                    ST_Distance_Sphere(geometry, ST_GeomFromText($centroid))::integer AS distance_m,
                    score
                FROM scored
                WHERE score >= {self.JW_THRESHOLD}
                ORDER BY score DESC, distance_m
                LIMIT $limit
            """
        return f"""
            WITH candidates AS (
                SELECT DISTINCT p.id, p.names.primary AS name, n.norm_name,
                    p.geometry, n.importance
                FROM places p
                JOIN name_index n ON p.id = n.id
                WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
                  AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
                  AND n.trigram IN ({placeholders})
                  AND n.importance >= $importance_floor
            )
            SELECT
                id AS rkey,
                name,
                ST_Distance_Sphere(geometry, ST_GeomFromText($centroid))::integer AS distance_m,
                jaro_winkler_similarity($norm_q, norm_name) AS score
            FROM candidates
            WHERE score >= {self.JW_THRESHOLD}
            ORDER BY score DESC, distance_m
            LIMIT $limit
        """

    def query_nearest(self, params: SearchParams, trigrams=None):
        assert "centroid" in params or "norm_q" in params, "Either centroid or q must be provided"

        has_spatial = "centroid" in params
        has_text = "norm_q" in params

        if has_text and has_spatial:
            return self._query_trigram_spatial(params, trigrams)
        elif has_text:
            return self._query_trigram_text(params, trigrams)
        else:
            # Spatial-only
            columns = self.record_columns()
            return f"""
                select
                    {columns},
                    ST_Distance_Sphere(geometry, ST_GeomFromText($centroid))::integer as distance_m
                from places
                where bbox.xmin > $xmin and bbox.ymin > $ymin
                  and bbox.xmax < $xmax and bbox.ymax < $ymax
                order by importance desc, distance_m
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
            "name": result.pop("name"),
            "variants": [],
            "attributes": result
        }

class OpenStreetMap(Database):
    collection = "org.atgeo.places.osm"

    def record_columns(self):
        return """
            rkey,
            name,
            latitude::decimal(10,6)::varchar AS latitude,
            longitude::decimal(10,6)::varchar AS longitude,
            primary_category,
            tags
        """

    def query_hydrate(self, count):
        columns = self.record_columns()
        placeholders = ", ".join(f"$h{i}" for i in range(count))
        return f"SELECT {columns} FROM places WHERE rkey IN ({placeholders})"

    def query_record(self):
        columns = self.record_columns()
        return f"""
            select
                {columns}
            from places
            where rkey = $rkey
        """

    def _query_trigram_text(self, params: SearchParams, trigrams: list) -> str:
        """
        Text-only trigram search using Jaro-Winkler similarity.
        Candidates are retrieved via trigram pre-filter; JW scores the outer query.
        Multi-token queries use a top-N CTE chain for token-level blending.
        """
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        token_count = sum(1 for k in params if k.startswith('t') and k[1:].isdigit())
        if token_count > 1:
            top_n = min(max(params.get("limit", 50) * 20, 200), 2000)
            token_values = ", ".join(f"($t{i})" for i in range(token_count))
            alpha = self.JW_TOKEN_ALPHA
            return f"""
                WITH candidates AS (
                    SELECT DISTINCT rkey, name, norm_name, importance
                    FROM name_index
                    WHERE trigram IN ({placeholders})
                      AND importance >= $importance_floor
                ),
                ranked AS (
                    SELECT c.*,
                        jaro_winkler_similarity($norm_q, c.norm_name) AS full_jw
                    FROM candidates c
                    ORDER BY full_jw DESC
                    LIMIT {top_n}
                ),
                name_tokens AS (
                    SELECT r.rkey, r.name,
                        unnest(list_filter(string_split(r.norm_name, ' '), x -> x != '')) AS nt
                    FROM ranked r
                ),
                token_scores AS (
                    SELECT nt.rkey, nt.name, q.qt,
                        max(jaro_winkler_similarity(q.qt, nt.nt)) AS best
                    FROM name_tokens nt
                    CROSS JOIN (VALUES {token_values}) AS q(qt)
                    GROUP BY nt.rkey, nt.name, q.qt
                ),
                token_avg AS (
                    SELECT rkey, name, avg(best) AS token_jw
                    FROM token_scores
                    GROUP BY rkey, name
                ),
                scored AS (
                    SELECT r.*,
                        {alpha} * r.full_jw + {1 - alpha} * COALESCE(t.token_jw, r.full_jw) AS score
                    FROM ranked r
                    LEFT JOIN token_avg t ON r.rkey = t.rkey AND r.name = t.name
                )
                SELECT
                    s.rkey,
                    s.name,
                    0 AS distance_m,
                    s.score
                FROM scored s
                WHERE s.score >= {self.JW_THRESHOLD}
                ORDER BY s.score DESC, s.importance DESC
                LIMIT $limit
            """
        return f"""
            WITH candidates AS (
                SELECT DISTINCT rkey, name, norm_name, importance
                FROM name_index
                WHERE trigram IN ({placeholders})
                  AND importance >= $importance_floor
            )
            SELECT
                c.rkey,
                c.name,
                0 AS distance_m,
                jaro_winkler_similarity($norm_q, c.norm_name) AS score
            FROM candidates c
            WHERE jaro_winkler_similarity($norm_q, c.norm_name) >= {self.JW_THRESHOLD}
            ORDER BY score DESC, c.importance DESC
            LIMIT $limit
        """

    def _query_trigram_spatial(self, params: SearchParams, trigrams: list) -> str:
        """
        Spatial + text trigram search using Jaro-Winkler similarity.
        Joins places to name_index with bbox + trigram IN filters.
        JW threshold applied to filter weak matches.
        Multi-token queries use a top-N CTE chain for token-level blending.
        """
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        token_count = sum(1 for k in params if k.startswith('t') and k[1:].isdigit())
        if token_count > 1:
            top_n = min(max(params.get("limit", 50) * 20, 200), 2000)
            token_values = ", ".join(f"($t{i})" for i in range(token_count))
            alpha = self.JW_TOKEN_ALPHA
            return f"""
                WITH candidates AS (
                    SELECT DISTINCT p.rkey, p.name, n.norm_name,
                        p.geom, n.importance
                    FROM places p
                    JOIN name_index n ON p.rkey = n.rkey
                    WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
                      AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
                      AND n.trigram IN ({placeholders})
                      AND n.importance >= $importance_floor
                ),
                ranked AS (
                    SELECT
                        rkey,
                        name, norm_name, geom, importance,
                        jaro_winkler_similarity($norm_q, norm_name) AS full_jw
                    FROM candidates
                    ORDER BY full_jw DESC
                    LIMIT {top_n}
                ),
                name_tokens AS (
                    SELECT r.rkey, r.name,
                        unnest(list_filter(string_split(r.norm_name, ' '), x -> x != '')) AS nt
                    FROM ranked r
                ),
                token_scores AS (
                    SELECT nt.rkey, nt.name, q.qt,
                        max(jaro_winkler_similarity(q.qt, nt.nt)) AS best
                    FROM name_tokens nt
                    CROSS JOIN (VALUES {token_values}) AS q(qt)
                    GROUP BY nt.rkey, nt.name, q.qt
                ),
                token_avg AS (
                    SELECT rkey, name, avg(best) AS token_jw
                    FROM token_scores
                    GROUP BY rkey, name
                ),
                scored AS (
                    SELECT r.*,
                        {alpha} * r.full_jw + {1 - alpha} * COALESCE(t.token_jw, r.full_jw) AS score
                    FROM ranked r
                    LEFT JOIN token_avg t ON r.rkey = t.rkey AND r.name = t.name
                )
                SELECT
                    rkey,
                    name,
                    ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer AS distance_m,
                    score
                FROM scored
                WHERE score >= {self.JW_THRESHOLD}
                ORDER BY score DESC, distance_m
                LIMIT $limit
            """
        return f"""
            WITH candidates AS (
                SELECT DISTINCT p.rkey, p.name, n.norm_name,
                    p.geom, n.importance
                FROM places p
                JOIN name_index n ON p.rkey = n.rkey
                WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
                  AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
                  AND n.trigram IN ({placeholders})
                  AND n.importance >= $importance_floor
            )
            SELECT
                rkey,
                name,
                ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer AS distance_m,
                jaro_winkler_similarity($norm_q, norm_name) AS score
            FROM candidates
            WHERE score >= {self.JW_THRESHOLD}
            ORDER BY score DESC, distance_m
            LIMIT $limit
        """

    def query_nearest(self, params: SearchParams, trigrams=None):
        assert "centroid" in params or "norm_q" in params, "Either centroid or q must be provided"

        has_spatial = "centroid" in params
        has_text = "norm_q" in params

        if has_text and has_spatial:
            return self._query_trigram_spatial(params, trigrams)
        elif has_text:
            return self._query_trigram_text(params, trigrams)
        else:
            columns = self.record_columns()
            return f"""
                select
                    {columns},
                    ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer as distance_m
                from places
                where bbox.xmin > $xmin and bbox.ymin > $ymin
                  and bbox.xmax < $xmax and bbox.ymax < $ymax
                order by importance desc, distance_m
                limit $limit;
            """

    @staticmethod
    def expand_rkey(rkey: str) -> str:
        """Expand compact OSM rkey prefix to full element type name.

        "n12345" → "node/12345"
        "w50637691" → "way/50637691"
        "r99" → "relation/99"
        """
        prefixes = {"n": "node/", "w": "way/", "r": "relation/"}
        prefix = rkey[0] if rkey else ""
        if prefix in prefixes:
            return prefixes[prefix] + rkey[1:]
        return rkey

    @staticmethod
    def compact_rkey(rkey: str) -> str:
        """Convert expanded rkey form to compact form.

        Maps "node/<id>" → "n<id>", "way/<id>" → "w<id>",
        "relation/<id>" → "r<id>". Passes through rkeys with no "/" or
        with an unrecognized prefix unchanged.
        """
        if "/" not in rkey:
            return rkey
        prefix, _, rest = rkey.partition("/")
        mapping = {"node": "n", "way": "w", "relation": "r"}
        if prefix not in mapping:
            return rkey
        return mapping[prefix] + rest

    def get_record(self, _repo: str, _collection: str, rkey: str):
        return super().get_record(_repo, _collection, self.compact_rkey(rkey))

    def process_record(self, result):
        # tags comes as a dict from DuckDB MAP type (absent in search results)
        tag_dict = dict(result.pop("tags", None) or {})

        # Parse primary_category (e.g., "amenity=cafe") into tag_dict
        primary_category = result.pop("primary_category", None)
        if primary_category:
            k, _, v = primary_category.partition("=")
            tag_dict[k] = v

        locations = [
            {
                "$type": "community.lexicon.location.geo",
                "latitude": result.pop("latitude"),
                "longitude": result.pop("longitude"),
            }
        ]

        # Build address from addr:* tags
        address_data = {}
        addr_map = [
            ("addr:country", "country"),
            ("addr:postcode", "postalCode"),
            ("addr:city", "locality"),
            ("addr:street", "street"),
        ]
        for tag_key, dest_key in addr_map:
            if tag_dict.get(tag_key):
                address_data[dest_key] = tag_dict.pop(tag_key)
        # Prepend housenumber to street if present
        if tag_dict.get("addr:housenumber") and address_data.get("street"):
            address_data["street"] = (
                tag_dict.pop("addr:housenumber") + " " + address_data["street"]
            )
        elif tag_dict.get("addr:housenumber"):
            tag_dict.pop("addr:housenumber")  # Remove orphan housenumber
        if address_data.get("country"):
            locations.append({
                "$type": "community.lexicon.location.address",
                **address_data
            })

        return {
            "$type": "org.atgeo.place",
            "collection": self.collection,
            "rkey": self.expand_rkey(result.pop("rkey")),
            "locations": locations,
            "name": result.pop("name"),
            "variants": [],
            "attributes": tag_dict
        }


if __name__ == "__main__":
    from pprint import pprint

    d = FoursquareOSP("db/fsq-osp.duckdb")
    result = d.nearest(bbox=(-122.48, 37.73, -122.39, 37.82))
    pprint(result)
    d.close()

    d = OvertureMaps("db/overture-maps.duckdb")
    result = d.nearest(bbox=(-122.48, 37.73, -122.39, 37.82))
    pprint(result)

    record = d.get_record("", "org.overturemaps.places", result[0]["rkey"])
    pprint(record)

    d.close()
