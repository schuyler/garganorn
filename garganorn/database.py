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
        """Connect to the database and load the spatial plugin."""
        if self.conn is None:
            # Connect in read-only mode
            self.conn = duckdb.connect(str(self.db_path), read_only=True)

            # Create a temporary directory for DuckDB to use
            self.temp_dir = tempfile.mkdtemp(prefix='duckdb_temp_')
            
            # Configure DuckDB to use our writable temp directory
            self.conn.execute(f"SET temp_directory='{self.temp_dir}'")
            
            # Load spatial extension
            self.conn.install_extension("spatial")
            self.conn.load_extension("spatial")
        return self.conn
    
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
        stmt = self.conn.execute(query, params)
        rows = stmt.fetchall()
        assert stmt.description is not None, "Query did not return any results."
        columns = tuple(c[0] for c in stmt.description)
        return [dict(zip(columns, row)) for row in rows]

    def query_record(self):
        raise NotImplementedError
    
    def process_record(self, result):
        return {
            "$type": "community.lexicon.location.place",
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
        return {
            "$type": "community.lexicon.location.place",
            "rkey": result.pop("rkey"),
            "locations": [
                {
                    "$type": "community.lexicon.location.geo",
                    "latitude": result.pop("latitude"),
                    "longitude": result.pop("longitude"),
                }
            ],
            "names": [
                {"text": result.pop("name"), "priority": 0}
            ],
            "attributes": result,
            "distance_m": result.pop("distance_m")
        }

    def nearest(self, latitude=None, longitude=None, q=None, expand_m=5000, limit=50):
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
        print(f"Searching with params: {params}")
        result = self.execute(
            self.query_nearest(params), params
        )
        return [self.process_nearest(item) for item in result]


class FoursquareOSP(Database):
    collection = "com.foursquare.places"

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
            country,
            date_created,
            date_refreshed,
            fsq_category_labels
        """
    
    def query_record(self):
        columns = self.record_columns()
        return f"""
            select
                {columns}
            from places
            where fsq_place_id = $rkey
        """

    def query_nearest(self, params: SearchParams):
        columns = self.record_columns()
        if params.get("centroid"):
            distance_m = "ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer"
            spatial_filter = "bbox.xmin > $xmin and bbox.ymin > $ymin and bbox.xmax < $xmax and bbox.ymax < $ymax"
        else:
            distance_m = "0"
            spatial_filter = ""
        if params.get("q"):
            text_filter = "(name ilike '%' || $q || '%')"
        else:
            text_filter = ""
        filter_conditions = " and ".join(filter(None, (spatial_filter, text_filter)))

        return f"""
            select
                {columns},
                {distance_m} as distance_m
            from places
            where {filter_conditions}
                and date_refreshed > '2020-03-15'
                and date_closed is null
            order by distance_m
            limit $limit;
        """

class OvertureMaps(Database):
    collection = "org.overturemaps.places"

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
            phones,
            brand,
            confidence::decimal(4,3)::varchar as confidence
        """

    def query_record(self):
        columns = self.record_columns()
        return f"""
            select
                {columns}
            from places
            where id = $rkey
        """
    
    def query_nearest(self, params: SearchParams):
        columns = self.record_columns()
        if params.get("centroid"):
            distance_m = "ST_Distance_Sphere(geometry, ST_GeomFromText($centroid))::integer"
            spatial_filter = "bbox.xmin > $xmin and bbox.ymin > $ymin and bbox.xmax < $xmax and bbox.ymax < $ymax"
        else:
            distance_m = "0"
            spatial_filter = ""
        if params.get("q"):
            text_filter = "(name ilike '%' || $q || '%')"
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
