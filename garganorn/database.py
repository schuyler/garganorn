import duckdb
from pathlib import Path
import math

DEG_TO_M = 111194.927
DEG_TO_RAD = math.pi / 180

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
        
    def connect(self):
        """Connect to the database and load the spatial plugin."""
        if self.conn is None:
            self.conn = duckdb.connect(str(self.db_path))
            # Load spatial extension
            self.conn.install_extension("spatial")
            self.conn.load_extension("spatial")
        return self.conn
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            
    def execute(self, query, params=None):
        """Execute a query on the database."""
        if not self.conn:
            self.connect()
        stmt = self.conn.execute(query, params)
        rows = stmt.fetchall()
        columns = tuple(c[0] for c in stmt.description)
        return [dict(zip(columns, row)) for row in rows]

    def query_nearest(self):
        raise NotImplementedError
    
    def process_nearest(self, result):
        return result

    def nearest(self, latitude, longitude, expand_m=5000, limit=50):
        # Expand the bounding box around the point by roughly expand_m meters
        expand_lat = expand_m / DEG_TO_M
        expand_lon = expand_lat / math.cos(latitude * DEG_TO_RAD) if math.fabs(latitude) < 90 else expand_lat
        bbox = (max((longitude - expand_lon, -180)),
                max((latitude - expand_lat, -90)),
                min((longitude + expand_lon, 180)),
                min((latitude + expand_lat, 90)))
        result = self.execute(
            self.query_nearest(), 
            {
                "centroid": f"POINT({longitude} {latitude})",
                "xmin": bbox[0],
                "ymin": bbox[1],
                "xmax": bbox[2],
                "ymax": bbox[3],
                "limit": limit
            }
        )
        return [self.process_nearest(item) for item in result]

class FoursquareOSP(Database):
    def query_nearest(self):
        return """
            select 
                concat('foursquare.com/v/', fsq_place_id) as id,
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
                fsq_category_labels,
                ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer as distance_m,
            from places
            where bbox.xmin > $xmin and bbox.ymin > $ymin and bbox.xmax < $xmax and bbox.ymax < $ymax
                and date_refreshed > '2020-03-15'
                and date_closed is null
            order by distance_m
            limit $limit;
            """


if __name__ == "__main__":
    from pprint import pprint
    d = FoursquareOSP("db/fsq-osp.duckdb")
    result = d.nearest(37.776145, -122.433898)
    pprint(result)
    d.close()