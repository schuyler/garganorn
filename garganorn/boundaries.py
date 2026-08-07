"""Admin boundary lookup and record resolution."""
from pathlib import Path

import duckdb


class BoundaryLookup:
    """Point-in-polygon lookup against Overture division boundaries.

    Queries a boundaries.duckdb file produced by the overture_division pipeline
    stage. The database schema uses a `places` table with columns `id`,
    `geometry`, and `level` — the atgeo containment level vocabulary
    (garganorn.levels.LEVEL_VOCAB), not raw Overture admin_level — matching
    the division export schema.
    """

    # Collection NSID emitted alongside bare rkeys in containment output.
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

        Returns a list of dicts ordered by level ascending (most general
        first, most specific last), ties broken by id ascending for
        determinism, each containing:
            collection: NSID of the related place's collection
            rkey: record key within that collection (bare id, not qualified)

        Only collection and rkey are returned. Name, level, and other division
        metadata are available from the division tile for that rkey; clients
        resolve them from the admin tile layer rather than duplicating them in
        every containment relation.
        """
        conn = self.connect()
        rows = conn.execute("""
            SELECT id FROM places
            WHERE ST_Contains(geometry, ST_Point($lon, $lat))
            ORDER BY level ASC, id ASC
        """, {"lat": lat, "lon": lon}).fetchall()
        return [{"collection": self.COLLECTION, "rkey": r[0]} for r in rows]

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None


