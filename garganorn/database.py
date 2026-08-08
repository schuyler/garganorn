import logging
from pathlib import Path
import tempfile
import os
import shutil

log = logging.getLogger(__name__)

import math
import duckdb

DEG_TO_M = 111194.927
DEG_TO_RAD = math.pi / 180


class Database:
    """DuckDB handler for gazetteer database with spatial capabilities."""
    collection: str = "org.atgeo"
    source_url: str = ""
    license_url: str = ""
    attribution: str = ""

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
                log.warning("Could not remove temp directory %s: %s", self.temp_dir, e)
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


class OverturePlaces(Database):
    collection = "org.atgeo.places.overture.place"
    source_key = "overture_place"
    source_pk = "id"
    attribution = "https://docs.overturemaps.org/attribution/"
    source_url = "https://overturemaps.org/"
    license_url = "https://docs.overturemaps.org/attribution/"


class OpenStreetMap(Database):
    collection = "org.atgeo.places.osm"
    source_key = "osm"
    source_pk = "rkey"
    attribution = "https://www.openstreetmap.org/copyright"
    source_url = "https://www.openstreetmap.org/"
    license_url = "https://opendatacommons.org/licenses/odbl/1-0/"


class OvertureDivisions(Database):
    collection = "org.atgeo.places.overture.division"
    source_key = "overture_division"
    source_pk = "id"
    attribution = "https://docs.overturemaps.org/attribution/"
    source_url = "https://overturemaps.org/"
    license_url = "https://docs.overturemaps.org/attribution/"
