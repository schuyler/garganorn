"""Tests for garganorn.database base classes."""
from garganorn.database import OverturePlaces


def test_connect_creates_connection(overture_db_path):
    """connect() sets conn and temp_dir, loads spatial extension."""
    db = OverturePlaces(overture_db_path)
    conn = db.connect()
    assert conn is not None
    assert db.conn is not None
    assert db.temp_dir is not None
    db.close()


def test_close_cleans_up(overture_db_path):
    """After close(), conn is None."""
    db = OverturePlaces(overture_db_path)
    db.connect()
    assert db.conn is not None
    db.close()
    assert db.conn is None


def test_execute_returns_dicts(overture_db_path):
    """execute('SELECT 1 AS x') returns [{'x': 1}]."""
    db = OverturePlaces(overture_db_path)
    db.connect()
    result = db.execute("SELECT 1 AS x")
    assert result == [{"x": 1}]
    db.close()
