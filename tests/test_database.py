"""Tests for garganorn.database base classes."""
import pytest
import duckdb

from garganorn.database import Database, FoursquareOSP, OvertureMaps


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_connect_creates_connection(fsq_db_path):
    """connect() sets conn and temp_dir, loads spatial extension."""
    db = FoursquareOSP(fsq_db_path)
    conn = db.connect()
    assert conn is not None
    assert db.conn is not None
    assert db.temp_dir is not None
    db.close()


def test_connect_fails_without_name_index(tmp_path):
    """connect() should raise RuntimeError when name_index table is missing."""
    db_path = tmp_path / "no_index.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("CREATE TABLE places (id VARCHAR)")
    conn.close()

    db = FoursquareOSP(db_path)
    with pytest.raises(RuntimeError, match="name_index"):
        db.connect()
    db.close()


def test_connect_fails_without_trigram_column(tmp_path):
    """connect() should raise RuntimeError when name_index exists but lacks trigram column."""
    db_path = tmp_path / "no_trigram.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("CREATE TABLE places (id VARCHAR)")
    conn.execute("CREATE TABLE name_index (token VARCHAR, id VARCHAR, importance INTEGER)")
    conn.close()

    db = FoursquareOSP(db_path)
    with pytest.raises(RuntimeError, match="trigram"):
        db.connect()
    db.close()


def test_close_cleans_up(fsq_db_path):
    """After close(), conn is None."""
    db = FoursquareOSP(fsq_db_path)
    db.connect()
    assert db.conn is not None
    db.close()
    assert db.conn is None


def test_execute_returns_dicts(fsq_db_path):
    """execute('SELECT 1 AS x') returns [{'x': 1}]."""
    db = FoursquareOSP(fsq_db_path)
    db.connect()
    result = db.execute("SELECT 1 AS x")
    assert result == [{"x": 1}]
    db.close()



# ---------------------------------------------------------------------------
# Unit tests for Database._strip_accents (static method)
# ---------------------------------------------------------------------------

def test_strip_accents_cafe():
    """café -> cafe."""
    assert Database._strip_accents("café") == "cafe"


def test_strip_accents_naive():
    """naïve -> naive."""
    assert Database._strip_accents("naïve") == "naive"


def test_strip_accents_plain():
    """Plain ASCII string is returned unchanged."""
    assert Database._strip_accents("hello") == "hello"


# ---------------------------------------------------------------------------
# Unit tests for Database._compute_trigrams (static method)
# ---------------------------------------------------------------------------

def test_compute_trigrams_basic():
    """'coffee' produces sorted list of its 3-char substrings."""
    result = Database._compute_trigrams("coffee")
    assert result == sorted(["cof", "off", "ffe", "fee"])


def test_compute_trigrams_with_spaces():
    """'pizza hut' includes cross-word trigrams spanning the space."""
    result = Database._compute_trigrams("pizza hut")
    assert "za " in result
    assert "a h" in result
    assert " hu" in result


def test_compute_trigrams_short_query():
    """Strings shorter than 3 chars produce no trigrams."""
    assert Database._compute_trigrams("ab") == []


def test_compute_trigrams_accents():
    """Accented input produces same trigrams as its ASCII equivalent."""
    assert Database._compute_trigrams("café") == Database._compute_trigrams("cafe")


def test_compute_trigrams_max_cap():
    """Output is capped at MAX_QUERY_TRIGRAMS for very long strings."""
    long_string = "abcdefghijklmnopqrstuvwxyz" * 10  # 260 chars -> 258 trigrams
    result = Database._compute_trigrams(long_string)
    assert len(result) <= Database.MAX_QUERY_TRIGRAMS
