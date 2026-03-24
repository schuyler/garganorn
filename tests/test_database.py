"""Tests for garganorn.database base classes."""
import pytest
import duckdb

from garganorn.database import Database, FoursquareOSP, OvertureMaps


# ---------------------------------------------------------------------------
# Unit tests for Database._tokenize_query
# Instance method: call on a concrete subclass instance.
# ---------------------------------------------------------------------------

def _make_db_instance():
    """Return a FoursquareOSP instance (no DB path needed for tokenize tests)."""
    return FoursquareOSP(":memory:")


def test_tokenize_simple():
    """_tokenize_query('hello world') -> ['hello', 'world']"""
    db = _make_db_instance()
    result = db._tokenize_query("hello world")
    assert result == ["hello", "world"]


def test_tokenize_strips_short_words():
    """Single-character words are filtered out."""
    db = _make_db_instance()
    result = db._tokenize_query("a hello b world")
    assert "a" not in result
    assert "b" not in result
    assert "hello" in result
    assert "world" in result


def test_tokenize_trims_to_max():
    """More than 7 words keeps 7 longest by length."""
    db = _make_db_instance()
    words = ["aa", "bbb", "cccc", "ddddd", "eeeeee", "fffffff", "gggggggg", "hhhhhhhhh"]
    query = " ".join(words)
    result = db._tokenize_query(query)
    assert len(result) <= 7
    # The shortest word should have been dropped
    assert "aa" not in result


def test_tokenize_whitespace():
    """Leading, trailing, and multiple internal spaces are handled."""
    db = _make_db_instance()
    result = db._tokenize_query("  hello   world  ")
    assert result == ["hello", "world"]


def test_tokenize_empty():
    """Empty string returns empty list."""
    db = _make_db_instance()
    result = db._tokenize_query("")
    assert result == []


# ---------------------------------------------------------------------------
# Unit tests for Database._build_name_index_join (static method)
# Signature: _build_name_index_join(n_tokens: int, join_key: str) -> str
# ---------------------------------------------------------------------------

def test_build_name_index_join_single_token():
    """1 token: no JOIN in generated SQL."""
    sql = Database._build_name_index_join(1, "fsq_place_id")
    assert "JOIN" not in sql.upper()


def test_build_name_index_join_two_tokens():
    """2 tokens: exactly one JOIN in generated SQL."""
    sql = Database._build_name_index_join(2, "fsq_place_id")
    assert sql.upper().count("JOIN") == 1


def test_build_name_index_join_max_tokens():
    """7 tokens: 6 JOINs in generated SQL."""
    sql = Database._build_name_index_join(7, "fsq_place_id")
    assert sql.upper().count("JOIN") == 6


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


def test_connect_detects_name_index_absent(fsq_db_path_no_index):
    """DB without name_index table -> connect() sets has_name_index to False."""
    db = FoursquareOSP(fsq_db_path_no_index)
    db.connect()
    assert db.has_name_index is False
    db.close()


def test_connect_detects_name_index_present(fsq_db_path):
    """DB with name_index table -> connect() sets has_name_index to True."""
    db = FoursquareOSP(fsq_db_path)
    db.connect()
    assert db.has_name_index is True
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


# ---------------------------------------------------------------------------
# Integration tests for has_trigram_index detection
# ---------------------------------------------------------------------------

def test_trigram_index_detection(tmp_path):
    """connect() sets has_trigram_index=True when name_index has trigram column."""
    import duckdb as _duckdb
    db_path = tmp_path / "trigram_detect.duckdb"
    conn = _duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            fsq_place_id VARCHAR,
            name VARCHAR,
            lat DOUBLE,
            lon DOUBLE,
            importance INTEGER
        )
    """)
    conn.close()

    db = FoursquareOSP(db_path)
    db.connect()
    assert db.has_trigram_index is True
    db.close()


def test_trigram_index_detection_missing(tmp_path):
    """connect() sets has_trigram_index=False when name_index has no trigram column."""
    import duckdb as _duckdb
    db_path = tmp_path / "no_trigram_detect.duckdb"
    conn = _duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE name_index (
            dm_code VARCHAR,
            fsq_place_id VARCHAR,
            name VARCHAR,
            importance INTEGER
        )
    """)
    conn.close()

    db = FoursquareOSP(db_path)
    db.connect()
    assert db.has_trigram_index is False
    db.close()
