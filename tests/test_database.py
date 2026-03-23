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


@pytest.mark.xfail(reason=(
    "Modern DuckDB always returns a description for all statements, "
    "making the `assert stmt.description is not None` guard unreachable. "
    "The AssertionError contract is documented but untriggerable in practice."
), strict=False)
def test_execute_non_select_raises_assertion_error(fsq_db_path):
    """DML statement raises AssertionError (documented contract).

    The execute() method asserts stmt.description is not None after execution.
    In older DuckDB versions, DML statements returned None for description.
    Modern DuckDB returns a Count/success description for all statement types,
    so this guard cannot be triggered. Test is marked xfail to document intent.
    """
    db = FoursquareOSP(fsq_db_path)
    db.connect()
    # In a writable context, INSERT returns a Count description — no AssertionError.
    # This xfail test documents the intended behavior of the guard.
    with pytest.raises(AssertionError):
        db.execute("SELECT")  # Placeholder — would need DuckDB version where DML returns None description
    db.close()
