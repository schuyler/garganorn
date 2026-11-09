"""Page-rank parquets for tests: the session's empty one, and writers."""
import duckdb

_empty_path = None


def set_empty_path(path):
    global _empty_path
    _empty_path = path


def empty_page_rank():
    """Path of the session's empty page-rank parquet (written by conftest)."""
    return _empty_path


def write_page_rank(path, ranks=None):
    """Write {qid: page_rank} as a page-rank parquet (qid VARCHAR, page_rank DOUBLE)."""
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE pr (qid VARCHAR, page_rank DOUBLE)")
        for qid, rank in (ranks or {}).items():
            con.execute("INSERT INTO pr VALUES (?, ?)", [qid, rank])
        con.execute(f"COPY pr TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()
    return str(path)
