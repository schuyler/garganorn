"""Shared test helper for spying on duckdb.connect calls.

This module is a plain Python module, not a pytest conftest. Import directly.
"""


def spy_on_duckdb_connect(monkeypatch, module):
    """Record every SQL statement executed on connections opened by `module`.

    Returns the list that accumulates statements, in order. The connection
    is a transparent proxy: __getattr__ forwards everything untouched, and
    execute() delegates to the real connection, so the code under test
    behaves exactly as it would unspied -- this observes query shape, it
    doesn't stub DuckDB.
    """
    real_connect = module.duckdb.connect
    statements = []

    class _RecordingConn:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *a, **kw):
            statements.append(sql)
            return self._real.execute(sql, *a, **kw)

        def __getattr__(self, name):
            return getattr(self._real, name)

    monkeypatch.setattr(
        module.duckdb, "connect", lambda *a, **kw: _RecordingConn(real_connect(*a, **kw))
    )
    return statements
