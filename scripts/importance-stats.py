#!/usr/bin/env python3
"""Analyze importance distribution in name_index."""

import duckdb
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "fsq-osp.duckdb"

conn = duckdb.connect(str(DB_PATH), read_only=True)

total = conn.execute('SELECT count(DISTINCT fsq_place_id) FROM name_index').fetchone()[0]
print(f'Total places: {total:,}')
for thresh in [0, 5, 10, 20, 30, 40, 50, 60, 80, 100]:
    ct = conn.execute(f'SELECT count(DISTINCT fsq_place_id) FROM name_index WHERE importance >= {thresh}').fetchone()[0]
    print(f'  >= {thresh:3d}: {ct:>7,} ({ct/total*100:5.1f}%)')

conn.close()
