# Phase 3 Design: Containment Restructure

## compute_containment() changes (`garganorn/quadtree.py`)

### Signature
```python
def compute_containment(con, boundaries_db, pk_expr, lon_expr, lat_expr,
                        collection_prefix="org.atgeo.places.overture.division"):
```

### Attach alias: `wof` → `bnd`
- Line 95: `ATTACH '{boundaries_db}' AS bnd (READ_ONLY)`
- Line 182: `DETACH bnd`

### Step 0: tile_boundaries temp table
Column mappings from WoF → division:
- `rkey` → `id`
- `name` → dropped
- `level` → `admin_level` (sort only, not in output)
- `geom` → `geometry`
- Table: `wof.boundaries` → `bnd.places`

```sql
CREATE OR REPLACE TEMP TABLE tile_boundaries AS
SELECT id, admin_level,
       ST_Intersection(geometry, ST_MakeEnvelope(?, ?, ?, ?)) AS geometry,
       greatest(min_latitude, ?) AS min_latitude,
       least(max_latitude, ?)    AS max_latitude,
       greatest(min_longitude, ?) AS min_longitude,
       least(max_longitude, ?)    AS max_longitude
FROM bnd.places
WHERE ST_Intersects(geometry, ST_MakeEnvelope(?, ?, ?, ?))
```

### Phase 1: containment pre-filter
```sql
CREATE OR REPLACE TEMP TABLE phase1 AS
SELECT id, admin_level FROM tile_boundaries
WHERE ST_Contains(geometry, ST_MakeEnvelope(?, ?, ?, ?))
```

### Combined insert: rkey-only output
```sql
INSERT INTO place_containment
WITH bulk_assign AS (
    SELECT {pk_expr} AS pk,
           '{collection_prefix}:' || ph.id AS rkey,
           ph.admin_level
    FROM places p
    CROSS JOIN phase1 ph
    WHERE LEFT(p.qk17, 6) = ?
),
edge_matches AS (
    SELECT {pk_expr} AS pk,
           '{collection_prefix}:' || b.id AS rkey,
           b.admin_level
    FROM places p
    JOIN tile_boundaries b
        ON {lat_expr} BETWEEN b.min_latitude AND b.max_latitude
       AND {lon_expr} BETWEEN b.min_longitude AND b.max_longitude
       AND ST_Contains(b.geometry, ST_Point({lon_expr}, {lat_expr}))
    WHERE LEFT(p.qk17, 6) = ?
      AND NOT EXISTS (
          SELECT 1 FROM phase1 ph WHERE ph.id = b.id
      )
),
all_matches AS (
    SELECT * FROM bulk_assign
    UNION ALL
    SELECT * FROM edge_matches
)
SELECT pk, to_json({{within: list(
    {{rkey: rkey}}
    ORDER BY admin_level ASC
)}})::VARCHAR
FROM all_matches
GROUP BY pk
```

Note: `admin_level` flows through temp tables for ORDER BY but is NOT in the output JSON.

## BoundaryLookup changes (`garganorn/boundaries.py`)

```python
COLLECTION = "org.atgeo.places.overture.division"

def containment(self, lat, lon):
    conn = self.connect()
    rows = conn.execute("""
        SELECT id FROM places
        WHERE ST_Contains(geometry, ST_Point($lon, $lat))
        ORDER BY admin_level ASC
    """, {"lat": lat, "lon": lon}).fetchall()
    return [{"rkey": f"{self.COLLECTION}:{r[0]}"} for r in rows]
```

## Test fixture changes (`tests/conftest.py`)

Add `DIVISION_BOUNDARIES` data + `_create_division_db()` alongside existing WoF fixtures (WoF tests kept for Phase 4). New `division_db_path` fixture. Update `boundary_lookup` fixture to use `division_db_path`.

Division DB schema: `places` table with `id`, `geometry`, `admin_level`, `names` struct, `min/max_latitude/longitude`.

## Test assertion changes

### test_boundaries.py
- Remove `name`/`level` key assertions
- Change rkey prefix to `org.atgeo.places.overture.division:`
- Verify ordering by rkey sequence (known admin_level order from test data)

### test_export.py
- `_SF_WITHIN_JSON`: rkey-only with division prefix
- Tests 1/3/4/6: remove `name`/`level` assertions, update prefix
- Test 6b: change name-based assertions to rkey-based

### test_server.py
- Mock return: `[{"rkey": "org.atgeo.places.overture.division:85922583"}]`
- Assert rkey instead of name

## server.py
No changes — passes through `within` list from BoundaryLookup opaquely.
