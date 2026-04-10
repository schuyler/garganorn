# Phase 2 Design: Division Import Pipeline

## Critical fixes from review

1. **Two separate parquet paths** (not single glob): division and division_area have different schemas. Use `${division_parquet}` and `${division_area_parquet}` template variables.
2. **`ST_Union_Agg()`** not `ST_Union()` for grouped geometry aggregation.
3. **Bbox location fields**: `north`/`south`/`east`/`west` as VARCHAR per `garganorn/lexicon/bbox.json`, not minLatitude/maxLatitude.

## Import SQL (`garganorn/sql/overture_division_import.sql`)

```sql
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

CREATE TABLE places AS
WITH division AS (
    SELECT id, names, subtype, country, region, wikidata,
           population, parent_division_id
    FROM '${division_parquet}'
),
division_area AS (
    SELECT division_id, admin_level,
           geometry::GEOMETRY AS geometry
    FROM '${division_area_parquet}'
    WHERE is_land = true
      AND geometry IS NOT NULL
      AND bbox.xmin >= ${xmin} AND bbox.xmax <= ${xmax}
      AND bbox.ymin >= ${ymin} AND bbox.ymax <= ${ymax}
),
merged_areas AS (
    SELECT division_id,
           ST_Union_Agg(geometry) AS geometry,
           min(admin_level) AS admin_level
    FROM division_area
    GROUP BY division_id
)
SELECT
    d.id,
    ma.geometry,
    d.names,
    d.subtype,
    d.country,
    d.region,
    ma.admin_level,
    d.wikidata,
    d.population,
    d.parent_division_id,
    {'xmin': ST_XMin(ma.geometry), 'ymin': ST_YMin(ma.geometry),
     'xmax': ST_XMax(ma.geometry), 'ymax': ST_YMax(ma.geometry)} AS bbox,
    ST_QuadKey(
        (ST_XMin(ma.geometry) + ST_XMax(ma.geometry)) / 2.0,
        (ST_YMin(ma.geometry) + ST_YMax(ma.geometry)) / 2.0, 17
    ) AS qk17,
    ST_YMin(ma.geometry) AS min_latitude,
    ST_YMax(ma.geometry) AS max_latitude,
    ST_XMin(ma.geometry) AS min_longitude,
    ST_XMax(ma.geometry) AS max_longitude,
    0 AS importance,
    []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] AS variants
FROM division d
JOIN merged_areas ma ON ma.division_id = d.id;
```

## Export SQL (`garganorn/sql/overture_division_export_tiles.sql`)

```sql
CREATE OR REPLACE VIEW tile_export AS
SELECT
    ta.tile_qk,
    to_json({
        uri: 'https://${repo}/org.atgeo.places.overture.division/' || p.id,
        value: {
            "$type": 'org.atgeo.place',
            rkey: p.id,
            name: p.names."primary",
            importance: p.importance,
            locations: [{
                "$type": 'community.lexicon.location.bbox',
                north: p.max_latitude::DECIMAL(10,6)::VARCHAR,
                south: p.min_latitude::DECIMAL(10,6)::VARCHAR,
                east: p.max_longitude::DECIMAL(10,6)::VARCHAR,
                west: p.min_longitude::DECIMAL(10,6)::VARCHAR
            }],
            variants: coalesce(p.variants, []),
            attributes: {
                subtype: p.subtype,
                country: p.country,
                region: p.region,
                admin_level: p.admin_level,
                wikidata: p.wikidata,
                population: p.population
            },
            relations: coalesce(pc.relations_json::JSON, '{}'::JSON)
        }
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.id
LEFT JOIN place_containment pc ON pc.place_id = p.id
ORDER BY ta.tile_qk;
```

## quadtree.py changes

### SOURCE_PK, ATTRIBUTION (add entries)
```python
"overture_division": "id",
```
```python
"overture_division": "https://docs.overturemaps.org/attribution/",
```

### _coord_exprs (extend overture case)
```python
if source in ("overture", "overture_division"):
```
Note: _coord_exprs is only called for containment, which divisions skip. But keeping it consistent for future use.

### argparse choices
Add `"overture_division"` to `--source` choices.

### Template variables for two-parquet-path approach
Add `--division-parquet` and `--division-area-parquet` CLI args (or derive from a single base path). The import SQL uses `${division_parquet}` and `${division_area_parquet}` instead of `${parquet_glob}`.

### Skip importance/variants
```python
if source not in ("overture_division",):
    run_sql("importance", f"{source}_importance.sql", ...)
    run_sql("variants", f"{source}_variants.sql")
```

### Boundary DB export (after write_manifest_db, before con.close)
```python
if source == "overture_division":
    boundaries_path = os.path.join(source_dir, "boundaries.duckdb")
    boundaries_tmp = boundaries_path + ".tmp"
    if os.path.exists(boundaries_tmp):
        os.remove(boundaries_tmp)
    log.info("[%s] boundary export: starting", source)
    con.execute(f"ATTACH '{boundaries_tmp}' AS bnd")
    con.execute("""
        CREATE TABLE bnd.places AS
        SELECT id, geometry, admin_level,
               min_latitude, max_latitude,
               min_longitude, max_longitude
        FROM places
        ORDER BY ST_Hilbert(geometry,
            {'min_x': -180.0, 'min_y': -90.0,
             'max_x': 180.0, 'max_y': 90.0}::BOX_2D)
    """)
    con.execute("CREATE INDEX bnd_places_rtree ON bnd.places USING RTREE(geometry)")
    con.execute("DETACH bnd")
    os.rename(boundaries_tmp, boundaries_path)
```
