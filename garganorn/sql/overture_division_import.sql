-- Import Overture Maps division and division_area parquet files into a places table.
--
-- Overture splits administrative boundary data across two parquet themes:
--   division       -- metadata (names, subtype, country, admin_level, wikidata, population)
--   division_area  -- geometries, one or more rows per division (filtered to is_land=true)
--
-- A single division can have multiple division_area rows (e.g. non-contiguous territories).
-- ST_Union_Agg merges them into one geometry per division. min(admin_level) handles the
-- rare case where a division's areas carry different admin_level values.
--
-- The bbox filter applies to division_area (which has bbox columns), not division.
-- Divisions with no matching land area after filtering are dropped via INNER JOIN.
--
-- importance=0 and variants=[] are inlined here rather than computed in separate SQL stages
-- (as done for fsq/overture/osm). Divisions have no density-based importance signal and
-- no name variants beyond what is in the names struct.

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
    -- Merge multiple land-area geometries per division into one.
    -- admin_level is taken from the minimum value across areas (should be uniform in practice).
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
    -- bbox is derived from the merged geometry; used for tile assignment and export
    {'xmin': ST_XMin(ma.geometry), 'ymin': ST_YMin(ma.geometry),
     'xmax': ST_XMax(ma.geometry), 'ymax': ST_YMax(ma.geometry)} AS bbox,
    -- qk17 placed at the geometry centroid for tile assignment
    ST_QuadKey(
        (ST_XMin(ma.geometry) + ST_XMax(ma.geometry)) / 2.0,
        (ST_YMin(ma.geometry) + ST_YMax(ma.geometry)) / 2.0, 17
    ) AS qk17,
    -- min/max extents stored flat for fast bbox-filter in containment queries
    ST_YMin(ma.geometry) AS min_latitude,
    ST_YMax(ma.geometry) AS max_latitude,
    ST_XMin(ma.geometry) AS min_longitude,
    ST_XMax(ma.geometry) AS max_longitude,
    0 AS importance,
    []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] AS variants
FROM division d
JOIN merged_areas ma ON ma.division_id = d.id;
