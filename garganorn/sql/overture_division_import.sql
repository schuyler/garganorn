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
-- Phase 2: importance and variants are computed inline during import (density+population+importance unified in import CTAS).
-- Divisions use a hybrid formula: density avg for localities, population log for all divisions.

DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

-- Load density parquet file as temp table (no IDF for divisions)
-- When parquet path is None, create empty temp table (LEFT JOINs produce NULL, importance defaults to 0)
${density_cte}

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
),
division_base AS (
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
        ST_XMax(ma.geometry) AS max_longitude
    FROM division d
    JOIN merged_areas ma ON ma.division_id = d.id
),
-- Compute average density for localities: find all z15 density tile centroids
-- that fall within the locality's bounding box.
division_density AS (
    SELECT p.id,
           coalesce(avg(d.density_score), 0) AS avg_density
    FROM division_base p
    LEFT JOIN density_tiles d
        ON d.centroid_lon BETWEEN p.min_longitude AND p.max_longitude
       AND d.centroid_lat BETWEEN p.min_latitude AND p.max_latitude
    WHERE p.subtype = 'locality'
    GROUP BY p.id
)
SELECT
    db.*,
    CASE
        WHEN db.subtype = 'locality' THEN
            round(60 * least(coalesce(dd.avg_density, 0) / ${density_norm}, 1.0)
                + 40 * least(ln(1 + coalesce(db.population, 0)) / ${pop_norm}, 1.0))::INTEGER
        ELSE
            round(40 * least(ln(1 + coalesce(db.population, 0)) / ${pop_norm}, 1.0))::INTEGER
    END AS importance,
    []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] AS variants
FROM division_base db
LEFT JOIN division_density dd USING (id);

-- Drop temp tables
DROP TABLE density_tiles;
