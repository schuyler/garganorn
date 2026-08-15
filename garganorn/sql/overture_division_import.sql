-- Import Overture Maps division and division_area parquet files into a places table.
--
-- Overture splits administrative boundary data across two parquet themes:
--   division       -- metadata (names, subtype, country, wikidata, population)
--   division_area  -- geometries, one or more rows per division (filtered to is_land=true)
--
-- A single division can have multiple division_area rows (e.g. non-contiguous territories).
-- ST_Union_Agg merges them into one geometry per division.
--
-- The atgeo containment `level` is derived from `subtype` alone via a
-- generated CASE expression, substituted below as a dollar-brace placeholder
-- (not repeated literally in this comment because substitution is plain
-- string replacement and would corrupt the comment text). It is rendered by
-- stage_division_import() in stages.py from garganorn.levels.LEVEL_VOCAB,
-- the single source of truth. Overture's raw
-- admin_level is 96% NULL and ambiguous within a subtype, so it is not
-- carried into places.
--
-- The bbox filter applies to division_area (which has bbox columns), not division.
-- Divisions with no matching land area after filtering are dropped via INNER JOIN.
--
-- Importance and variants are computed inline during import. Divisions use a
-- hybrid formula: density avg for localities, population log for all divisions.

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
    SELECT division_id,
           geometry::GEOMETRY AS geometry
    FROM '${division_area_parquet}'
    WHERE is_land = true
      AND geometry IS NOT NULL
      AND bbox.xmax >= ${xmin} AND bbox.xmin <= ${xmax}
      AND bbox.ymax >= ${ymin} AND bbox.ymin <= ${ymax}
),
merged_areas AS (
    -- Merge multiple land-area geometries per division into one.
    SELECT division_id,
           ST_Union_Agg(geometry) AS geometry
    FROM division_area
    GROUP BY division_id
),
merged_areas_interior AS (
    SELECT division_id, geometry,
           ST_PointOnSurface(geometry) AS interior_point
    FROM merged_areas
),
division_base AS (
    SELECT
        d.id,
        ma.geometry,
        d.names,
        d.subtype,
        d.country,
        d.region,
        -- atgeo containment level, derived from subtype alone (no ELSE branch:
        -- an unmapped subtype must produce NULL here so the fail-loud validator
        -- and the post-CTAS NULL-level assertion in stage_division_import()
        -- catch it).
        ${level_case} AS level,
        d.wikidata,
        greatest(coalesce(d.population, 0), 0) AS population,
        d.parent_division_id,
        -- bbox is derived from the merged geometry; used for tile assignment and export
        {'xmin': ST_XMin(ma.geometry), 'ymin': ST_YMin(ma.geometry),
         'xmax': ST_XMax(ma.geometry), 'ymax': ST_YMax(ma.geometry)} AS bbox,
        -- qk17 placed at the interior point -- the same point containment
        -- tests -- so a division's tile and its containment answer agree.
        -- A bbox midpoint can fall outside a crescent or multipart division
        -- entirely, which puts the record in a tile it isn't in.
        qk17(ST_X(ma.interior_point), ST_Y(ma.interior_point)) AS qk17,
        -- min/max extents stored flat for fast bbox-filter in containment queries
        ST_YMin(ma.geometry) AS min_latitude,
        ST_YMax(ma.geometry) AS max_latitude,
        ST_XMin(ma.geometry) AS min_longitude,
        ST_XMax(ma.geometry) AS max_longitude,
        ST_X(ma.interior_point) AS interior_lon,
        ST_Y(ma.interior_point) AS interior_lat
    FROM division d
    JOIN merged_areas_interior ma ON ma.division_id = d.id
),
-- Compute average density for localities: find all z15 density tiles whose
-- bbox intersects the locality's bounding box. Uses bbox-overlap join
-- (not centroid-point-in-bbox) to ensure small localities receive density scores.
division_density AS (
    SELECT p.id,
           coalesce(avg(d.density_score), 0) AS avg_density
    FROM division_base p
    LEFT JOIN density_tiles d
        ON d.tile_xmin <= p.max_longitude
       AND d.tile_xmax >= p.min_longitude
       AND d.tile_ymin <= p.max_latitude
       AND d.tile_ymax >= p.min_latitude
    WHERE p.subtype = 'locality'
    GROUP BY p.id
)
SELECT
    db.*,
    CASE
        WHEN db.subtype = 'locality' THEN
            round(60 * least(coalesce(dd.avg_density, 0) / ${density_norm}, 1.0)
                + 40 * least(ln(1 + db.population) / ${pop_norm}, 1.0))::INTEGER
        ELSE
            round(40 * least(ln(1 + db.population) / ${pop_norm}, 1.0))::INTEGER
    END AS importance,
    []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] AS variants
FROM division_base db
LEFT JOIN division_density dd USING (id);

-- Drop temp tables
DROP TABLE density_tiles;
