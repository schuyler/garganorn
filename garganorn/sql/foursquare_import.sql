-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

-- Load density and IDF parquet files as temp tables
-- When parquet path is None, create empty temp tables (LEFT JOINs produce NULL, importance defaults to 0)
${density_cte}
${idf_cte}

-- Import Foursquare data and compute importance in one pass (Phase 2: density+IDF+importance unified in import CTAS)
CREATE TABLE places AS
WITH fsq_base AS (
    SELECT * EXCLUDE (geom),
           geom::GEOMETRY AS geom,
           ST_QuadKey(longitude, latitude, 17) AS qk17
    FROM '${parquet_glob}'
    WHERE bbox.xmin >= ${xmin} AND bbox.xmax <= ${xmax}
      AND bbox.ymin >= ${ymin} AND bbox.ymax <= ${ymax}
      AND date_refreshed > '2020-03-15'
      AND date_closed IS NULL
      AND longitude != 0 AND latitude != 0
      AND geom IS NOT NULL
),
fsq_density AS (
    SELECT f.fsq_place_id,
           coalesce(d.density_score, 0) AS density_score
    FROM fsq_base f
    LEFT JOIN density_tiles d ON d.tile_qk15 = left(f.qk17, 15)
),
fsq_idf AS (
    SELECT f.fsq_place_id,
           coalesce(max(i.idf_score), 0) AS idf_score
    FROM fsq_base f,
         unnest(f.fsq_category_ids) AS t(category)
    LEFT JOIN idf_scores i ON i.category = t.category
    WHERE f.fsq_category_ids IS NOT NULL
    GROUP BY f.fsq_place_id
)
SELECT b.*,
       round(
           60 * least(coalesce(fd.density_score, 0) / ${density_norm}, 1.0)
         + 40 * least(coalesce(fi.idf_score, 0) / ${idf_norm}, 1.0)
       )::INTEGER AS importance,
       []::VARCHAR[] AS variants
FROM fsq_base b
LEFT JOIN fsq_density fd USING (fsq_place_id)
LEFT JOIN fsq_idf fi USING (fsq_place_id);

-- Drop temp tables
DROP TABLE density_tiles;
DROP TABLE idf_scores;
