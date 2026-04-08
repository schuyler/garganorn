-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

CREATE TABLE places AS
SELECT * EXCLUDE (geom), geom::GEOMETRY AS geom
FROM '${parquet_glob}'
WHERE bbox.xmin >= ${xmin} AND bbox.xmax <= ${xmax}
  AND bbox.ymin >= ${ymin} AND bbox.ymax <= ${ymax}
  AND date_refreshed > '2020-03-15'
  AND date_closed IS NULL
  AND longitude != 0 AND latitude != 0
  AND geom IS NOT NULL;

-- Compute quadkey at max zoom (used for density, tile assignment, and export)
ALTER TABLE places ADD COLUMN qk17 VARCHAR;
UPDATE places SET qk17 = ST_QuadKey(longitude, latitude, 17);
