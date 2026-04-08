-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

CREATE TABLE places AS
SELECT * EXCLUDE (geometry), geometry::GEOMETRY AS geometry
FROM '${parquet_glob}'
WHERE bbox.xmin >= ${xmin} AND bbox.xmax <= ${xmax}
  AND bbox.ymin >= ${ymin} AND bbox.ymax <= ${ymax}
  AND geometry IS NOT NULL;

-- Compute quadkey at max zoom (used for density, tile assignment, and export)
ALTER TABLE places ADD COLUMN qk17 VARCHAR;
UPDATE places SET qk17 = ST_QuadKey((bbox.xmin + bbox.xmax) / 2.0,
                                     (bbox.ymin + bbox.ymax) / 2.0, 17);
