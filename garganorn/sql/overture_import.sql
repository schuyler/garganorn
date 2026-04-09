-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

CREATE TABLE places AS
SELECT * EXCLUDE (geometry), geometry::GEOMETRY AS geometry,
       ST_QuadKey((bbox.xmin + bbox.xmax) / 2.0, (bbox.ymin + bbox.ymax) / 2.0, 17) AS qk17  -- inline avoids the two-pass approach
FROM '${parquet_glob}'
WHERE bbox.xmin >= ${xmin} AND bbox.xmax <= ${xmax}
  AND bbox.ymin >= ${ymin} AND bbox.ymax <= ${ymax}
  AND geometry IS NOT NULL;
