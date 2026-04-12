-- Extract z15 density tiles from Overture place parquet.
-- Runs globally (no bbox filter) on the source parquet file.
-- Groups places by their z15 quadtile and computes ln(1 + count) as density_score.
-- Output is used in importance computation across all place sources.
-- Centroid columns: average of place bbox centers per z15 tile.
-- Used by division_importance_backfill.sql for spatial join with division bboxes.
INSTALL spatial; LOAD spatial;
DROP TABLE IF EXISTS density_tiles;
CREATE TABLE density_tiles AS
SELECT left(ST_QuadKey((bbox.xmin + bbox.xmax) / 2.0,
                       (bbox.ymin + bbox.ymax) / 2.0, 17), 15) AS tile_qk15,
       ln(1 + count(*)) AS density_score,
       avg((bbox.xmin + bbox.xmax) / 2.0) AS centroid_lon,
       avg((bbox.ymin + bbox.ymax) / 2.0) AS centroid_lat
FROM '${parquet_glob}'
GROUP BY tile_qk15;
