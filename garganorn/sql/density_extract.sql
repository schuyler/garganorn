-- Extract z15 density tiles from Overture place parquet.
-- Runs globally (no bbox filter) on the source parquet file.
-- Groups places by their z15 quadtile and computes ln(1 + count) as density_score.
-- Output is used in importance computation across all place sources.
-- Tile bounds are computed in Python post-processing via quadkey_to_bbox().
INSTALL spatial; LOAD spatial;
DROP TABLE IF EXISTS density_tiles;
CREATE TABLE density_tiles AS
SELECT left(ST_QuadKey((bbox.xmin + bbox.xmax) / 2.0,
                       (bbox.ymin + bbox.ymax) / 2.0, 17), 15) AS tile_qk15,
       ln(1 + count(*)) AS density_score
FROM '${parquet_glob}'
GROUP BY tile_qk15;
