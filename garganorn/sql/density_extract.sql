-- Extract z15 density tiles from Overture place parquet.
-- Runs globally (no bbox filter) on the source parquet file.
-- Groups places by their z15 quadtile and computes ln(1 + count) as density_score.
-- Output is used in importance computation across all place sources.
-- Tile bounds are computed in SQL via the qk_env() macro (garganorn/sql/qk_env_macro.sql),
-- evaluated once per row and split into xmin/ymin/xmax/ymax via ST_XMin/ST_YMin/ST_XMax/ST_YMax.
INSTALL spatial; LOAD spatial;
DROP TABLE IF EXISTS density_tiles;
CREATE TABLE density_tiles AS
WITH density AS (
    SELECT left(qk17((bbox.xmin + bbox.xmax) / 2.0,
                     (bbox.ymin + bbox.ymax) / 2.0), 15) AS tile_qk15,
           ln(1 + count(*)) AS density_score
    FROM '${parquet_glob}'
    GROUP BY tile_qk15
),
bounded AS (
    SELECT tile_qk15, density_score, qk_env(tile_qk15) AS env
    FROM density
)
SELECT tile_qk15,
       density_score,
       ST_XMin(env) AS tile_xmin,
       ST_YMin(env) AS tile_ymin,
       ST_XMax(env) AS tile_xmax,
       ST_YMax(env) AS tile_ymax
FROM bounded;
