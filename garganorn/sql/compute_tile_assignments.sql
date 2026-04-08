DROP TABLE IF EXISTS tile_assignments;

-- Count places per tile at each zoom level
DROP TABLE IF EXISTS tile_counts;
CREATE TEMP TABLE tile_counts AS
SELECT level, left(qk17, level) AS qk, count(*) AS cnt
FROM places, generate_series(${min_zoom}, ${max_zoom}) AS t(level)
WHERE qk17 IS NOT NULL
GROUP BY level, left(qk17, level);

-- Find coarsest zoom where tile count <= max_per_tile, then assign each place to a tile
CREATE TABLE tile_assignments AS
WITH place_zoom AS (
    SELECT p.${pk_expr} AS place_id, t.level, left(p.qk17, t.level) AS qk
    FROM places p
    CROSS JOIN generate_series(${min_zoom}, ${max_zoom}) AS t(level)
    WHERE p.qk17 IS NOT NULL
),
best_zoom AS (
    SELECT pz.place_id, min(pz.level) AS level
    FROM place_zoom pz
    JOIN tile_counts tc ON tc.level = pz.level AND tc.qk = pz.qk
    WHERE tc.cnt <= ${max_per_tile}
    GROUP BY pz.place_id
)
SELECT p.${pk_expr} AS place_id,
       left(p.qk17, coalesce(bz.level, ${max_zoom})) AS tile_qk
FROM places p
LEFT JOIN best_zoom bz ON bz.place_id = p.${pk_expr}
WHERE p.qk17 IS NOT NULL
ORDER BY tile_qk;  -- sort enables streaming GROUP BY in export query

-- Drop temp tables (tile_counts is TEMP and would auto-drop at connection close,
-- but dropping explicitly frees memory sooner)
DROP TABLE tile_counts;
