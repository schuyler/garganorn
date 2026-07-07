-- covering_level.sql: one non-terminal iteration of the covering level loop
-- (z < cover_max_zoom).  Run once per zoom level from cover_min_zoom to
-- cover_max_zoom - 1.
--
-- Preconditions: l_current and covering_out temp tables exist (see covering_seed.sql).
--
-- Steps:
--   1. Materialize ST_Contains flag once per row (dominant cost; do not evaluate twice).
--   2. Emit interior rows to covering_out.
--   3. Expand non-interior rows ×4 (children '0','1','2','3'), re-clip to
--      child envelope, drop degenerate clips.
--   4. Rename l_next → l_current for the next iteration.

CREATE TEMP TABLE l_flagged AS
SELECT *, ST_Contains(geom, qk_env(tile_qk)) AS is_interior
FROM l_current;

INSERT INTO covering_out
SELECT tile_qk, boundary_id, level, 'interior'
FROM l_flagged
WHERE is_interior;

-- Clipping correctness: clipping boundary geometry to a tile's own envelope
-- preserves containment/intersection classification for that tile.  For tile T
-- and boundary geometry G:
--   ST_Contains(G ∩ clip_env, T) ⟺ ST_Contains(G, T)
--   ST_Intersects(G ∩ clip_env, T) ⟺ ST_Intersects(G, T)
-- when clip_env ⊇ T's envelope (each level clips against the tile's own
-- envelope, so this holds).
CREATE TEMP TABLE l_next AS
SELECT * FROM (
    SELECT boundary_id,
           level,
           tile_qk || d.d AS tile_qk,
           ST_Intersection(geom, qk_env(tile_qk || d.d)) AS geom
    FROM l_flagged,
         (VALUES ('0'), ('1'), ('2'), ('3')) d(d)
    WHERE NOT is_interior
) _expanded
WHERE ST_IsValid(geom) AND ST_Area(geom) > 0;

DROP TABLE l_flagged;
DROP TABLE l_current;
ALTER TABLE l_next RENAME TO l_current
