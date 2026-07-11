-- covering_seed.sql: create l_current from the boundary × seed-tile join.
--
-- Preconditions (set up by stage_covering in covering.py before running this):
--   - LOAD spatial; macros qk_tile_x, qk_tile_y, qk_env created
--   - ATTACH boundaries_db READ_ONLY AS bnd
--   - TEMP TABLE z4_tiles(qk VARCHAR, xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
--     populated with all cover_min_zoom tiles (256 rows at z4)
--   - TEMP TABLE covering_out(tile_qk, boundary_id, level, kind) created
--
-- D7: boundaries whose min_longitude > max_longitude cross the antimeridian;
-- their bbox is represented as two lobes.  The join condition handles both
-- the normal (min <= max) and antimeridian (min > max) cases.
--
-- Post-condition: TEMP TABLE l_current(boundary_id, level, tile_qk, geom)
-- contains one row per (boundary, seed tile) pair where the boundary
-- intersects the tile and the clipped geometry is valid and non-degenerate.

CREATE TEMP TABLE l_current AS
SELECT * FROM (
    SELECT b.id AS boundary_id,
           b.level,
           t.qk AS tile_qk,
           ST_Intersection(b.geometry, qk_env(t.qk)) AS geom
    FROM bnd.places b
    JOIN z4_tiles t
      ON b.min_latitude  <= t.ymax AND b.max_latitude  >= t.ymin
     AND (CASE WHEN b.min_longitude <= b.max_longitude
               THEN b.min_longitude <= t.xmax AND b.max_longitude >= t.xmin
               ELSE b.min_longitude <= t.xmax OR  b.max_longitude >= t.xmin
          END)
    WHERE ST_Intersects(b.geometry, qk_env(t.qk))
) _seed
WHERE ST_IsValid(geom) AND ST_Area(geom) > 0
