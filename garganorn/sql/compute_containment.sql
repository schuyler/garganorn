-- compute_containment.sql: per-prefix containment query template.
--
-- Parameters substituted by compute_containment() in stages.py (dollar-brace
-- placeholders; not repeated literally in these comments because substitution
-- is plain string replacement and would corrupt the comment text):
--   interior_arms     : UNION ALL of interior-arm SELECTs (one per zoom level L
--                        in [cover_min_zoom, cover_max_zoom])
--   edge_arms         : UNION ALL of edge-arm SELECTs (one per zoom level L in
--                        [cover_min_leaf_zoom, cover_max_zoom])
--   collection_prefix : rkey NSID prefix (org.atgeo.places.overture.division)
--
-- Preconditions (set up by compute_containment before executing this):
--   - LOAD spatial; ATTACH boundaries_db READ_ONLY AS bnd
--   - TABLE tile_assignments(place_id, tile_qk) in the working connection
--   - TEMP TABLE cov, built once per z4 group from covering/<z4_prefix>.parquet
--   - TEMP TABLE p(place_id, qk17, lon, lat), built once per batch from a
--     qk17 range filter on places_slim. Deliberately a real table, not a
--     CTE filtering places_slim inline: DuckDB plans a CTE against the full
--     ~75M-row backing relation regardless of filter selectivity, which
--     measured a catastrophic (100x+) memory blowup once the edge arm joined
--     that plan against a geometrically complex boundary (observed: Canada/
--     Nunavut, ~200k vertices). Pre-materializing the tiny per-batch subset,
--     exactly like `cov`, keeps the join's true input size visible to the
--     planner. Confirmed empirically: identical query, identical output,
--     ~150MB vs 30GB+ depending on whether `p` is pre-built or inlined.
--
-- The edge arm tests the stored fragment (`cov.geom`) with ST_Covers instead
-- of joining bnd.places -- the fragment IS the geometry to test, at most
-- tile-sized and at most V vertices, which is the cost bound this stage
-- exists to establish. This also retires D7's min_longitude CASE: it lived
-- on the bnd.places bbox pre-filter, which no longer exists here (D7 remains
-- live on the build side, in covering_seed.sql and bbox_to_quadkeys).
--
-- Output: (tile_qk, place_id, relations_json) sorted (tile_qk, place_id).
-- Written directly to ${output_tmp} via COPY; caller renames to final path.

WITH interior AS (
${interior_arms}
),
edge AS (${edge_arms}
),
matches AS (
    SELECT * FROM interior
    UNION ALL
    SELECT * FROM edge
)
SELECT ta.tile_qk,
       m.place_id,
       to_json({within: list({rkey: '${collection_prefix}:' || m.boundary_id,
                               name: bp.names."primary",
                               level: m.level}
                              ORDER BY m.level ASC, m.boundary_id ASC)})::VARCHAR AS relations_json
FROM matches m
JOIN tile_assignments ta ON ta.place_id = m.place_id
JOIN bnd.places bp ON bp.id = m.boundary_id
GROUP BY ta.tile_qk, m.place_id
ORDER BY ta.tile_qk, m.place_id
