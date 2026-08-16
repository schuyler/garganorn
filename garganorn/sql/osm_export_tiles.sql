CREATE OR REPLACE VIEW tile_export AS
SELECT
    ta.tile_qk,
    ta.place_id,
    CASE left(p.rkey, 1)
        WHEN 'n' THEN 'node:' || substr(p.rkey, 2)
        WHEN 'w' THEN 'way:' || substr(p.rkey, 2)
        WHEN 'r' THEN 'relation:' || substr(p.rkey, 2)
        ELSE p.rkey
    END AS rkey,
    to_json({
        "$type": 'org.atgeo.place',
        rkey: CASE left(p.rkey, 1)
                  WHEN 'n' THEN 'node:' || substr(p.rkey, 2)
                  WHEN 'w' THEN 'way:' || substr(p.rkey, 2)
                  WHEN 'r' THEN 'relation:' || substr(p.rkey, 2)
                  ELSE p.rkey
              END,
        name: p.name,
        importance: p.importance,
        locations: [{
            "$type": 'community.lexicon.location.geo',
            latitude: p.latitude::DECIMAL(10,6)::VARCHAR,
            longitude: p.longitude::DECIMAL(10,6)::VARCHAR
        }],
        variants: coalesce(p.variants, []),
        attributes: CASE
            WHEN p.primary_category IS NOT NULL
            THEN map_concat(
                coalesce(p.tags, MAP([], [])),
                map(
                    [split_part(p.primary_category, '=', 1)],
                    [split_part(p.primary_category, '=', 2)]
                )
            )
            ELSE coalesce(p.tags, MAP([], []))
        END,
        relations: coalesce(pc.relations_json::JSON, '{}'::JSON)
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.rkey
-- Keyed on (place_id, tile_qk): a place can carry two tile_assignments rows
-- (its own band and the z1-z5 summary band), each with its own containment row.
LEFT JOIN place_containment pc ON pc.place_id = p.rkey AND pc.tile_qk = ta.tile_qk;
-- No ORDER BY: stage_export sorts per partition (pass 2), not here.
