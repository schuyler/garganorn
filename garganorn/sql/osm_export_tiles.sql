-- strip_json_nulls is defined in json_macros.sql, loaded onto the
-- connection before this file runs (stage_export in stages.py; tests use
-- tests/quadtree_helpers.py's _load_sql, which does the same).

CREATE OR REPLACE VIEW tile_export AS
SELECT
    ta.tile_qk,
    ta.place_id,
    to_json({
        value: {
            "$type": 'org.atgeo.place',
            origin: 'urn:atgeo:' || ${manifest_key},
            name: p.name,
            locations: [{
                "$type": 'community.lexicon.location.geo',
                latitude: p.latitude::DECIMAL(10,6)::VARCHAR,
                longitude: p.longitude::DECIMAL(10,6)::VARCHAR
            }],
            variants: list_transform(coalesce(p.variants, []), v -> strip_json_nulls(to_json(v))),
            attributes: CASE
                WHEN p.primary_category IS NOT NULL
                THEN map_concat(
                    coalesce(p.tags, MAP([], [])),
                    map(
                        [split_part(p.primary_category, '=', 1)],
                        [substr(p.primary_category, strpos(p.primary_category, '=') + 1)]
                    )
                )
                ELSE coalesce(p.tags, MAP([], []))
            END,
            relations: coalesce(pc.relations_json::JSON, '{}'::JSON)
        },
        importance: p.importance
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.rkey
-- Keyed on (place_id, tile_qk): a place can carry two tile_assignments rows
-- (its own band and the z1-z5 summary band), each with its own containment row.
LEFT JOIN place_containment pc ON pc.place_id = p.rkey AND pc.tile_qk = ta.tile_qk;
-- No ORDER BY: stage_export sorts per partition (pass 2), not here.
