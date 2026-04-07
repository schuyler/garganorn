CREATE OR REPLACE VIEW tile_export AS
SELECT
    ta.tile_qk,
    to_json({
        attribution: '${attribution}',
        records: list({
            uri: 'https://${repo}/org.atgeo.places.osm/' ||
                 CASE left(p.rkey, 1)
                     WHEN 'n' THEN 'node:' || substr(p.rkey, 2)
                     WHEN 'w' THEN 'way:' || substr(p.rkey, 2)
                     WHEN 'r' THEN 'relation:' || substr(p.rkey, 2)
                     ELSE p.rkey
                 END,
            value: {
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
                relations: '{}'::JSON
            }
        })
    })::VARCHAR AS tile_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.rkey
GROUP BY ta.tile_qk;
