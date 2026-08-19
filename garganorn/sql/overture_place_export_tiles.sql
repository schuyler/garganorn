-- strip_json_nulls is defined in json_macros.sql, loaded onto the
-- connection before this file runs (stage_export in stages.py; tests use
-- tests/quadtree_helpers.py's _load_sql, which does the same).

-- Overture tile JSON export view.
-- Inputs:
--   places          — Overture places table (imported via stage_import →
--                     overture_place_import.sql)
--   tile_assignments — columns: place_id VARCHAR, tile_qk VARCHAR
-- Substitution params: ${repo}
-- Addresses are rendered inline via list_transform/list_filter — no pre-materialization.

CREATE OR REPLACE VIEW tile_export AS
SELECT
    ta.tile_qk,
    ta.place_id,
    p.id AS rkey,
    to_json({
        "$type": 'org.atgeo.place',
        rkey: p.id,
        name: p.names."primary",
        importance: p.importance,
        locations: (
            '[' || to_json({
                "$type": 'community.lexicon.location.geo',
                -- bbox mean avoids spatial function overhead; identical to centroid for point geometries (the vast majority)
                latitude: ((p.bbox.ymin + p.bbox.ymax) / 2)::DECIMAL(10,6)::VARCHAR,
                longitude: ((p.bbox.xmin + p.bbox.xmax) / 2)::DECIMAL(10,6)::VARCHAR
            })
            || CASE WHEN len(list_filter(coalesce(p.addresses, []), addr -> addr.country IS NOT NULL)) > 0
                THEN ', ' || array_to_string(
                    list_transform(
                        list_filter(coalesce(p.addresses, []), addr -> addr.country IS NOT NULL),
                        addr -> strip_json_nulls(to_json({
                            "$type": 'community.lexicon.location.address',
                            country: addr.country,
                            region: CASE
                                        WHEN position('-' IN addr.region) > 0
                                        THEN substr(addr.region, position('-' IN addr.region) + 1)
                                        ELSE addr.region
                                    END,
                            locality: addr.locality,
                            street: addr.freeform,
                            postalCode: addr.postcode
                        }))::VARCHAR
                    ),
                    ', '
                )
                ELSE ''
            END
            || ']'
        )::JSON,
        variants: list_transform(coalesce(p.variants, []), v -> strip_json_nulls(to_json(v))),
        attributes: strip_json_nulls(to_json({
            id: p.id,
            names: p.names,
            categories: p.categories,
            websites: p.websites,
            socials: p.socials,
            emails: p.emails,
            phones: p.phones,
            brand: p.brand,
            confidence: p.confidence::DECIMAL(4,3)::VARCHAR,
            version: p.version,
            sources: p.sources
        })),
        relations: coalesce(pc.relations_json::JSON, '{}'::JSON)
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.id
-- Keyed on (place_id, tile_qk): a place can carry two tile_assignments rows
-- (its own band and the z1-z5 summary band), each with its own containment row.
LEFT JOIN place_containment pc ON pc.place_id = p.id AND pc.tile_qk = ta.tile_qk;
-- No ORDER BY: stage_export sorts per partition (pass 2), not here.
