-- Overture tile JSON export view.
-- Inputs:
--   places          — Overture places table (imported via import-overture-extract.sh)
--   tile_assignments — columns: place_id VARCHAR, tile_qk VARCHAR
-- Substitution params: ${repo}
-- Addresses are rendered inline via list_transform/list_filter — no pre-materialization.

CREATE OR REPLACE VIEW tile_export AS
SELECT
    ta.tile_qk,
    to_json({
        uri: 'https://${repo}/org.atgeo.places.overture/' || p.id,
        value: {
            "$type": 'org.atgeo.place',
            rkey: p.id,
            name: p.names."primary",
            importance: p.importance,
            locations: list_concat(
                [{
                    "$type": 'community.lexicon.location.geo',
                    -- bbox mean avoids spatial function overhead; identical to centroid for point geometries (the vast majority)
                    latitude: ((p.bbox.ymin + p.bbox.ymax) / 2)::DECIMAL(10,6)::VARCHAR,
                    longitude: ((p.bbox.xmin + p.bbox.xmax) / 2)::DECIMAL(10,6)::VARCHAR
                }],
                list_transform(
                    list_filter(coalesce(p.addresses, []), addr -> addr.country IS NOT NULL),
                    addr -> {
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
                    }
                )
            ),
            variants: coalesce(p.variants, []),
            attributes: {
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
            },
            relations: '{}'::JSON
        }
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.id
ORDER BY ta.tile_qk;
