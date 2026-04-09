-- Overture tile JSON export view.
-- Inputs:
--   places          — Overture places table (imported via import-overture-extract.sh)
--   tile_assignments — columns: place_id VARCHAR, tile_qk VARCHAR
-- Substitution params: ${repo}

-- Pre-materialize per-place address aggregation to avoid re-evaluation per tile in the VIEW.
DROP TABLE IF EXISTS place_addresses;
CREATE TEMP TABLE place_addresses AS
SELECT
    p.id AS place_id,
    list({
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
    } ORDER BY addr.country) AS address_locations
FROM places p,
     unnest(p.addresses) AS t(addr)
WHERE addr.country IS NOT NULL
GROUP BY p.id;

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
                    latitude: st_y(st_centroid(p.geometry))::DECIMAL(10,6)::VARCHAR,
                    longitude: st_x(st_centroid(p.geometry))::DECIMAL(10,6)::VARCHAR
                }],
                coalesce(pa.address_locations, [])
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
LEFT JOIN place_addresses pa ON pa.place_id = p.id
ORDER BY ta.tile_qk;
