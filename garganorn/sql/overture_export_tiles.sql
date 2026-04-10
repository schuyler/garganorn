-- Strip null-valued keys from a JSON object. Needed because locations are
-- built as independent structs (geo, address) to avoid DuckDB's list_concat
-- struct-union, which adds spurious null fields from the other type. The
-- $."key" path syntax handles the $type key (leading $ is JSONPath root).
-- TODO: Replace with native json_strip_nulls() once a DuckDB release ships
-- it (merged in PR #21748, 2026-04-02; not yet in v1.5.1).
CREATE OR REPLACE MACRO strip_json_nulls(js) AS
    map_from_entries(
        [(key, json_extract(js, '$."' || key || '"')) FOR key IN json_keys(js)
         IF json_extract(js, '$."' || key || '"') <> ('null'::JSON)]
    )::JSON;

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
        uri: 'https://${repo}/org.atgeo.places.overture.place/' || p.id,
        value: {
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
            relations: coalesce(pc.relations_json::JSON, '{}'::JSON)
        }
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.id
LEFT JOIN place_containment pc ON pc.place_id = p.id
ORDER BY ta.tile_qk;
