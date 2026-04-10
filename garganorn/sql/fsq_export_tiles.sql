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

CREATE OR REPLACE VIEW tile_export AS
SELECT
    ta.tile_qk,
    to_json({
        uri: 'https://${repo}/org.atgeo.places.foursquare/' || p.fsq_place_id,
        value: {
            "$type": 'org.atgeo.place',
            rkey: p.fsq_place_id,
            name: p.name,
            importance: p.importance,
            locations: (
                '[' || to_json({
                    "$type": 'community.lexicon.location.geo',
                    latitude: p.latitude::DECIMAL(10,6)::VARCHAR,
                    longitude: p.longitude::DECIMAL(10,6)::VARCHAR
                })
                || CASE WHEN p.country IS NOT NULL
                    THEN ', ' || strip_json_nulls(to_json({
                        "$type": 'community.lexicon.location.address',
                        country: p.country,
                        region: p.region,
                        locality: p.locality,
                        street: p.address,
                        postalCode: p.postcode
                    }))
                    ELSE ''
                END
                || ']'
            )::JSON,
            variants: coalesce(p.variants, []),
            attributes: {
                fsq_place_id: p.fsq_place_id,
                date_created: p.date_created,
                date_refreshed: p.date_refreshed,
                admin_region: p.admin_region,
                post_town: p.post_town,
                po_box: p.po_box,
                tel: p.tel,
                website: p.website,
                email: p.email,
                facebook_id: p.facebook_id,
                instagram: p.instagram,
                twitter: p.twitter,
                fsq_category_ids: p.fsq_category_ids,
                fsq_category_labels: p.fsq_category_labels,
                placemaker_url: p.placemaker_url
            },
            relations: coalesce(pc.relations_json::JSON, '{}'::JSON)
        }
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.fsq_place_id
LEFT JOIN place_containment pc ON pc.place_id = p.fsq_place_id
ORDER BY ta.tile_qk;
