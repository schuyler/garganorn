DROP TABLE IF EXISTS tile_export;
CREATE TABLE tile_export AS
SELECT
    ta.tile_qk,
    to_json({
        attribution: '${attribution}',
        records: list({
            uri: 'https://${repo}/org.atgeo.places.foursquare/' || p.fsq_place_id,
            value: {
                "$type": 'org.atgeo.place',
                rkey: p.fsq_place_id,
                name: p.name,
                importance: p.importance,
                locations: list_concat(
                    [{
                        "$type": 'community.lexicon.location.geo',
                        latitude: p.latitude::DECIMAL(10,6)::VARCHAR,
                        longitude: p.longitude::DECIMAL(10,6)::VARCHAR
                    }],
                    CASE WHEN p.country IS NOT NULL THEN [{
                        "$type": 'community.lexicon.location.address',
                        country: p.country,
                        region: p.region,
                        locality: p.locality,
                        street: p.address,
                        postalCode: p.postcode
                    }] ELSE []::STRUCT("$type" VARCHAR, country VARCHAR, region VARCHAR, locality VARCHAR, street VARCHAR, postalCode VARCHAR)[] END
                ),
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
                relations: MAP {}
            }
        })
    })::VARCHAR AS tile_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.fsq_place_id
GROUP BY ta.tile_qk;
