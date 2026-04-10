-- Export tile records for the overture_division source.
--
-- Divisions are administrative boundaries (polygons), not points. The location
-- is expressed as a community.lexicon.location.bbox using the geometry's bounding
-- extents rather than a point. No geometry is included in the tile output — the
-- full polygon is available in boundaries.duckdb for containment queries.
--
-- Attributes include admin_level (OSM-style 1–11 hierarchy), subtype (e.g.
-- "country", "region", "county"), country/region ISO codes, wikidata QID, and
-- population where present in the source data.
--
-- relations carries containment assignments populated by compute_containment
-- (which divisions contain this one). Left join means records without
-- containment data get an empty relations object.

CREATE OR REPLACE VIEW tile_export AS
SELECT
    ta.tile_qk,
    to_json({
        uri: 'https://${repo}/org.atgeo.places.overture.division/' || p.id,
        value: {
            "$type": 'org.atgeo.place',
            rkey: p.id,
            name: p.names."primary",
            importance: p.importance,
            locations: [{
                "$type": 'community.lexicon.location.bbox',
                north: p.max_latitude::DECIMAL(10,6)::VARCHAR,
                south: p.min_latitude::DECIMAL(10,6)::VARCHAR,
                east: p.max_longitude::DECIMAL(10,6)::VARCHAR,
                west: p.min_longitude::DECIMAL(10,6)::VARCHAR
            }],
            variants: coalesce(p.variants, []),
            attributes: {
                subtype: p.subtype,
                country: p.country,
                region: p.region,
                admin_level: p.admin_level,
                wikidata: p.wikidata,
                population: p.population
            },
            relations: coalesce(pc.relations_json::JSON, '{}'::JSON)
        }
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.id
LEFT JOIN place_containment pc ON pc.place_id = p.id
ORDER BY ta.tile_qk;
