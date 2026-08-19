-- Export tile records for the overture_division source.
--
-- Divisions are administrative boundaries (polygons), not points. The location
-- is expressed as a community.lexicon.location.bbox using the geometry's bounding
-- extents rather than a point. No geometry is included in the tile output — the
-- full polygon is available in boundaries.duckdb for containment queries.
--
-- Attributes include level (the atgeo containment level vocabulary,
-- garganorn.levels.LEVEL_VOCAB, derived from subtype -- not raw Overture
-- admin_level), subtype (e.g. "country", "region", "county"), country/region
-- ISO codes, wikidata QID, and population where present in the source data.
--
-- relations carries containment assignments populated by
-- stage_division_containment (which divisions contain this one). Left join
-- means records without containment data -- a division whose hierarchy chain
-- holds only itself -- get an empty relations object.

-- strip_json_nulls is defined in json_macros.sql, loaded onto the
-- connection before this file runs (stage_export in stages.py; tests use
-- tests/quadtree_helpers.py's _load_sql, which does the same).

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
        locations: [{
            "$type": 'community.lexicon.location.bbox',
            north: p.max_latitude::DECIMAL(10,6)::VARCHAR,
            south: p.min_latitude::DECIMAL(10,6)::VARCHAR,
            east: p.max_longitude::DECIMAL(10,6)::VARCHAR,
            west: p.min_longitude::DECIMAL(10,6)::VARCHAR
        }],
        variants: list_transform(coalesce(p.variants, []), v -> strip_json_nulls(to_json(v))),
        attributes: strip_json_nulls(to_json({
            subtype: p.subtype,
            country: p.country,
            region: p.region,
            level: p.level,
            wikidata: p.wikidata,
            population: p.population
        })),
        relations: coalesce(pc.relations_json::JSON, '{}'::JSON)
    })::VARCHAR AS record_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.id
-- Keyed on (place_id, tile_qk), not place_id alone: a division is referenced
-- by every tile its geometry reaches, and stage_division_containment emits one
-- row per (tile_qk, place_id), so both sides carry N rows for an N-tile division.
-- Joining on place_id alone would pair every tile with every containment row
-- and export N^2 copies of the record.
LEFT JOIN place_containment pc
       ON pc.place_id = p.id AND pc.tile_qk = ta.tile_qk;
-- No ORDER BY: stage_export sorts per partition (pass 2), not here.
