-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

CREATE OR REPLACE MACRO atgeo_valid_language(lang) AS (
    lang IS NOT NULL AND regexp_matches(lang, '^(i|[a-z]{2,3})(-[A-Za-z0-9-]+)?$')
);

-- osm_dropped_suffix/osm_variant_type_lang derive the `variants` field from
-- OSM name tags: tag key -> type/language, per the mapping and drop-list
-- below. OR REPLACE because fixtures that reuse a connection across
-- multiple import calls would otherwise hit CREATE MACRO's error on a
-- second definition.
--
-- osm_dropped_suffix: annotation suffixes that are not names. Exact match or
-- an "X:"-prefixed localized form, e.g. 'prefix:ru', for every family except
-- word_stress, which is a suffix match (regexp, not an unescaped LIKE
-- '%word_stress' -- DuckDB LIKE treats '_' as a single-character wildcard).
CREATE OR REPLACE MACRO osm_dropped_suffix(suffix) AS (
    suffix = 'prefix' OR suffix LIKE 'prefix:%'
    OR suffix = 'etymology' OR suffix LIKE 'etymology:%'
    OR suffix = 'signed' OR suffix LIKE 'signed:%'
    OR suffix = 'pronunciation' OR suffix LIKE 'pronunciation:%'
    OR suffix = 'adjective' OR suffix LIKE 'adjective:%'
    OR suffix = 'genitive' OR suffix LIKE 'genitive:%'
    OR suffix = 'wikidata' OR suffix LIKE 'wikidata:%'
    OR suffix = 'wikipedia' OR suffix LIKE 'wikipedia:%'
    OR suffix = 'source' OR suffix LIKE 'source:%'
    OR suffix = 'note' OR suffix LIKE 'note:%'
    OR suffix = 'disaster' OR suffix LIKE 'disaster:%'
    OR suffix = 'historic' OR suffix LIKE 'historic:%'
    OR suffix = 'sector' OR suffix LIKE 'sector:%'
    OR suffix = 'technical' OR suffix LIKE 'technical:%'
    OR suffix = 'direction' OR suffix LIKE 'direction:%'
    OR suffix = 'start_date' OR suffix LIKE 'start_date:%'
    OR suffix = 'end_date' OR suffix LIKE 'end_date:%'
    OR suffix = 'check_date' OR suffix LIKE 'check_date:%'
    OR suffix = 'survey:date' OR suffix LIKE 'survey:date:%'
    OR regexp_matches(suffix, 'word_stress$')
);

-- osm_variant_suffix: tag-key suffix -> STRUCT(type, language) for the
-- name:*-family prefixes (name:, alt_name:, official_name:, short_name:,
-- loc_name:, old_name:). Classifies year ranges, abbreviation/carnaval
-- markers, and localized-form suffixes (lang_script, lang+digit variant,
-- lang:scheme) before falling back to treating the whole suffix as a
-- language -- atgeo_valid_language nulls whatever of that fallback isn't
-- a valid BCP-47 primary subtag.
CREATE OR REPLACE MACRO osm_variant_suffix(default_type, suffix) AS (
    CASE
        WHEN osm_dropped_suffix(suffix) THEN NULL
        WHEN regexp_matches(suffix, '^-?\d{4}-?(\d{4})?$')
            THEN {'type': 'historical', 'language': NULL}
        WHEN suffix = 'abbr' THEN {'type': 'short', 'language': NULL}
        WHEN suffix = 'carnaval' THEN {'type': 'colloquial', 'language': NULL}
        -- underscore-joined only: a hyphenated suffix (zh-hant, en-us) is
        -- already valid BCP-47 and must fall through to ELSE untouched.
        WHEN regexp_matches(suffix, '^[a-z]{2,3}_[a-zA-Z0-9]+$')
            THEN {'type': default_type, 'language': regexp_extract(suffix, '^([a-z]{2,3})_', 1)}
        WHEN regexp_matches(suffix, '^[a-z]{2,3}\d{1,2}$')
            THEN {'type': default_type, 'language': regexp_extract(suffix, '^([a-z]{2,3})', 1)}
        WHEN regexp_matches(suffix, '^[a-z]{2,3}:')
            THEN {'type': default_type, 'language': regexp_extract(suffix, '^([a-z]{2,3}):', 1)}
        ELSE {'type': default_type, 'language': suffix}
    END
);

-- osm_variant_type_lang: tag key -> STRUCT(type, language), or NULL for any
-- key that isn't a recognized variant tag. `suffix` is everything after the
-- first colon (substr, not split_part, so a multi-segment suffix like
-- name:zh:pinyin isn't truncated before testing against the drop-list).
CREATE OR REPLACE MACRO osm_variant_type_lang(tag_key) AS (
    CASE
        WHEN tag_key = 'alt_name' THEN {'type': 'alternate', 'language': NULL}
        WHEN tag_key = 'int_name' THEN {'type': 'alternate', 'language': NULL}
        WHEN tag_key = 'official_name' THEN {'type': 'official', 'language': NULL}
        WHEN tag_key = 'short_name' THEN {'type': 'short', 'language': NULL}
        WHEN tag_key = 'loc_name' THEN {'type': 'colloquial', 'language': NULL}
        WHEN tag_key = 'old_name' THEN {'type': 'historical', 'language': NULL}
        WHEN tag_key LIKE 'name:%' THEN osm_variant_suffix('alternate', substr(tag_key, length('name:') + 1))
        WHEN tag_key LIKE 'alt_name:%' THEN osm_variant_suffix('alternate', substr(tag_key, length('alt_name:') + 1))
        WHEN tag_key LIKE 'official_name:%' THEN osm_variant_suffix('official', substr(tag_key, length('official_name:') + 1))
        WHEN tag_key LIKE 'short_name:%' THEN osm_variant_suffix('short', substr(tag_key, length('short_name:') + 1))
        WHEN tag_key LIKE 'loc_name:%' THEN osm_variant_suffix('colloquial', substr(tag_key, length('loc_name:') + 1))
        WHEN tag_key LIKE 'old_name:%' THEN osm_variant_suffix('historical', substr(tag_key, length('old_name:') + 1))
        ELSE NULL
    END
);

-- Load density and IDF parquet files as temp tables
-- When parquet path is None, create empty temp tables (LEFT JOINs produce NULL, importance defaults to 0)
${density_cte}
${idf_cte}

-- Import OSM nodes, ways, and relations, compute importance inline
CREATE TABLE places (
    osm_type         VARCHAR,
    osm_id           BIGINT,
    rkey             VARCHAR,
    name             VARCHAR,
    latitude         DOUBLE,
    longitude        DOUBLE,
    geom             GEOMETRY,
    primary_category VARCHAR,
    tags             MAP(VARCHAR, VARCHAR),
    bbox             STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
    importance       INTEGER DEFAULT 0,
    qk17             VARCHAR,              -- quadkey at zoom 17; computed inline during INSERT
    variants         STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
);

-- Spill discipline: DuckDB re-materializes a CTE at FULL width once per
-- reference beyond its own definition, which is what exhausted the temp
-- volume on the global Overture import. filtered and way_base below are each
-- scanned exactly once beyond their definition -- density and idf are joined
-- directly against pre-deduplicated lookup tables in each final SELECT,
-- rather than via a helper CTE that re-scans them and is joined back on a
-- computed key.
--
-- Two CTEs in the way pipeline below do NOT hold to that, and are known debt:
-- qualifying_ways is the raw-parquet way base carrying the full nds
-- node-ref array and the tags MAP -- the widest rows in this file -- and is
-- scanned twice beyond its definition, by way_node_refs and way_base.
-- way_node_refs is an UNNEST(nds) explosion carrying one row per node
-- reference in a way, and is likewise scanned twice, by needed_node_ids and
-- way_centroids. The relation pipeline further down carries the same
-- declared-debt shape across four more CTEs, detailed in its own comment
-- block below. Measured on 10.7M OSM rows at a 32GB memory limit: zero
-- bytes of temp disk for the node/way pipeline; the relation pipeline's
-- INSERT postdates that measurement and is not yet measured. Re-run the
-- spill probe before and after changing any of the three pipelines.
INSERT INTO places
WITH filtered AS (
    SELECT
        'n' AS osm_type,
        id AS osm_id,
        tags['name'] AS name,
        lat AS latitude,
        lon AS longitude,
        tags,
        ${osm_category_case} AS primary_category
    FROM read_parquet('${node_parquet}')
    WHERE lat IS NOT NULL AND lon IS NOT NULL
      AND (
        (tags['amenity'] IS NOT NULL
         AND tags['amenity'] NOT IN (
             'parking', 'parking_space', 'bench', 'waste_basket',
             'bicycle_parking', 'shelter', 'recycling', 'toilets',
             'post_box', 'drinking_water', 'vending_machine',
             'waste_disposal', 'hunting_stand', 'parking_entrance',
             'grit_bin', 'give_box', 'bbq')
         AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['shop'] IS NOT NULL
            AND tags['shop'] NOT IN ('yes', 'vacant')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['tourism'] IS NOT NULL AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['leisure'] IN (
                'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
                'golf_course', 'stadium', 'sports_hall', 'marina',
                'nature_reserve', 'garden', 'playground', 'dog_park',
                'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
                'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
                'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
                'escape_game', 'hackerspace', 'bird_hide')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['office'] IS NOT NULL AND tags['office'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['craft'] IS NOT NULL AND tags['craft'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['healthcare'] IS NOT NULL AND tags['healthcare'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['historic'] IN (
                'castle', 'monument', 'memorial', 'archaeological_site',
                'ruins', 'fort', 'manor', 'church', 'city_gate',
                'building', 'mine', 'wreck', 'wayside_shrine', 'tomb',
                'citywalls', 'monastery', 'battlefield', 'aircraft')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock',
                'water', 'wood', 'wetland', 'cliff', 'strait', 'peninsula')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon', 'bridge', 'wastewater_plant', 'water_works',
                'pumping_station', 'adit', 'mineshaft')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport', 'helipad')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance', 'yard')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square',
                'locality', 'isolated_dwelling', 'farm', 'islet', 'city_block')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['landuse'] IN (
                'cemetery', 'industrial', 'quarry', 'allotments',
                'military', 'winter_sports')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['waterway'] IN ('dam', 'waterfall')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['power'] IN ('plant', 'substation')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['boundary'] IN (
                'national_park', 'protected_area', 'aboriginal_lands')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['highway'] IN ('services', 'rest_area', 'trailhead')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['barrier'] IN ('toll_booth')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['emergency'] IN ('ambulance_station')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['telecom'] IN ('data_center')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
      )
)
SELECT
    f.osm_type,
    f.osm_id,
    f.osm_type || f.osm_id::VARCHAR AS rkey,
    f.name,
    f.latitude,
    f.longitude,
    ST_Point(f.longitude, f.latitude) AS geom,
    f.primary_category,
    map_from_entries(
        list_filter(
            map_entries(f.tags),
            e -> e.key != split_part(f.primary_category, '=', 1)
               AND (
                   e.key LIKE 'name:%'
                   OR e.key IN (
                       'alt_name', 'old_name', 'official_name',
                       'short_name', 'loc_name', 'int_name',
                       'cuisine', 'sport', 'religion', 'denomination',
                       'opening_hours', 'phone', 'website', 'wikidata',
                       'wheelchair', 'internet_access',
                       'addr:street', 'addr:housenumber', 'addr:city',
                       'addr:postcode', 'addr:country')
               )
        )
    ) AS tags,
    {'xmin': f.longitude - 0.0001,
     'ymin': f.latitude - 0.0001,
     'xmax': f.longitude + 0.0001,
     'ymax': f.latitude + 0.0001} AS bbox,
    round(
        60 * least(coalesce(nd.density_score, 0) / ${density_norm}, 1.0)
      + 40 * least(coalesce(ni.idf_score, 0) / ${idf_norm}, 1.0)
    )::INTEGER AS importance,
    qk17(f.longitude, f.latitude) AS qk17,
    list_sort(flatten(
        list_transform(
            list_filter(
                list_transform(map_entries(f.tags),
                    e -> {'key': e.key, 'value': e.value,
                          'tl': osm_variant_type_lang(e.key)}),
                e -> e.tl IS NOT NULL
            ),
            e -> list_transform(
                list_filter(
                    list_transform(str_split(e.value, ';'), v -> TRIM(v)),
                    v -> v != '' AND v != TRIM(f.name)
                ),
                v -> {'name': v, 'type': e.tl.type,
                      'language': CASE WHEN atgeo_valid_language(e.tl.language) THEN e.tl.language ELSE NULL END}
            )
        )
    )) AS variants
FROM filtered f
LEFT JOIN (
    -- Pre-dedupe density_tiles on the join key: without this, a duplicate
    -- tile_qk15 key fans the LEFT JOIN below out into duplicate places rows.
    SELECT tile_qk15, any_value(density_score) AS density_score
    FROM density_tiles
    GROUP BY tile_qk15
) nd ON nd.tile_qk15 = left(qk17(f.longitude, f.latitude), 15)
LEFT JOIN (
    -- Pre-dedupe idf_scores on the join key for the same reason.
    SELECT category, any_value(idf_score) AS idf_score
    FROM idf_scores
    GROUP BY category
) ni ON ni.category = f.primary_category
WHERE f.primary_category IS NOT NULL
  AND f.name IS NOT NULL AND TRIM(f.name) != ''
  AND f.longitude >= ${xmin} AND f.longitude <= ${xmax}
  AND f.latitude >= ${ymin} AND f.latitude <= ${ymax};

INSERT INTO places
WITH qualifying_ways AS (
    SELECT
        id AS osm_id,
        tags['name'] AS name,
        nds,
        tags,
        ${osm_category_case} AS primary_category
    FROM read_parquet('${way_parquet}')
    WHERE (
        (tags['amenity'] IS NOT NULL
         AND tags['amenity'] NOT IN (
             'parking', 'parking_space', 'bench', 'waste_basket',
             'bicycle_parking', 'shelter', 'recycling', 'toilets',
             'post_box', 'drinking_water', 'vending_machine',
             'waste_disposal', 'hunting_stand', 'parking_entrance',
             'grit_bin', 'give_box', 'bbq')
         AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['shop'] IS NOT NULL
            AND tags['shop'] NOT IN ('yes', 'vacant')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['tourism'] IS NOT NULL AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['leisure'] IN (
                'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
                'golf_course', 'stadium', 'sports_hall', 'marina',
                'nature_reserve', 'garden', 'playground', 'dog_park',
                'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
                'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
                'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
                'escape_game', 'hackerspace', 'bird_hide')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['office'] IS NOT NULL AND tags['office'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['craft'] IS NOT NULL AND tags['craft'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['healthcare'] IS NOT NULL AND tags['healthcare'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['historic'] IN (
                'castle', 'monument', 'memorial', 'archaeological_site',
                'ruins', 'fort', 'manor', 'church', 'city_gate',
                'building', 'mine', 'wreck', 'wayside_shrine', 'tomb',
                'citywalls', 'monastery', 'battlefield', 'aircraft')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock',
                'water', 'wood', 'wetland', 'cliff', 'strait', 'peninsula')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon', 'bridge', 'wastewater_plant', 'water_works',
                'pumping_station', 'adit', 'mineshaft')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport', 'helipad')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance', 'yard')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square',
                'locality', 'isolated_dwelling', 'farm', 'islet', 'city_block')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['landuse'] IN (
                'cemetery', 'industrial', 'quarry', 'allotments',
                'military', 'winter_sports')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['waterway'] IN ('dam', 'waterfall')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['power'] IN ('plant', 'substation')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['boundary'] IN (
                'national_park', 'protected_area', 'aboriginal_lands')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['highway'] IN ('services', 'rest_area', 'trailhead')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['barrier'] IN ('toll_booth')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['emergency'] IN ('ambulance_station')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['telecom'] IN ('data_center')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['building'] IS NOT NULL
            AND tags['building'] NOT IN (
                'house', 'detached', 'semidetached_house', 'terrace',
                'garage', 'garages', 'shed', 'hut', 'barn', 'greenhouse',
                'static_caravan', 'roof',
                'no', 'construction', 'ruins')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
    )
),
way_node_refs AS (
    SELECT
        osm_id,
        UNNEST(nds).ref AS node_ref
    FROM qualifying_ways
),
needed_node_ids AS (
    SELECT DISTINCT node_ref AS id
    FROM way_node_refs
),
node_coords AS (
    SELECT n.id, n.lat, n.lon
    FROM read_parquet('${node_parquet}') n
    SEMI JOIN needed_node_ids nn ON n.id = nn.id
    WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
),
way_centroids AS (
    SELECT
        wnr.osm_id,
        avg(nc.lat) AS latitude,
        avg(nc.lon) AS longitude
    FROM way_node_refs wnr
    JOIN node_coords nc ON wnr.node_ref = nc.id
    GROUP BY wnr.osm_id
    HAVING avg(nc.lat) IS NOT NULL AND avg(nc.lon) IS NOT NULL  -- Filter ways with no valid nodes
),
way_base AS (
    SELECT
        qw.osm_id,
        qw.name,
        qw.tags,
        qw.primary_category,
        wc.latitude,
        wc.longitude
    FROM qualifying_ways qw
    JOIN way_centroids wc ON qw.osm_id = wc.osm_id
    WHERE qw.primary_category IS NOT NULL
      AND qw.name IS NOT NULL AND TRIM(qw.name) != ''
      AND wc.longitude >= ${xmin} AND wc.longitude <= ${xmax}
      AND wc.latitude >= ${ymin} AND wc.latitude <= ${ymax}
)
SELECT
    'w' AS osm_type,
    wb.osm_id,
    'w' || wb.osm_id::VARCHAR AS rkey,
    wb.name,
    wb.latitude,
    wb.longitude,
    ST_Point(wb.longitude, wb.latitude) AS geom,
    wb.primary_category,
    map_from_entries(
        list_filter(
            map_entries(wb.tags),
            e -> e.key != split_part(wb.primary_category, '=', 1)
               AND (
                   e.key LIKE 'name:%'
                   OR e.key IN (
                       'alt_name', 'old_name', 'official_name',
                       'short_name', 'loc_name', 'int_name',
                       'cuisine', 'sport', 'religion', 'denomination',
                       'opening_hours', 'phone', 'website', 'wikidata',
                       'wheelchair', 'internet_access',
                       'addr:street', 'addr:housenumber', 'addr:city',
                       'addr:postcode', 'addr:country')
               )
        )
    ) AS tags,
    {'xmin': wb.longitude - 0.0001,
     'ymin': wb.latitude - 0.0001,
     'xmax': wb.longitude + 0.0001,
     'ymax': wb.latitude + 0.0001} AS bbox,
    round(
        60 * least(coalesce(wd.density_score, 0) / ${density_norm}, 1.0)
      + 40 * least(coalesce(wi.idf_score, 0) / ${idf_norm}, 1.0)
    )::INTEGER AS importance,
    qk17(wb.longitude, wb.latitude) AS qk17,
    list_sort(flatten(
        list_transform(
            list_filter(
                list_transform(map_entries(wb.tags),
                    e -> {'key': e.key, 'value': e.value,
                          'tl': osm_variant_type_lang(e.key)}),
                e -> e.tl IS NOT NULL
            ),
            e -> list_transform(
                list_filter(
                    list_transform(str_split(e.value, ';'), v -> TRIM(v)),
                    v -> v != '' AND v != TRIM(wb.name)
                ),
                v -> {'name': v, 'type': e.tl.type,
                      'language': CASE WHEN atgeo_valid_language(e.tl.language) THEN e.tl.language ELSE NULL END}
            )
        )
    )) AS variants
FROM way_base wb
LEFT JOIN (
    -- Pre-dedupe density_tiles on the join key for the same reason as the
    -- node import above.
    SELECT tile_qk15, any_value(density_score) AS density_score
    FROM density_tiles
    GROUP BY tile_qk15
) wd ON wd.tile_qk15 = left(qk17(wb.longitude, wb.latitude), 15)
LEFT JOIN (
    -- Pre-dedupe idf_scores on the join key for the same reason.
    SELECT category, any_value(idf_score) AS idf_score
    FROM idf_scores
    GROUP BY category
) wi ON wi.category = wb.primary_category;

-- Relation pipeline spill discipline, stated against the same invariant:
-- qualifying_relations (tags MAP + members list, the widest rows here) is
-- scanned twice beyond its definition -- by rel_members and by rel_base --
-- the same declared-debt shape as qualifying_ways. rel_members is narrow
-- (osm_id, member type, ref) and defined with SELECT DISTINCT on exactly
-- those columns so the id lists and anti-joins below see no unnest
-- fanout, but is itself scanned four times beyond its definition -- by
-- rel_way_ids, rel_way_member_node_refs, rel_node_member_refs, and the
-- first suppression anti-join -- and rel_way_member_node_refs/
-- rel_node_member_refs are each scanned twice, by rel_needed_node_ids and
-- rel_all_member_node_refs. rel_ways/rel_way_node_refs mirror
-- way_node_refs/node_coords, including the SEMI JOIN that restricts the
-- way-parquet scan to referenced way ids and the node-parquet scan to
-- referenced node ids.
-- places is scanned once per anti-join below, each through a narrow
-- projection -- two more declared scans, alongside the two above.
-- Re-run the spill probe before and after changing this pipeline too.
INSERT INTO places
WITH qualifying_relations AS (
    SELECT
        id AS osm_id,
        tags['name'] AS name,
        members,
        tags,
        ${osm_category_case} AS primary_category
    FROM read_parquet('${relation_parquet}')
    WHERE (
        (tags['amenity'] IS NOT NULL
         AND tags['amenity'] NOT IN (
             'parking', 'parking_space', 'bench', 'waste_basket',
             'bicycle_parking', 'shelter', 'recycling', 'toilets',
             'post_box', 'drinking_water', 'vending_machine',
             'waste_disposal', 'hunting_stand', 'parking_entrance',
             'grit_bin', 'give_box', 'bbq')
         AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['shop'] IS NOT NULL
            AND tags['shop'] NOT IN ('yes', 'vacant')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['tourism'] IS NOT NULL AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['leisure'] IN (
                'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
                'golf_course', 'stadium', 'sports_hall', 'marina',
                'nature_reserve', 'garden', 'playground', 'dog_park',
                'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
                'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
                'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
                'escape_game', 'hackerspace', 'bird_hide')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['office'] IS NOT NULL AND tags['office'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['craft'] IS NOT NULL AND tags['craft'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['healthcare'] IS NOT NULL AND tags['healthcare'] != 'yes'
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['historic'] IN (
                'castle', 'monument', 'memorial', 'archaeological_site',
                'ruins', 'fort', 'manor', 'church', 'city_gate',
                'building', 'mine', 'wreck', 'wayside_shrine', 'tomb',
                'citywalls', 'monastery', 'battlefield', 'aircraft')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock',
                'water', 'wood', 'wetland', 'cliff', 'strait', 'peninsula')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon', 'bridge', 'wastewater_plant', 'water_works',
                'pumping_station', 'adit', 'mineshaft')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport', 'helipad')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance', 'yard')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square',
                'locality', 'isolated_dwelling', 'farm', 'islet', 'city_block')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['landuse'] IN (
                'cemetery', 'industrial', 'quarry', 'allotments',
                'military', 'winter_sports')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['waterway'] IN ('dam', 'waterfall')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['power'] IN ('plant', 'substation')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['boundary'] IN (
                'national_park', 'protected_area', 'aboriginal_lands')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['highway'] IN ('services', 'rest_area', 'trailhead')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['barrier'] IN ('toll_booth')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['emergency'] IN ('ambulance_station')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['telecom'] IN ('data_center')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['building'] IS NOT NULL
            AND tags['building'] NOT IN (
                'house', 'detached', 'semidetached_house', 'terrace',
                'garage', 'garages', 'shed', 'hut', 'barn', 'greenhouse',
                'static_caravan', 'roof',
                'no', 'construction', 'ruins')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
    )
),
-- Narrow: (osm_id, member type, ref) only -- role plays no part in
-- resolution or suppression below. DISTINCT on all three columns so a
-- member repeated in one relation's list (e.g. the same way as both
-- outer and inner ring) doesn't double-weight the centroid or fan out
-- the anti-joins.
rel_members AS (
    SELECT DISTINCT
        qr.osm_id,
        u.m.type AS member_type,
        u.m.ref AS member_ref
    FROM qualifying_relations qr, UNNEST(qr.members) AS u(m)
),
rel_way_ids AS (
    SELECT DISTINCT member_ref AS way_id
    FROM rel_members
    WHERE member_type = 'way'
),
-- Mirrors way_node_refs/node_coords: SEMI JOIN restricts the way-parquet
-- scan to only the way ids referenced by a qualifying relation.
rel_ways AS (
    SELECT w.id AS way_id, w.nds
    FROM read_parquet('${way_parquet}') w
    SEMI JOIN rel_way_ids rwi ON w.id = rwi.way_id
),
rel_way_node_refs AS (
    SELECT way_id, UNNEST(nds).ref AS node_ref
    FROM rel_ways
),
rel_way_member_node_refs AS (
    SELECT rm.osm_id, wnr.node_ref
    FROM rel_members rm
    JOIN rel_way_node_refs wnr
        ON rm.member_type = 'way' AND rm.member_ref = wnr.way_id
),
rel_node_member_refs AS (
    SELECT osm_id, member_ref AS node_ref
    FROM rel_members
    WHERE member_type = 'node'
),
rel_needed_node_ids AS (
    SELECT node_ref AS id FROM rel_node_member_refs
    UNION
    SELECT node_ref AS id FROM rel_way_member_node_refs
),
-- Single SEMI JOIN over the node parquet, covering both direct node
-- members and way-member nodes together -- one scan, not two.
rel_node_coords AS (
    SELECT n.id, n.lat, n.lon
    FROM read_parquet('${node_parquet}') n
    SEMI JOIN rel_needed_node_ids rn ON n.id = rn.id
    WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
),
rel_all_member_node_refs AS (
    SELECT osm_id, node_ref FROM rel_node_member_refs
    UNION ALL
    SELECT osm_id, node_ref FROM rel_way_member_node_refs
),
-- Centroid and member extent (needed for suppression) from one shared
-- aggregation, the same flat-average estimator and density bias
-- way_centroids already accepts.
rel_centroids AS (
    SELECT
        a.osm_id,
        avg(nc.lat) AS latitude,
        avg(nc.lon) AS longitude,
        min(nc.lat) AS ext_ymin,
        max(nc.lat) AS ext_ymax,
        min(nc.lon) AS ext_xmin,
        max(nc.lon) AS ext_xmax
    FROM rel_all_member_node_refs a
    JOIN rel_node_coords nc ON a.node_ref = nc.id
    GROUP BY a.osm_id
    HAVING avg(nc.lat) IS NOT NULL AND avg(nc.lon) IS NOT NULL  -- Filter relations with no resolvable member coords
),
rel_base AS (
    SELECT
        qr.osm_id,
        qr.name,
        qr.tags,
        qr.primary_category,
        rc.latitude,
        rc.longitude,
        rc.ext_xmin,
        rc.ext_ymin,
        rc.ext_xmax,
        rc.ext_ymax
    FROM qualifying_relations qr
    JOIN rel_centroids rc ON qr.osm_id = rc.osm_id
    WHERE qr.primary_category IS NOT NULL
      AND qr.name IS NOT NULL AND TRIM(qr.name) != ''
      AND rc.longitude >= ${xmin} AND rc.longitude <= ${xmax}
      AND rc.latitude >= ${ymin} AND rc.latitude <= ${ymax}
)
SELECT
    'r' AS osm_type,
    rb.osm_id,
    'r' || rb.osm_id::VARCHAR AS rkey,
    rb.name,
    rb.latitude,
    rb.longitude,
    ST_Point(rb.longitude, rb.latitude) AS geom,
    rb.primary_category,
    map_from_entries(
        list_filter(
            map_entries(rb.tags),
            e -> e.key != split_part(rb.primary_category, '=', 1)
               AND (
                   e.key LIKE 'name:%'
                   OR e.key IN (
                       'alt_name', 'old_name', 'official_name',
                       'short_name', 'loc_name', 'int_name',
                       'cuisine', 'sport', 'religion', 'denomination',
                       'opening_hours', 'phone', 'website', 'wikidata',
                       'wheelchair', 'internet_access',
                       'addr:street', 'addr:housenumber', 'addr:city',
                       'addr:postcode', 'addr:country')
               )
        )
    ) AS tags,
    {'xmin': rb.longitude - 0.0001,
     'ymin': rb.latitude - 0.0001,
     'xmax': rb.longitude + 0.0001,
     'ymax': rb.latitude + 0.0001} AS bbox,
    round(
        60 * least(coalesce(rd.density_score, 0) / ${density_norm}, 1.0)
      + 40 * least(coalesce(ri.idf_score, 0) / ${idf_norm}, 1.0)
    )::INTEGER AS importance,
    qk17(rb.longitude, rb.latitude) AS qk17,
    list_sort(flatten(
        list_transform(
            list_filter(
                list_transform(map_entries(rb.tags),
                    e -> {'key': e.key, 'value': e.value,
                          'tl': osm_variant_type_lang(e.key)}),
                e -> e.tl IS NOT NULL
            ),
            e -> list_transform(
                list_filter(
                    list_transform(str_split(e.value, ';'), v -> TRIM(v)),
                    v -> v != '' AND v != TRIM(rb.name)
                ),
                v -> {'name': v, 'type': e.tl.type,
                      'language': CASE WHEN atgeo_valid_language(e.tl.language) THEN e.tl.language ELSE NULL END}
            )
        )
    )) AS variants
FROM rel_base rb
LEFT JOIN (
    -- Pre-dedupe density_tiles on the join key for the same reason as the
    -- node and way imports above.
    SELECT tile_qk15, any_value(density_score) AS density_score
    FROM density_tiles
    GROUP BY tile_qk15
) rd ON rd.tile_qk15 = left(qk17(rb.longitude, rb.latitude), 15)
LEFT JOIN (
    -- Pre-dedupe idf_scores on the join key for the same reason.
    SELECT category, any_value(idf_score) AS idf_score
    FROM idf_scores
    GROUP BY category
) ri ON ri.category = rb.primary_category
-- Suppression 1/2 (member-linked): drop the relation if a node
-- member matches an existing osm_type='n' places row, or a way member an
-- existing osm_type='w' row, WITH equal trimmed name. Membership alone
-- is not identity -- a relation routinely links to a member that is a
-- legitimately distinct place -- so the name-equality clause is
-- load-bearing. NOT EXISTS, not NOT IN: NOT IN against a subquery that
-- can yield a NULL evaluates to NULL (not TRUE) for every row and
-- silently suppresses everything, whereas NOT EXISTS is a strict
-- boolean regardless of NULLs in the subquery.
WHERE NOT EXISTS (
    SELECT 1
    FROM rel_members rm
    JOIN (
        SELECT osm_type, osm_id, TRIM(name) AS name_trim FROM places
    ) p
      ON (rm.member_type = 'node' AND p.osm_type = 'n' AND p.osm_id = rm.member_ref)
      OR (rm.member_type = 'way' AND p.osm_type = 'w' AND p.osm_id = rm.member_ref)
    WHERE rm.osm_id = rb.osm_id
      AND p.name_trim = TRIM(rb.name)
)
-- Suppression 2/2 (name+location): drop the relation if any
-- existing places row (any branch) has an equal trimmed name and its
-- point lies within the relation's member extent -- membership is not
-- required here.
AND NOT EXISTS (
    SELECT 1
    FROM (
        SELECT TRIM(name) AS name_trim, longitude, latitude FROM places
    ) p
    WHERE p.name_trim = TRIM(rb.name)
      AND p.longitude BETWEEN rb.ext_xmin AND rb.ext_xmax
      AND p.latitude BETWEEN rb.ext_ymin AND rb.ext_ymax
);

DELETE FROM places WHERE geom IS NULL;

CREATE INDEX idx_rkey ON places(rkey);

-- Drop temp tables
DROP TABLE density_tiles;
DROP TABLE idf_scores;
