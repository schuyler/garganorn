-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

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
    OR regexp_matches(suffix, 'word_stress$')
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
        WHEN tag_key LIKE 'name:%' THEN
            CASE
                WHEN osm_dropped_suffix(substr(tag_key, length('name:') + 1)) THEN NULL
                WHEN substr(tag_key, length('name:') + 1) = 'abbr'
                    THEN {'type': 'short', 'language': NULL}
                WHEN regexp_matches(substr(tag_key, length('name:') + 1), '^-\d{4}(-\d{4})?$')
                    THEN {'type': 'historical', 'language': NULL}
                WHEN substr(tag_key, length('name:') + 1) = 'carnaval'
                    THEN {'type': 'colloquial', 'language': NULL}
                ELSE {'type': 'alternate', 'language': substr(tag_key, length('name:') + 1)}
            END
        WHEN tag_key LIKE 'alt_name:%' THEN
            CASE WHEN osm_dropped_suffix(substr(tag_key, length('alt_name:') + 1)) THEN NULL
                 ELSE {'type': 'alternate', 'language': substr(tag_key, length('alt_name:') + 1)}
            END
        WHEN tag_key LIKE 'official_name:%' THEN
            CASE WHEN osm_dropped_suffix(substr(tag_key, length('official_name:') + 1)) THEN NULL
                 ELSE {'type': 'official', 'language': substr(tag_key, length('official_name:') + 1)}
            END
        WHEN tag_key LIKE 'short_name:%' THEN
            CASE WHEN osm_dropped_suffix(substr(tag_key, length('short_name:') + 1)) THEN NULL
                 ELSE {'type': 'short', 'language': substr(tag_key, length('short_name:') + 1)}
            END
        WHEN tag_key LIKE 'loc_name:%' THEN
            CASE WHEN osm_dropped_suffix(substr(tag_key, length('loc_name:') + 1)) THEN NULL
                 ELSE {'type': 'colloquial', 'language': substr(tag_key, length('loc_name:') + 1)}
            END
        WHEN tag_key LIKE 'old_name:%' THEN
            CASE WHEN osm_dropped_suffix(substr(tag_key, length('old_name:') + 1)) THEN NULL
                 ELSE {'type': 'historical', 'language': substr(tag_key, length('old_name:') + 1)}
            END
        ELSE NULL
    END
);

-- Load density and IDF parquet files as temp tables
-- When parquet path is None, create empty temp tables (LEFT JOINs produce NULL, importance defaults to 0)
${density_cte}
${idf_cte}

-- Import OSM nodes and ways, compute importance inline
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
-- qualifying_ways (:159) is the raw-parquet way base carrying the full nds
-- node-ref array and the tags MAP -- the widest rows in this file -- and is
-- scanned twice beyond its definition, by way_node_refs and way_base.
-- way_node_refs (:221) is an UNNEST(nds) explosion carrying one row per node
-- reference in a way, and is likewise scanned twice, by needed_node_ids and
-- way_centroids. Measured on 10.7M OSM rows at a 32GB memory limit: zero
-- bytes of temp disk, so neither spills at present scale. Nothing structural
-- holds that. Re-run the spill probe before and after changing either.
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
                'escape_game', 'hackerspace')
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
                'building', 'mine', 'wreck')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square')
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
                v -> {'name': v, 'type': e.tl.type, 'language': e.tl.language}
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
                'escape_game', 'hackerspace')
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
                'building', 'mine', 'wreck')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
            AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL AND TRIM(tags['name']) != '')
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square')
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
                v -> {'name': v, 'type': e.tl.type, 'language': e.tl.language}
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

DELETE FROM places WHERE geom IS NULL;

CREATE INDEX idx_rkey ON places(rkey);

-- Drop temp tables
DROP TABLE density_tiles;
DROP TABLE idf_scores;
