SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

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
    importance       INTEGER DEFAULT 0
);

INSERT INTO places
WITH filtered AS (
    SELECT
        'n' AS osm_type,
        id AS osm_id,
        tags['name'] AS name,
        lat AS latitude,
        lon AS longitude,
        tags,
        CASE
            WHEN tags['amenity'] IS NOT NULL THEN 'amenity=' || tags['amenity']
            WHEN tags['shop'] IS NOT NULL THEN 'shop=' || tags['shop']
            WHEN tags['tourism'] IS NOT NULL THEN 'tourism=' || tags['tourism']
            WHEN tags['leisure'] IS NOT NULL THEN 'leisure=' || tags['leisure']
            WHEN tags['office'] IS NOT NULL THEN 'office=' || tags['office']
            WHEN tags['craft'] IS NOT NULL THEN 'craft=' || tags['craft']
            WHEN tags['healthcare'] IS NOT NULL THEN 'healthcare=' || tags['healthcare']
            WHEN tags['historic'] IS NOT NULL THEN 'historic=' || tags['historic']
            WHEN tags['natural'] IS NOT NULL THEN 'natural=' || tags['natural']
            WHEN tags['man_made'] IS NOT NULL THEN 'man_made=' || tags['man_made']
            WHEN tags['aeroway'] IS NOT NULL THEN 'aeroway=' || tags['aeroway']
            WHEN tags['railway'] IS NOT NULL THEN 'railway=' || tags['railway']
            WHEN tags['public_transport'] IS NOT NULL
                THEN 'public_transport=' || tags['public_transport']
            WHEN tags['place'] IS NOT NULL THEN 'place=' || tags['place']
        END AS primary_category
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
         AND tags['name'] IS NOT NULL)
        OR (tags['shop'] IS NOT NULL
            AND tags['shop'] NOT IN ('yes', 'vacant')
            AND tags['name'] IS NOT NULL)
        OR (tags['tourism'] IS NOT NULL AND tags['name'] IS NOT NULL)
        OR (tags['leisure'] IN (
                'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
                'golf_course', 'stadium', 'sports_hall', 'marina',
                'nature_reserve', 'garden', 'playground', 'dog_park',
                'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
                'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
                'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
                'escape_game', 'hackerspace')
            AND tags['name'] IS NOT NULL)
        OR (tags['office'] IS NOT NULL AND tags['office'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['craft'] IS NOT NULL AND tags['craft'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['healthcare'] IS NOT NULL AND tags['healthcare'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['historic'] IN (
                'castle', 'monument', 'memorial', 'archaeological_site',
                'ruins', 'fort', 'manor', 'church', 'city_gate',
                'building', 'mine', 'wreck')
            AND tags['name'] IS NOT NULL)
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock')
            AND tags['name'] IS NOT NULL)
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon')
            AND tags['name'] IS NOT NULL)
        OR tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
        OR tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL)
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square')
            AND tags['name'] IS NOT NULL)
      )
)
SELECT
    osm_type,
    osm_id,
    osm_type || osm_id::VARCHAR AS rkey,
    name,
    latitude,
    longitude,
    ST_Point(longitude, latitude) AS geom,
    primary_category,
    map_from_entries(
        list_filter(
            map_entries(tags),
            e -> e.key != split_part(primary_category, '=', 1)
               AND e.key IN (
                   'cuisine', 'sport', 'religion', 'denomination',
                   'opening_hours', 'phone', 'website', 'wikidata',
                   'wheelchair', 'internet_access',
                   'addr:street', 'addr:housenumber', 'addr:city',
                   'addr:postcode', 'addr:country')
        )
    ) AS tags,
    {'xmin': longitude - 0.0001,
     'ymin': latitude - 0.0001,
     'xmax': longitude + 0.0001,
     'ymax': latitude + 0.0001} AS bbox,
    0 AS importance
FROM filtered
WHERE primary_category IS NOT NULL
  AND name IS NOT NULL
  AND longitude BETWEEN ${xmin} AND ${xmax}
  AND latitude BETWEEN ${ymin} AND ${ymax};

INSERT INTO places
WITH qualifying_ways AS (
    SELECT
        id AS osm_id,
        tags['name'] AS name,
        nds,
        tags,
        CASE
            WHEN tags['amenity'] IS NOT NULL THEN 'amenity=' || tags['amenity']
            WHEN tags['shop'] IS NOT NULL THEN 'shop=' || tags['shop']
            WHEN tags['tourism'] IS NOT NULL THEN 'tourism=' || tags['tourism']
            WHEN tags['leisure'] IS NOT NULL THEN 'leisure=' || tags['leisure']
            WHEN tags['office'] IS NOT NULL THEN 'office=' || tags['office']
            WHEN tags['craft'] IS NOT NULL THEN 'craft=' || tags['craft']
            WHEN tags['healthcare'] IS NOT NULL THEN 'healthcare=' || tags['healthcare']
            WHEN tags['historic'] IS NOT NULL THEN 'historic=' || tags['historic']
            WHEN tags['natural'] IS NOT NULL THEN 'natural=' || tags['natural']
            WHEN tags['man_made'] IS NOT NULL THEN 'man_made=' || tags['man_made']
            WHEN tags['aeroway'] IS NOT NULL THEN 'aeroway=' || tags['aeroway']
            WHEN tags['railway'] IS NOT NULL THEN 'railway=' || tags['railway']
            WHEN tags['public_transport'] IS NOT NULL
                THEN 'public_transport=' || tags['public_transport']
            WHEN tags['place'] IS NOT NULL THEN 'place=' || tags['place']
        END AS primary_category
    FROM read_parquet('${way_parquet}')
    WHERE (
        (tags['amenity'] IS NOT NULL
         AND tags['amenity'] NOT IN (
             'parking', 'parking_space', 'bench', 'waste_basket',
             'bicycle_parking', 'shelter', 'recycling', 'toilets',
             'post_box', 'drinking_water', 'vending_machine',
             'waste_disposal', 'hunting_stand', 'parking_entrance',
             'grit_bin', 'give_box', 'bbq')
         AND tags['name'] IS NOT NULL)
        OR (tags['shop'] IS NOT NULL
            AND tags['shop'] NOT IN ('yes', 'vacant')
            AND tags['name'] IS NOT NULL)
        OR (tags['tourism'] IS NOT NULL AND tags['name'] IS NOT NULL)
        OR (tags['leisure'] IN (
                'park', 'sports_centre', 'fitness_centre', 'swimming_pool',
                'golf_course', 'stadium', 'sports_hall', 'marina',
                'nature_reserve', 'garden', 'playground', 'dog_park',
                'ice_rink', 'water_park', 'miniature_golf', 'bowling_alley',
                'beach_resort', 'resort', 'horse_riding', 'dance', 'sauna',
                'amusement_arcade', 'adult_gaming_centre', 'trampoline_park',
                'escape_game', 'hackerspace')
            AND tags['name'] IS NOT NULL)
        OR (tags['office'] IS NOT NULL AND tags['office'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['craft'] IS NOT NULL AND tags['craft'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['healthcare'] IS NOT NULL AND tags['healthcare'] != 'yes'
            AND tags['name'] IS NOT NULL)
        OR (tags['historic'] IN (
                'castle', 'monument', 'memorial', 'archaeological_site',
                'ruins', 'fort', 'manor', 'church', 'city_gate',
                'building', 'mine', 'wreck')
            AND tags['name'] IS NOT NULL)
        OR (tags['natural'] IN (
                'peak', 'beach', 'spring', 'bay', 'cave_entrance',
                'volcano', 'glacier', 'hot_spring', 'cape', 'hill',
                'valley', 'saddle', 'ridge', 'geyser', 'arch', 'gorge', 'rock')
            AND tags['name'] IS NOT NULL)
        OR (tags['man_made'] IN (
                'lighthouse', 'tower', 'pier', 'observatory', 'windmill',
                'water_tower', 'works', 'chimney', 'obelisk', 'watermill',
                'beacon')
            AND tags['name'] IS NOT NULL)
        OR tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
        OR tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL)
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square')
            AND tags['name'] IS NOT NULL)
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
)
SELECT
    'w' AS osm_type,
    qw.osm_id,
    'w' || qw.osm_id::VARCHAR AS rkey,
    qw.name,
    wc.latitude,
    wc.longitude,
    ST_Point(wc.longitude, wc.latitude) AS geom,
    qw.primary_category,
    map_from_entries(
        list_filter(
            map_entries(qw.tags),
            e -> e.key != split_part(qw.primary_category, '=', 1)
               AND e.key IN (
                   'cuisine', 'sport', 'religion', 'denomination',
                   'opening_hours', 'phone', 'website', 'wikidata',
                   'wheelchair', 'internet_access',
                   'addr:street', 'addr:housenumber', 'addr:city',
                   'addr:postcode', 'addr:country')
        )
    ) AS tags,
    {'xmin': wc.longitude - 0.0001,
     'ymin': wc.latitude - 0.0001,
     'xmax': wc.longitude + 0.0001,
     'ymax': wc.latitude + 0.0001} AS bbox,
    0 AS importance
-- NOTE: INNER JOIN silently drops ways whose member nodes are absent from
-- ${node_parquet} (e.g. cross-shard references or null-coordinate nodes).
-- This is expected behavior for bounded-region imports.
FROM qualifying_ways qw
JOIN way_centroids wc ON qw.osm_id = wc.osm_id
WHERE qw.primary_category IS NOT NULL
  AND qw.name IS NOT NULL
  AND wc.longitude BETWEEN ${xmin} AND ${xmax}
  AND wc.latitude BETWEEN ${ymin} AND ${ymax};

DELETE FROM places WHERE geom IS NULL;

-- Compute quadkey at max zoom (used for density, tile assignment, and export)
ALTER TABLE places ADD COLUMN qk17 VARCHAR;
UPDATE places SET qk17 = ST_QuadKey(longitude, latitude, 17);

CREATE INDEX idx_rkey ON places(rkey);
