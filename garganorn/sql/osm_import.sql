-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

-- Load density and IDF parquet files as temp tables
-- When parquet path is None, create empty temp tables (LEFT JOINs produce NULL, importance defaults to 0)
${density_cte}
${idf_cte}

-- Import OSM nodes and ways, compute importance inline (Phase 2: density+IDF+importance unified in import CTAS)
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
        OR (tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
            AND tags['name'] IS NOT NULL)
        OR (tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
            AND tags['name'] IS NOT NULL)
        OR (tags['public_transport'] = 'station' AND tags['name'] IS NOT NULL)
        OR (tags['place'] IN (
                'city', 'town', 'village', 'hamlet', 'suburb',
                'neighbourhood', 'quarter', 'island', 'square')
            AND tags['name'] IS NOT NULL)
      )
),
node_density AS (
    SELECT
        osm_type || osm_id::VARCHAR AS rkey,
        coalesce(d.density_score, 0) AS density_score
    FROM filtered f
    LEFT JOIN density_tiles d ON d.tile_qk15 = left(ST_QuadKey(f.longitude, f.latitude, 17), 15)
    WHERE f.longitude IS NOT NULL AND f.latitude IS NOT NULL
),
node_idf AS (
    SELECT
        osm_type || osm_id::VARCHAR AS rkey,
        coalesce(i.idf_score, 0) AS idf_score
    FROM filtered f
    LEFT JOIN idf_scores i ON i.category = f.primary_category
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
    ST_QuadKey(f.longitude, f.latitude, 17) AS qk17,
    []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] AS variants
FROM filtered f
LEFT JOIN node_density nd ON nd.rkey = f.osm_type || f.osm_id::VARCHAR
LEFT JOIN node_idf ni ON ni.rkey = f.osm_type || f.osm_id::VARCHAR
WHERE f.primary_category IS NOT NULL
  AND f.name IS NOT NULL
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
        OR (tags['aeroway'] IN ('aerodrome', 'terminal', 'heliport')
            AND tags['name'] IS NOT NULL)
        OR (tags['railway'] IN ('station', 'halt', 'tram_stop', 'subway_entrance')
            AND tags['name'] IS NOT NULL)
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
      AND qw.name IS NOT NULL
      AND wc.longitude >= ${xmin} AND wc.longitude <= ${xmax}
      AND wc.latitude >= ${ymin} AND wc.latitude <= ${ymax}
),
way_density AS (
    SELECT
        'w' || osm_id::VARCHAR AS rkey,
        coalesce(d.density_score, 0) AS density_score
    FROM way_base wb
    LEFT JOIN density_tiles d ON d.tile_qk15 = left(ST_QuadKey(wb.longitude, wb.latitude, 17), 15)
),
way_idf AS (
    SELECT
        'w' || osm_id::VARCHAR AS rkey,
        coalesce(i.idf_score, 0) AS idf_score
    FROM way_base wb
    LEFT JOIN idf_scores i ON i.category = wb.primary_category
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
    ST_QuadKey(wb.longitude, wb.latitude, 17) AS qk17,
    []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] AS variants
FROM way_base wb
LEFT JOIN way_density wd ON wd.rkey = 'w' || wb.osm_id::VARCHAR
LEFT JOIN way_idf wi ON wi.rkey = 'w' || wb.osm_id::VARCHAR;

DELETE FROM places WHERE geom IS NULL;

CREATE INDEX idx_rkey ON places(rkey);

-- Drop temp tables
DROP TABLE density_tiles;
DROP TABLE idf_scores;
