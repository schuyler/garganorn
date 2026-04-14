-- Compute IDF scores per OSM primary category from raw node and way parquets.
-- Uses UNION ALL to combine node and way data, applies the same CASE expression
-- from osm_import.sql to extract primary_category from tags.
-- Filters to named elements only (tags['name'] IS NOT NULL).
-- No category whitelists/blacklists (global IDF).
DROP TABLE IF EXISTS idf_scores;
CREATE TABLE idf_scores AS
SELECT
    primary_category AS category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM (
    SELECT
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
    WHERE tags['name'] IS NOT NULL

    UNION ALL

    SELECT
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
    WHERE tags['name'] IS NOT NULL
) combined
CROSS JOIN (
    SELECT count(*) AS total
    FROM (
        SELECT 1
        FROM read_parquet('${node_parquet}')
        WHERE tags['name'] IS NOT NULL
          AND (
            tags['amenity'] IS NOT NULL
            OR tags['shop'] IS NOT NULL
            OR tags['tourism'] IS NOT NULL
            OR tags['leisure'] IS NOT NULL
            OR tags['office'] IS NOT NULL
            OR tags['craft'] IS NOT NULL
            OR tags['healthcare'] IS NOT NULL
            OR tags['historic'] IS NOT NULL
            OR tags['natural'] IS NOT NULL
            OR tags['man_made'] IS NOT NULL
            OR tags['aeroway'] IS NOT NULL
            OR tags['railway'] IS NOT NULL
            OR tags['public_transport'] IS NOT NULL
            OR tags['place'] IS NOT NULL
          )

        UNION ALL

        SELECT 1
        FROM read_parquet('${way_parquet}')
        WHERE tags['name'] IS NOT NULL
          AND (
            tags['amenity'] IS NOT NULL
            OR tags['shop'] IS NOT NULL
            OR tags['tourism'] IS NOT NULL
            OR tags['leisure'] IS NOT NULL
            OR tags['office'] IS NOT NULL
            OR tags['craft'] IS NOT NULL
            OR tags['healthcare'] IS NOT NULL
            OR tags['historic'] IS NOT NULL
            OR tags['natural'] IS NOT NULL
            OR tags['man_made'] IS NOT NULL
            OR tags['aeroway'] IS NOT NULL
            OR tags['railway'] IS NOT NULL
            OR tags['public_transport'] IS NOT NULL
            OR tags['place'] IS NOT NULL
          )
    ) named_categorized
) N
WHERE primary_category IS NOT NULL
GROUP BY primary_category, N.total;
