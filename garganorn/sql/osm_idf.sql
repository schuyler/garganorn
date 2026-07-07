-- Compute IDF scores per OSM primary category from raw node and way parquets.
-- Uses _osm_category_case.sql snippet substitution to share CASE expression with osm_import.sql.
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
        ${osm_category_case} AS primary_category
    FROM read_parquet('${node_parquet}')
    WHERE tags['name'] IS NOT NULL

    UNION ALL

    SELECT
        ${osm_category_case} AS primary_category
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
