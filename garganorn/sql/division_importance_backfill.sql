-- Backfill division importance from density + population.
-- Localities: 60% density + 40% population. Non-localities: population only.

DROP TABLE IF EXISTS density_tiles;
DROP TABLE IF EXISTS division_density;

CREATE TEMP TABLE density_tiles AS
SELECT * FROM read_parquet('${density_parquet}');

-- Compute average density for localities: find all z15 density tile centroids
-- that fall within the locality's bounding box.
-- Only localities participate in the spatial join (performance optimization).
CREATE TEMP TABLE division_density AS
SELECT p.id,
       coalesce(avg(d.density_score), 0) AS avg_density
FROM places p
LEFT JOIN density_tiles d
    ON d.centroid_lon BETWEEN p.min_longitude AND p.max_longitude
   AND d.centroid_lat BETWEEN p.min_latitude AND p.max_latitude
WHERE p.subtype = 'locality'
GROUP BY p.id;

DROP TABLE IF EXISTS places_scored;
CREATE TABLE places_scored AS
SELECT p.* EXCLUDE (importance),
    CASE
        WHEN p.subtype = 'locality' THEN
            round(60 * least(coalesce(dd.avg_density, 0) / ${density_norm}, 1.0)
                + 40 * least(ln(1 + coalesce(p.population, 0)) / ${pop_norm}, 1.0))::INTEGER
        ELSE
            round(40 * least(ln(1 + coalesce(p.population, 0)) / ${pop_norm}, 1.0))::INTEGER
    END AS importance
FROM places p
LEFT JOIN division_density dd USING (id);

DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
DROP TABLE division_density;
DROP TABLE density_tiles;
