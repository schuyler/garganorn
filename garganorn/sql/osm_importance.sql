CREATE TEMP TABLE t_idf AS
SELECT primary_category AS category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM places
CROSS JOIN (SELECT count(*) AS total FROM places WHERE primary_category IS NOT NULL) N
WHERE primary_category IS NOT NULL
GROUP BY primary_category, N.total;

CREATE TEMP TABLE place_density AS
SELECT rkey,
       ln(1 + count(*) OVER (
           PARTITION BY left(qk17, 15)
       )) AS density_score
FROM places;

CREATE TEMP TABLE place_idf AS
SELECT
    p.rkey,
    coalesce(idf.idf_score, 0) AS idf_score
FROM places p
LEFT JOIN t_idf idf ON idf.category = p.primary_category;

CREATE TABLE places_scored AS
SELECT p.* EXCLUDE (importance),
       round(
           60 * least(coalesce(d.density_score, 0) / ${density_norm}, 1.0)
         + 40 * least(coalesce(i.idf_score, 0) / ${idf_norm}, 1.0)
       )::INTEGER AS importance
FROM places p
LEFT JOIN place_density d USING (rkey)
LEFT JOIN place_idf i USING (rkey);

DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
-- idx_rkey is preserved through this RENAME.
-- osm_variants.sql does its own CTAS which destroys it and recreates it there.
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_idf;
