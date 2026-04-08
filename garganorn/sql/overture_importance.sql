CREATE TEMP TABLE t_idf AS
SELECT
    categories.primary AS category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM places
CROSS JOIN (SELECT count(*) AS total FROM places WHERE categories.primary IS NOT NULL) N
WHERE categories.primary IS NOT NULL
GROUP BY categories.primary, N.total;

CREATE TEMP TABLE place_density AS
SELECT id,
       ln(1 + count(*) OVER (
           PARTITION BY left(qk17, 15)
       )) AS density_score
FROM places;

CREATE TEMP TABLE place_idf AS
SELECT
    p.id,
    coalesce(idf.idf_score, 0) AS idf_score
FROM places p
LEFT JOIN t_idf idf ON idf.category = p.categories.primary;

-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places_scored;
CREATE TABLE places_scored AS
SELECT p.*,
       round(
           60 * least(coalesce(d.density_score, 0) / ${density_norm}, 1.0)
         + 40 * least(coalesce(i.idf_score, 0) / ${idf_norm}, 1.0)
       )::INTEGER AS importance
FROM places p
LEFT JOIN place_density d USING (id)
LEFT JOIN place_idf i USING (id);

DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_idf;
