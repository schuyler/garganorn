CREATE TEMP TABLE t_idf AS
SELECT
    category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM (
    SELECT unnest(fsq_category_ids) AS category
    FROM places
    WHERE fsq_category_ids IS NOT NULL
) cats
CROSS JOIN (SELECT count(*) AS total FROM places WHERE fsq_category_ids IS NOT NULL) N
GROUP BY category, N.total;

CREATE TEMP TABLE place_density AS
SELECT fsq_place_id,
       ln(1 + count(*) OVER (
           PARTITION BY left(qk17, 15)
       )) AS density_score
FROM places;

CREATE TEMP TABLE place_idf AS
SELECT
    p.fsq_place_id,
    coalesce(max(idf.idf_score), 0) AS idf_score
FROM places p,
    unnest(p.fsq_category_ids) AS t(category)
LEFT JOIN t_idf idf ON idf.category = t.category
WHERE p.fsq_category_ids IS NOT NULL
GROUP BY p.fsq_place_id;

-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places_scored;
CREATE TABLE places_scored AS
SELECT p.*,
       round(
           60 * least(coalesce(d.density_score, 0) / ${density_norm}, 1.0)
         + 40 * least(coalesce(i.idf_score, 0) / ${idf_norm}, 1.0)
       )::INTEGER AS importance
FROM places p
LEFT JOIN place_density d USING (fsq_place_id)
LEFT JOIN place_idf i USING (fsq_place_id);

DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_idf;
