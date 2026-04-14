-- Compute IDF scores per Overture primary category from raw parquet.
-- Reads Overture place parquet directly and computes
-- ln(N_total / n_places) per primary category where N counts named places
-- with non-null categories.
DROP TABLE IF EXISTS idf_scores;
CREATE TABLE idf_scores AS
SELECT
    categories.primary AS category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM '${parquet_glob}'
CROSS JOIN (
    SELECT count(*) AS total
    FROM '${parquet_glob}'
    WHERE categories.primary IS NOT NULL
      AND names.primary IS NOT NULL
) N
WHERE categories.primary IS NOT NULL
  AND names.primary IS NOT NULL
GROUP BY categories.primary, N.total;
