-- Compute IDF scores per Overture taxonomy leaf from raw parquet.
-- Reads Overture place parquet directly and computes
-- ln(N_total / n_places) per taxonomy.primary leaf where N counts named
-- places with a non-null leaf.
DROP TABLE IF EXISTS idf_scores;
CREATE TABLE idf_scores AS
SELECT
    taxonomy.primary AS category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM '${parquet_glob}'
CROSS JOIN (
    SELECT count(*) AS total
    FROM '${parquet_glob}'
    WHERE taxonomy.primary IS NOT NULL
      AND names.primary IS NOT NULL
) N
WHERE taxonomy.primary IS NOT NULL
  AND names.primary IS NOT NULL
GROUP BY taxonomy.primary, N.total;
