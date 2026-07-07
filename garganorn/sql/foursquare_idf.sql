-- Compute IDF scores per FSQ category from raw parquet.
-- Reads FSQ parquet directly, unnests fsq_category_ids, and computes
-- ln(N_total / n_places) per category where N counts named places
-- with non-null categories.
DROP TABLE IF EXISTS idf_scores;
CREATE TABLE idf_scores AS
SELECT
    category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM (
    SELECT unnest(fsq_category_ids) AS category
    FROM '${parquet_glob}'
    WHERE fsq_category_ids IS NOT NULL
      AND name IS NOT NULL
) cats
CROSS JOIN (
    SELECT count(*) AS total
    FROM '${parquet_glob}'
    WHERE fsq_category_ids IS NOT NULL
      AND name IS NOT NULL
) N
GROUP BY category, N.total;
