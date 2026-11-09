-- Notability pass over a finished import table: every row's base importance
-- becomes round(b + (100 - b) * lift), with b = ${base_scale} * base. The lift
-- is the QID's page rank, ramped, times a damping for names many places share;
-- a row with no QID, or a QID absent from the page-rank file, has lift 0.
-- ${table}, ${qid} and ${name} are the table and its QID and primary-name
-- expressions; ${page_rank} is the page-rank parquet's SQL string literal.
-- Updates in place: the table may be a temp table, and the planet OSM table
-- is not copied.

CREATE TEMP TABLE notability_lift AS
WITH name_counts AS (
    SELECT lower(strip_accents(trim(${name}))) AS k, count(*) AS nc
    FROM ${table}
    GROUP BY k
),
qid_name_counts AS (
    -- the QID's largest name count, so a localized name on a chain's QID is damped too
    SELECT q.qid, max(c.nc) AS nc
    FROM (SELECT ${qid} AS qid, lower(strip_accents(trim(${name}))) AS k
          FROM ${table} WHERE ${qid} IS NOT NULL) q
    JOIN name_counts c USING (k)
    GROUP BY q.qid
)
SELECT n.qid,
       ${weight}
         * ln(1 + ${ramp} * least(greatest((pr.page_rank - ${pr_floor}) / (1 - ${pr_floor}), 0), 1))
           / ln(1 + ${ramp})
         * (1 - least(greatest(ln(n.nc::DOUBLE / ${nc_floor}) / ln(${nc_ceiling}::DOUBLE / ${nc_floor}), 0), 1))
         AS lift
FROM qid_name_counts n
JOIN read_parquet(${page_rank}) pr ON pr.qid = n.qid;

UPDATE ${table}
SET importance = round(
        ${base_scale} * importance
        + (100 - ${base_scale} * importance)
          * coalesce((SELECT lift FROM notability_lift WHERE notability_lift.qid = ${qid}), 0)
    )::INTEGER;

DROP TABLE notability_lift;
