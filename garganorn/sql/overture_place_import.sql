-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

-- Load density and IDF parquet files as temp tables
-- When parquet path is None, create empty temp tables (LEFT JOINs produce NULL, importance defaults to 0)
${density_cte}
${idf_cte}

-- Import Overture places and compute importance + variants in one pass (Phase 2: density+IDF+importance+variants unified in import CTAS)
CREATE TABLE places AS
WITH ov_base AS (
    SELECT * EXCLUDE (geometry), geometry::GEOMETRY AS geometry,
           ST_QuadKey((bbox.xmin + bbox.xmax) / 2.0, (bbox.ymin + bbox.ymax) / 2.0, 17) AS qk17
    FROM '${parquet_glob}'
    WHERE bbox.xmin >= ${xmin} AND bbox.xmax <= ${xmax}
      AND bbox.ymin >= ${ymin} AND bbox.ymax <= ${ymax}
      AND geometry IS NOT NULL
),
ov_density AS (
    SELECT b.id,
           coalesce(d.density_score, 0) AS density_score
    FROM ov_base b
    LEFT JOIN density_tiles d ON d.tile_qk15 = left(b.qk17, 15)
),
ov_idf AS (
    SELECT b.id,
           coalesce(i.idf_score, 0) AS idf_score
    FROM ov_base b
    LEFT JOIN idf_scores i ON i.category = b.categories.primary
),
-- Compute variants from names.common and names.rules (absorbed from overture_place_variants.sql)
common_entries AS (
    SELECT id,
        unnest.key AS language,
        unnest."value" AS name
    FROM ov_base,
         unnest(map_entries(names.common))
    WHERE names.common IS NOT NULL
),
rule_entries AS (
    SELECT id,
        unnest.language,
        unnest."value" AS name,
        CASE unnest.variant
            WHEN 'common'     THEN 'alternate'
            WHEN 'official'   THEN 'official'
            WHEN 'alternate'  THEN 'alternate'
            WHEN 'short'      THEN 'short'
            ELSE 'alternate'
        END AS type
    FROM ov_base,
         unnest(names.rules)
    WHERE names.rules IS NOT NULL
),
all_variants AS (
    SELECT id, name, 'alternate' AS type, language FROM common_entries
    UNION ALL
    SELECT id, name, type, language FROM rule_entries
),
ov_variants AS (
    SELECT id,
           list({'name': name, 'type': type, 'language': language}
                ORDER BY name) AS variants
    FROM all_variants
    WHERE name IS NOT NULL AND name != ''
    GROUP BY id
)
SELECT b.*,
       round(
           60 * least(coalesce(od.density_score, 0) / ${density_norm}, 1.0)
         + 40 * least(coalesce(oi.idf_score, 0) / ${idf_norm}, 1.0)
       )::INTEGER AS importance,
       coalesce(v.variants, []) AS variants
FROM ov_base b
LEFT JOIN ov_density od USING (id)
LEFT JOIN ov_idf oi USING (id)
LEFT JOIN ov_variants v USING (id);

-- Drop temp tables
DROP TABLE density_tiles;
DROP TABLE idf_scores;
