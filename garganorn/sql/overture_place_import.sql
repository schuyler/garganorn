-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

-- Load density and IDF parquet files as temp tables
-- When parquet path is None, create empty temp tables (LEFT JOINs produce NULL, importance defaults to 0)
${density_cte}
${idf_cte}

-- Import Overture places and compute importance + variants in one pass (Phase 2: density+IDF+importance+variants unified in import CTAS)
-- ov_base is scanned exactly
-- once beyond its own definition (the final SELECT below). Density/idf are
-- joined directly against pre-deduplicated lookup tables instead of via a
-- helper CTE that re-scans ov_base, and variants is computed per-row with
-- list_transform/list_concat/list_filter/list_sort -- no unnest, no
-- GROUP BY -- so there is no full-width re-materialization or shuffle of
-- ov_base to spill temp disk.
CREATE TABLE places AS
WITH ov_base AS (
    -- geometry is dropped here, not carried downstream: it is only needed
    -- for the existence filter below, and qk17 comes from bbox, not geometry.
    -- Carrying a recast GEOMETRY column through the joins/CTAS below instead
    -- costs nothing at small scale but inflates sort/join spill enormously
    -- at full Overture places scale, for zero downstream benefit.
    SELECT * EXCLUDE (geometry),
           -- Validate coordinate range before calling ST_QuadKey
           CASE WHEN (LEAST(bbox.xmin, bbox.xmax) + GREATEST(bbox.xmin, bbox.xmax)) / 2.0 BETWEEN -180 AND 180
                  AND (LEAST(bbox.ymin, bbox.ymax) + GREATEST(bbox.ymin, bbox.ymax)) / 2.0 BETWEEN -90 AND 90
                THEN ST_QuadKey(
                    (LEAST(bbox.xmin, bbox.xmax) + GREATEST(bbox.xmin, bbox.xmax)) / 2.0,
                    (LEAST(bbox.ymin, bbox.ymax) + GREATEST(bbox.ymin, bbox.ymax)) / 2.0, 17)
                ELSE NULL END AS qk17
    FROM '${parquet_glob}'
    WHERE bbox.xmax >= ${xmin} AND bbox.xmin <= ${xmax}
      AND bbox.ymax >= ${ymin} AND bbox.ymin <= ${ymax}
      AND geometry IS NOT NULL
      AND names.primary IS NOT NULL AND TRIM(names.primary) != ''
)
SELECT b.*,
       round(
           60 * least(coalesce(od.density_score, 0) / ${density_norm}, 1.0)
         + 40 * least(coalesce(oi.idf_score, 0) / ${idf_norm}, 1.0)
       )::INTEGER AS importance,
       -- Compute variants from names.common and names.rules per-row
       -- (absorbed from overture_place_variants.sql). names.common entries
       -- are always typed 'alternate'; names.rules entries are mapped by
       -- their `variant` string. Concatenating the two lists and filtering
       -- out NULL/empty names replaces the old unnest -> UNION ALL ->
       -- GROUP BY id pipeline with a single per-row expression -- no
       -- explosion, no re-aggregation. Duplicates are NOT deduped, matching
       -- prior behavior (list_concat with no DISTINCT).
       list_sort(list_filter(
           list_concat(
               coalesce(list_transform(map_entries(b.names.common),
                   e -> {'name': e.value, 'type': 'alternate', 'language': e.key}), []),
               coalesce(list_transform(b.names.rules,
                   r -> {'name': r.value,
                         'type': CASE r.variant
                                   WHEN 'common'     THEN 'alternate'
                                   WHEN 'official'   THEN 'official'
                                   WHEN 'alternate'  THEN 'alternate'
                                   WHEN 'short'      THEN 'short'
                                   ELSE 'alternate'
                                 END,
                         'language': r.language}), [])),
           v -> v.name IS NOT NULL AND TRIM(v.name) != '')) AS variants
FROM ov_base b
LEFT JOIN (
    -- Pre-dedupe density_tiles on the join key: without this, a duplicate
    -- tile_qk15 key fans the LEFT JOIN below out into duplicate places rows.
    SELECT tile_qk15, any_value(density_score) AS density_score
    FROM density_tiles
    GROUP BY tile_qk15
) od ON od.tile_qk15 = left(b.qk17, 15)
LEFT JOIN (
    -- Pre-dedupe idf_scores on the join key for the same reason.
    SELECT category, any_value(idf_score) AS idf_score
    FROM idf_scores
    GROUP BY category
) oi ON oi.category = b.categories.primary;

-- Drop temp tables
DROP TABLE density_tiles;
DROP TABLE idf_scores;
