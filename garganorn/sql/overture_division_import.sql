-- Import Overture Maps division and division_area parquet files into a places table.
--
-- Overture splits administrative boundary data across two parquet themes:
--   division       -- metadata (names, subtype, country, wikidata, population)
--   division_area  -- geometries, one or more rows per division (filtered to is_land=true)
--
-- ST_Union_Agg merges a division's area rows into one geometry. In 2026-07-22.0
-- there is exactly one row per division -- non-contiguity is carried inside a
-- single MULTIPOLYGON -- but the schema permits more, so the aggregate stays.
--
-- The atgeo containment `level` is derived from `subtype` alone via a
-- generated CASE expression, substituted below as a dollar-brace placeholder
-- (not repeated literally in this comment because substitution is plain
-- string replacement and would corrupt the comment text). It is rendered by
-- stage_division_import() in stages.py from garganorn.levels.LEVEL_VOCAB,
-- the single source of truth. Overture's raw
-- admin_level is 96% NULL and ambiguous within a subtype, so it is not
-- carried into places.
--
-- The bbox filter applies to division_area (which has bbox columns), not division.
-- Divisions with no matching land area after filtering are dropped via INNER JOIN.
--
-- Importance and variants are computed inline during import. The base
-- importance is the larger of a population signal and a bounding-box area
-- signal, each saturating at 1, whatever the subtype; stage_division_import
-- then applies the notability pass (notability.sql).

DROP TABLE IF EXISTS places;
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

CREATE OR REPLACE MACRO atgeo_valid_language(lang) AS (
    lang IS NOT NULL AND regexp_matches(lang, '^(i|[a-z]{2,3})(-[A-Za-z0-9-]+)?$')
);

CREATE TEMP TABLE division_base AS
WITH division AS (
    SELECT id, names, subtype, country, region, wikidata,
           population, parent_division_id
    FROM '${division_parquet}'
),
division_area AS (
    SELECT division_id,
           geometry::GEOMETRY AS geometry
    FROM '${division_area_parquet}'
    WHERE is_land = true
      AND geometry IS NOT NULL
      AND bbox.xmax >= ${xmin} AND bbox.xmin <= ${xmax}
      AND bbox.ymax >= ${ymin} AND bbox.ymin <= ${ymax}
),
merged_areas AS (
    -- Merge multiple land-area geometries per division into one.
    SELECT division_id,
           ST_Union_Agg(geometry) AS geometry
    FROM division_area
    GROUP BY division_id
),
merged_areas_interior AS (
    SELECT division_id, geometry,
           ST_PointOnSurface(geometry) AS interior_point
    FROM merged_areas
),
-- Minimal longitude interval of a division wider than 180 degrees planar: cut at
-- the largest gap between polygon parts, unless the gap across +-180 is at least
-- as large (then planar is minimal). A gap is measured from the running maximum
-- end of earlier parts, so a part nested in a wider one opens none.
wide_parts AS (
    SELECT division_id, ST_XMin(p.geom) AS s, ST_XMax(p.geom) AS e
    FROM (SELECT division_id, unnest(ST_Dump(geometry)) AS p
          FROM merged_areas_interior
          WHERE ST_XMax(geometry) - ST_XMin(geometry) > 180)
),
wide_gaps AS (
    SELECT division_id, s AS west, prev_e AS east, s - prev_e AS gap
    FROM (SELECT division_id, s,
                 max(e) OVER (PARTITION BY division_id ORDER BY s
                              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_e
          FROM wide_parts)
),
wide_extent AS (
    SELECT g.division_id, g.cut.west, g.cut.east
    FROM (SELECT division_id, max(gap) AS gap,
                 arg_max(struct_pack(west := west, east := east), gap) AS cut
          FROM wide_gaps GROUP BY division_id) g
    JOIN (SELECT division_id, 360 - (max(e) - min(s)) AS across
          FROM wide_parts GROUP BY division_id) a USING (division_id)
    WHERE g.gap > a.across
)
SELECT
    d.id,
    ma.geometry,
    d.names,
    d.subtype,
    d.country,
    d.region,
    -- atgeo containment level, derived from subtype alone (no ELSE branch:
    -- an unmapped subtype must produce NULL here so the fail-loud validator
    -- and the post-CTAS NULL-level assertion in stage_division_import()
    -- catch it).
    ${level_case} AS level,
    d.wikidata,
    greatest(coalesce(d.population, 0), 0) AS population,
    d.parent_division_id,
    -- bbox is derived from the merged geometry; used for tile assignment and export
    {'xmin': ST_XMin(ma.geometry), 'ymin': ST_YMin(ma.geometry),
     'xmax': ST_XMax(ma.geometry), 'ymax': ST_YMax(ma.geometry)} AS bbox,
    -- qk17 placed at the interior point -- the same point containment
    -- tests -- so a division's tile and its containment answer agree.
    -- A bbox midpoint can fall outside a crescent or multipart division
    -- entirely, which puts the record in a tile it isn't in.
    qk17(ST_X(ma.interior_point), ST_Y(ma.interior_point)) AS qk17,
    -- min/max extents stored flat for fast bbox-filter in containment queries
    ST_YMin(ma.geometry) AS min_latitude,
    ST_YMax(ma.geometry) AS max_latitude,
    coalesce(we.west, ST_XMin(ma.geometry)) AS min_longitude,
    coalesce(we.east, ST_XMax(ma.geometry)) AS max_longitude,
    ST_X(ma.interior_point) AS interior_lon,
    ST_Y(ma.interior_point) AS interior_lat
FROM division d
JOIN merged_areas_interior ma ON ma.division_id = d.id
LEFT JOIN wide_extent we ON we.division_id = d.id;

CREATE TABLE places AS
SELECT
    db.*,
    -- Planar area of the bounding box in km2; the longitude span is widened by
    -- 360 when it crosses the antimeridian.
    round(100 * greatest(
        least(ln(1 + db.population) / ${pop_norm}, 1.0),
        least(ln(1 + (CASE WHEN db.max_longitude - db.min_longitude < 0
                           THEN db.max_longitude - db.min_longitude + 360
                           ELSE db.max_longitude - db.min_longitude END)
                     * 111.32 * cos(radians((db.min_latitude + db.max_latitude) / 2))
                     * (db.max_latitude - db.min_latitude) * 110.57)
                 / ${area_norm}, 1.0)))::INTEGER AS importance,
    -- Compute variants from names.common and names.rules per-row, same
    -- expression and semantics as overture_place_import.sql (no dedup).
    list_sort(list_filter(
        list_concat(
            coalesce(list_transform(map_entries(db.names.common),
                e -> {'name': e.value, 'type': 'alternate',
                      'language': CASE WHEN atgeo_valid_language(e.key) THEN e.key ELSE NULL END}), []),
            coalesce(list_transform(db.names.rules,
                r -> {'name': r.value,
                      'type': CASE r.variant
                                WHEN 'common'     THEN 'alternate'
                                WHEN 'official'   THEN 'official'
                                WHEN 'alternate'  THEN 'alternate'
                                WHEN 'short'      THEN 'short'
                                ELSE 'alternate'
                              END,
                      'language': CASE WHEN atgeo_valid_language(r.language) THEN r.language ELSE NULL END}), [])),
        v -> v.name IS NOT NULL AND TRIM(v.name) != '')) AS variants
FROM division_base db;

-- Drop temp tables
DROP TABLE division_base;
