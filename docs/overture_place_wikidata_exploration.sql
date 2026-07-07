-- Explore Wikidata coverage in Overture Places parquet files.
-- Run with: duckdb -f docs/overture_place_wikidata_exploration.sql
-- Adjust the path below to match your local cache.

-- 1. Full schema
DESCRIBE SELECT * FROM 'cache/overture/2026-03-18.0/places/*.parquet' LIMIT 0;

-- 2. What dataset values appear in sources[]?
SELECT dataset, count(*) AS n
FROM (
    SELECT unnest([s.dataset FOR s IN sources]) AS dataset
    FROM 'cache/overture/2026-03-18.0/places/*.parquet'
)
GROUP BY 1 ORDER BY 2 DESC;

-- 3. brand.wikidata coverage
SELECT count(*) AS total,
       count(brand.wikidata) AS has_brand_wikidata
FROM 'cache/overture/2026-03-18.0/places/*.parquet';

-- 4. Sample sources entries to understand structure
SELECT id, names.primary AS name, sources
FROM 'cache/overture/2026-03-18.0/places/*.parquet'
LIMIT 10;

-- 5. Sample brand.wikidata entries
SELECT id, names.primary AS name, brand.wikidata, brand.names.primary AS brand_name
FROM 'cache/overture/2026-03-18.0/places/*.parquet'
WHERE brand.wikidata IS NOT NULL
LIMIT 20;

-- 6. Look for Golden Gate Bridge
SELECT id, names.primary AS name, sources, brand
FROM 'cache/overture/2026-03-18.0/places/*.parquet'
WHERE names.primary ILIKE '%golden gate bridge%';

-- 7. Any column with 'wiki' in the name?
SELECT column_name, column_type
FROM (DESCRIBE SELECT * FROM 'cache/overture/2026-03-18.0/places/*.parquet' LIMIT 0)
WHERE lower(column_name) LIKE '%wiki%';
