-- Re-reads source parquet rather than querying the places table because the
-- OSM import pipeline strips name:* and alt_name/old_name tags from places
-- (only a category-key allowlist is preserved). Variant names are only
-- available in the original parquet.
-- Parameters: ${node_parquet}, ${way_parquet}
CREATE TEMP TABLE raw_variants AS
WITH tag_entries AS (
    SELECT
        'n' || id::VARCHAR AS rkey,
        unnest(map_entries(tags)) AS e
    FROM read_parquet('${node_parquet}')
    WHERE tags['name'] IS NOT NULL
    UNION ALL
    SELECT
        'w' || id::VARCHAR AS rkey,
        unnest(map_entries(tags)) AS e
    FROM read_parquet('${way_parquet}')
    WHERE tags['name'] IS NOT NULL
),
name_tags AS (
    SELECT rkey, e.key, e.value
    FROM tag_entries
    WHERE e.key LIKE 'name:%'
       OR e.key IN ('alt_name','old_name','official_name',
                    'short_name','loc_name','int_name')
),
split_values AS (
    SELECT rkey,
        trim(s.value) AS name,
        CASE
            WHEN key LIKE 'name:%' THEN 'alternate'
            WHEN key = 'alt_name' THEN 'alternate'
            WHEN key = 'old_name' THEN 'historical'
            WHEN key = 'official_name' THEN 'official'
            WHEN key = 'short_name' THEN 'short'
            WHEN key = 'loc_name' THEN 'colloquial'
            WHEN key = 'int_name' THEN 'alternate'
        END AS type,
        CASE
            WHEN key LIKE 'name:%' THEN replace(key, 'name:', '')
            ELSE NULL
        END AS language
    FROM name_tags,
         unnest(string_split(value, ';')) AS s(value)
    WHERE trim(s.value) != ''
)
SELECT rkey,
       list({'name': name, 'type': type, 'language': language}
            ORDER BY name) AS variants
FROM split_values
GROUP BY rkey;

CREATE TABLE places_with_variants AS
SELECT p.*,
       coalesce(rv.variants, []) AS variants
FROM places p
LEFT JOIN raw_variants rv USING (rkey);
DROP TABLE places;
ALTER TABLE places_with_variants RENAME TO places;
CREATE INDEX idx_rkey ON places(rkey);
DROP TABLE raw_variants;
