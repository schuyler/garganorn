-- Reads variant name tags from places.tags (preserved during import).
-- No parquet parameters needed.
CREATE TEMP TABLE raw_variants AS
WITH tag_entries AS (
    SELECT
        rkey,
        unnest(map_entries(tags)) AS e
    FROM places
    WHERE tags IS NOT NULL
),
name_tags AS (
    SELECT rkey, e.key AS key, e.value AS value
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
