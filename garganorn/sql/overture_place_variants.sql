CREATE TEMP TABLE overture_variants AS
WITH common_entries AS (
    SELECT id,
        unnest.key AS language,
        unnest."value" AS name
    FROM places,
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
    FROM places,
         unnest(names.rules)
    WHERE names.rules IS NOT NULL
),
all_variants AS (
    SELECT id, name, 'alternate' AS type, language FROM common_entries
    UNION ALL
    SELECT id, name, type, language FROM rule_entries
)
SELECT id, list({'name': name, 'type': type, 'language': language}
                ORDER BY name) AS variants
FROM all_variants
WHERE name IS NOT NULL AND name != ''
GROUP BY id;

-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places_with_variants;
CREATE TABLE places_with_variants AS
SELECT p.*,
       coalesce(ov.variants, []) AS variants
FROM places p
LEFT JOIN overture_variants ov USING (id);
DROP TABLE places;
ALTER TABLE places_with_variants RENAME TO places;
DROP TABLE overture_variants;
