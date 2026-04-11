-- Drop any leftover table from a prior run to make the script idempotent.
DROP TABLE IF EXISTS places_with_variants;
CREATE TABLE places_with_variants AS
SELECT *, []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] AS variants
FROM places;
DROP TABLE places;
ALTER TABLE places_with_variants RENAME TO places;
