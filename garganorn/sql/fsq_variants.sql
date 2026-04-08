CREATE TABLE places_with_variants AS
SELECT *, []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] AS variants
FROM places;
DROP TABLE places;
ALTER TABLE places_with_variants RENAME TO places;
