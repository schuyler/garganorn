-- division_containment.sql: division-in-division containment from Overture
-- hierarchies, in place of compute_containment's geometric join.
--
-- Parameters substituted by stage_division_containment() in stages.py
-- (dollar-brace placeholders; not repeated literally in these comments
-- because substitution is plain string replacement and would corrupt the
-- comment text):
--   division_parquet          : raw division parquet glob (hierarchies col)
--   places_parquet             : imported places.parquet (level, names)
--   tile_assignments_parquet   : tile_assignments_combined.parquet
--   collection_prefix          : rkey NSID prefix
--
-- Two chained unnests (nested unnest in one SELECT isn't supported) because
-- hierarchies is a list of chains, each chain a list of (division_id,
-- subtype, name) structs; flatten and dedup rather than assume a single
-- chain. level and name are taken from the ancestor's own row in
-- places_parquet, not the hierarchies struct, so a client sees the same name
-- in a reference as it would fetching that record directly.
--
-- Output: (tile_qk, place_id, relations_json), unsorted -- the caller sorts
-- per output file.
WITH chains AS (
    SELECT d.id AS place_id, unnest(d.hierarchies) AS chain
    FROM '${division_parquet}' d
),
chain AS (
    SELECT place_id, unnest(chain) AS h
    FROM chains
),
ancestors AS (
    SELECT DISTINCT place_id, h.division_id AS ancestor_id
    FROM chain
    WHERE h.division_id != place_id
),
resolved AS (
    SELECT a.place_id, a.ancestor_id, p.level, p.names."primary" AS name
    FROM ancestors a
    JOIN read_parquet('${places_parquet}') p ON p.id = a.ancestor_id
)
SELECT ta.tile_qk, r.place_id,
       to_json({within: list({rkey: '${collection_prefix}:' || r.ancestor_id,
                              name: r.name,
                              level: r.level}
                             ORDER BY r.level ASC, r.ancestor_id ASC)})::VARCHAR
           AS relations_json
FROM resolved r
JOIN read_parquet('${tile_assignments_parquet}') ta ON ta.place_id = r.place_id
GROUP BY ta.tile_qk, r.place_id
