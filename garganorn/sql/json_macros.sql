-- Strip null-valued keys from a JSON object. Used at export time wherever a
-- struct-to-JSON conversion would otherwise carry every declared key
-- (DuckDB struct literals always do), including variant.language across all
-- three sources and the overture_place/overture_division attributes blocks.
-- The $."key" path syntax handles the $type key (leading $ is JSONPath root).
-- TODO: Replace with native json_strip_nulls() once a DuckDB release ships
-- it (merged in PR #21748, 2026-04-02; not in the pinned duckdb==1.4.4).
CREATE OR REPLACE MACRO strip_json_nulls(js) AS
    map_from_entries(
        [(key, json_extract(js, '$."' || key || '"')) FOR key IN json_keys(js)
         IF json_extract(js, '$."' || key || '"') <> ('null'::JSON)]
    )::JSON;
