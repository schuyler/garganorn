-- search_index.sql: the tables and normaliser of search.duckdb (the schema
-- section), then named sections the build runs in order. Each "-- name: X" line
-- starts section X. The intermediate tables live in the attached scratch
-- database, never in search.duckdb. ${tiles} is a SQL list of tile paths.

-- name: schema

-- name_tokens(s): lower, strip accents, drop apostrophes, punctuation runs to
-- spaces, final sigma to sigma, split on whitespace.
CREATE MACRO name_tokens(s) AS
    list_filter(
        string_split_regex(
            replace(
                regexp_replace(
                    replace(replace(replace(strip_accents(lower(s)), '''', ''), '’', ''), 'ʼ', ''),
                    '\pP+', ' ', 'g'),
                'ς', 'σ'),
            '[\pZ\s]+'),
        t -> t != '');

-- One row per record; rid is its rank under importance DESC, key ASC.
CREATE TABLE search_records (rid INTEGER, key VARCHAR, importance INTEGER);

-- One row per distinct (block, record), sorted by block, rid. block is the
-- first two characters of a name token of two or more characters. The bounds
-- are ceil(west), floor(east), ceil(south), floor(north); names holds each
-- name with at least one token, as ' ' || its tokens joined by ' '.
CREATE TABLE search_blocks (
    block VARCHAR, rid INTEGER, scope VARCHAR,
    w SMALLINT, e SMALLINT, s SMALLINT, n SMALLINT,
    names VARCHAR[]
);

-- name: tile_records
CREATE TABLE scratch.tile_records AS
SELECT DISTINCT ON (r.value.origin)
    substr(r.value.origin, 11) AS key,
    coalesce(r.importance, 0) AS importance,
    ceil(coalesce(
        list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.geo')[1].longitude,
        list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.bbox')[1].west)::DOUBLE)::SMALLINT AS w,
    floor(coalesce(
        list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.geo')[1].longitude,
        list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.bbox')[1].east)::DOUBLE)::SMALLINT AS e,
    ceil(coalesce(
        list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.geo')[1].latitude,
        list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.bbox')[1].south)::DOUBLE)::SMALLINT AS s,
    floor(coalesce(
        list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.geo')[1].latitude,
        list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.bbox')[1].north)::DOUBLE)::SMALLINT AS n,
    list_prepend(r.value.name, list_transform(coalesce(r.value.variants, []), v -> v.name)) AS raw_names
FROM (
    SELECT unnest(records) AS r
    FROM read_json(${tiles}, maximum_object_size = 67108864, columns = {
        records: 'STRUCT(
            importance INTEGER,
            value STRUCT(
                origin VARCHAR, name VARCHAR,
                variants STRUCT(name VARCHAR)[],
                locations STRUCT("$type" VARCHAR, latitude VARCHAR, longitude VARCHAR,
                                 west VARCHAR, south VARCHAR, east VARCHAR, north VARCHAR)[]
            )
        )[]'
    })
);

-- name: search_records
INSERT INTO search_records
SELECT row_number() OVER (ORDER BY importance DESC, key) - 1, key, importance
FROM scratch.tile_records
ORDER BY 1;

-- name: blocks_unsorted
CREATE TABLE scratch.blocks_unsorted AS
SELECT
    unnest(list_distinct(list_transform(
        list_filter(flatten(list_transform(raw_names, n -> name_tokens(n))), t -> length(t) >= 2),
        t -> left(t, 2)))) AS block,
    rid,
    split_part(key, ':', 1) || ':' || split_part(key, ':', 2) AS scope,
    w, e, s, n,
    list_transform(
        list_filter(list_transform(raw_names, n -> name_tokens(n)), tl -> len(tl) > 0),
        tl -> ' ' || array_to_string(tl, ' '))
FROM scratch.tile_records JOIN search_records USING (key);

-- name: block_counts
SELECT block, count(*) FROM scratch.blocks_unsorted GROUP BY block ORDER BY block;

-- name: blocks_chunk
INSERT INTO search_blocks
SELECT * FROM scratch.blocks_unsorted
WHERE block BETWEEN ? AND ?
ORDER BY block, rid;
