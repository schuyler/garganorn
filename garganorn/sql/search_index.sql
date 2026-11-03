-- search_index.sql: the tables and normaliser of search.duckdb, then (after the
-- "-- fill" line) the fill from the run's tiles. ${tiles} is a SQL list of tile paths.

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

-- One row per record; rid is its rank under page_rank DESC NULLS LAST,
-- importance DESC, key ASC. ${page_rank} is a SQL string: the page-rank parquet.
CREATE TABLE search_records (rid INTEGER, key VARCHAR, importance INTEGER, page_rank DOUBLE);

-- One row per distinct (block, record), sorted by block, rid. block is the
-- first two characters of a name token of two or more characters. The bounds
-- are ceil(west), floor(east), ceil(south), floor(north); names holds each
-- name as ' ' || its tokens joined by ' '.
CREATE TABLE search_blocks (
    block VARCHAR, rid INTEGER, scope VARCHAR,
    w SMALLINT, e SMALLINT, s SMALLINT, n SMALLINT,
    names VARCHAR[]
);

-- fill
CREATE TEMP TABLE tile_records AS
SELECT DISTINCT ON (r.value.origin)
    substr(r.value.origin, 11) AS key,
    coalesce(r.importance, 0) AS importance,
    r.value.attributes.wikidata AS wikidata,
    list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.geo')[1] AS geo,
    list_filter(r.value.locations, l -> l."$type" = 'community.lexicon.location.bbox')[1] AS bbox,
    list_prepend(r.value.name, list_transform(coalesce(r.value.variants, []), v -> v.name)) AS raw_names
FROM (
    SELECT unnest(records) AS r
    FROM read_json(${tiles}, maximum_object_size = 67108864, columns = {
        records: 'STRUCT(
            importance INTEGER,
            value STRUCT(
                origin VARCHAR, name VARCHAR,
                attributes STRUCT(wikidata VARCHAR),
                variants STRUCT(name VARCHAR)[],
                locations STRUCT("$type" VARCHAR, latitude VARCHAR, longitude VARCHAR,
                                 west VARCHAR, south VARCHAR, east VARCHAR, north VARCHAR)[]
            )
        )[]'
    })
);

INSERT INTO search_records
SELECT row_number() OVER (ORDER BY page_rank DESC NULLS LAST, importance DESC, key) - 1,
       key, importance, page_rank
FROM tile_records LEFT JOIN read_parquet(${page_rank}) ON qid = wikidata
ORDER BY 1;

INSERT INTO search_blocks
SELECT
    unnest(list_distinct(list_transform(
        list_filter(flatten(list_transform(raw_names, n -> name_tokens(n))), t -> length(t) >= 2),
        t -> left(t, 2)))) AS block,
    rid,
    split_part(key, ':', 1) || ':' || split_part(key, ':', 2) AS scope,
    ceil(coalesce(geo.longitude, bbox.west)::DOUBLE)::SMALLINT,
    floor(coalesce(geo.longitude, bbox.east)::DOUBLE)::SMALLINT,
    ceil(coalesce(geo.latitude, bbox.south)::DOUBLE)::SMALLINT,
    floor(coalesce(geo.latitude, bbox.north)::DOUBLE)::SMALLINT,
    list_transform(raw_names, n -> ' ' || array_to_string(name_tokens(n), ' '))
FROM tile_records JOIN search_records USING (key)
ORDER BY block, rid;
