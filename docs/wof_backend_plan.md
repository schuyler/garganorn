# Who's on First Backend Plan

Add a `WhosOnFirst` database subclass following the same pattern as
`FoursquareOSP` and `OvertureMaps`. No new endpoints, no hierarchy queries
-- just a new data source with the same interface.

## Decisions

1. **Collection name**: `org.atgeo.places.whosonfirst`
   (confirmed — follows `org.atgeo.places.foursquare`, `org.atgeo.places.overture`
   convention).

2. **Placetype filtering**: Include all WOF placetypes. No filtering.

3. **Multilingual names**: Yes. Extract `name:*_x_preferred` variants from
   the GeoJSON properties at import time. The lexicon already supports a
   `names` array with `lang`/`priority` fields. See "Multilingual Names"
   section.

4. **Importance scoring**: Use the existing density/IDF mechanism. Add `wof`
   as a source to `build-density.sh` and `build-idf.sh`. See "Importance
   Scoring" section.

## Data Source

WOF data is distributed as per-country Git repositories of individual
GeoJSON files under the `whosonfirst-data` GitHub organization. The
intended distribution method is cloning the repos.

### Repository structure

- Org: `https://github.com/whosonfirst-data/`
- 261 per-country admin repos: `whosonfirst-data-admin-{iso}` (e.g.,
  `whosonfirst-data-admin-us` at ~1.9 GB)
- Total size across all admin repos: ~15 GB
- Other repos: `whosonfirst-data-postalcode-{iso}`,
  `whosonfirst-data-constituency-{iso}`, etc.
- Default branch: `master`

### Acquisition

**Full world (default):** Clone all 261 `whosonfirst-data-admin-*` repos.
The import script should support pointing at a directory containing all
cloned repos (e.g., `~/wof-data/` containing
`whosonfirst-data-admin-us/`, `whosonfirst-data-admin-gb/`, etc.).

A helper script or Makefile target to clone/update all repos would be
useful:

```bash
# Clone all WOF admin repos (first time)
gh repo list whosonfirst-data --limit 300 --json name -q \
    '.[].name | select(startswith("whosonfirst-data-admin-"))' |
    xargs -P4 -I{} git clone --depth 1 \
        https://github.com/whosonfirst-data/{}.git ${wof_dir}/{}

# Update existing clones
find ${wof_dir} -name .git -type d -execdir git pull --ff-only \;
```

**Single country:** Clone one repo. Pass it as the repo path.

**Regional bbox extract:** Clone the relevant repos, import with bbox
filter.

### File layout

Each record is a GeoJSON Feature file at a path derived from its WOF ID:

```
data/{id[0:3]}/{id[3:6]}/{id[6:]}/{id}.geojson
```

Example: WOF ID `85922583` → `data/859/225/83/85922583.geojson`

Alt-geometry files (`*-alt-*.geojson`) exist alongside records and must be
excluded from import.

### GeoJSON record structure

Verified against the actual San Francisco record (`85922583.geojson`):

```
Top-level keys: id, type, properties, bbox, geometry
```

**Top-level fields:**
- `bbox`: `[xmin, ymin, xmax, ymax]` — real bounding box (GeoJSON standard)
- `geometry`: Full polygon/multipolygon boundary
- `id`: integer WOF ID

**Key properties (verified):**
- `wof:id` (int) — unique identifier
- `wof:name` (str) — primary name
- `wof:placetype` (str) — e.g., `locality`, `region`, `neighbourhood`
- `wof:country` (str) — ISO country code
- `wof:parent_id` (int) — parent place
- `wof:belongsto` (list[int]) — ancestor chain
- `wof:lastmodified` (int) — Unix timestamp
- `wof:population` (int) — population (when available)
- `wof:population_rank` (int) — 0-18 scale
- `wof:concordances` (dict) — external IDs (`gn:id`, `wd:id`, etc.)
- `wof:superseded_by` (list) — replacement IDs
- `mz:is_current` (int) — 1 if active
- `iso:country` (str) — ISO 3166-1
- `geom:latitude` / `geom:longitude` (float) — centroid
- `geom:bbox` (str) — `"xmin,ymin,xmax,ymax"`
- `lbl:latitude` / `lbl:longitude` (float) — label placement point

**Name properties (verified):**
- Format: `name:{iso639-3}_x_{variant}` where variant is `preferred`,
  `variant`, `colloquial`, `unknown`
- Value: list of strings (e.g., `name:zho_x_preferred: ["旧金山"]`)
- San Francisco has ~100+ language variants

### Accessing repo data

For import, either:
- `git clone --depth 1` the relevant country repo(s)
- Download the repo tarball via GitHub API
- Point to a local checkout

## Import Script: `scripts/import-wof-extract.sh`

### Usage

```
./scripts/import-wof-extract.sh <wof_data_dir> [xmin ymin xmax ymax]
```

- `wof_data_dir`: directory containing one or more cloned WOF repos. The
  script globs `${wof_data_dir}/*/data/**/*.geojson` to find all GeoJSON
  files across all repos.
- Bbox args are optional. If omitted, imports the full world (no spatial
  filter).
- For a single-country import, point at a single repo:
  `./scripts/import-wof-extract.sh ~/wof/whosonfirst-data-admin-us`

### Structure

Follow the existing import script pattern:

1. Check for duckdb binary
2. Validate wof_data_dir exists and contains GeoJSON files
3. If bbox args provided, parse and validate them
4. Auto-build density and IDF parquets if missing
5. Generate `.sql` file
6. Execute with `duckdb -bail "${output_dir}/wof-places.duckdb.tmp" -c
   ".read ${output_dir}/import-wof.sql"`
7. Clean up `.sql` file on success
8. `mv` `.tmp` to `wof-places.duckdb`

Output filename: `wof-places.duckdb`

### Reading GeoJSON files with DuckDB

DuckDB's `read_json` can glob GeoJSON files from the repo. The key
challenge is that WOF records have hundreds of heterogeneous property keys
(especially `name:*` variants), so auto-schema-inference will produce a
massive schema.

Approach: read with `read_json` specifying only the columns we need, plus
keep the raw `properties` as JSON for name extraction. The glob pattern
spans all repos in the data directory.

```sql
INSTALL spatial;
LOAD spatial;
INSTALL json;
LOAD json;

CREATE TABLE raw_places AS
SELECT
    CAST(id AS VARCHAR) AS wof_id,
    json_extract_string(properties, '$."wof:name"') AS name,
    json_extract_string(properties, '$."wof:placetype"') AS placetype,
    json_extract_string(properties, '$."wof:country"') AS country,
    CAST(json_extract(properties, '$."geom:latitude"') AS DOUBLE) AS latitude,
    CAST(json_extract(properties, '$."geom:longitude"') AS DOUBLE) AS longitude,
    CAST(json_extract(properties, '$."wof:parent_id"') AS VARCHAR)
        AS parent_id,
    CAST(json_extract(properties, '$."wof:lastmodified"') AS INTEGER)
        AS lastmodified,
    CAST(json_extract(properties, '$."mz:is_current"') AS INTEGER)
        AS is_current,
    bbox AS geojson_bbox,
    properties
FROM read_json('${wof_data_dir}/*/data/**/*.geojson',
    format='auto',
    maximum_object_size=104857600,
    filename=true,
    union_by_name=true)
WHERE filename NOT LIKE '%-alt-%';
```

Then build the `places` table with bbox from the GeoJSON top-level `bbox`
array and spatial index. Bbox filtering is conditional — omitted for full
world imports:

```sql
CREATE TABLE places AS
SELECT
    wof_id, name, placetype, country, latitude, longitude,
    ST_Point(longitude, latitude) AS geom,
    parent_id, lastmodified,
    {'xmin': geojson_bbox[1], 'ymin': geojson_bbox[2],
     'xmax': geojson_bbox[3], 'ymax': geojson_bbox[4]
    }::STRUCT(xmin DOUBLE, ymin DOUBLE,
              xmax DOUBLE, ymax DOUBLE) AS bbox
FROM raw_places
WHERE is_current = 1
  AND latitude != 0 AND longitude != 0
  -- bbox filter appended only if bbox args provided:
  -- AND latitude BETWEEN ${ymin} AND ${ymax}
  -- AND longitude BETWEEN ${xmin} AND ${xmax}
;

CREATE INDEX places_rtree ON places USING RTREE (geom);
```

The import script conditionally appends the bbox WHERE clause only when bbox
args are provided. For full-world imports, only the `is_current` and
non-zero coordinate filters apply.

Notes:
- Uses **real bounding boxes** from the GeoJSON `bbox` field, not a
  synthetic point buffer. This is more accurate for admin places that cover
  large areas.
- `wof_id` is VARCHAR for consistency with other backends.
- Filter to active-only via `is_current = 1`.
- Alt-geometry files excluded via `filename NOT LIKE '%-alt-%'`.
- The `raw_places` temp table retains `properties` JSON for name extraction.
- The glob `${wof_data_dir}/*/data/**/*.geojson` spans all repos in the
  data directory. For a single-repo import, the glob still works (matches
  `repo/data/**/*.geojson`).

### Performance considerations

Reading many individual GeoJSON files via glob may be slow. If performance
is a problem:
- Pre-concatenate files into newline-delimited JSON:
  `find data -name "*.geojson" ! -name "*-alt-*" -exec cat {} + > all.ndjson`
- Then read with `read_json('all.ndjson')`
- Or consider converting to parquet as an intermediate step

Test with a real repo before deciding.

## Multilingual Names

### Verified name format

From the actual SF record:
- `name:eng_x_preferred`: `["San Francisco"]`
- `name:zho_x_preferred`: `["旧金山"]`
- `name:ara_x_preferred`: `["سان فرانسيسكو"]`
- `name:eng_x_colloquial`: `["City by the Bay", "Fog City", "Frisco"]`
- `name:eng_x_variant`: `["S Francisco", "Sanfran", "Frisco"]`

Language code is ISO 639-3 (3-letter). Variant suffixes: `_x_preferred`,
`_x_variant`, `_x_colloquial`, `_x_unknown`.

### Import approach

Extract names from the `properties` JSON retained in `raw_places`:

```sql
CREATE TABLE names AS
-- Primary name (from wof:name, priority 0)
SELECT wof_id, 'und' AS lang, name AS name_text, 0 AS priority
FROM places
WHERE name IS NOT NULL

UNION ALL

-- Preferred name variants from properties JSON (priority 1)
SELECT
    rp.wof_id,
    split_part(split_part(k.key, ':', 2), '_x_', 1) AS lang,
    json_extract_string(
        json_extract(rp.properties, '$."' || k.key || '"'), '$[0]'
    ) AS name_text,
    1 AS priority
FROM raw_places rp,
    unnest(json_keys(rp.properties)) AS k(key)
WHERE k.key LIKE 'name:%\_x\_preferred' ESCAPE '\'
  AND rp.wof_id IN (SELECT wof_id FROM places);
```

Notes:
- The `unnest(json_keys(...))` + filter approach handles the dynamic
  `name:*` keys without knowing them in advance.
- Only `_x_preferred` variants are imported for v1. `_x_variant` and
  `_x_colloquial` could be added later.
- The primary `wof:name` gets lang `und` (undetermined) and priority 0.
- This SQL needs validation against DuckDB's actual JSON function behavior
  with these key patterns (colons in property names may need quoting).

### Name index changes

Generate trigrams from all name variants:

```sql
CREATE TABLE name_index AS
WITH name_prep AS (
    SELECT
        n.wof_id,
        n.name_text AS name,
        lower(strip_accents(n.name_text)) AS norm_name,
        p.latitude::decimal(10,6)::varchar AS latitude,
        p.longitude::decimal(10,6)::varchar AS longitude,
        p.placetype,
        p.country,
        coalesce(p.importance, 0) AS importance
    FROM names n
    JOIN places p ON n.wof_id = p.wof_id
    WHERE n.name_text IS NOT NULL AND length(n.name_text) > 0
),
trigrams AS (
    SELECT DISTINCT
        substr(np.norm_name, pos, 3) AS trigram,
        np.wof_id, np.name, np.latitude, np.longitude,
        np.placetype, np.country, np.importance
    FROM name_prep np
    CROSS JOIN generate_series(1, length(np.norm_name) - 2) AS gs(pos)
    WHERE length(np.norm_name) >= 3
)
SELECT trigram, wof_id, name, latitude, longitude,
       placetype, country, importance
FROM trigrams
ORDER BY trigram;

ANALYZE;
```

The `name_index` denormalizes `placetype` and `country` for text-only search
results (analogous to how FSQ denormalizes address fields). Each name
variant gets its own set of trigram rows, so searching in any language
works.

### `process_record` changes

`query_record` joins the `names` table. The `names` array in output
includes lang tags:

```python
"names": [
    {"text": "San Francisco", "lang": "und", "priority": 0},
    {"text": "San Francisco", "lang": "eng", "priority": 1},
    {"text": "旧金山", "lang": "zho", "priority": 1},
]
```

### Design notes

- Trigrams on non-Latin scripts (CJK, Arabic) provide limited matching
  quality but still enable some search. CJK text is often short enough that
  3-character windows capture meaningful substrings.
- The name_index will be significantly larger (many rows per place for
  places with many translations). San Francisco has ~100+ language variants.
  Most places will have far fewer.
- Deduplication: some language variants have the same text as the primary
  name (e.g., many European languages use "San Francisco" unchanged).
  Consider deduplicating in the name_index to avoid redundant trigram rows.

## Importance Scoring

Use the existing density/IDF mechanism, not a placetype-based ranking.

### `build-density.sh`

Add `wof` as a third source option. The WOF path reads lat/lon from the
GeoJSON files across all repos and counts S2 cells, same as FSQ/Overture
paths read from their source parquets.

```
./scripts/build-density.sh wof <wof_data_dir>
```

The density SQL for WOF:
```sql
INSTALL geography FROM community;
LOAD geography;

INSERT INTO cell_counts
SELECT
    14 AS level,
    s2_cell_parent(s2_cellfromlonlat(
        CAST(json_extract(properties, '$."geom:longitude"') AS DOUBLE),
        CAST(json_extract(properties, '$."geom:latitude"') AS DOUBLE)
    ), 14) AS cell_id,
    count(*) AS pt_count
FROM read_json('${wof_data_dir}/*/data/**/*.geojson',
    format='auto', maximum_object_size=104857600,
    filename=true, union_by_name=true)
WHERE filename NOT LIKE '%-alt-%'
  AND CAST(json_extract(properties, '$."mz:is_current"') AS INTEGER) = 1
  AND CAST(json_extract(properties, '$."geom:longitude"') AS DOUBLE) != 0
  AND CAST(json_extract(properties, '$."geom:latitude"') AS DOUBLE) != 0
GROUP BY cell_id;
```

Output: `db/density-wof-YYYY-MM.parquet` + `db/density-wof.parquet` symlink.

Note: reading all GeoJSON files for density is slower than reading parquets.
If this is too slow, consider a preprocessing step to create a lightweight
parquet of just lat/lon/placetype from the repo first.

### `build-idf.sh`

Add `wof` as a third source option. Uses `placetype` as the category.

```
./scripts/build-idf.sh wof <wof_data_dir>
```

The IDF SQL for WOF:
```sql
INSERT INTO category_idf
SELECT
    json_extract_string(properties, '$."wof:placetype"') AS category,
    count(*) AS n_places,
    ln(N.total::double / count(*)::double) AS idf_score
FROM read_json('${wof_data_dir}/*/data/**/*.geojson',
    format='auto', maximum_object_size=104857600,
    filename=true, union_by_name=true) src
CROSS JOIN (
    SELECT count(*) AS total
    FROM read_json('${wof_data_dir}/*/data/**/*.geojson',
        format='auto', maximum_object_size=104857600,
        filename=true, union_by_name=true)
    WHERE filename NOT LIKE '%-alt-%'
      AND CAST(json_extract(properties, '$."mz:is_current"') AS INTEGER) = 1
) N
WHERE filename NOT LIKE '%-alt-%'
  AND CAST(json_extract(properties, '$."mz:is_current"') AS INTEGER) = 1
GROUP BY category, N.total;
```

Output: `db/category_idf-wof-YYYY-MM.parquet` +
`db/category_idf-wof.parquet` symlink.

### Import script usage

Same formula as FSQ/Overture: `importance = round(60 * density + 40 * IDF)`.

```sql
ALTER TABLE places ADD COLUMN importance INTEGER DEFAULT 0;
-- (same density/IDF join pattern as FSQ/Overture import scripts,
--  using wof_id instead of fsq_place_id/id)
```

Requires the `geography` extension for `s2_cellfromlonlat`. The
density_file and idf_file are auto-built if missing, following the existing
pattern in the import scripts.

## Database Subclass: `WhosOnFirst`

Add to `garganorn/database.py` after the `OvertureMaps` class.

```python
class WhosOnFirst(Database):
    collection = "org.atgeo.places.whosonfirst"

    def record_columns(self):
        return """
            wof_id AS rkey,
            wof_id,
            name,
            latitude::decimal(10,6)::varchar AS latitude,
            longitude::decimal(10,6)::varchar AS longitude,
            placetype,
            country,
            parent_id,
            lastmodified
        """

    def search_columns(self):
        return """
            wof_id AS rkey,
            name,
            latitude::decimal(10,6)::varchar AS latitude,
            longitude::decimal(10,6)::varchar AS longitude,
            placetype,
            country
        """

    def query_record(self):
        columns = self.record_columns()
        return f"""
            SELECT {columns}
            FROM places
            WHERE wof_id = $rkey
        """

    def _query_trigram_text(self, params, trigrams):
        n_query = len(trigrams)
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        return f"""
            SELECT
                wof_id AS rkey,
                name,
                latitude,
                longitude,
                placetype,
                country,
                0 AS distance_m,
                count(DISTINCT trigram)::float
                    / (greatest(length(lower(strip_accents(name))) - 2, 1)
                       + {n_query}
                       - count(DISTINCT trigram))::float AS score
            FROM name_index
            WHERE trigram IN ({placeholders})
              AND importance >= $importance_floor
            GROUP BY wof_id, name, latitude, longitude,
                     placetype, country
            HAVING count(DISTINCT trigram)::float
                / (greatest(length(lower(strip_accents(name))) - 2, 1)
                   + {n_query}
                   - count(DISTINCT trigram))::float
                >= {self.JACCARD_THRESHOLD}
            ORDER BY score DESC, max(importance) DESC
            LIMIT $limit
        """

    def _query_trigram_spatial(self, params, trigrams):
        n_query = len(trigrams)
        placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))
        # Read lat/lon from name_index (VARCHAR) for consistency
        # with the text-only path. Join places only for geom + bbox.
        return f"""
            SELECT
                n.wof_id AS rkey,
                n.name,
                n.latitude,
                n.longitude,
                n.placetype,
                n.country,
                min(ST_Distance_Sphere(
                    p.geom, ST_GeomFromText($centroid)
                )::integer) AS distance_m,
                count(DISTINCT n.trigram)::float
                    / (greatest(
                           length(lower(strip_accents(n.name))) - 2, 1)
                       + {n_query}
                       - count(DISTINCT n.trigram))::float AS score
            FROM places p
            JOIN name_index n ON p.wof_id = n.wof_id
            WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
              AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
              AND n.trigram IN ({placeholders})
              AND n.importance >= $importance_floor
            GROUP BY n.wof_id, n.name, n.latitude, n.longitude,
                     n.placetype, n.country, p.geom
            ORDER BY score DESC, distance_m
            LIMIT $limit
        """

    def query_nearest(self, params, trigrams=None):
        assert "centroid" in params or "q" in params, \
            "Either centroid or q must be provided"

        has_spatial = "centroid" in params
        has_text = "q" in params

        if has_text and has_spatial:
            return self._query_trigram_spatial(params, trigrams)
        elif has_text:
            return self._query_trigram_text(params, trigrams)
        else:
            columns = self.search_columns()
            return f"""
                SELECT
                    {columns},
                    ST_Distance_Sphere(
                        geom, ST_GeomFromText($centroid)
                    )::integer AS distance_m
                FROM places
                WHERE bbox.xmin > $xmin AND bbox.ymin > $ymin
                  AND bbox.xmax < $xmax AND bbox.ymax < $ymax
                ORDER BY distance_m
                LIMIT $limit
            """

    def process_record(self, result):
        locations = [
            {
                "$type": "community.lexicon.location.geo",
                "latitude": result.pop("latitude"),
                "longitude": result.pop("longitude"),
            }
        ]

        country = result.pop("country", None)
        if country:
            locations.append({
                "$type": "community.lexicon.location.address",
                "country": country,
            })

        return {
            "$type": "org.atgeo.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations": locations,
            "names": [
                {"text": result.pop("name"), "priority": 0}
            ],
            "attributes": result,
        }
```

### Notes

- `process_record` needs updating for multilingual names. The sketch above
  handles the single-name case; the multi-name version will join the `names`
  table in `query_record` and build the full names array with lang tags.
- `_query_trigram_spatial` reads lat/lon from `name_index` (VARCHAR) for
  consistency with the text-only path.
- `country` is unconditionally popped so it never bleeds into `attributes`.
- Assert message matches existing backends.
- `lastmodified` ends up in `attributes` as a raw Unix timestamp integer.
- Remaining `attributes` after pops: `wof_id`, `placetype`, `parent_id`,
  `lastmodified` (for `query_record` path); `placetype` (for search paths).

## Config Registration

### `garganorn/config.py`

Add `WhosOnFirst` to imports and `DATABASE_TYPES`:

```python
from .database import FoursquareOSP, OvertureMaps, WhosOnFirst

DATABASE_TYPES = {
    "foursquare": FoursquareOSP,
    "overture": OvertureMaps,
    "whosonfirst": WhosOnFirst,
}
```

### `garganorn/__init__.py`

Add `WhosOnFirst` to exports:

```python
from .database import FoursquareOSP, OvertureMaps, WhosOnFirst
```

### Config YAML

```yaml
databases:
  - type: whosonfirst
    path: db/wof-places.duckdb
```

## Test Fixtures

### Test Data

San Francisco area WOF places, following the existing fixture pattern:

```python
WOF_PLACES = [
    # (wof_id, name, placetype, country, lat, lon, parent_id)
    ("85922583", "San Francisco", "locality", "US",
     37.7749, -122.4194, "85688637"),
    ("85688637", "California", "region", "US",
     37.2719, -119.2702, "85633793"),
    ("85633793", "United States", "country", "US",
     39.7837, -100.4458, "0"),
    ("85865945", "Mission District", "neighbourhood", "US",
     37.7599, -122.4148, "85922583"),
    ("85865939", "Noe Valley", "neighbourhood", "US",
     37.7502, -122.4337, "85922583"),
]

WOF_NAMES = [
    # (wof_id, lang, name_text, priority)
    ("85922583", "und", "San Francisco", 0),
    ("85922583", "eng", "San Francisco", 1),
    ("85922583", "zho", "旧金山", 1),
    ("85922583", "ara", "سان فرانسيسكو", 1),
    ("85688637", "und", "California", 0),
    ("85688637", "eng", "California", 1),
    # ... etc
]
```

Importance is computed via density/IDF in the test fixture, matching the
production import pipeline.

### `_create_wof_db()` Function

Creates DuckDB with:
- `places` table: `wof_id VARCHAR`, `name VARCHAR`, `placetype VARCHAR`,
  `country VARCHAR`, `latitude DOUBLE`, `longitude DOUBLE`,
  `geom GEOMETRY`, `parent_id VARCHAR`, `lastmodified INTEGER`,
  `bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)`,
  `importance INTEGER`
- `names` table: `wof_id VARCHAR`, `lang VARCHAR`, `name_text VARCHAR`,
  `priority INTEGER`
- `name_index` table: `trigram VARCHAR`, `wof_id VARCHAR`, `name VARCHAR`,
  `latitude VARCHAR`, `longitude VARCHAR`, `placetype VARCHAR`,
  `country VARCHAR`, `importance INTEGER`

Uses `_generate_trigrams()` helper from existing fixtures. Generates
trigrams for all name variants, not just primary names.

### Pytest Fixtures

- `wof_db_path` (session-scoped): creates temp DuckDB via `_create_wof_db`
- `wof_db` (function-scoped): creates `WhosOnFirst` instance, connects,
  yields, closes

### Test File: `tests/test_whosonfirst.py`

Following the pattern of `test_foursquare.py` and `test_overture.py`:

**Unit tests** (no DB):
- `test_query_nearest_spatial_only`
- `test_query_nearest_requires_centroid_or_q`
- `test_process_record_with_country`
- `test_process_record_no_country`

**Integration tests** (use `wof_db` fixture):
- `test_nearest_spatial`
- `test_nearest_text`
- `test_get_record_found`
- `test_get_record_not_found`
- `test_trigram_nearest_text_exact_match`
- `test_trigram_nearest_spatial_with_text`
- `test_trigram_nearest_unrelated_query`
- `test_multilingual_name_search` (search by non-English name variant)

**Config test** (in `test_config.py`):
- `test_whosonfirst_type_creates_whosonfirst`

## Risks and Open Questions

1. **DuckDB read_json performance with many small files**: A full-world
   import reads GeoJSON files from 261 repos (~15 GB). This may be slow.
   Mitigation: test with a single country repo first; if too slow, consider
   a preprocessing step to concatenate into NDJSON or convert to parquet.

2. **JSON key extraction with colons**: WOF property keys contain colons
   (`wof:name`, `name:eng_x_preferred`). DuckDB's `json_extract` should
   handle these with proper quoting (`'$."wof:name"'`), but needs
   validation.

3. **name_index size**: Places with many language variants (SF has 100+)
   generate many trigram rows. Monitor index size; consider deduplicating
   names with identical text across languages.

4. **Density/IDF from GeoJSON**: The `build-density.sh` and `build-idf.sh`
   scripts currently read parquets or S3 URLs. Adding a GeoJSON glob path
   is a new pattern. Performance may require a parquet preprocessing step.

5. **read_json schema inference**: With `union_by_name=true`, DuckDB will
   attempt to merge schemas across all files. If this causes issues with
   heterogeneous WOF records, may need explicit `columns` parameter or
   `json_format='records'`.

## Implementation Pipeline

1. Spike: clone `whosonfirst-data-admin-us`, test DuckDB `read_json` with
   glob pattern on the real data. Validate JSON extraction, bbox handling,
   name extraction SQL. This de-risks the major unknowns.
2. Update `build-density.sh` and `build-idf.sh` with `wof` source
3. Red: write failing tests (test fixtures + test file)
4. Green: implement `WhosOnFirst` class + config registration
5. Import script: `scripts/import-wof-extract.sh`
6. Final test verification: full suite green
7. Acceptance check
