# Name Variant Storage and Retrieval

Implementation spec for adding multilingual and variant name data to garganorn.

## 1. Schema Changes

### 1.1 `places` table: new `variants` column

All three source databases (OSM, Overture, FSQ) gain a `variants` column:

```sql
variants LIST(STRUCT(name VARCHAR, type VARCHAR, language VARCHAR))
```

Added to the `CREATE TABLE places` DDL in each import script. Default value is `[]` (empty list). Foursquare rows always store `[]` since the source has no variant data.

### 1.2 `name_index` table: new `is_variant` column

Each import script's name_index gains a boolean flag to distinguish primary names from variant names:

```sql
CREATE TABLE name_index (
    trigram     VARCHAR,
    rkey        VARCHAR,   -- or id/fsq_place_id per source
    name        VARCHAR,
    norm_name   VARCHAR,
    importance  INTEGER,
    is_variant  BOOLEAN DEFAULT FALSE
);
```

Primary name rows get `is_variant = FALSE`. Variant name rows get `is_variant = TRUE`. This allows ranking adjustments without changing the trigram join structure.

## 2. Lexicon Changes

In `garganorn/lexicon/place.json`, the `variant` def's `type` property description is updated to include `historical`:

```json
"type": {
    "type": "string",
    "description": "The kind of variant: official, alternate, short, colloquial, historical."
}
```

No structural change -- the field is a free-form string, not a `knownValues` enum. The description is the only thing that changes.

## 3. Import Pipeline Changes

### 3.1 OSM (`scripts/import-osm.sh`)

#### 3.1.1 osmium tags-filter: no change needed

The `osmium tags-filter` stage filters by category tags (`amenity`, `shop`, etc.), not by `name`. The name variant tags (`name:*`, `alt_name`, `old_name`, etc.) are already present on the filtered elements because osmium preserves all tags on matching elements. No filter changes required.

#### 3.1.2 Tag whitelist expansion

The `map_from_entries(list_filter(...))` clauses that build the `tags` column currently whitelist specific tag keys. Name variant tags are **not** needed in `tags` -- they go into the `variants` column instead. The tag whitelist stays unchanged.

#### 3.1.3 Extracting variants

After the importance scoring table rebuild (`CREATE TABLE places_scored ... ; DROP TABLE places; ALTER TABLE places_scored RENAME TO places`), variant extraction runs as a separate pass that reads the parquet files a second time.

**Variant extraction runs after the importance scoring table rebuild.**

This avoids adding/dropping a temporary column during the pipeline:

```sql
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

ALTER TABLE places ADD COLUMN variants
    LIST(STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)) DEFAULT [];

UPDATE places p SET variants = coalesce(
    (SELECT rv.variants FROM raw_variants rv WHERE rv.rkey = p.rkey),
    []
);
DROP TABLE raw_variants;
```

This reads parquet twice but avoids schema changes to the places table during the pipeline, and the parquet files are cached locally. The second read only extracts MAP entries, which is lightweight.

### 3.2 Overture (`scripts/import-overture-extract.sh`)

The Overture places schema has:
- `names.primary` -- VARCHAR, already used
- `names.common` -- MAP(VARCHAR, VARCHAR) mapping language codes to names
- `names.rules` -- LIST of STRUCT with fields for variant/value/language

**Variant extraction runs after the importance scoring table rebuild** (`CREATE TABLE places_scored ... ; DROP TABLE places; ALTER TABLE places_scored RENAME TO places`).

#### 3.2.1 Extracting `names.common` and `names.rules`

```sql
CREATE TEMP TABLE overture_variants AS
WITH common_entries AS (
    SELECT id,
        e.key AS language,
        e."value" AS name
    FROM places,
         unnest(map_entries(names.common)) AS e
    WHERE names.common IS NOT NULL
),
rule_entries AS (
    SELECT id,
        r.language,
        r."value" AS name,
        CASE r.variant
            WHEN 'common'     THEN 'alternate'
            WHEN 'official'   THEN 'official'
            WHEN 'alternate'  THEN 'alternate'
            WHEN 'short'      THEN 'short'
            ELSE 'alternate'
        END AS type
    FROM places,
         unnest(names.rules) AS r
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

ALTER TABLE places ADD COLUMN variants
    LIST(STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)) DEFAULT [];

UPDATE places p SET variants = coalesce(
    (SELECT ov.variants FROM overture_variants ov WHERE ov.id = p.id),
    []
);
DROP TABLE overture_variants;
```

Note: `r."value"` is quoted to avoid conflict with DuckDB's `VALUE` keyword. The Overture `names.rules` struct may use variant names that don't map 1:1 to garganorn types. The CASE above maps known Overture variants; anything else falls back to `alternate`.

#### 3.2.2 Schema initialization

The Overture import currently creates the places table with `CREATE TABLE places AS SELECT * FROM '...' LIMIT 0`, which inherits the Overture schema. The `variants` column must be added after table creation since it's a garganorn-specific column, not present in the source data. The `ALTER TABLE ... ADD COLUMN` approach above handles this.

### 3.3 Foursquare (`scripts/import-fsq-extract.sh`)

No source variant data. **Add the column after the importance scoring table rebuild** (`CREATE TABLE places_scored ... ; DROP TABLE places; ALTER TABLE places_scored RENAME TO places`):

```sql
ALTER TABLE places ADD COLUMN variants
    LIST(STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)) DEFAULT [];
```

The column will contain `[]` for every row.

## 4. Database Layer Changes

### 4.1 `Database.process_record` (base class)

Currently returns `"variants": []`. Change to populate from a `variants` column in the result if present:

```python
def process_record(self, result):
    raw_variants = result.pop("variants", None)
    variants = []
    if raw_variants:
        for v in raw_variants:
            entry = {"name": v["name"]}
            if v.get("type"):
                entry["type"] = v["type"]
            if v.get("language"):
                entry["language"] = v["language"]
            variants.append(entry)
    return {
        "$type": "org.atgeo.place",
        "collection": self.collection,
        "rkey": result.pop("rkey"),
        "locations": [...],
        "name": result.pop("name"),
        "variants": variants,
        "attributes": result,
    }
```

### 4.2 Subclass `record_columns()`

Each subclass's `record_columns()` must include `variants` in the SELECT list:

- **OSM**: Add `variants` to the columns string.
- **Overture**: Add `variants` to the columns string.
- **Foursquare**: Add `variants` to the columns string.

### 4.3 Subclass `process_record()` overrides

Each of the three subclasses overrides `process_record()` and currently hard-codes `"variants": []`. The fix is to pop `variants` from `result` **before** the `"attributes": result` line in each override.

**`FoursquareOSP.process_record()`** — current return:
```python
return {
    "$type": "org.atgeo.place",
    "collection": self.collection,
    "rkey": result.pop("rkey"),
    "locations": locations,
    "name": result.pop("name"),
    "variants": [],
    "attributes": result
}
```
Replace with:
```python
variants = result.pop("variants", []) or []
return {
    "$type": "org.atgeo.place",
    "collection": self.collection,
    "rkey": result.pop("rkey"),
    "locations": locations,
    "name": result.pop("name"),
    "variants": variants,
    "attributes": result
}
```

**`OvertureMaps.process_record()`** — current return:
```python
return {
    "$type": "org.atgeo.place",
    "collection": self.collection,
    "rkey": result.pop("rkey"),
    "locations": locations,
    "name": result.pop("name"),
    "variants": [],
    "attributes": result
}
```
Replace with:
```python
variants = result.pop("variants", []) or []
return {
    "$type": "org.atgeo.place",
    "collection": self.collection,
    "rkey": result.pop("rkey"),
    "locations": locations,
    "name": result.pop("name"),
    "variants": variants,
    "attributes": result
}
```

**`OpenStreetMap.process_record()`** — current return:
```python
return {
    "$type": "org.atgeo.place",
    "collection": self.collection,
    "rkey": self.expand_rkey(result.pop("rkey")),
    "locations": locations,
    "name": result.pop("name"),
    "variants": [],
    "attributes": tag_dict
}
```
Replace with (note: `variants` must be popped from `result`, not `tag_dict`, since it comes from the SQL result before `tags` is unpacked):
```python
variants = result.pop("variants", []) or []
return {
    "$type": "org.atgeo.place",
    "collection": self.collection,
    "rkey": self.expand_rkey(result.pop("rkey")),
    "locations": locations,
    "name": result.pop("name"),
    "variants": variants,
    "attributes": tag_dict
}
```

## 5. Search Impact

### 5.1 Name index population

Each import script's name_index build stage currently indexes only the primary name. Add a second INSERT that indexes variant names. The ID column name differs per source.

**OSM** (uses `rkey`):
```sql
INSERT INTO name_index
WITH variant_names AS (
    SELECT rkey,
           v.name,
           lower(strip_accents(v.name)) AS norm_name,
           importance,
           TRUE AS is_variant
    FROM places,
         unnest(variants) AS v
    WHERE v.name IS NOT NULL AND length(v.name) >= 3
)
SELECT substr(vn.norm_name, pos, 3) AS trigram,
       vn.rkey,
       vn.name,
       vn.norm_name,
       vn.importance,
       vn.is_variant
FROM variant_names vn
CROSS JOIN generate_series(1, length(vn.norm_name) - 2) AS gs(pos);
```

**Overture** (uses `id`):
```sql
INSERT INTO name_index
WITH variant_names AS (
    SELECT id,
           v.name,
           lower(strip_accents(v.name)) AS norm_name,
           importance,
           TRUE AS is_variant
    FROM places,
         unnest(variants) AS v
    WHERE v.name IS NOT NULL AND length(v.name) >= 3
)
SELECT substr(vn.norm_name, pos, 3) AS trigram,
       vn.id,
       vn.name,
       vn.norm_name,
       vn.importance,
       vn.is_variant
FROM variant_names vn
CROSS JOIN generate_series(1, length(vn.norm_name) - 2) AS gs(pos);
```

**FSQ** (uses `fsq_place_id`): No variant data; no INSERT needed.

The existing primary name INSERT in each script needs `FALSE AS is_variant` added.

### 5.2 Ranking considerations

When a search matches a variant name rather than the primary name, two questions arise:

1. **Should variant matches rank lower?** Probably not by default -- a user searching "Koln" should find "Cologne" (primary) via the "Koln" variant at the same rank. The `is_variant` flag enables a future penalty if needed but the initial implementation should treat them equally.

2. **Which name to display?** The search returns `name` from name_index, which for variant matches will be the variant name, not the primary. The hydration step then fetches the full record including primary name. The API response includes both `name` (primary) and `variants` (all). Clients can decide what to display. No change to the search query structure needed.

3. **Deduplication**: A place may now appear multiple times in search results (once for primary, once for a variant). The existing `SELECT DISTINCT rkey` in the candidates CTE handles this -- it deduplicates by place ID. However, `norm_name` will differ between primary and variant matches for the same place, which means JW scoring may pick one over the other. The current behavior (picking whichever norm_name DuckDB returns for the DISTINCT) is acceptable for now. A future improvement could take the max JW score across all matching names for a given rkey.

### 5.3 Index size impact

Variant names will increase name_index size. Estimated growth:
- OSM: Most POIs have 0-2 `name:*` tags. Expect 2-3x name_index growth.
- Overture: `names.common` often has 5-20 language entries for significant places but 0 for most POIs. Expect 1.5-2x growth.
- FSQ: No change.

The trigram-sorted zone map structure remains effective. The `ORDER BY trigram` sort step will take proportionally longer. No structural changes needed.

## 6. Migration

**Re-import required.** The `variants` column must be populated from source data (raw OSM tags, Overture names struct) that is not preserved in the current database. There is no way to retroactively populate it from the existing places table.

This is consistent with the project's current approach: each import script produces a fresh `.duckdb` file, and the server reads it in read-only mode. A re-import with the updated scripts is the migration path.

## 7. Implementation Order

1. Lexicon: Update `place.json` variant type description.
2. OSM import: Add `variants` column, extract from tags, index in name_index.
3. Overture import: Add `variants` column, extract from names struct, index in name_index.
4. FSQ import: Add empty `variants` column.
5. Database layer: Update `record_columns()` and `process_record()` in all subclasses.
6. Tests: Verify variant extraction for OSM and Overture, verify API response shape, verify search finds variant names.

Steps 2-4 are independent and can be parallelized.
