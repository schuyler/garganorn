# name_index Import Design: Shell Script Modifications

## Overview

This document specifies modifications to `import-fsq-extract.sh` and
`import-overture-extract.sh` to build a `name_index` table after the rtree
index. The `name_index` supports text-only search queries ranked by an
importance score derived from S2 cell density data.

Each script has two code paths:
- **With density**: when `cell_counts.parquet` is present, load the
  `geography` extension and join against density data to compute importance.
- **Fallback**: when the file is absent, set `importance = 0` for all rows.

The density file path is detected at shell level and interpolated as an
absolute path into the generated SQL.

### Spec inconsistency note

The spec overview (line 14) states that the `geography` extension "is required
only during the density build, not during place import." However, the
with-density join condition uses `s2_cellfromlonlat` and `s2_cell_parent`,
which are `geography` extension functions. The "Consuming" section (lines
168-169) correctly states that `geography` is required at import time for these
calls. The overview statement is wrong; `geography` is required during place
import when density data is present. The fallback path (no density file) does
not require `geography`.

## Shell-Level Density File Detection

Both scripts use the same detection logic. The density file is resolved to an
absolute path to avoid DuckDB working-directory ambiguity.

```bash
# Density file detection — insert after output_dir is set, before SQL generation
density_file=""
for candidate in "${output_dir}/cell_counts.parquet" "${output_dir}/../cell_counts.parquet"; do
    if [ -f "$candidate" ]; then
        density_file="$(realpath "$candidate")"
        echo "Density file found: ${density_file}"
        break
    fi
done
if [ -z "$density_file" ]; then
    echo "No density file found. name_index will use importance = 0."
fi
```

## Foursquare OSP: name_index SQL

Append to the generated SQL file after the rtree index creation.

### FSQ with density

```bash
if [ -n "$density_file" ]; then
cat >> "${output_dir}/import.sql" <<EOF

.print "Loading geography extension..."
INSTALL geography FROM community;
LOAD geography;

.print "Building name_index with density..."
CREATE TABLE name_index AS
SELECT
    token,
    p.fsq_place_id,
    p.name,
    p.latitude,
    p.longitude,
    p.address,
    p.locality,
    p.postcode,
    p.region,
    p.country,
    coalesce(ln(1 + c.pt_count), 0) AS importance
FROM (
    SELECT
        unnest(string_split(lower(strip_accents(name)), ' ')) AS token,
        fsq_place_id,
        name,
        latitude::decimal(10,6)::varchar AS latitude,
        longitude::decimal(10,6)::varchar AS longitude,
        latitude AS lat_raw,
        longitude AS lon_raw,
        address, locality, postcode, region, country
    FROM places
    WHERE name IS NOT NULL AND length(name) > 0
) p
LEFT JOIN read_parquet('${density_file}') c
    ON c.level = 12
    AND c.cell_id = s2_cell_parent(
        s2_cellfromlonlat(p.lon_raw, p.lat_raw), 12
    )
WHERE length(p.token) > 1
ORDER BY token, importance DESC;

.print "Creating name_index token index..."
CREATE INDEX name_index_token ON name_index (token);

.print "Analyzing..."
ANALYZE;
EOF
```

**Notes:**
- `lat_raw` / `lon_raw` are the original numeric columns, kept in the
  subquery for the `s2_cellfromlonlat` join but not selected into the final
  table.
- `latitude` and `longitude` in the output are cast to `decimal(10,6)::varchar`
  to match the format used in `database.py` queries.
- `read_parquet('${density_file}')` uses the shell-interpolated absolute path.

### FSQ fallback

```bash
else
cat >> "${output_dir}/import.sql" <<EOF

.print "Building name_index (no density data)..."
CREATE TABLE name_index AS
SELECT
    token,
    fsq_place_id,
    name,
    latitude,
    longitude,
    address,
    locality,
    postcode,
    region,
    country,
    0 AS importance
FROM (
    SELECT
        unnest(string_split(lower(strip_accents(name)), ' ')) AS token,
        fsq_place_id,
        name,
        latitude::decimal(10,6)::varchar AS latitude,
        longitude::decimal(10,6)::varchar AS longitude,
        address, locality, postcode, region, country
    FROM places
    WHERE name IS NOT NULL AND length(name) > 0
) sub
WHERE length(token) > 1
ORDER BY token, importance DESC;

.print "Creating name_index token index..."
CREATE INDEX name_index_token ON name_index (token);

.print "Analyzing..."
ANALYZE;
EOF
fi
```

## Overture Maps: name_index SQL

Append to the generated SQL file after the rtree index creation.

### Overture with density

```bash
if [ -n "$density_file" ]; then
cat >> "${output_dir}/import-overture.sql" <<EOF

.print "Loading geography extension..."
INSTALL geography FROM community;
LOAD geography;

.print "Building name_index with density..."
CREATE TABLE name_index AS
SELECT
    token,
    p.id,
    p.name,
    p.latitude,
    p.longitude,
    p.importance
FROM (
    SELECT
        sub.token,
        sub.id,
        sub.name,
        sub.latitude,
        sub.longitude,
        sub.lat_num,
        sub.lon_num,
        coalesce(ln(1 + c.pt_count), 0) AS importance
    FROM (
        SELECT
            unnest(string_split(lower(strip_accents(names.primary)), ' ')) AS token,
            id,
            names.primary AS name,
            st_y(st_centroid(geometry))::decimal(10,6)::varchar AS latitude,
            st_x(st_centroid(geometry))::decimal(10,6)::varchar AS longitude,
            st_y(st_centroid(geometry)) AS lat_num,
            st_x(st_centroid(geometry)) AS lon_num
        FROM places
        WHERE names.primary IS NOT NULL AND length(names.primary) > 0
    ) sub
    LEFT JOIN read_parquet('${density_file}') c
        ON c.level = 12
        AND c.cell_id = s2_cell_parent(
            s2_cellfromlonlat(sub.lon_num, sub.lat_num), 12
        )
    WHERE length(sub.token) > 1
) p
ORDER BY p.token, p.importance DESC;

.print "Creating name_index token index..."
CREATE INDEX name_index_token ON name_index (token);

.print "Analyzing..."
ANALYZE;
EOF
```

**Notes:**
- Overture places have `geometry` (not separate lat/lon columns) and
  `names.primary` (not `name`).
- `lat_num` / `lon_num` are the raw numeric centroid coordinates used for the
  S2 join. They are not carried into the final table.
- `latitude` and `longitude` in the output are `decimal(10,6)::varchar`,
  matching the format in `OvertureMaps.record_columns()` in `database.py`.
- The Overture `name_index` does not include address columns because Overture
  addresses are structured (nested `addresses` array), not flat strings. The
  text-only search query selects only `id AS rkey, name, latitude, longitude`.

### Overture fallback

```bash
else
cat >> "${output_dir}/import-overture.sql" <<EOF

.print "Building name_index (no density data)..."
CREATE TABLE name_index AS
SELECT
    token,
    id,
    name,
    latitude,
    longitude,
    0 AS importance
FROM (
    SELECT
        unnest(string_split(lower(strip_accents(names.primary)), ' ')) AS token,
        id,
        names.primary AS name,
        st_y(st_centroid(geometry))::decimal(10,6)::varchar AS latitude,
        st_x(st_centroid(geometry))::decimal(10,6)::varchar AS longitude
    FROM places
    WHERE names.primary IS NOT NULL AND length(names.primary) > 0
) sub
WHERE length(token) > 1
ORDER BY token, importance DESC;

.print "Creating name_index token index..."
CREATE INDEX name_index_token ON name_index (token);

.print "Analyzing..."
ANALYZE;
EOF
fi
```

## name_index Schema Summary

### Foursquare

| Column         | Type    | Source                                   |
|----------------|---------|------------------------------------------|
| token          | VARCHAR | `lower(strip_accents(name))` split words |
| fsq_place_id   | VARCHAR | `places.fsq_place_id`                   |
| name           | VARCHAR | `places.name`                            |
| latitude       | VARCHAR | `latitude::decimal(10,6)::varchar`       |
| longitude      | VARCHAR | `longitude::decimal(10,6)::varchar`      |
| address        | VARCHAR | `places.address`                         |
| locality       | VARCHAR | `places.locality`                        |
| postcode       | VARCHAR | `places.postcode`                        |
| region         | VARCHAR | `places.region`                          |
| country        | VARCHAR | `places.country`                         |
| importance     | DOUBLE  | `ln(1 + pt_count)` or `0`               |

### Overture

| Column     | Type    | Source                                           |
|------------|---------|--------------------------------------------------|
| token      | VARCHAR | `lower(strip_accents(names.primary))` split words|
| id         | VARCHAR | `places.id`                                      |
| name       | VARCHAR | `places.names.primary`                           |
| latitude   | VARCHAR | `st_y(st_centroid(geometry))::decimal(10,6)::varchar` |
| longitude  | VARCHAR | `st_x(st_centroid(geometry))::decimal(10,6)::varchar` |
| importance | DOUBLE  | `ln(1 + pt_count)` or `0`                        |

## Integration into Existing Scripts

### import-fsq-extract.sh

Insert the density detection block after line 62 (`mkdir -p "$output_dir"`).
Insert the name_index SQL generation (the `if/else/fi` block above) after
line 93 (after the rtree index `cat >> ...` block), before the `time duckdb`
invocation.

### import-overture-extract.sh

Insert the density detection block after line 54 (`mkdir -p "$output_dir"`).
Insert the name_index SQL generation (the `if/else/fi` block above) after
line 114 (after the rtree index `cat >> ...` block), before the `time duckdb`
invocation.

### Execution order within the generated SQL

Both scripts produce a single SQL file that DuckDB executes sequentially:

```
1. INSTALL/LOAD spatial
2. CREATE TABLE places ...
3. INSERT INTO places ... (per parquet file)
4. DELETE invalid rows
5. CREATE INDEX places_rtree ...
6. [if density] INSTALL/LOAD geography
7. CREATE TABLE name_index ...
8. CREATE INDEX name_index_token ...
9. ANALYZE
```

## Query Pattern (for reference)

The `name_index` is queried from `database.py`. The query pattern from the
spec, adapted per dataset:

### Foursquare

```sql
SELECT fsq_place_id AS rkey, name, latitude, longitude,
       address, locality, postcode, region, country,
       0 AS distance_m
FROM name_index
WHERE token = lower(strip_accents($token))
  AND name ILIKE '%' || $q || '%'
ORDER BY importance DESC
LIMIT $limit;
```

### Overture

```sql
SELECT id AS rkey, name, latitude, longitude,
       0 AS distance_m
FROM name_index
WHERE token = lower(strip_accents($token))
  AND name ILIKE '%' || $q || '%'
ORDER BY importance DESC
LIMIT $limit;
```
