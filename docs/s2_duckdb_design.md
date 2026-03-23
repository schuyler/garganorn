# S2 Cell Density Table Design for Garganorn

## Overview

This document describes two pre-built artifacts used to compute an
"importance" score for each place record, used to rank text-only search
results in the `name_index` table:

1. A **density table** that pre-aggregates place counts into S2 cells at
   multiple resolution levels. Places in denser neighborhoods score higher.

2. A **category IDF table** that assigns an inverse-frequency score to each
   place category. Places with rarer categories (e.g. cities, airports)
   score higher than places with common categories (e.g. restaurants,
   coffee shops).

Both tables are built as **standalone artifacts**, separate from and
independent of the place import pipeline. Global density patterns and
category distributions change very slowly (annually at most), while places
themselves are refreshed much more frequently. Decoupling the builds avoids
unnecessary rebuilds and removes the `geography` extension as a hard
dependency of the import scripts.

The density build produces a single Parquet file (`cell_counts.parquet`)
and the category IDF build produces `category_idf.parquet`. Place import
scripts attach these read-only when building the `name_index`. If either
file is not available, import falls back gracefully — missing density
contributes 0, missing IDF contributes 0.

The density build uses the
[`geography`](https://duckdb.org/community_extensions/extensions/geography)
community extension for DuckDB, which wraps Google's S2 geometry library and
provides native `S2_CELL` and `S2_CELL_CENTER` types backed by 64-bit
unsigned integers. This extension is required only during the density build
and during place import (for the density join), not for the IDF build.

## S2 Level Reference

Cell sizes vary by latitude and orientation. The values below are from the
[official S2 Cell Statistics](https://s2geometry.io/resources/s2cell_statistics.html).

| Level | Avg Area | Edge Length (typical) | Global Cell Count |
|------:|---------:|----------------------:|------------------:|
| 6     | 20,755 km² | 108–156 km         | 24 K              |
| 7     | 5,189 km²  | 54–78 km           | 98 K              |
| 8     | 1,297 km²  | 27–39 km           | 393 K             |
| 9     | 324 km²    | 14–20 km           | 1.6 M             |
| 10    | 81 km²     | 7–10 km            | 6 M               |
| 11    | 20 km²     | 3–5 km             | 25 M              |
| 12    | 5 km²      | 1.7–2 km           | 100 M             |
| 13    | 1.27 km²   | 850 m–1.2 km       | 402 M             |
| 14    | 0.32 km²   | 425–613 m          | 1.6 B             |

Only cells that actually contain places are stored.

## Density Table Schema

```sql
INSTALL geography FROM community;
LOAD geography;

CREATE TABLE cell_counts (
    level    TINYINT NOT NULL,  -- S2 level (6–14)
    cell_id  UBIGINT NOT NULL,  -- S2_CELL at the given level
    pt_count UBIGINT NOT NULL   -- number of places in this cell
);
```

### Sort Order

The table **must** be sorted by `(level, cell_id)` for efficient lookups.
DuckDB's columnar storage uses zonemaps (min/max per row group) to skip
irrelevant blocks during scans. With sorted data, a filter on `level` and
`cell_id` resolves to a small sequential read.

## Storage Estimate

S2 uses an aperture-4 quadtree: each coarser level has ~4× fewer distinct
cells. The total across all 9 levels (14 down to 6) is:

```
N × (1 + 1/4 + 1/16 + ... + 1/4^8) ≈ N × 1.333
```

This is ~33% overhead relative to storing only the finest level. Each row is
18 bytes of payload (1 + 8 + 8, before compression). For a dataset with 10M
occupied level-14 cells, the full table is ~13.3M rows — trivially small.

## Density Build (Standalone Process)

The density build is a self-contained script that reads from any global
places dataset and produces `cell_counts.parquet`. It runs independently of
and less frequently than the place import pipeline.

### Source Data

The build can use any global places dataset. The Foursquare OSP and Overture
Maps datasets both work; the resulting density patterns are nearly identical
since both cover the same populated areas. Use whichever is most convenient.

### Step 1: Aggregate places at the finest level

For the Foursquare OSP dataset, places have `longitude` and `latitude`
columns:

```sql
INSERT INTO cell_counts
SELECT
    14 AS level,
    s2_cell_parent(s2_cellfromlonlat(longitude, latitude), 14) AS cell_id,
    count(*) AS pt_count
FROM places
WHERE longitude != 0 AND latitude != 0
GROUP BY cell_id;
```

For the Overture Maps dataset, places have a `geometry` column:

```sql
INSERT INTO cell_counts
SELECT
    14 AS level,
    s2_cell_parent(
        s2_cellfromlonlat(
            st_x(st_centroid(geometry)),
            st_y(st_centroid(geometry))
        ), 14
    ) AS cell_id,
    count(*) AS pt_count
FROM places
WHERE geometry IS NOT NULL
GROUP BY cell_id;
```

### Step 2: Cascade upward, one level at a time

Each step reads from the level below and groups ~4 rows per output row.

```sql
-- Repeat for each level from 13 down to 6
INSERT INTO cell_counts
SELECT
    :parent_level AS level,
    s2_cell_parent(cell_id, :parent_level) AS cell_id,
    sum(pt_count) AS pt_count
FROM cell_counts
WHERE level = :child_level
GROUP BY cell_id;
```

Where `:child_level` is the level just built and `:parent_level` is one
less. The cascade from 13 down to 6 requires 8 INSERT statements.

### Step 3: Export as sorted Parquet

```sql
COPY (
    SELECT * FROM cell_counts ORDER BY level, cell_id
) TO 'cell_counts.parquet' (FORMAT PARQUET);
```

The output file is the sole artifact of the density build. It is versioned
by date (e.g. `cell_counts-2025-06.parquet`) and stored alongside or
distributed with the place databases.

### Build Frequency

Annually, or whenever a major new places dataset is adopted. Global density
patterns (which cities are dense, which areas are sparse) change on
timescales of years to decades. Rebuilding more often than annually provides
negligible benefit.

## Category IDF Table

### Concept

Inverse Document Frequency (IDF) applied to place categories. If N is the
total number of places and n_c is the number of places with category c,
then `ln(N / n_c)` gives a high score to rare categories and a low score
to common ones.

This captures the intuition that a city, airport, or embassy is more
"important" as a search result than one of millions of coffee shops — even
if they happen to share a name.

### Schema

```sql
CREATE TABLE category_idf (
    collection VARCHAR NOT NULL,  -- dataset identifier (e.g. 'foursquare', 'overture')
    category   VARCHAR NOT NULL,  -- category ID (Foursquare) or primary name (Overture)
    n_places   UBIGINT NOT NULL,  -- count of places with this category
    idf_score  DOUBLE  NOT NULL   -- ln(N / n_places)
);
```

### Sort Order

Sorted by `(collection, category)` for zonemap efficiency.

### Storage Estimate

Foursquare has ~1,000 category IDs; Overture has ~2,000 category values.
The full table is ~3,000 rows — negligible.

### Category IDF Build (Standalone Process)

The IDF build reads from the same global places datasets as the density
build and produces `category_idf.parquet`. It can run as part of the same
build pipeline or independently.

#### Source Data

Unlike the density build, the IDF build is dataset-specific — Foursquare
and Overture use completely different category systems, so both must be
processed. The `collection` column distinguishes them.

#### Foursquare OSP

Foursquare places have an `fsq_category_ids` column containing an array of
category ID strings. A place may belong to multiple categories. We unnest
the array so that each category is counted independently.

```sql
INSERT INTO category_idf
SELECT
    'foursquare' AS collection,
    category,
    count(*) AS n_places,
    ln(N.total::double / count(*)::double) AS idf_score
FROM (
    SELECT unnest(fsq_category_ids) AS category
    FROM places
    WHERE fsq_category_ids IS NOT NULL
) cats
CROSS JOIN (
    SELECT count(*) AS total FROM places
) N
GROUP BY category, N.total;
```

#### Overture Maps

Overture places have a `categories.primary` field (a single string) and an
optional `categories.alternate` array. We use the primary category for the
IDF calculation, since it's always present and represents the most specific
classification.

```sql
INSERT INTO category_idf
SELECT
    'overture' AS collection,
    categories.primary AS category,
    count(*) AS n_places,
    ln(N.total::double / count(*)::double) AS idf_score
FROM places
CROSS JOIN (
    SELECT count(*) AS total FROM places
    WHERE categories.primary IS NOT NULL
) N
WHERE categories.primary IS NOT NULL
GROUP BY categories.primary, N.total;
```

#### Export as sorted Parquet

```sql
COPY (
    SELECT * FROM category_idf ORDER BY collection, category
) TO 'category_idf.parquet' (FORMAT PARQUET);
```

### Multi-Category Places

When a place has multiple categories (Foursquare's `fsq_category_ids`
array, Overture's `categories.alternate`), the **maximum IDF** across all
of the place's categories is used as its category importance. A place
tagged as both "Restaurant" (common, low IDF) and "Historic Landmark"
(rare, high IDF) gets the higher score.

This is handled during the name_index join at import time, not in the IDF
table itself.

### Build Frequency

Less often than density — category distributions are essentially static
unless the taxonomy itself changes. Rebuilding when adopting a new dataset
release is sufficient.

## Importance Scoring

Place importance combines two independent signals:

1. **Density** (`density_score`): `ln(1 + pt_count)` from the S2 level-12
   cell, capturing neighborhood-scale density. A place in a dense urban
   area ranks higher than the same name in a sparse rural area.

2. **Category IDF** (`idf_score`): `ln(N / n_category)` from the category
   IDF table, capturing type rarity. A city or airport ranks higher than a
   coffee shop.

### Combined formula

```sql
importance = coalesce(ln(1 + c.pt_count), 0)
           + coalesce(max(idf.idf_score), 0)
```

This is additive. Both signals are log-scaled and contribute independently.
The mathematical equivalent is:

```
ln((1 + pt_count) × N / n_category)
```

which can be read as: "how dense is this neighborhood, times how rare is
this place type."

### Range analysis

| Signal   | Minimum | Typical range | Maximum  |
|----------|--------:|--------------:|---------:|
| Density  | 0       | 0.7 – 7.0    | ~9.2     |
| IDF      | ~2.3    | 4.0 – 9.0    | ~11.5    |
| Combined | ~2.3    | 5.0 – 15.0   | ~20.7    |

The density minimum of 0 occurs when the density file is absent or a cell
has no places (shouldn't happen in practice). The IDF minimum of ~2.3
corresponds to the most common category (~10% of the dataset). The IDF
maximum of ~11.5 corresponds to a category with ~10 places in a 100M-place
dataset.

The two signals have comparable ranges, so equal weighting (α=β=1) is a
reasonable starting point. If tuning is needed, the formula becomes
`α × density + β × idf`, but there's no reason to introduce that
complexity until search result quality demands it.

### Why level 12 for density?

At ~2 km / ~5 km², level 12 captures neighborhood-scale density. Finer
levels (14) would show little differentiation in dense areas (most cells
have 1–3 places). Coarser levels (9–10) would blur the distinction between
neighborhoods within the same city.

### Why max IDF for multi-category places?

A place's most distinctive category is the strongest signal of its
importance. Taking the max avoids diluting the score by averaging in common
co-categories (e.g. a "Historic Landmark" that's also tagged as a "Tourist
Attraction" shouldn't be penalized for the latter being common).

## Consuming Pre-built Artifacts During Place Import

Place import scripts use the density and IDF tables to assign importance
scores when building the `name_index`. Both files are attached as read-only
Parquet sources. The `geography` extension is required at import time for
the density join's `s2_cellfromlonlat` / `s2_cell_parent` calls.

### Joining importance into name_index (Foursquare)

```sql
CREATE TABLE name_index AS
SELECT
    token,
    p.fsq_place_id,
    p.name,
    p.latitude::decimal(10,6)::varchar AS latitude,
    p.longitude::decimal(10,6)::varchar AS longitude,
    p.address, p.locality, p.postcode, p.region, p.country,
    coalesce(ln(1 + c.pt_count), 0)
        + coalesce(p.max_idf, 0) AS importance
FROM (
    SELECT
        unnest(string_split(lower(strip_accents(name)), ' ')) AS token,
        fsq_place_id, name, latitude, longitude,
        address, locality, postcode, region, country,
        max_idf
    FROM (
        SELECT
            p.*,
            max(idf.idf_score) AS max_idf
        FROM places p
        LEFT JOIN read_parquet('category_idf.parquet') idf
            ON idf.collection = 'foursquare'
            AND idf.category = unnest(p.fsq_category_ids)
        WHERE p.name IS NOT NULL AND length(p.name) > 0
        GROUP BY ALL
    )
) p
LEFT JOIN read_parquet('cell_counts.parquet') c
    ON c.level = 12
    AND c.cell_id = s2_cell_parent(
        s2_cellfromlonlat(p.longitude, p.latitude), 12
    )
WHERE length(p.token) > 1
ORDER BY token, importance DESC;
```

**Note:** The IDF join unnests `fsq_category_ids` and takes the max score
per place, then the density join adds the spatial component. Both are LEFT
JOINs so missing data contributes 0 via `coalesce`.

**Implementation warning:** The inner subquery uses
`unnest(p.fsq_category_ids)` inside a LEFT JOIN with `GROUP BY ALL`, which
is valid DuckDB syntax but may not be optimized well by the query planner
on large datasets. If this query is slow or produces unexpected results,
replace the nested subquery with a CTE that pre-computes the max IDF per
place before the main join:

```sql
CREATE TABLE name_index AS
WITH place_idf AS (
    SELECT
        p.fsq_place_id,
        max(idf.idf_score) AS max_idf
    FROM places p,
        unnest(p.fsq_category_ids) AS t(category)
    LEFT JOIN read_parquet('category_idf.parquet') idf
        ON idf.collection = 'foursquare'
        AND idf.category = t.category
    WHERE p.fsq_category_ids IS NOT NULL
    GROUP BY p.fsq_place_id
)
SELECT
    token,
    p.fsq_place_id,
    p.name,
    p.latitude::decimal(10,6)::varchar AS latitude,
    p.longitude::decimal(10,6)::varchar AS longitude,
    p.address, p.locality, p.postcode, p.region, p.country,
    coalesce(ln(1 + c.pt_count), 0)
        + coalesce(pi.max_idf, 0) AS importance
FROM (
    SELECT
        unnest(string_split(lower(strip_accents(name)), ' ')) AS token,
        fsq_place_id, name, latitude, longitude,
        address, locality, postcode, region, country
    FROM places
    WHERE name IS NOT NULL AND length(name) > 0
) p
LEFT JOIN place_idf pi USING (fsq_place_id)
LEFT JOIN read_parquet('cell_counts.parquet') c
    ON c.level = 12
    AND c.cell_id = s2_cell_parent(
        s2_cellfromlonlat(p.longitude, p.latitude), 12
    )
WHERE length(p.token) > 1
ORDER BY token, importance DESC;
```

Test both variants against real data and use whichever performs better.

### Joining importance into name_index (Overture Maps)

```sql
CREATE TABLE name_index AS
SELECT
    token,
    p.id AS rkey,
    p.name,
    st_y(st_centroid(p.geometry))::decimal(10,6)::varchar AS latitude,
    st_x(st_centroid(p.geometry))::decimal(10,6)::varchar AS longitude,
    coalesce(ln(1 + c.pt_count), 0)
        + coalesce(idf.idf_score, 0) AS importance
FROM (
    SELECT
        unnest(string_split(lower(strip_accents(names.primary)), ' ')) AS token,
        *
    FROM places
    WHERE names.primary IS NOT NULL AND length(names.primary) > 0
) p
LEFT JOIN read_parquet('category_idf.parquet') idf
    ON idf.collection = 'overture'
    AND idf.category = p.categories.primary
LEFT JOIN read_parquet('cell_counts.parquet') c
    ON c.level = 12
    AND c.cell_id = s2_cell_parent(
        s2_cellfromlonlat(
            st_x(st_centroid(p.geometry)),
            st_y(st_centroid(p.geometry))
        ), 12
    )
WHERE length(p.token) > 1
ORDER BY token, importance DESC;
```

Overture uses a single `categories.primary` string, so the IDF join is a
simple equality — no unnest or max needed.

### Graceful fallback without pre-built data

If neither the density file nor the IDF file is available at import time,
the `name_index` build falls back to the current behavior:

```sql
-- Fallback: no density or IDF data available
CREATE TABLE name_index AS
SELECT
    token, fsq_place_id, name, latitude, longitude,
    address, locality, postcode, region, country,
    0 AS importance
FROM (
    SELECT
        unnest(string_split(lower(strip_accents(name)), ' ')) AS token,
        fsq_place_id, name, latitude, longitude,
        address, locality, postcode, region, country
    FROM places
    WHERE name IS NOT NULL AND length(name) > 0
) sub
WHERE length(token) > 1
ORDER BY token, importance DESC;
```

The import script should check for each file independently and include
whichever joins are possible. If only one file is present, the other
signal contributes 0.

## Integration Summary

```
DENSITY BUILD (standalone, annual)
    1. Load global places dataset (Foursquare or Overture)
    2. Aggregate at level 14
    3. Cascade to levels 13–6
    4. Export cell_counts.parquet

CATEGORY IDF BUILD (standalone, annual or less)
    1. Load global places datasets (Foursquare AND Overture)
    2. Unnest/extract categories, count places per category
    3. Compute ln(N / n_c) per category
    4. Export category_idf.parquet

PLACE IMPORT (per-dataset, monthly or as needed)
    1. Create places table, load from parquet
    2. Clean up (remove nulls/zeros)
    3. Create spatial index (rtree)
    4. Build name_index with importance from:
       - cell_counts.parquet (density, if available)
       - category_idf.parquet (category IDF, if available)
    5. ANALYZE
```

The density and IDF files are read-only inputs to the place import, not
products of it. All three pipelines share no mutable state.

## Query Patterns

### Text-only search (existing, unchanged)

The existing `name_index` query in `database.py` already sorts by
`importance DESC`. No query-side changes are needed — the improved
importance values flow through automatically:

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

### Direct density lookup (future use)

If density queries are ever needed directly:

```sql
-- Count at a specific level
SELECT pt_count
FROM read_parquet('cell_counts.parquet')
WHERE level = 12
  AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon, :lat), 12);

-- Counts across all levels for a point
SELECT level, pt_count
FROM read_parquet('cell_counts.parquet')
WHERE (level = 6  AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 6))
   OR (level = 7  AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 7))
   OR (level = 8  AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 8))
   OR (level = 9  AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 9))
   OR (level = 10 AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 10))
   OR (level = 11 AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 11))
   OR (level = 12 AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 12))
   OR (level = 13 AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 13))
   OR (level = 14 AND cell_id = s2_cell_parent(s2_cellfromlonlat(:lon,:lat), 14))
ORDER BY level;
```

## Extension Setup

```sql
-- One-time install
INSTALL geography FROM community;

-- Load at session start (density build and place import)
LOAD geography;
```

Key functions used in this design:

| Function | Purpose |
|----------|---------|
| `s2_cellfromlonlat(lon, lat)` | Encode a point as S2_CELL_CENTER (level 30) |
| `s2_cell_parent(cell, level)` | Derive a coarser cell at any level (0–30) |

Both produce `UBIGINT` values, so standard DuckDB integer operations,
sorting, and grouping apply natively.

## Design Rationale

**Why separate the density build from place import?** Global density
patterns change on timescales of years. Places are refreshed monthly or more
often. Coupling the two means rebuilding the density table on every import
for no benefit. Separating them removes unnecessary work from the import
pipeline and eliminates the `geography` extension as a hard dependency of
import (it becomes optional, needed only for the join).

**Why pre-materialize all levels (6–14)?** The cost is only ~33% additional
storage. While only level 12 is used for importance scoring today, having
the full range available enables future use cases (visualization at different
zoom levels, alternative scoring formulas, direct density queries) without
rebuilding.

**Why S2 over H3 or Geohash?** S2 cell IDs are 64-bit integers with strict
hierarchical containment — `s2_cell_parent` is a deterministic bit
operation, and every child rolls up to exactly one parent. The `geography`
community extension provides native DuckDB types and functions.

**Why Parquet as the interchange format?** Parquet is DuckDB's native
columnar format. A sorted Parquet file preserves the `(level, cell_id)`
sort order, enabling zonemap-based block skipping on reads. It's a single
file, trivially copyable, and requires no running database process to
consume.

**Why log scale for density?** Raw counts can range from 1 to tens of
thousands. `ln(1 + n)` compresses this to a usable ~0.7–9.2 range,
preventing extreme-density areas from completely dominating while still
providing clear ordering signal.

**Why IDF for category importance?** Categories follow a power-law
distribution — a few types (restaurants, shops) account for a large share
of all places, while many types (embassies, stadiums, cities) have very few
instances. IDF naturally captures this: `ln(N / n_c)` assigns low scores to
ubiquitous types and high scores to rare ones, without requiring manual
curation of a category importance hierarchy.

**Why max IDF rather than mean for multi-category places?** A place's most
distinctive category is the strongest signal. Averaging would dilute the
score when a rare category co-occurs with common ones (e.g. a "Historic
Landmark" also tagged as "Tourist Attraction"). Taking the max preserves
the strongest signal.

**Why additive combination of density and IDF?** Both signals are
log-scaled with comparable ranges (~0–9 for density, ~2–12 for IDF).
Addition keeps the two signals independent — a rare place type scores high
even in a sparse area, and a place in a dense area scores high even if its
type is common. Multiplicative combination would suppress places that score
low on either axis. Additive also simplifies debugging and tuning: each
signal's contribution is directly readable in the final score.
