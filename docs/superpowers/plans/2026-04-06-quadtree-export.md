# Quadtree Static JSON Export — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate static gzipped JSON tile files from raw parquet inputs (FSQ, Overture, OSM), partitioned by Bing quadkey so each tile contains ≤`max_per_tile` records (default 1000), and serve tile URLs via a new `getCoverage` XRPC endpoint. This is additive — no existing code is removed or refactored.

**Architecture:** SQL fragments in `garganorn/sql/` handle all heavy processing (import, importance scoring, variant extraction, tile partitioning, JSON construction) in DuckDB. Python (`garganorn/quadtree.py`) orchestrates SQL execution and writes gzipped output to disk. A manifest JSON file lists quadkeys per collection, loaded at startup for fast bbox→URL lookup in the getCoverage endpoint. Each collection (FSQ/Overture/OSM) gets its own tile tree.

**Tech Stack:** Python, Flask, DuckDB (spatial extension only), lexrpc, gzip

**Key design decisions:**
- Zoom range 6–17. At zoom 17, tiles that still exceed `max_per_tile` records are accepted as-is. `max_per_tile` defaults to 1000 and is tunable via CLI (`--max-per-tile`) and `run_pipeline()` parameter.
- `qk17` (quadkey at zoom 17) computed once per row at import time. Reused for density (`left(qk17, 15)`), tile partitioning (`left(qk17, level)`), and subdirectory layout (`qk[:6]`). No geography/S2 extension needed.
- JSON constructed entirely in DuckDB (`to_json()` with struct literals and `list()` aggregates). Python only writes gzipped bytes to disk.
- Tile records match the `org.atgeo.place` schema (nested `locations`/`attributes`/`variants`/`relations`) plus `importance` for client-side filtering. URIs use `https://{repo}/{collection}/{rkey}`. `collection` is NOT included in the record value.
- No iterative UPDATEs. Tile assignment uses one `tile_counts` table with a `level` column + `min(level)` to find coarsest valid zoom per place.
- File-backed temp DuckDB database (not `:memory:`) to handle large datasets without OOM. Deleted after export.
- Existing import scripts, DuckDB databases, `searchRecords`, and `getRecord` are untouched.

**Operational instructions:**

- Branch all worktrees from, and merge back to, `feat/quadtree` *not* `main`.

---

## File Structure

**New files to create:**
- `garganorn/sql/fsq_import.sql` — FSQ parquet→places table with bbox + quality filters
- `garganorn/sql/fsq_importance.sql` — FSQ importance (quadkey density + category IDF)
- `garganorn/sql/fsq_variants.sql` — FSQ variants (empty column)
- `garganorn/sql/overture_import.sql` — Overture parquet→places table
- `garganorn/sql/overture_importance.sql` — Overture importance scoring
- `garganorn/sql/overture_variants.sql` — Overture variant extraction (names.common + names.rules)
- `garganorn/sql/osm_import.sql` — OSM parquet→places table (from osm-pbf-parquet output)
- `garganorn/sql/osm_importance.sql` — OSM importance scoring
- `garganorn/sql/osm_variants.sql` — OSM variant extraction (name:* tags)
- `garganorn/sql/fsq_export_tiles.sql` — FSQ per-tile JSON construction in DuckDB
- `garganorn/sql/overture_export_tiles.sql` — Overture per-tile JSON construction in DuckDB
- `garganorn/sql/osm_export_tiles.sql` — OSM per-tile JSON construction in DuckDB
- `garganorn/quadtree.py` — Pipeline orchestrator, manifest writer, CLI entry point, TileManifest class, quadkey_to_bbox
- `garganorn/lexicon/getCoverage.json` — Query lexicon
- `garganorn/lexicon/coverageResult.json` — Record lexicon for tile contents
- `tests/test_quadtree.py` — Tests for quadtree module
- `tests/test_get_coverage.py` — Tests for getCoverage endpoint

**Files to modify:**
- `garganorn/server.py` — Add getCoverage handler, accept TileManifest
- `garganorn/config.py` — Parse tiles config section
- `garganorn/__main__.py` — Wire TileManifest into Server
- `config.yaml` — Add `tiles` section
- `pyproject.toml` — Add `"sql/**/*.sql"` to package-data

---

## Task 1: SQL Directory + FSQ Import SQL

**Files:**
- Create: `garganorn/sql/fsq_import.sql`
- Create: `garganorn/sql/fsq_importance.sql`
- Create: `garganorn/sql/fsq_variants.sql`

Extract FSQ import logic from `scripts/import-fsq-extract.sh` into standalone SQL fragments. These are parameterized with `${variable}` placeholders that Python substitutes before execution.

**fsq_import.sql** — Import places from FSQ parquet with filters:
```sql
SET memory_limit='${memory_limit}';
INSTALL spatial; LOAD spatial;

CREATE TABLE places AS
SELECT * EXCLUDE (geom), geom::GEOMETRY AS geom
FROM '${parquet_glob}'
WHERE bbox.xmin >= ${xmin} AND bbox.xmax <= ${xmax}
  AND bbox.ymin >= ${ymin} AND bbox.ymax <= ${ymax}
  AND date_refreshed > '2020-03-15'
  AND date_closed IS NULL
  AND longitude != 0 AND latitude != 0
  AND geom IS NOT NULL;

-- Compute quadkey at max zoom (used for density, tile assignment, and export)
ALTER TABLE places ADD COLUMN qk17 VARCHAR;
UPDATE places SET qk17 = ST_QuadKey(longitude, latitude, 17);
```


**fsq_importance.sql** — Compute importance (60% quadkey density + 40% category IDF). Density uses `left(qk17, 15)` (~1.2km cells at equator, comparable to S2 level 12). Requires `qk17` column to already exist on the places table. Uses `fsq_place_id` as join key. FSQ unnests `fsq_category_ids` array, takes `max(idf_score)` per place:
```sql
CREATE TEMP TABLE t_idf AS
SELECT
    category,
    count(*) AS n_places,
    ln(N.total::DOUBLE / count(*)::DOUBLE) AS idf_score
FROM (
    SELECT unnest(fsq_category_ids) AS category
    FROM places
    WHERE fsq_category_ids IS NOT NULL
) cats
CROSS JOIN (SELECT count(*) AS total FROM places) N
GROUP BY category, N.total;

CREATE TEMP TABLE place_density AS
SELECT fsq_place_id,
       ln(1 + count(*) OVER (
           PARTITION BY left(qk17, 15)
       )) AS density_score
FROM places;

CREATE TEMP TABLE place_idf AS
SELECT
    p.fsq_place_id,
    coalesce(max(idf.idf_score), 0) AS idf_score
FROM places p,
    unnest(p.fsq_category_ids) AS t(category)
LEFT JOIN t_idf idf ON idf.category = t.category
WHERE p.fsq_category_ids IS NOT NULL
GROUP BY p.fsq_place_id;

CREATE TABLE places_scored AS
SELECT p.*,
       round(
           60 * least(coalesce(d.density_score, 0) / 10.0, 1.0)
         + 40 * least(coalesce(i.idf_score, 0) / 18.0, 1.0)
       )::INTEGER AS importance
FROM places p
LEFT JOIN place_density d USING (fsq_place_id)
LEFT JOIN place_idf i USING (fsq_place_id);

DROP TABLE places;
ALTER TABLE places_scored RENAME TO places;
DROP TABLE place_density;
DROP TABLE place_idf;
DROP TABLE t_idf;
```

**fsq_variants.sql** — FSQ has no variant source:
```sql
ALTER TABLE places ADD COLUMN variants
    STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [];
```

- [x] **Step 1:** Create `garganorn/sql/` directory
- [x] **Step 2:** Write failing tests — load each SQL file, substitute test params, execute against in-memory DuckDB with fixture data, verify expected table/columns exist
- [x] **Step 3:** Run tests, verify fail
- [x] **Step 4:** Write `fsq_import.sql` — extract from import-fsq-extract.sh lines 116–138
- [x] **Step 5:** Write `fsq_importance.sql` — extract from import-fsq-extract.sh lines 148–194
- [x] **Step 6:** Write `fsq_variants.sql`
- [x] **Step 7:** Run tests, verify pass
- [x] **Step 8:** Commit: `feat: extract FSQ import SQL into garganorn/sql/` (6fd6c55 on feat/quadtree)

---

## Task 2a: Overture Import SQL

**Files:**
- Create: `garganorn/sql/overture_import.sql`
- Create: `garganorn/sql/overture_importance.sql`
- Create: `garganorn/sql/overture_variants.sql`

Same pattern as Task 1 but for Overture Maps. Extract from `scripts/import-overture-extract.sh`. Each SQL also computes the `qk17` column. Importance uses `left(qk17, 15)` for density instead of S2.

**Overture** (from `scripts/import-overture-extract.sh`):
- **import** (lines 136–163): Creates places table, bbox filter, null geometry filter. Uses `id` as primary key. Coordinates from bbox midpoint `(bbox.xmin+bbox.xmax)/2`. Computes `qk17 = ST_QuadKey((bbox.xmin+bbox.xmax)/2, (bbox.ymin+bbox.ymax)/2, 17)`.
- **importance** (lines 174–222): Density uses `left(qk17, 15)`. IDF on `categories.primary` (scalar, not array — direct LEFT JOIN, no unnest). Join key is `id`.
- **variants** (lines 225–271): Complex extraction from `names.common` (MAP → `unnest(map_entries(...))`) and `names.rules` (struct array → `unnest(names.rules)`). Produces `overture_variants` temp table with `list(...)` aggregation, joined to places.

IMPORTANT: Column names, types, and join key (`id`) differ from FSQ. The SQL must match the Overture schema exactly.

- [x] **Step 1:** Write failing tests for the 3 Overture SQL files — same pattern as Task 1 (fixture parquet, execute, verify tables/columns/types)
- [x] **Step 2:** Run tests, verify fail
- [x] **Step 3:** Write `overture_import.sql`
- [x] **Step 4:** Write `overture_importance.sql`
- [x] **Step 5:** Write `overture_variants.sql`
- [x] **Step 6:** Run tests, verify pass
- [x] **Step 7:** Commit: `feat: extract Overture import SQL into garganorn/sql/` (dff6dd2 on feat/quadtree)

---

## Task 2b: OSM Import SQL

**Files:**
- Create: `garganorn/sql/osm_import.sql`
- Create: `garganorn/sql/osm_importance.sql`
- Create: `garganorn/sql/osm_variants.sql`

Same pattern as Task 1 but for OpenStreetMap. Extract from `scripts/import-osm.sh`. Each SQL also computes the `qk17` column. Importance uses `left(qk17, 15)` for density instead of S2.

**OSM** (from `scripts/import-osm.sh`):
- **import**: Reads osm-pbf-parquet output parquet (NOT raw PBF). Tags column is a MAP type (not VARCHAR[]). Primary key is `rkey`. Computes `qk17 = ST_QuadKey(longitude, latitude, 17)`. Check import-osm.sh for exact table creation and filters.
- **importance** (lines ~462–507): Density uses `left(qk17, 15)`. IDF on `primary_category` (scalar, no unnest).
- **variants** (lines ~521–577): Extracted from `tags` MAP entries matching `name:*` pattern.

IMPORTANT: Tags are MAP type (not VARCHAR[]). Primary key is `rkey` (not `fsq_place_id` or `id`). The SQL must match the OSM schema exactly.

- [x] **Step 1:** Write failing tests for the 3 OSM SQL files — same pattern as Task 1 (fixture parquet with MAP tags column, execute, verify tables/columns/types)
- [x] **Step 2:** Run tests, verify fail
- [x] **Step 3:** Write `osm_import.sql`
- [x] **Step 4:** Write `osm_importance.sql`
- [x] **Step 5:** Write `osm_variants.sql`
- [x] **Step 6:** Run tests, verify pass
- [x] **Step 7:** Commit: `feat: extract OSM import SQL into garganorn/sql/` (f5a6c62 on feat/quadtree)

---

## Task 2c: Cross-Source Import SQL Refactoring Review

**Scope:** All nine SQL files produced by Tasks 1, 2a, and 2b:
- `garganorn/sql/fsq_import.sql`, `fsq_importance.sql`, `fsq_variants.sql`
- `garganorn/sql/overture_import.sql`, `overture_importance.sql`, `overture_variants.sql`
- `garganorn/sql/osm_import.sql`, `osm_importance.sql`, `osm_variants.sql`

Read all nine files and identify opportunities for refactoring across the three import pipelines. Look for:
- Identical or near-identical SQL patterns that could be extracted into a shared fragment
- Structural inconsistencies between sources that should be harmonized (not necessarily merged)
- Shared density/IDF scoring logic that could become a single parameterized template
- Placeholder naming inconsistencies across files

Produce a written finding (markdown) listing opportunities, their location, and a recommendation (extract/harmonize/leave-as-is) with rationale. This is analysis only — no code changes in this task.

- [x] **Step 1:** Read all nine SQL files
- [x] **Step 2:** Write refactoring findings to `docs/import-sql-refactor-findings.md` (file subsequently deleted by author)
- [x] **Step 3:** Review findings for completeness and correctness
- [x] **Step 4:** Decide which findings (if any) to act on before Task 3

---

## Task 3: Quadkey Tile Partitioning

**Files:**
- Create: `garganorn/sql/compute_tile_assignments.sql`

For each place, compute which quadkey tile it belongs to such that no tile has more than `max_per_tile` records. At zoom 17 (the finest level), accept tiles exceeding the limit as-is. Requires `qk17` column to already exist on the places table (computed in the import step).

Algorithm:
1. Build one tile_counts table with a `level` column by truncating `qk17` to each zoom level 6–17 and counting
2. Find the coarsest (min) zoom where each place's tile has ≤ `max_per_tile` records
3. Zoom 17 is the unconditional fallback (no count check)

This uses progressive string truncation (`left(qk17, level)`) instead of recomputing `ST_QuadKey` at each zoom. One ST_QuadKey call per row (in the import step), zero in the partitioning step.

Per-source primary key (used for tile_assignments join — NOT rowid, which is unstable after table renames):
- **FSQ:** `fsq_place_id`
- **Overture:** `id`
- **OSM:** `rkey`

**compute_tile_assignments.sql:**
```sql
DROP TABLE IF EXISTS tile_assignments;

-- Count places per tile at each zoom level
DROP TABLE IF EXISTS tile_counts;
CREATE TEMP TABLE tile_counts AS
SELECT level, left(qk17, level) AS qk, count(*) AS cnt
FROM places, generate_series(${min_zoom}, ${max_zoom}) AS t(level)
WHERE qk17 IS NOT NULL
GROUP BY level, left(qk17, level);

-- Pre-compute each place's quadkey prefix at every zoom level
DROP TABLE IF EXISTS place_zoom;
CREATE TEMP TABLE place_zoom AS
SELECT p.${pk_expr} AS place_id, t.level, left(p.qk17, t.level) AS qk
FROM places p
CROSS JOIN generate_series(${min_zoom}, ${max_zoom}) AS t(level)
WHERE p.qk17 IS NOT NULL;

-- Find coarsest zoom where tile count <= max_per_tile
CREATE TABLE tile_assignments AS
WITH best_zoom AS (
    SELECT pz.place_id, min(pz.level) AS level
    FROM place_zoom pz
    JOIN tile_counts tc ON tc.level = pz.level AND tc.qk = pz.qk
    WHERE tc.cnt <= ${max_per_tile}
    GROUP BY pz.place_id
)
SELECT p.${pk_expr} AS place_id,
       left(p.qk17, coalesce(bz.level, ${max_zoom})) AS tile_qk
FROM places p
LEFT JOIN best_zoom bz ON bz.place_id = p.${pk_expr}
WHERE p.qk17 IS NOT NULL;

-- Drop temp tables (tile_counts is TEMP and would auto-drop at connection close,
-- but dropping explicitly frees memory sooner)
DROP TABLE tile_counts;
DROP TABLE place_zoom;
```

Python substitutes `${pk_expr}`, `${min_zoom}` (6), `${max_zoom}` (17), and `${max_per_tile}` (from `run_pipeline()` parameter, default 1000).

- [x] **Step 1:** Write failing test for tile assignment SQL — create in-memory DuckDB with test places + qk17 column, run SQL, verify no tile exceeds `max_per_tile` except at zoom 17
- [x] **Step 2:** Run test, verify fail
- [x] **Step 3:** Write `compute_tile_assignments.sql`
- [x] **Step 4:** Run test, verify pass
- [x] **Step 5:** Write test for edge case: all places in one cell at zoom 17 (verify fallback works)
- [x] **Step 6:** Run test, verify pass
- [x] **Step 7:** Commit: `feat: add quadkey tile partitioning SQL` (38a6cc0 on feat/quadtree)

---

## Task 4: Per-Tile JSON Export (DuckDB-Constructed)

**Files:**
- Create: `garganorn/sql/fsq_export_tiles.sql`
- Create: `garganorn/sql/overture_export_tiles.sql`
- Create: `garganorn/sql/osm_export_tiles.sql`
- Add to: `garganorn/quadtree.py` — `export_tiles()`, `run_pipeline()`, `write_manifest()`

JSON is constructed entirely in DuckDB. Python reads the result and writes gzipped bytes to disk.

The export SQL must produce records matching the `org.atgeo.place` record schema (see `garganorn/lexicon/place.json`). Each record has a nested structure with `locations` array (geo + optional address), `variants`, `attributes` dict, and `relations` object. This mirrors what `process_record()` in `database.py` produces for each source. The export SQL replicates `process_record()` logic in DuckDB SQL.

**Per-record structure** (same for all sources, matches `process_record()` output plus `importance`):
```json
{
    "uri": "https://places.atgeo.org/{collection}/{rkey}",
    "value": {
        "$type": "org.atgeo.place",
        "rkey": "{rkey}",
        "name": "...",
        "importance": 42,
        "locations": [
            {"$type": "community.lexicon.location.geo", "latitude": "...", "longitude": "..."},
            {"$type": "community.lexicon.location.address", "country": "...", ...}
        ],
        "variants": [{"name": "...", "type": "...", "language": "..."}],
        "attributes": { ... source-dependent ... },
        "relations": {}
    }
}
```

**FSQ export SQL** — produces one row per tile with `tile_qk` and `tile_json` columns. Replicates `FoursquareOSP.process_record()` (database.py:576–622). Collection is `org.atgeo.places.foursquare`. The `locations` array starts with a geo object and conditionally appends an address if `country` is non-null. The `attributes` dict includes all remaining FSQ columns (`fsq_place_id`, `date_created`, `date_refreshed`, `tel`, `website`, `email`, `facebook_id`, `instagram`, `twitter`, `fsq_category_ids`, `fsq_category_labels`, `placemaker_url`). Address fields (`address`, `locality`, `region`, `postcode`, `country`) are consumed by the locations array, not duplicated in attributes:

```sql
SELECT
    ta.tile_qk,
    to_json({
        attribution: '${attribution}',
        records: list({
            uri: 'https://${repo}/org.atgeo.places.foursquare/' || p.fsq_place_id,
            value: {
                "$type": 'org.atgeo.place',
                rkey: p.fsq_place_id,
                name: p.name,
                importance: p.importance,
                locations: list_concat(
                    [{
                        "$type": 'community.lexicon.location.geo',
                        latitude: p.latitude::decimal(10,6)::varchar,
                        longitude: p.longitude::decimal(10,6)::varchar
                    }],
                    CASE WHEN p.country IS NOT NULL THEN [{
                        "$type": 'community.lexicon.location.address',
                        country: p.country,
                        region: p.region,
                        locality: p.locality,
                        street: p.address,
                        postalCode: p.postcode
                    }] ELSE [] END
                ),
                variants: coalesce(p.variants, []),
                attributes: {
                    fsq_place_id: p.fsq_place_id,
                    date_created: p.date_created,
                    date_refreshed: p.date_refreshed,
                    admin_region: p.admin_region,
                    post_town: p.post_town,
                    po_box: p.po_box,
                    tel: p.tel,
                    website: p.website,
                    email: p.email,
                    facebook_id: p.facebook_id,
                    instagram: p.instagram,
                    twitter: p.twitter,
                    fsq_category_ids: p.fsq_category_ids,
                    fsq_category_labels: p.fsq_category_labels,
                    placemaker_url: p.placemaker_url
                },
                relations: {}
            }
        })
    }) AS tile_json
FROM places p
JOIN tile_assignments ta ON ta.place_id = p.fsq_place_id
GROUP BY ta.tile_qk;
```

**Overture export SQL** — Replicates `OvertureMaps.process_record()` (database.py:897–956). Coordinates from bbox midpoint `(bbox.xmin+bbox.xmax)/2`, `(bbox.ymin+bbox.ymax)/2`. Address logic must match process_record exactly: iterate the `addresses` array, split `region` on `-` (take suffix), map `freeform` → `street`, and emit an address location object for each entry that has a non-null `country`. Multiple address objects per record are possible. Remove `addresses` from attributes after consuming. Attributes include `names`, `categories`, `websites`, `socials`, `emails`, `phones`, `brand`, `confidence`, `version`, `sources`. Join on `ta.place_id = p.id`. Include `importance` in each record.

**OSM export SQL** — Replicates `OpenStreetMap.process_record()` (database.py:1255–1316). Must expand the rkey in DuckDB SQL (`n12345` → `node:12345`, `w` → `way:`, `r` → `relation:` — use `CASE` on `rkey[1]` with string concatenation). Must parse `primary_category` (e.g. `amenity=cafe`) into the tags dict (use `str_split` on `=`). Address assembly is NOT required — skip the `addr:*` → locations logic for tile export to keep the SQL tractable. Attributes are the full tag dict (as a JSON object). Join on `ta.place_id = p.rkey`. Include `importance` in each record.

NOTE: The Overture and OSM export SQL is more complex than FSQ. The implementer should use `process_record()` as the authoritative reference. If any transformation is impractical in pure SQL, it can be done as a DuckDB UDF or a post-processing step, but prefer SQL.

**The `relations` field** is included as empty `{}` (object, not array — see `place.json` schema: it has `within` and `same_as` array properties). Will be populated in future work.

**export_tiles()** in quadtree.py — reads DuckDB result, writes gzipped files:
```python
def export_tiles(con, output_dir: str, source: str) -> dict:
    """Query DuckDB for per-tile JSON and write gzipped files. Returns {qk: record_count}."""
    sql = (Path(__file__).parent / "sql" / f"{source}_export_tiles.sql").read_text()
    sql = sql.replace("${attribution}", ATTRIBUTION[source])
    sql = sql.replace("${repo}", REPO)
    result = con.execute(sql).fetchall()
    log.info("export: queried %d tiles from DuckDB", len(result))
    manifest = {}
    for i, (tile_qk, tile_json) in enumerate(result):
        subdir = os.path.join(output_dir, tile_qk[:6])
        os.makedirs(subdir, exist_ok=True)
        with gzip.open(os.path.join(subdir, f"{tile_qk}.json.gz"), "wb") as f:
            f.write(tile_json.encode("utf-8"))
        manifest[tile_qk] = len(json.loads(tile_json)["records"])
        if (i + 1) % 1000 == 0:
            log.info("export: wrote %d / %d tiles", i + 1, len(result))
    return manifest
```

Attribution URLs (matching Database subclass `.attribution` attributes):
- FSQ: `"https://docs.foursquare.com/data-products/docs/access-fsq-os-places"`
- Overture: `"https://docs.overturemaps.org/attribution/"`
- OSM: `"https://www.openstreetmap.org/copyright"`

**run_pipeline()** orchestrates the full flow for one source:
```python
def run_pipeline(source, parquet_glob, bbox, output_dir, memory_limit="48GB", max_per_tile=1000):
    output_dir = os.path.join(output_dir, source)
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, f".{source}_work.duckdb")
    con = duckdb.connect(db_path)  # file-backed to avoid OOM on large datasets
    sql_dir = Path(__file__).parent / "sql"

    t0 = time.monotonic()

    def run_sql(stage, filename, **params):
        log.info("[%s] %s: starting", source, stage)
        sql = (sql_dir / filename).read_text()
        for k, v in params.items():
            sql = sql.replace(f"${{{k}}}", str(v))
        con.execute(sql)
        count = con.execute("SELECT count(*) FROM places").fetchone()[0]
        log.info("[%s] %s: done (%.1fs, %d places)",
                 source, stage, time.monotonic() - t0, count)

    run_sql("import", f"{source}_import.sql", memory_limit=memory_limit,
            parquet_glob=parquet_glob,
            xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3])
    run_sql("importance", f"{source}_importance.sql")
    run_sql("variants", f"{source}_variants.sql")

    pk_expr = SOURCE_PK[source]
    log.info("[%s] tile assignment: starting", source)
    run_sql("tile assignment", "compute_tile_assignments.sql",
            pk_expr=pk_expr, min_zoom=6, max_zoom=17, max_per_tile=max_per_tile)

    log.info("[%s] export: starting", source)
    manifest = export_tiles(con, output_dir, source)
    log.info("[%s] export: %d tiles, %d records (%.1fs)",
             source, len(manifest),
             sum(manifest.values()), time.monotonic() - t0)

    write_manifest(manifest, output_dir, source)
    con.close()
    os.remove(db_path)
    log.info("[%s] pipeline complete (%.1fs total)", source, time.monotonic() - t0)
```

Where `SOURCE_PK` maps source to its primary key column:
```python
SOURCE_PK = {
    "fsq": "fsq_place_id",
    "overture": "id",
    "osm": "rkey",
}
```

**write_manifest():**
```python
def write_manifest(manifest, output_dir, source):
    data = {
        "source": source,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "quadkeys": sorted(manifest.keys()),
    }
    with open(os.path.join(output_dir, "manifest.json"), "w") as f:
        json.dump(data, f, indent=2)
```

The manifest stores the list of quadkeys. URLs are deterministic: `{base_url}/{qk[:6]}/{qk}.json.gz`.

- [ ] **Step 1:** Write `fsq_export_tiles.sql` — match field names from database.py process_record for FSQ
- [ ] **Step 2:** Write `overture_export_tiles.sql` — match field names from database.py process_record for Overture
- [ ] **Step 3:** Write `osm_export_tiles.sql` — match field names from database.py process_record for OSM
- [ ] **Step 4:** Write failing test for export_tiles (create test DuckDB with fixture data + tile_assignments, run export, verify gzipped JSON structure)
- [ ] **Step 5:** Run test, verify fail
- [ ] **Step 6:** Implement export_tiles in quadtree.py
- [ ] **Step 7:** Run test, verify pass
- [ ] **Step 8:** Write failing test for run_pipeline (end-to-end with small fixture data)
- [ ] **Step 9:** Run test, verify fail
- [ ] **Step 10:** Implement run_pipeline
- [ ] **Step 11:** Run test, verify pass
- [ ] **Step 12:** Write failing test for write_manifest
- [ ] **Step 13:** Run test, verify fail
- [ ] **Step 14:** Implement write_manifest
- [ ] **Step 15:** Run test, verify pass
- [ ] **Step 16:** Commit: `feat: add DuckDB-native tile JSON export pipeline`

---

## Task 5: CLI Entry Point

**Files:**
- Modify: `garganorn/quadtree.py` — add `main()` with argparse
- Modify: `pyproject.toml` — add sql package-data

CLI invocation:
```bash
python -m garganorn.quadtree --source fsq \
    --parquet 'db/cache/fsq/2025-03-06/*.parquet' \
    --bbox -74.1 40.6 -73.8 40.9 \
    --output tiles \
    --config config.yaml \
    --memory-limit 48GB \
    --max-per-tile 1000
```

Arguments:
- `--source` (required): fsq, overture, or osm
- `--parquet` (required): glob pattern for input parquet files
- `--bbox` (required): xmin ymin xmax ymax (4 floats)
- `--output` (required): base output directory (source name is appended automatically, e.g. `tiles` → `tiles/fsq`)
- `--config` (optional): path to config.yaml; if provided, `tiles.memory_limit` and `tiles.max_per_tile` are used as defaults
- `--memory-limit` (optional): DuckDB memory limit. Overrides config file. Falls back to `"48GB"` if neither CLI nor config specifies a value.
- `--max-per-tile` (optional): maximum records per tile before subdividing to a finer zoom level. Overrides config file. Falls back to `1000` if neither CLI nor config specifies a value.

Update `pyproject.toml`:
```toml
[tool.setuptools.package-data]
garganorn = ["sql/**/*.sql"]
```

- [ ] **Step 1:** Write failing test for CLI argument parsing
- [ ] **Step 2:** Run test, verify fail
- [ ] **Step 3:** Implement `main()` with argparse
- [ ] **Step 4:** Run test, verify pass
- [ ] **Step 5:** Update pyproject.toml package-data
- [ ] **Step 6:** Commit: `feat: add quadtree CLI entry point`

---

## Task 6: getCoverage XRPC Endpoint + Lexicons

**Files:**
- Create: `garganorn/lexicon/getCoverage.json` — query lexicon
- Create: `garganorn/lexicon/coverageResult.json` — record lexicon
- Modify: `garganorn/server.py` — add getCoverage handler
- Modify: `garganorn/config.py` — parse tiles config
- Modify: `garganorn/__main__.py` — wire TileManifest into Server
- Modify: `config.yaml` — add tiles section
- Create: `tests/test_get_coverage.py`

**getCoverage query lexicon** (`getCoverage.json`):
```json
{
    "lexicon": 1,
    "id": "org.atgeo.getCoverage",
    "defs": {
        "main": {
            "type": "query",
            "parameters": {
                "type": "params",
                "required": ["collection", "bbox"],
                "properties": {
                    "collection": {"type": "string"},
                    "bbox": {"type": "string", "description": "xmin,ymin,xmax,ymax"}
                }
            },
            "output": {
                "encoding": "application/json",
                "schema": {
                    "type": "object",
                    "required": ["tiles"],
                    "properties": {
                        "tiles": {
                            "type": "array",
                            "items": {"type": "string", "format": "uri"}
                        }
                    }
                }
            },
            "errors": [
                {"name": "BboxTooLarge", "description": "Bounding box covers too many tiles"}
            ]
        }
    }
}
```

**coverageResult record lexicon** — defines the schema of a tile JSON file. Check existing lexicons in `garganorn/lexicon/` for format conventions (flat directory, `{name}.json`).

**TileManifest class** (in quadtree.py):

> **TODO:** The naive linear scan over all quadkeys is O(n) per request. Before implementing, research spatial index options for fast bbox→quadkey lookup. Check PyPI for quadtree/spatial index packages (e.g. `pyquadkey2`, `mercantile`, or similar). A simple alternative: build a dict keyed by zoom-6 prefix, so `get_tiles_for_bbox` only scans tiles whose zoom-6 ancestor intersects the query bbox. Pick the approach during implementation.

```python
class TileManifest:
    def __init__(self, manifest_path: str, base_url: str):
        with open(manifest_path) as f:
            data = json.load(f)
        self.quadkeys = set(data["quadkeys"])
        self.base_url = base_url.rstrip("/")

    def get_tiles_for_bbox(self, xmin, ymin, xmax, ymax, max_tiles=50):
        urls = []
        for qk in self.quadkeys:
            tile_bbox = quadkey_to_bbox(qk)
            if bboxes_intersect(tile_bbox, (xmin, ymin, xmax, ymax)):
                urls.append(f"{self.base_url}/{qk[:6]}/{qk}.json.gz")
                if len(urls) >= max_tiles:
                    raise BboxTooLarge(len(urls), max_tiles)
        return sorted(urls)
```

**quadkey_to_bbox()** — standard Bing Maps tile math:
```python
def quadkey_to_bbox(quadkey: str) -> tuple[float, float, float, float]:
    x, y, level = 0, 0, len(quadkey)
    for i, ch in enumerate(quadkey):
        bit = level - i - 1
        mask = 1 << bit
        digit = int(ch)
        if digit & 1:
            x |= mask
        if digit & 2:
            y |= mask
    n = 2 ** level
    lon_min = x / n * 360 - 180
    lon_max = (x + 1) / n * 360 - 180
    import math
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return (lon_min, lat_min, lon_max, lat_max)
```

**bboxes_intersect():**
```python
def bboxes_intersect(a, b):
    return a[0] <= b[2] and a[2] >= b[0] and a[1] <= b[3] and a[3] >= b[1]
```

**Server changes** — add getCoverage handler following existing pattern in server.py:
```python
# In __init__, accept tile_manifests and max_coverage_tiles
self.tile_manifests = tile_manifests or {}
self.max_coverage_tiles = max_coverage_tiles

# Register
methods["org.atgeo.getCoverage"] = self._get_coverage

def _get_coverage(self, params):
    collection = params["collection"]
    bbox = self._parse_bbox(params["bbox"])
    manifest = self.tile_manifests.get(collection)
    if manifest is None:
        raise ValueError(f"Unknown collection: {collection}")
    tiles = manifest.get_tiles_for_bbox(*bbox, max_tiles=self.max_coverage_tiles)
    return {"tiles": tiles}
```

**Config** — add tiles section to config.yaml:
```yaml
tiles:
  max_per_tile: 1000
  memory_limit: 48GB
  collections:
    org.atgeo.places.foursquare:
      manifest: tiles/fsq/manifest.json
      base_url: https://places.atgeo.org/tiles/fsq
      cache_ttl: 86400
    org.atgeo.places.overture:
      manifest: tiles/overture/manifest.json
      base_url: https://places.atgeo.org/tiles/overture
      cache_ttl: 86400
    org.atgeo.places.osm:
      manifest: tiles/osm/manifest.json
      base_url: https://places.atgeo.org/tiles/osm
      cache_ttl: 86400
  max_coverage_tiles: 50
```

`max_per_tile` and `memory_limit` are used by the export pipeline (Task 4/5). `collections` and `max_coverage_tiles` are used by the server (Task 6). All live under `tiles` since they're part of the same feature.

Update `config.py` to parse tiles config. Currently `load_config()` returns a 3-tuple `(repo, dbs, boundaries_path)`. Extend to 4-tuple `(repo, dbs, boundaries_path, tiles_config)` and update the unpacking in `__main__.py`.
Update `__main__.py` to create TileManifest instances from tiles_config and pass to Server.

- [ ] **Step 1:** Write failing test for `quadkey_to_bbox` (known quadkey → known bbox)
- [ ] **Step 2:** Run test, verify fail
- [ ] **Step 3:** Implement `quadkey_to_bbox`
- [ ] **Step 4:** Run test, verify pass
- [ ] **Step 5:** Write failing test for `bboxes_intersect`
- [ ] **Step 6:** Run test, verify fail
- [ ] **Step 7:** Implement `bboxes_intersect`
- [ ] **Step 8:** Run test, verify pass
- [ ] **Step 9:** Write failing test for `TileManifest.get_tiles_for_bbox`
- [ ] **Step 10:** Run test, verify fail
- [ ] **Step 11:** Implement `TileManifest`
- [ ] **Step 12:** Run test, verify pass
- [ ] **Step 13:** Write `getCoverage.json` query lexicon
- [ ] **Step 14:** Write `coverageResult.json` record lexicon
- [ ] **Step 15:** Write failing test for getCoverage XRPC endpoint
- [ ] **Step 16:** Run test, verify fail
- [ ] **Step 17:** Implement getCoverage handler in server.py
- [ ] **Step 18:** Update config.py to parse tiles config
- [ ] **Step 19:** Update __main__.py to wire TileManifest
- [ ] **Step 20:** Run all tests, verify pass
- [ ] **Step 21:** Commit: `feat: add getCoverage XRPC endpoint with tile manifests`

---

## Task 7: Integration Tests

**Files:**
- Create: `tests/test_integration_quadtree.py`

End-to-end test using small fixture data (follow patterns from `tests/conftest.py`):
1. Create in-memory DuckDB with test places for FSQ
2. Run full pipeline (import → importance → variants → tile assignment → JSON export)
3. Verify tile files exist, are valid gzipped JSON, match expected schema
4. Verify manifest lists correct quadkeys
5. Verify getCoverage returns correct URLs for test bbox

Edge cases:
- Empty bbox (no places) → no tiles, empty manifest
- BboxTooLarge → error response
- Single place → one tile at zoom 6
- Dense cluster → subdivides to higher zoom levels

- [ ] **Step 1:** Write integration test for full FSQ pipeline with fixture data
- [ ] **Step 2:** Run test, verify fail
- [ ] **Step 3:** Fix integration issues
- [ ] **Step 4:** Run test, verify pass
- [ ] **Step 5:** Write BboxTooLarge edge case test
- [ ] **Step 6:** Run test, verify pass
- [ ] **Step 7:** Write empty-bbox test
- [ ] **Step 8:** Run test, verify pass
- [ ] **Step 9:** Commit: `test: add quadtree integration tests`

---

## Notes

- **Memory limits:** Replicate existing script patterns. Import uses `SET memory_limit='48GB'`. During CTAS sorts, reduce to `SET memory_limit='16GB'` to avoid OOM (learned from prior work).
- **DuckDB zone maps:** Any CTAS that will be scanned by a column must ORDER BY that column for zone map efficiency.
- **No name_index:** The tile export does not build the trigram name_index table — that's only needed for searchRecords which remains backed by DuckDB.
- **Importance in tiles:** Each tile record includes `importance` (integer) so clients can filter locally by significance without re-querying the server.
- **OSM address assembly skipped:** The OSM `addr:*` → address location logic from `process_record()` is omitted from tile export SQL to keep the SQL tractable. OSM tile records will have geo locations only.
- **relations field:** Included as empty `{}` in tile records for now. Will be populated in future work.
- **Existing code untouched:** Import scripts, DuckDB databases, searchRecords, getRecord all continue to work as-is.
