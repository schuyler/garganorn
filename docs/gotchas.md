# Gotchas

Behaviors of the tools garganorn builds on — DuckDB, GeoParquet, Overture,
QuackOSM — that cost real time to learn and are not discoverable from their
documentation. Each entry states the behavior, where it bites, and why it
matters.

These are facts about software we did not write. They would still be true in a
rewrite. Decisions about garganorn's own architecture belong in
`design-constraints.md` instead.

---

## DuckDB

### R-tree indexes only activate in top-level WHERE clauses

R-tree spatial indexes are not used in JOIN ON conditions or subqueries.
Workaround: materialize filtered results to a temp table via a top-level WHERE
clause, then join against the temp table.

**Applies to**: `garganorn/sql/covering_seed.sql` (containment pre-filter)

**Why it matters**: Without this workaround, spatial queries degrade to
full-table scans against large geometry columns, causing 100x+ latency.

### Zone maps require sorted columns

Zone maps (min/max statistics per row group) only accelerate queries when the
filtered column is physically sorted in the parquet file. Removing ORDER BY from
a CTAS that writes parquet can cause 10-24x query regression on filtered reads.

**Applies to**: All `*_import.sql` files, `density_extract.sql`, manifest writes

**Why it matters**: The pipeline uses parquet as its interchange format.
Unsorted parquet defeats DuckDB's built-in zonemap pushdown.

### CTAS is fast, UPDATE/ALTER TABLE is slow

Columnar storage means every column mutation is a full table copy. Be
INSERT/CTAS-only; never UPDATE a column or ALTER TABLE ADD COLUMN on a large
table.

**Applies to**: All SQL files, `stages.py`

**Why it matters**: An UPDATE on a 38M-row table can take as long as the
original import.

### CTAS ORDER BY can OOM during sort

CTAS with ORDER BY sorts multi-threaded but can exhaust memory on large
datasets. Lower `memory_limit` below system RAM to leave headroom.

**Applies to**: `overture_division_import.sql` (ST_Hilbert sort),
`export_boundaries_db()` (Hilbert sort)

**Why it matters**: OOM during sort kills the pipeline. The memory limit
parameter exists to prevent this.

### `unnest()` on NULL arrays silently drops the row

Unnesting a NULL or empty array removes the row from the result set entirely —
no error, no NULL output row. Correct SQL semantics, but it changes row counts
unexpectedly.

**Applies to**: `overture_place_import.sql` (name variant unnesting)

**Why it matters**: Places with NULL category arrays get no IDF score (defaults
to 0 via coalesce).

### No unbounded complex-state aggregation

DuckDB cannot spill intermediate state for `list()`, `string_agg()`, and other
holistic aggregates. Any query using them must run over a bounded partition —
per-tile, or per qk4 prefix.

**Applies to**: every SQL file; `compute_containment` partitions at z6 for
exactly this reason

**Why it matters**: it is the most reliable way to exhaust memory on the 72 GB
target, and it has done so. Treat it as a review criterion for new SQL, not a
thing to discover at scale.

### A join is sized off the source relation, not the filtered one

A query that filters a large relation inline and joins the result, leaving the
filter in a CTE rather than materializing it, is planned against the source
cardinality rather than the post-filter one. The cost lands as memory or as
time:

- **memory, 100x+**: a GEOS-backed predicate against Canada/Nunavut's
  200k-vertex Arctic Archipelago coastlines at global scale, not any data defect.
- **time, 250x**: 553,249 localities filtered out of a 1,071,108-row CTE and
  joined to 5,262,549 density tiles on four float comparisons, with no spatial
  predicate at all — 15.6s with the probe side materialized, ~64 min as a
  `CTE_SCAN`.

**Applies to**: `stages.py:compute_containment()`, where `p` is materialized as
its own `CREATE TEMP TABLE` (not left as a CTE over `places_slim`) for exactly
this reason; `garganorn/sql/overture_division_import.sql`, which materializes
`division_base` ahead of the `division_density` join for the same reason.

**Why it matters**: an expensive per-row predicate is not the trigger — the
second case has none — so any join whose probe side is a filtered CTE over a
large relation needs materializing first, and rewriting the filter for zone-map
pruning or reducing thread count does not substitute. `EXPLAIN` cannot tell the
two apart: same operator, same conditions, same estimates. Only `EXPLAIN ANALYZE`
or JSON profiling separates them, and those timings sum across threads — an
operator can report 4x the query's wall clock on four cores. Two open upstream
issues make this plausible as an engine limitation:
[duckdb/duckdb#14087](https://github.com/duckdb/duckdb/issues/14087),
[duckdb/duckdb#18330](https://github.com/duckdb/duckdb/issues/18330).

### `ST_Union_Agg` over single-row groups still costs

Grouping the division areas by `division_id` costs 30.8s with `ST_Union_Agg`
against 2.6s with `any_value` over the same decoded input — and every one of the
1,071,108 groups holds exactly one row, so nothing is merged. Spatially it is a
no-op: `ST_Equals` holds for every row, vertex counts, validity and geometry
types are all unchanged. It does re-serialize, normalizing ring order, so the WKB
bytes differ for all 38,019 MULTIPOLYGONs.

**Applies to**: `garganorn/sql/overture_division_import.sql` (`merged_areas` CTE)

**Why it matters**: the cost is time, not the memory pressure the shape suggests.
Any equivalence check on a change here has to compare with `ST_Equals` — a
checksum reads the re-serialized multipolygons as a regression.

### Macro bodies are validated eagerly — LOAD extensions first

A macro body is validated at `CREATE MACRO` time, not at call time. So
`CREATE OR REPLACE MACRO qk_env(qk) AS ST_MakeEnvelope(...)` throws
`Catalog Error: Scalar Function with name "st_makeenvelope" is not in the catalog`
immediately if `INSTALL spatial; LOAD spatial` has not run on that connection.

Correct order on any ephemeral connection: load the extension, create the macros,
then run the SQL that uses them.

**Applies to**: `covering.py:_load_qk_env_macros`; any standalone runner of a
`.sql` that references a macro

**Why it matters**: putting `LOAD spatial` inside the `.sql` that runs *after* the
macros are created is too late, and the error names the function rather than the
ordering.

### `ST_QuadKey` wraps past the Mercator limit instead of clamping

The bug is southern-only. At -85.05112877980659 the Mercator fraction reaches
1.0, so the tile row works out to `2**zoom` — one past the last — and DuckDB
masks it back to row 0: `ST_QuadKey(0, -85.05112877980659, 17)` returns a
*northern* quadkey, as does any latitude below it. The northern limit lands in
row 0 too, but there row 0 is the right answer, so nothing looks wrong.
Coordinates are never rejected, so the failure is silent either way.

**Applies to**: every `ST_QuadKey` call; garganorn routes them all through the
`qk17` macro in `qk_env_macro.sql`

**Why it matters**: clamping latitude to the Mercator limit does not avoid this —
the limit is itself the value that wraps. See the Coordinate System entry in
`design-constraints.md` for the latitude garganorn clamps to instead.

### No native `json_strip_nulls()`

The custom `strip_json_nulls()` helper exists only because DuckDB has no native
equivalent ([PR #21748](https://github.com/duckdb/duckdb/pull/21748)). It may
fail on JSON keys containing `{`, `}`, `"`, or `,`. No failures observed in
practice.

**Applies to**: `garganorn/sql/overture_place_export_tiles.sql`,
`garganorn/sql/overture_division_export_tiles.sql`

**Why it matters**: replace it with the native function when that lands rather
than hardening the workaround.

### Version spread — dev pins 1.5.1, production runs 1.4.4

All SQL must run unchanged on both. Consequences:

- `ATTACH 'p' AS x (READ_ONLY)` is the only attach form that parses on both
  (`... READ_ONLY AS` parses on neither).
- `COPY`'s `KV_METADATA` option is 1.4+ only, which is why parquet/DB metadata
  lives in JSON sidecars (`.meta.json`, `_meta.json`, `manifest.json`) rather
  than embedded key-value metadata.
- `map_filter` does not exist — use
  `map_from_entries(list_filter(map_entries(m), e -> ...))`.
- The `.transform()` method syntax is unsupported — use `list_transform(list, fn)`.
- QuackOSM breaks on 1.5.x (GEOMETRY type conflict).

**Applies to**: all SQL files; run `.venv/bin/pytest` to test against the dev pin.

---

## GeoParquet

### A column of entirely NULL geometry reads back as BLOB

Parquet has no geometry type. DuckDB's `GEOMETRY` is stored as a `BYTE_ARRAY` of
WKB, and the only thing marking it as geometry is one file-level metadata key,
`geo` (GeoParquet). DuckDB writes that key only when the column has something to
describe.

**A file whose geometry column is entirely NULL gets no `geo` key, so
`read_parquet` types the column `BLOB`** — with the spatial extension fully
loaded. Verified on 1.4.4:

```
1 row, geom NULL              -> no 'geo' key -> BLOB
1 row, geom = a point         -> 'geo' key    -> GEOMETRY
2 rows, one NULL one a point  -> 'geo' key    -> GEOMETRY
```

One non-NULL value anywhere in the file is enough; zero is the only failing case.

**Why it matters**: this bites at *bind* time, not row time. `ST_Covers(BLOB,
GEOMETRY)` fails to resolve before any row is examined, so a partition whose
predicate would have matched nothing still kills the query. Recovery at the read
site (`SELECT * REPLACE (CAST(geom AS GEOMETRY) AS geom)`, or `ST_GeomFromWKB`)
works and is a no-op when the column already typed correctly.

Two independent routes produce a BLOB and are easy to conflate: the spatial
extension not being loaded on read (documented, obvious) and this one (per-file,
contents-dependent, silent). A doc mentioning only the first is not evidence the
second was considered.

The general form: **a parquet artifact's column types are a property of the data
written, not of the schema that produced it.** Any "the schema is X" claim about a
parquet artifact needs checking against a file that exercises the all-NULL case.

A read that never selects the geometry column doesn't need the spatial extension
either: `stage_division_tile_references` opens its connection with no `LOAD
spatial` and reads `covering/*.parquet` selecting only `tile_qk` and
`boundary_id`, never `geom`.

---

## Geographic data

### Antimeridian bboxes are two lobes

Where a bbox filter is derived from a geometry's min/max longitude,
`min_longitude > max_longitude` means the feature crosses ±180°, and a naive
`BETWEEN` drops it. The correct filter is `(lon >= min OR lon <= max)`, or a
tile-seed set built as the union of the two lobes.

**Applies to**: `stages.py` (`bboxes_intersect` wrap branches) and
`covering_seed.sql`'s join implement this. The import-side bbox filter
does not, so features crossing ±180° are dropped there.

**Why it matters**: it affects Pacific data — eastern Russia, Fiji, Antarctica. A
global build makes it reachable where a CONUS-bounded one did not.

---

## Overture Maps

### Old releases age off S3

Overture removes old releases from its public S3 bucket
(`overturemaps-us-west-2`). A release cached on disk months ago may no longer be
downloadable — a prefix cached locally returned `KeyCount=0` while only the two
newest releases were still listable.

List current releases:

```
curl -s "https://overturemaps-us-west-2.s3.amazonaws.com/?list-type=2&prefix=release/&delimiter=/" \
  | grep -oE 'release/[0-9][^/<]+/'
```

Themes live under `release/<rel>/theme=<t>/type=<ty>/`. Places =
`theme=places/type=place`; divisions = `theme=divisions/type={division,division_area}`.

**Why it matters**: to reproduce a data run exactly, keep the cached parquet — you
cannot rely on re-downloading it. Never cache two releases at once: the config
globs across them and double-counts.

### `download-overture.sh` is global by default, with no bbox filter

It unconditionally lists and downloads every `places` part-file for a release.
`--divisions-only` restricts to `division`/`division_area`.

**Why it matters**: cache size does not imply geographic scope in either
direction. A small Overture cache may be genuinely global-but-cheap, or split
across two release dates because a pinned release aged off mid-way. Check the
flags actually used rather than inferring from size.

---

## QuackOSM

### Tags are `MAP(VARCHAR, VARCHAR)`, not `VARCHAR[]`

`tags['key']` returns the value directly. **`tags['key'][1]` extracts the first
character** of that value rather than the first element of a list — silently, with
no error.

**Applies to**: `garganorn/sql/osm_import.sql`, `garganorn/sql/overture_place_export_tiles.sql`,
`garganorn/sql/overture_division_export_tiles.sql`

**Why it matters**: the `[1]` form is what a reader familiar with array-valued
tags will write, and it produces plausible one-character garbage instead of
failing.

### Temp disk is ~2.4x PBF size

Not the 10x an early estimate assumed.
