# Query Optimization Plan

Captures the concrete optimization plan for the next implementation session.
Written against commit d99d637 (Jaro-Winkler scoring, main branch).

## Implementation Status

**Completed (this pass):**
- Optimization 2 (norm_name): Pre-computed `lower(strip_accents(name))` stored in `name_index`. All query paths use `norm_name`/`$norm_q` instead of runtime normalization. OSM trigram generation bug fixed (missing `strip_accents`).
- Optimization 4 (schema normalization): Display columns stripped from `name_index` (now 5 columns: trigram, id, name, norm_name, importance). Text-only query paths JOIN `places` after scoring. Overture text results now return real addresses. OSM text results now return primary_category.

**Deferred:**
- Optimization 1 (min_trigram_hits): Threshold formula needs empirical validation against production queries.

**Skipped:**
- Optimization 3 (ART index): Zonemaps on sorted column are sufficient; selectivity threshold prevents use on multi-trigram queries.

**Remaining:**
- Optimization 5 (trigram IDF selection): Depends on 1-4 being stable.

---

## Performance Baseline

Server: garganorn-1, 4 cores, 64 GB RAM, DuckDB. FSQ: 38.7M places, ~616M trigram rows.

| Query | Tokens | Current (ms) |
|-------|--------|-------------|
| Starbucks (text) | 1 | 191 |
| Chez Panisse (text) | 2 | 312 |
| Golden Gate Bridge (text) | 3 | 477 |
| SF Airport (text) | 3 | 662 |
| Palo Alto Caltrain (text) | 3 | 526 |
| SF Intl Airport (text) | 4 | 1398 |
| UCSF (text) | 5 | 1369 |
| UCSF Medical Center (text) | 6 | 2126 |
| Spatial + 3-token | 3 | 176–220 |
| Spatial + 1-token | 1 | 83 |

The spatial path is fast because the bbox filter prunes candidates aggressively before the trigram scan. The text-only path has no spatial pre-filter; the trigram `IN` clause is the only gate, which produces enormous candidate sets on common trigrams.

---

## Implementation Scope — Current Pass

This pass implements **Optimization 2 (norm_name)** and **Optimization 4 (schema normalization)**.

**Optimization 1 (min_trigram_hits) is deferred.** The threshold formula `max(1, len(trigrams) // 3)` requires empirical validation against production data before committing. Abbreviations, partial matches, and queries where trigram overlap is inherently low (e.g., short tokens, non-Latin scripts) may produce valid JW matches that this filter would silently drop. The formula needs to be tested against a representative sample of production queries before it is safe to use as a hard cut.

The **OSM trigram bug fix** (missing `strip_accents` in trigram generation) is included in this pass as part of the import script changes for Optimization 2.

---

## Optimization 1: Minimum Trigram Match Count (highest impact)

### Problem

`WHERE trigram IN ($g0, $g1, ...)` matches every row that contains ANY of the query trigrams. "Golden Gate Bridge" generates 16 trigrams; at threshold=1 this returns ~993K candidates. At threshold=3 it drops to ~65K — a 15× reduction. The current query has no minimum match requirement.

### Mechanism

Replace `SELECT DISTINCT` in the candidates CTE with `GROUP BY` + `HAVING`. This collapses the multiple per-place rows (one per matching trigram) into a single row, counting how many distinct query trigrams matched, and drops any place below the threshold.

**Threshold formula** (compute in Python before query construction):

```python
min_trigram_hits = max(1, len(trigrams) // 3)
```

Single-trigram queries stay at threshold=1 (no change). Three-token queries require at least one-third of query trigrams to match.

### SQL change — candidates CTE pattern (applies to all 6 query methods)

**Before:**
```sql
WITH candidates AS (
    SELECT DISTINCT {id_col}, name, {display_cols}, importance
    FROM name_index
    WHERE trigram IN ({placeholders})
      AND importance >= $importance_floor
)
```

**After:**
```sql
WITH candidates AS (
    SELECT {id_col}, name, {display_cols}, importance,
           COUNT(DISTINCT trigram) AS trigram_hits
    FROM name_index
    WHERE trigram IN ({placeholders})
      AND importance >= $importance_floor
    GROUP BY {id_col}, name, {display_cols}, importance
    HAVING COUNT(DISTINCT trigram) >= {min_trigram_hits}
)
```

`trigram_hits` is not used downstream; include it only if it proves useful for scoring later.

### Per-method changes in `database.py`

There are 6 query methods across 3 classes. All follow the same pattern.

**`FoursquareOSP`** (`garganorn/database.py`, lines 301–493):
- `_query_trigram_text` — single-token path (line 368) and multi-token path (line 314): both candidates CTEs
- `_query_trigram_spatial` — single-token (line 464) and multi-token (line 405): both candidates CTEs

**`OvertureMaps`** (lines 600–771):
- `_query_trigram_text` — single-token (line 662) and multi-token (line 612)
- `_query_trigram_spatial` — single-token (line 748) and multi-token (line 695)

**`OpenStreetMap`** (lines 882–1051):
- `_query_trigram_text` — single-token (line 943) and multi-token (line 894)
- `_query_trigram_spatial` — single-token (line 1029) and multi-token (line 975)

**Python change** in `nearest()` (line 229–244): compute `min_trigram_hits` and add to params:

```python
trigrams = self._compute_trigrams(q)
for i, tri in enumerate(trigrams):
    params[f"g{i}"] = tri
params["min_trigram_hits"] = max(1, len(trigrams) // 3)
```

Then reference `$min_trigram_hits` in the HAVING clause rather than a hard-coded literal.

### FSQ spatial candidates CTE note

The spatial path joins `places` to `name_index` instead of reading `name_index` alone. The GROUP BY must include all non-aggregated columns. For FSQ spatial multi-token (line 405–416):

```sql
WITH candidates AS (
    SELECT p.fsq_place_id, p.name, p.latitude, p.longitude,
           p.address, p.locality, p.postcode, p.region, p.country,
           p.geom, n.importance,
           COUNT(DISTINCT n.trigram) AS trigram_hits
    FROM places p
    JOIN name_index n ON p.fsq_place_id = n.fsq_place_id
    WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
      AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
      AND n.trigram IN ({placeholders})
      AND n.importance >= $importance_floor
    GROUP BY p.fsq_place_id, p.name, p.latitude, p.longitude,
             p.address, p.locality, p.postcode, p.region, p.country,
             p.geom, n.importance
    HAVING COUNT(DISTINCT n.trigram) >= $min_trigram_hits
)
```

### Expected impact

3–10× speedup on multi-token text-only queries. Single-token queries unchanged. Spatial queries already fast; marginal improvement on the name_index scan.

---

## Optimization 2: Pre-computed `norm_name`

### Problem

Every candidate row currently computes `lower(strip_accents(name))` at query time. With 65K–993K candidates per query, this is repeated string normalization that could be done once at import time.

Additionally, `$q` is normalized inside the SQL: `lower(strip_accents($q))` is computed once per query per occurrence, which is minor but eliminates a function call.

### Import changes

**FSQ** (`scripts/import-fsq-extract.sh`), lines 196–239:

The `name_prep` CTE already computes `norm_name` (line 202). It is not carried through the `trigrams` CTE to the final SELECT. Fix: add `norm_name` to the `trigrams` CTE column list and to the final `select` from `trigrams`.

```sql
-- In name_prep CTE: already present
lower(strip_accents(name)) as norm_name,

-- In trigrams CTE: ADD norm_name
select distinct
    substr(np.norm_name, pos, 3) as trigram,
    np.fsq_place_id,
    np.name,
    np.norm_name,           -- ADD THIS
    np.latitude,
    ...

-- In final select from trigrams: ADD norm_name
select
    trigram,
    fsq_place_id,
    name,
    norm_name,              -- ADD THIS
    latitude,
    ...
```

**Overture** (`scripts/import-overture-extract.sh`), lines 183–209:

Same pattern. `norm_name` is in `name_prep` (line 188) but missing from `trigrams` CTE (lines 195–207) and the final select (line 207).

**OSM** (`scripts/import-osm.sh`), lines 470–493:

The name_index build here is different — it uses a flat INSERT with `lower(substr(name, pos, 3))` as the trigram (line 486), and does not compute `norm_name` at all. Changes needed:

1. Add `norm_name` column to the `CREATE TABLE IF NOT EXISTS name_index` DDL (line 471).
2. In the INSERT, compute `norm_name` as `lower(strip_accents(name))`:

```sql
INSERT INTO name_index
SELECT osm_type || osm_id::VARCHAR AS rkey,
       name,
       lower(strip_accents(name)) AS norm_name,   -- ADD THIS
       latitude::decimal(10,6)::varchar AS latitude,
       longitude::decimal(10,6)::varchar AS longitude,
       importance,
       lower(substr(lower(strip_accents(name)), pos, 3)) AS trigram  -- FIX: use norm_name for trigram
FROM (
    SELECT osm_type, osm_id, name, latitude, longitude, importance,
           generate_series AS pos
    FROM places, generate_series(1, length(name) - 2)
    WHERE name IS NOT NULL AND length(name) >= 3
) t;
```

Note: OSM currently uses `lower(substr(name, pos, 3))` (line 486), which does not strip accents. The trigram generation should use the accent-stripped form for consistency. Fix this in the same pass.

### Query changes in `database.py`

Two locations per query method:

1. **Full-string JW**: replace `jaro_winkler_similarity(lower(strip_accents($q)), lower(strip_accents(c.name)))` with `jaro_winkler_similarity($norm_q, c.norm_name)`.

2. **Token-level JW** in `name_tokens` and `token_scores` CTEs: replace `lower(strip_accents(r.name))` with `r.norm_name` in the `string_split` call, and use `norm_name` as the source for `nt`.

**Python change** in `nearest()`: pre-normalize the query string and add to params:

```python
norm_q = Database._strip_accents(q.lower())
params["norm_q"] = norm_q
# tokens already use strip_accents at line 238; they can become:
tokens = [t for t in norm_q.split() if t][:Database.MAX_QUERY_TOKENS]
```

The `candidates` CTE SELECT must include `norm_name` for downstream CTEs to reference it.

### Files to modify

- `garganorn/database.py` — all 6 `_query_trigram_*` methods plus `nearest()`
- `scripts/import-fsq-extract.sh`
- `scripts/import-overture-extract.sh`
- `scripts/import-osm.sh`
- `tests/conftest.py` — test fixtures must include the `norm_name` column

### Expected impact

Modest CPU reduction per candidate row. When eventually combined with optimization 1, the absolute savings will be larger because fewer rows survive the candidates filter.

---

## Implementation order — current pass (Optimization 2 only)

1. Add `norm_name` to all 3 import scripts (FSQ and Overture: add to trigrams CTE and final SELECT; OSM: add column to `CREATE TABLE` DDL and compute in INSERT).
2. Fix OSM trigram generation to use `strip_accents` (in the same import script change).
3. Update all 6 query methods in `database.py` to use `norm_name`/`$norm_q` instead of computing `lower(strip_accents(...))` at query time.
4. Update test fixtures in `tests/conftest.py` to include the `norm_name` column.
5. Re-run import scripts to rebuild databases with `norm_name` column.
6. Benchmark against baseline table above.

---

## Optimization 3: ART Index on `trigram` Column

### Status: Skipped

**Zonemaps are sufficient.** `name_index` is `ORDER BY trigram`, so DuckDB's zonemaps (min/max per row group) identify the exact row groups containing each trigram. With ~40K distinct trigrams across ~5,000 row groups, each trigram's rows are contiguous within 1–2 row groups. The ART index would add row-level precision, but DuckDB's vectorized scan is already efficient at filtering within a row group.

**Selectivity threshold.** DuckDB uses an ART index scan only when estimated matches fall below `max(2048, 0.001 × table_cardinality)`. For 616M rows, the threshold is 616K. A multi-trigram `IN (...)` query with 40 trigrams estimates ~616K matches — at or above the threshold. The queries that most need optimization are exactly the ones where the planner would not use the index.

**Memory overhead.** ART indexes on 616M rows are not buffer-managed — they must fit entirely in RAM and are not subject to DuckDB's eviction policy. The production server has 64 GB; uncontrolled memory commitment at this scale is a liability.

**Wrong bottleneck.** The slow path is candidate set size and per-row JW scoring, not the trigram scan itself. Optimizations 1, 2, and 4 address those directly.

---

## Optimization 4: Normalize `name_index` Schema

**Prerequisite**: Verify 1+2 deliver the expected speedup and the new schema is stable before undertaking this larger change.

### Problem

`name_index` stores display columns (`address`, `locality`, `postcode`, `region`, `country`, `latitude`, `longitude`) repeated for every trigram of a name. These columns serve only the text-only query path. The spatial path already joins `places`. Removing them:
- Reduces row width, allowing more rows per page → better scan throughput.
- Reduces total table size on a 616M-row table.
- Requires the text-only path to join `places` for display columns.

### Proposed `name_index` schema (all data sources)

```
trigram      VARCHAR
{id_col}     VARCHAR  (fsq_place_id / id / rkey)
name         VARCHAR
norm_name    VARCHAR
importance   INTEGER
```

### Import changes — all 3 scripts

Remove all display columns from the `trigrams` CTE and the final SELECT:
- FSQ: remove `latitude`, `longitude`, `address`, `locality`, `postcode`, `region`, `country`
- Overture: remove `latitude`, `longitude` (Overture `name_index` already omits address; geometry is in places)
- OSM: remove `latitude`, `longitude`

### Query changes — text-only paths (both single and multi-token)

After scoring, JOIN `places` to retrieve display columns. Example for FSQ single-token text path:

```sql
WITH candidates AS (
    SELECT fsq_place_id, name, norm_name, importance,
           COUNT(DISTINCT trigram) AS trigram_hits
    FROM name_index
    WHERE trigram IN ({placeholders})
      AND importance >= $importance_floor
    GROUP BY fsq_place_id, name, norm_name, importance
    HAVING COUNT(DISTINCT trigram) >= $min_trigram_hits
),
scored AS (
    SELECT fsq_place_id, name, importance,
           jaro_winkler_similarity($norm_q, norm_name) AS score
    FROM candidates
    WHERE jaro_winkler_similarity($norm_q, norm_name) >= {self.JW_THRESHOLD}
)
SELECT
    s.fsq_place_id AS rkey,
    s.name,
    p.latitude::decimal(10,6)::varchar AS latitude,
    p.longitude::decimal(10,6)::varchar AS longitude,
    p.address,
    p.locality,
    p.postcode,
    p.region,
    p.country,
    0 AS distance_m,
    s.score
FROM scored s
JOIN places p ON s.fsq_place_id = p.fsq_place_id
ORDER BY s.score DESC, s.importance DESC
LIMIT $limit
```

The multi-token path is the same: the final SELECT joins `places` instead of reading display columns from candidates.

**Spatial paths**: already join `places`; just remove references to display columns in the candidates CTE (which no longer carries them).

### Files to modify

All 3 import scripts and all 6 query methods in `garganorn/database.py`.

### Expected impact

Smaller table → faster full scans. The added JOIN is on a small scored result set (at most a few hundred rows), so it adds negligible cost.

---

## Optimization 5: Trigram IDF Selection (additive, lower priority)

### Mechanism

Pre-compute per-trigram document frequency at import time:

```sql
CREATE TABLE trigram_stats AS
SELECT trigram, COUNT(DISTINCT {id_col}) AS doc_freq
FROM name_index
GROUP BY trigram;
```

At server startup, load `trigram_stats` into a Python dict keyed by trigram. At query time, sort query trigrams by ascending `doc_freq` and take only the K most selective:

```python
K = max(5, len(trigrams) // 2)
trigrams_sorted = sorted(trigrams, key=lambda t: idf_map.get(t, 0))
trigrams = trigrams_sorted[:K]
```

This is additive with optimization 1: use selective trigrams AND require a minimum match count.

### Files to modify

- All 3 import scripts: add `trigram_stats` table creation
- `garganorn/database.py`: load `trigram_stats` at startup (in `connect()`), filter trigrams in `nearest()`

### Note

Implement after 1–4 are stable. The IDF table adds import time and memory at startup. Only worthwhile if 1–4 leave a meaningful gap.

---

## Implementation Sequence

| Step | Optimizations | Files | Gate |
|------|--------------|-------|------|
| 1 | #2 (norm_name) + OSM trigram bug fix | `database.py` (6 methods + `nearest()`), 3 import scripts, `tests/conftest.py` | Rebuild DBs, benchmark |
| 2 | #1 (min_trigram_hits) | `database.py` (`nearest()` + 6 methods) | Empirical threshold validation against production data |
| 3 | #4 (normalize schema) | 3 import scripts, `database.py` (6 methods) | Requires steps 1–2 stable |
| 4 | #5 (IDF selection) | 3 import scripts, `database.py` | After steps 1–3 stable |

Note: Optimization 3 (ART index) is skipped — see that section for rationale.

Do not proceed to step 3 (schema normalization) without confirming steps 1–2 are correct and the speedup is validated. Schema normalization requires a full re-import on all data sources.

Do not implement step 2 (min_trigram_hits) without first running the threshold formula against a representative sample of production queries to confirm it does not produce false negatives on abbreviations or partial matches.
