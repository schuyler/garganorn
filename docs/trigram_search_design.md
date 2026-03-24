# Trigram Search Design

Replaces Double Metaphone retrieval with character trigram retrieval.
Replaces token-level Jaro-Winkler scoring with full-string JW scoring.

## 1. Index Schema

### FSQ `name_index`

```sql
CREATE TABLE name_index (
    trigram   VARCHAR,   -- 3-char substring of lower(strip_accents(name))
    fsq_place_id VARCHAR,
    name     VARCHAR,    -- original place name (for JW scoring)
    latitude VARCHAR,
    longitude VARCHAR,
    address  VARCHAR,
    locality VARCHAR,
    postcode VARCHAR,
    region   VARCHAR,
    country  VARCHAR,
    importance INTEGER   -- 0-100 normalized score from places table
)
```

### Overture `name_index`

```sql
CREATE TABLE name_index (
    trigram   VARCHAR,   -- 3-char substring of lower(strip_accents(names.primary))
    id       VARCHAR,
    name     VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    importance INTEGER   -- 0-100 normalized score from places table
)
```

Both tables: one row per distinct `(trigram, place_id)` pair. Sorted by
`trigram` for DuckDB zone map pruning (`ORDER BY trigram`).

Removed columns (vs. current schema): `dm_code`, `token`, `n_place_tokens`,
`n_place_codes`.

## 2. Import Script SQL

### 2.1 FSQ importance pre-computation (runs before name_index build)

Importance is computed as a normalized 0–100 INTEGER and stored in the
`places` table before the name_index is built. The name_index build then
reads `importance` directly from `places`.

```sql
-- Step 1: add importance column to places
ALTER TABLE places ADD COLUMN importance INTEGER DEFAULT 0;
UPDATE places SET importance = sub.importance
FROM (
    SELECT
        p.fsq_place_id,
        round(
            60 * least(coalesce(ln(1 + c.pt_count), 0) / 10.0, 1.0)
          + 40 * least(coalesce(max(idf.idf_score), 0) / 18.0, 1.0)
        )::INTEGER AS importance
    FROM places p
    LEFT JOIN read_parquet('${density_file}') c
        ON c.level = 12
        AND c.cell_id = s2_cell_parent(s2_cellfromlonlat(p.longitude, p.latitude), 12)
    LEFT JOIN read_parquet('${idf_file}') idf
        ON idf.collection = 'foursquare'
        AND idf.category = ANY(p.fsq_category_ids)
    GROUP BY p.fsq_place_id, c.pt_count
) sub
WHERE places.fsq_place_id = sub.fsq_place_id;

-- Step 2: build name_index (reads importance from places)
CREATE TABLE name_index AS
WITH name_prep AS (
    SELECT
        fsq_place_id,
        name,
        lower(strip_accents(name)) AS norm_name,
        latitude::decimal(10,6)::varchar AS latitude,
        longitude::decimal(10,6)::varchar AS longitude,
        address, locality, postcode, region, country,
        coalesce(importance, 0) AS importance
    FROM places
    WHERE name IS NOT NULL AND length(name) > 0
),
trigrams AS (
    SELECT DISTINCT
        substr(np.norm_name, pos, 3) AS trigram,
        np.fsq_place_id,
        np.name,
        np.latitude,
        np.longitude,
        np.address,
        np.locality,
        np.postcode,
        np.region,
        np.country,
        np.importance
    FROM name_prep np
    CROSS JOIN generate_series(1, length(np.norm_name) - 2) AS gs(pos)
    WHERE length(np.norm_name) >= 3
)
SELECT
    trigram,
    fsq_place_id,
    name,
    latitude,
    longitude,
    address,
    locality,
    postcode,
    region,
    country,
    importance
FROM trigrams
ORDER BY trigram;
```

The 0–100 formula weights:
- 60% density: `min(ln(1 + pt_count) / 10.0, 1.0)` — soft cap at pt_count ~22000
- 40% IDF: `min(max_category_idf / 18.0, 1.0)` — soft cap slightly above observed max

Notes:
- `DISTINCT` in the `trigrams` CTE deduplicates `(trigram, fsq_place_id)`
  pairs. A name like "banana" produces trigram "ana" twice from positions 2 and
  4; `DISTINCT` collapses these.
- Names shorter than 3 characters produce zero trigrams and are excluded. This
  is acceptable — such names are rare and unsearchable via trigrams anyway.
- `generate_series(1, length(norm_name) - 2)` produces positions 1..N-2 for
  DuckDB's 1-based `substr`.

### 2.2 Overture equivalent

Same two-step pattern for Overture: pre-compute importance into `places`,
then read it during name_index build. Overture uses `categories.primary`
(single category) rather than the `fsq_category_ids` array.

**Overture without density:** Same pattern — omit density/IDF joins and
leave importance at the default 0.

### 2.2 (old) Other three branches (pre-normalization design)

These branches (density-only, IDF-only, neither) are superseded by the
two-step pre-computation approach above. Density and IDF are now both
required inputs; the import scripts do not have no-density or no-IDF
fallback branches.

### 2.3 Extension changes

- `splink_udfs` is **no longer installed or loaded** at import time.
- `geography` is still needed when `has_density=true` (for `s2_cellfromlonlat`).
- When `has_density=false` AND `has_idf=false`, no community extensions are needed at import time.

### 2.4 Shell script line-level changes

**`scripts/import-fsq-extract.sh`**

Lines 143–147 (density+IDF branch, extension block):
```
# REMOVE:
.print "Loading extensions for phonetic index + density..."
install splink_udfs from community;
load splink_udfs;
# KEEP: geography install/load that follows (needed for s2_cellfromlonlat)
```

Lines 150–212 (density+IDF branch, CTE block):
- Remove the `tokens` CTE (lines 150–165, the `string_split`/`unnest` block).
- Remove the `codes` CTE (lines 166–189, the `double_metaphone`/`dm_code`/`token` block).
- Remove the `place_code_counts` CTE (lines 192–196, the `n_place_codes` block).
- Replace the `SELECT ... FROM codes fc ...` final query (lines 197–212) with the `name_prep` + `trigrams` CTEs and final SELECT shown in §2.1.
- The `place_idf` CTE (injected between `name_prep` and `trigrams`) replaces no existing CTE — it was already present for IDF scoring; keep it unchanged.

Lines 216–276 (no-density branch): same removals and replacements as above.

**`scripts/import-overture-extract.sh`**

Lines 161–165 (density+IDF branch, extension block):
```
# REMOVE:
.print "Loading extensions for phonetic index + density..."
install splink_udfs from community;
load splink_udfs;
```

Lines 168–220 (density+IDF branch, CTE block): same CTE replacements as FSQ — remove `tokens`, `codes`, `place_code_counts`; replace final SELECT with the Overture trigram query from §2.1.

Lines 224–274 (no-density branch): same removals and replacements.

### 2.5 Known limitation: SELECT DISTINCT cost at scale

The `SELECT DISTINCT` on `(trigram, place_id)` forces a sort or hash aggregate
over the full trigram set. For a dataset with N places and average name length
L, this processes roughly `N * (L-2)` rows. At 321K places with average name
length ~15, that is ~4M rows — manageable. At full FSQ scale (~100M places),
this becomes ~1.3B rows. The import is a one-time batch operation, so this
cost is acceptable. If it becomes a bottleneck, the `DISTINCT` can be replaced
with a `GROUP BY` in the trigrams CTE.

## 3. database.py Changes

### 3.1 Removed

- `_compute_phonetic_codes()` — entire method
- `_tokenize_query()` — entire method (no longer needed)
- `_build_name_index_join()` — static method for token self-joins
- `_JOIN_ALIASES` constant
- `MAX_QUERY_TOKENS` constant
- `has_phonetic_index` attribute
- `splink_udfs` extension loading in `connect()`
- All `_query_phonetic_*` methods (4 total, 2 per subclass)
- All `_query_name_index` methods (2 total, 1 per subclass)
- Token-level scoring expressions, length penalty, `greatest()` blend
- `process_nearest` no longer pops `jaccard` (already dead code)

### 3.2 New: `_compute_trigrams(q)`

Pure Python, lives on `Database` base class.

```python
import unicodedata

@staticmethod
def _strip_accents(s: str) -> str:
    """Strip accents from a string, matching DuckDB's strip_accents()."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )

MAX_QUERY_TRIGRAMS = 50  # class-level constant on Database

@staticmethod
def _compute_trigrams(q: str) -> list:
    """
    Compute character trigrams from a query string.
    Lowercases, strips accents, then generates all 3-char substrings.
    Returns a deduplicated, sorted list capped at MAX_QUERY_TRIGRAMS.
    """
    norm = Database._strip_accents(q.lower())
    if len(norm) < 3:
        return []
    trigrams = sorted({norm[i:i+3] for i in range(len(norm) - 2)})
    return trigrams[:Database.MAX_QUERY_TRIGRAMS]
```

Notes:
- `sorted({...})` is used instead of `list({...})` to ensure deterministic
  iteration order. Set iteration in Python is non-deterministic, which would
  make the `$g0, $g1, ...` parameter bindings non-reproducible across runs.
- `MAX_QUERY_TRIGRAMS = 50` caps the trigram list. A name of N characters
  produces at most N-2 unique trigrams. At 50 trigrams, queries up to
  ~17 characters are completely unaffected (a 17-char query produces at most 15
  unique trigrams). Longer queries are truncated to the first 50 unique trigrams
  in sorted order. This prevents runaway `IN (...)` clauses on very long or
  adversarial inputs.
- `_strip_accents` uses NFD normalization + Mn category filtering, which
  matches DuckDB's `strip_accents()` behavior for Latin scripts.

### 3.3 Capability detection in `connect()`

Replace the current phonetic detection with trigram detection:

```python
# Detect trigram index in name_index
self.has_trigram_index = False
if self.has_name_index:
    columns = self.conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'name_index' AND column_name = 'trigram'"
    ).fetchall()
    self.has_trigram_index = len(columns) > 0
```

Remove: `has_phonetic_index` attribute, `splink_udfs` loading block.

### 3.4 `nearest()` method changes

Replace token binding with trigram binding:

```python
def nearest(self, latitude=None, longitude=None, q=None, expand_m=5000, limit=50):
    self.connect()
    params: SearchParams = {"limit": limit}
    if latitude is not None and longitude is not None:
        # ... bounding box expansion (unchanged) ...
        params.update({...})  # centroid, xmin, ymin, xmax, ymax
    if q:
        params["q"] = q
        trigrams = self._compute_trigrams(q)
        for i, tri in enumerate(trigrams):
            params[f"g{i}"] = tri
    else:
        trigrams = []
    result = self.execute(self.query_nearest(params, trigrams=trigrams), params)
    return [self.process_nearest(item) for item in result]
```

Trigram parameters use prefix `g` (for trigram) to avoid collision with other
parameter namespaces. The old `t0..tN` prefix is freed.

### 3.5 Query routing in `query_nearest()`

The routing logic simplifies. For both `FoursquareOSP` and `OvertureMaps`:

```python
def query_nearest(self, params: SearchParams, trigrams: list = None):
    trigrams = trigrams or []
    if params.get("centroid"):
        if params.get("q") and self.has_trigram_index:
            return self._query_trigram_spatial(params, trigrams)
        # spatial-only or spatial+ILIKE fallback
        ...
    elif params.get("q"):
        if self.has_trigram_index:
            return self._query_trigram_text(params, trigrams)
        if self.has_name_index:
            # Legacy token self-join path — can be removed once
            # all databases are rebuilt with trigram schema.
            return self._query_name_index(params)
        # ILIKE fallback
        ...
```

Decision: Keep `_query_name_index` (token self-join) as a fallback for
databases that have the old schema. It will be hit only when `name_index`
exists but has no `trigram` column. This avoids a hard migration requirement.
If you prefer a clean break, remove it and `_build_name_index_join` entirely.

**Complete FSQ routing example** (Overture follows the same pattern):

```python
def query_nearest(self, params: SearchParams, trigrams: list = None):
    trigrams = trigrams or []
    if params.get("centroid"):
        if params.get("q") and self.has_trigram_index and trigrams:
            return self._query_trigram_spatial(params, trigrams)
        elif params.get("q") and self.has_name_index:
            return self._query_name_index(params)  # legacy token self-join
        elif params.get("q"):
            return self._query_spatial_ilike(params)
        else:
            return self._query_spatial_only(params)
    elif params.get("q"):
        if self.has_trigram_index and trigrams:
            return self._query_trigram_text(params, trigrams)
        if self.has_name_index:
            return self._query_name_index(params)  # legacy token self-join
        return self._query_ilike(params)
    else:
        raise ValueError("query_nearest requires at least one of: lat/lon, q")
```

This replaces `has_phonetic_index` checks with `has_trigram_index`. The empty
`trigrams` guard (`and trigrams`) ensures that a sub-3-char query (which
produces no trigrams) falls through to the ILIKE path rather than issuing an
empty `IN ()` clause.

### 3.6 FoursquareOSP: `_query_trigram_text()`

Complete query method:

```python
def _query_trigram_text(self, params: SearchParams, trigrams: list) -> str:
    """
    Trigram retrieval + full-string JW scoring, text-only.
    trigrams: pre-computed list from nearest(), already bound as $g0..$gN.
    """
    if not trigrams:
        return ("SELECT NULL AS rkey, NULL AS name, NULL AS latitude, "
                "NULL AS longitude, NULL AS address, NULL AS locality, "
                "NULL AS postcode, NULL AS region, NULL AS country, "
                "0 AS distance_m WHERE false")

    trigram_placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))

    return f"""
        WITH candidates AS (
            SELECT DISTINCT
                fsq_place_id,
                name,
                latitude,
                longitude,
                address,
                locality,
                postcode,
                region,
                country,
                importance
            FROM name_index
            WHERE trigram IN ({trigram_placeholders})
            ORDER BY importance DESC
            LIMIT 5000
        )
        SELECT
            fsq_place_id AS rkey,
            name,
            latitude,
            longitude,
            address,
            locality,
            postcode,
            region,
            country,
            0 AS distance_m,
            jaro_winkler_similarity(lower($q), lower(name)) AS score
        FROM candidates
        WHERE jaro_winkler_similarity(lower($q), lower(name)) >= {self.JW_THRESHOLD}
        ORDER BY score DESC, importance DESC
        LIMIT $limit
    """
```

Key points:
- `SELECT DISTINCT` in the candidates CTE collapses multiple trigram matches
  for the same place into one row. Without it, a place matching 5 trigrams
  would appear 5 times. This works correctly because `importance` is a
  place-level attribute stored redundantly on every trigram row for the same
  place (all rows for a given `fsq_place_id` carry the same `importance`
  value). `DISTINCT` therefore always collapses to a single, consistent row.
  If importance ever became non-uniform across trigram rows (e.g., due to a
  bug), a `GROUP BY fsq_place_id, name, ... + max(importance)` would be
  safer, at the cost of verbosity.
- `LIMIT 5000` on the candidates CTE caps the retrieval phase. This is applied
  after `ORDER BY importance DESC`, so high-importance places are retained
  preferentially.
- Scoring is a single expression: `jaro_winkler_similarity(lower($q), lower(name))`.
- DuckDB's `jaro_winkler_similarity` is a built-in function (no extension needed).
- The `WHERE` clause in the outer SELECT uses the raw expression (not a
  column alias) because DuckDB does not allow `HAVING` without `GROUP BY`.
  Using `WHERE score >= threshold` would also work since `score` is a
  SELECT alias, but the explicit expression is clearer.

### 3.7 FoursquareOSP: `_query_trigram_spatial()`

```python
def _query_trigram_spatial(self, params: SearchParams, trigrams: list) -> str:
    """
    Spatial + trigram retrieval + full-string JW scoring.
    trigrams: pre-computed list from nearest(), already bound as $g0..$gN.
    """
    if not trigrams:
        return ("SELECT NULL AS rkey, NULL AS name, NULL AS latitude, "
                "NULL AS longitude, NULL AS address, NULL AS locality, "
                "NULL AS postcode, NULL AS region, NULL AS country, "
                "0 AS distance_m WHERE false")

    trigram_placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))

    return f"""
        WITH candidates AS (
            SELECT DISTINCT
                p.fsq_place_id,
                p.name,
                p.latitude::decimal(10,6)::varchar AS latitude,
                p.longitude::decimal(10,6)::varchar AS longitude,
                p.address,
                p.locality,
                p.postcode,
                p.region,
                p.country,
                ST_Distance_Sphere(p.geom, ST_GeomFromText($centroid))::integer AS distance_m,
                n.importance
            FROM places p
            JOIN name_index n ON p.fsq_place_id = n.fsq_place_id
            WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
              AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
              AND n.trigram IN ({trigram_placeholders})
            LIMIT 5000
        )
        SELECT
            fsq_place_id AS rkey,
            name,
            latitude,
            longitude,
            address,
            locality,
            postcode,
            region,
            country,
            distance_m,
            jaro_winkler_similarity(lower($q), lower(name)) AS score
        FROM candidates
        WHERE jaro_winkler_similarity(lower($q), lower(name)) >= {self.JW_THRESHOLD}
        ORDER BY score DESC, distance_m
        LIMIT $limit
    """
```

Note: The spatial query joins `places` and `name_index` because the bbox
filter is on `places.bbox`. The `SELECT DISTINCT` deduplicates rows where
multiple trigrams matched the same place. `distance_m` is computed once per
place thanks to the `DISTINCT`. As with the text query, this is correct
because `importance` is a place-level attribute (see §3.6).

### 3.8 OvertureMaps: `_query_trigram_text()`

Same structure as FSQ, with Overture column differences:

```python
def _query_trigram_text(self, params: SearchParams, trigrams: list) -> str:
    # trigrams pre-computed by nearest(), already bound as $g0..$gN
    if not trigrams:
        return ("SELECT NULL AS rkey, NULL AS name, NULL AS latitude, "
                "NULL AS longitude, NULL AS addresses, "
                "0 AS distance_m WHERE false")

    trigram_placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))

    return f"""
        WITH candidates AS (
            SELECT DISTINCT
                id,
                name,
                latitude,
                longitude,
                importance
            FROM name_index
            WHERE trigram IN ({trigram_placeholders})
            ORDER BY importance DESC
            LIMIT 5000
        )
        SELECT
            id AS rkey,
            name,
            latitude,
            longitude,
            NULL AS addresses,
            0 AS distance_m,
            jaro_winkler_similarity(lower($q), lower(name)) AS score
        FROM candidates
        WHERE jaro_winkler_similarity(lower($q), lower(name)) >= {self.JW_THRESHOLD}
        ORDER BY score DESC, importance DESC
        LIMIT $limit
    """
```

### 3.9 OvertureMaps: `_query_trigram_spatial()`

```python
def _query_trigram_spatial(self, params: SearchParams, trigrams: list) -> str:
    # trigrams pre-computed by nearest(), already bound as $g0..$gN
    if not trigrams:
        return ("SELECT NULL AS rkey, NULL AS name, NULL AS latitude, "
                "NULL AS longitude, NULL AS addresses, "
                "0 AS distance_m WHERE false")

    trigram_placeholders = ", ".join(f"$g{i}" for i in range(len(trigrams)))

    return f"""
        WITH candidates AS (
            SELECT DISTINCT
                p.id,
                p.names.primary AS name,
                st_y(st_centroid(p.geometry))::decimal(10,6)::varchar AS latitude,
                st_x(st_centroid(p.geometry))::decimal(10,6)::varchar AS longitude,
                p.addresses,
                ST_Distance_Sphere(p.geometry, ST_GeomFromText($centroid))::integer AS distance_m,
                n.importance
            FROM places p
            JOIN name_index n ON p.id = n.id
            WHERE p.bbox.xmin > $xmin AND p.bbox.ymin > $ymin
              AND p.bbox.xmax < $xmax AND p.bbox.ymax < $ymax
              AND n.trigram IN ({trigram_placeholders})
            LIMIT 5000
        )
        SELECT
            id AS rkey,
            name,
            latitude,
            longitude,
            addresses,
            distance_m,
            jaro_winkler_similarity(lower($q), lower(name)) AS score
        FROM candidates
        WHERE jaro_winkler_similarity(lower($q), lower(name)) >= {self.JW_THRESHOLD}
        ORDER BY score DESC, distance_m
        LIMIT $limit
    """
```

## 4. LIMIT 5000 Analysis

With full-name trigrams, the selectivity profile differs from DM codes:

- **Common trigrams** (e.g., `"the"`, `"ing"`, `"ion"`) will match many
  places, similar to common DM codes.
- **Cross-word trigrams** (e.g., `"a h"` from "pizza hut", `"za "` from
  "pizza ") are more selective because they encode word boundaries specific
  to the full name.
- A typical query "pizza hut" produces 6 trigrams: `piz, izz, zza, za , a h,
  hut`. The union of all matches is filtered to DISTINCT place_ids, then
  capped at 5000.

The LIMIT is applied after `ORDER BY importance DESC`, so popular/dense places
survive the cap preferentially. This is the same strategy as the current DM
code design.

**Recommendation:** Keep LIMIT 5000. The cross-word trigrams provide more
selectivity than DM codes for multi-word queries. For single-word queries,
the trigrams from a 5+ character word are already selective enough. Very
short queries (3-4 chars) produce 1-2 trigrams that may be common, but
5000 candidates is sufficient headroom for JW scoring to separate them.

If profiling shows 5000 is insufficient for specific query patterns, it can
be raised. The JW scoring phase on 5000 rows is sub-millisecond.

## 5. Test Fixture Changes

### 5.1 Schema

Replace the phonetic fixture functions (`_create_fsq_phonetic_db`,
`_create_overture_phonetic_db`) with trigram fixture functions. Also update
the existing `_create_fsq_db` and `_create_overture_db` to use trigrams
instead of tokens.

FSQ trigram `name_index`:

```python
conn.execute("""
    CREATE TABLE name_index (
        trigram VARCHAR,
        fsq_place_id VARCHAR,
        name VARCHAR,
        latitude VARCHAR,
        longitude VARCHAR,
        address VARCHAR,
        locality VARCHAR,
        postcode VARCHAR,
        region VARCHAR,
        country VARCHAR,
        importance INTEGER
    )
""")
for row in FSQ_PLACES:
    fsq_id, name, lat, lon, address, locality, postcode, region, _, _, _, country = row
    norm = _strip_accents(name.lower())
    seen = set()
    for i in range(len(norm) - 2):
        tri = norm[i:i+3]
        if tri not in seen:
            seen.add(tri)
            conn.execute(
                "INSERT INTO name_index VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [tri, fsq_id, name,
                 f"{lat:.6f}", f"{lon:.6f}",
                 address, locality, postcode, region, country, 1.0]
            )
```

Overture trigram `name_index`:

```python
conn.execute("""
    CREATE TABLE name_index (
        trigram VARCHAR,
        id VARCHAR,
        name VARCHAR,
        latitude VARCHAR,
        longitude VARCHAR,
        importance INTEGER
    )
""")
for row in OVERTURE_PLACES:
    ovr_id, name, lat, lon, *_ = row
    norm = _strip_accents(name.lower())
    seen = set()
    for i in range(len(norm) - 2):
        tri = norm[i:i+3]
        if tri not in seen:
            seen.add(tri)
            conn.execute(
                "INSERT INTO name_index VALUES (?,?,?,?,?,?)",
                [tri, ovr_id, name,
                 f"{lat:.6f}", f"{lon:.6f}", 1.0]
            )
```

### 5.2 Helper function

Add `_strip_accents()` at module level in `conftest.py`, identical to the
`Database._strip_accents()` static method:

```python
import unicodedata

def _strip_accents(s: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )
```

### 5.3 Fixture renaming

| Old fixture | New fixture | Notes |
|-------------|-------------|-------|
| `fsq_phonetic_db_path` | `fsq_trigram_db_path` | Session-scoped path |
| `fsq_phonetic_db` | `fsq_trigram_db` | Function-scoped instance |
| `overture_phonetic_db_path` | `overture_trigram_db_path` | Session-scoped path |
| `overture_phonetic_db` | `overture_trigram_db` | Function-scoped instance |
| `_create_fsq_phonetic_db` | `_create_fsq_trigram_db` | Builder function |
| `_create_overture_phonetic_db` | `_create_overture_trigram_db` | Builder function |

The phonetic token code dictionaries (`FSQ_PHONETIC_TOKEN_CODES`,
`OVERTURE_PHONETIC_TOKEN_CODES`) are removed entirely.

### 5.4 Existing fixture updates

The existing `_create_fsq_db(with_name_index=True)` and
`_create_overture_db(with_name_index=True)` should **also** use the trigram
schema (not the old token schema). These fixtures exercise the basic name
index path. Update them to generate trigrams instead of tokens.

The `with_name_index=False` variants remain unchanged (no name_index table).

### 5.5 Capability detection in fixtures

The trigram fixtures no longer need to skip based on `splink_udfs`:

```python
@pytest.fixture
def fsq_trigram_db(fsq_trigram_db_path):
    db = FoursquareOSP(fsq_trigram_db_path)
    db.connect()
    assert db.has_trigram_index  # Should always be True with trigram fixtures
    yield db
    db.close()
```

### 5.6 Test renaming

All test functions referencing "phonetic" should be renamed to "trigram".
Tests that checked for `dm_code`, `token`, `n_place_tokens`, or `splink_udfs`
should be replaced with trigram-equivalent tests. Specifically:

- `test_phonetic_index_requires_n_place_tokens` -> `test_trigram_index_detection`
  (checks `has_trigram_index` is True when `trigram` column exists, False otherwise)
- `test_phonetic_index_with_both_columns` -> removed (no multi-column detection)
- All `test_*phonetic*` tests -> rename to `test_*trigram*` and update assertions

## 6. Compatibility Notes

### 6.1 Database migration

Old databases (with `dm_code`/`token` schema) will not have a `trigram`
column. `connect()` will set `has_trigram_index = False` and fall through to
either the legacy token self-join path (if `has_name_index` is True) or ILIKE
fallback.

To migrate: re-run the import script. There is no incremental migration path.

### 6.2 Extension dependencies

| Extension | Import time | Query time |
|-----------|------------|------------|
| `spatial` | Yes | Yes |
| `geography` | When `has_density=true` | No |
| `splink_udfs` | **No** (removed) | **No** (removed) |

The removal of `splink_udfs` at query time simplifies deployment — no
community extension needed at runtime.

### 6.3 Backward compatibility in `query_nearest`

The routing logic should fall through gracefully:

1. `has_trigram_index = True` -> trigram query methods
2. `has_trigram_index = False`, `has_name_index = True` -> legacy token self-join
3. `has_name_index = False` -> ILIKE fallback

This means old databases still work (degraded to token self-join), and new
databases get trigram search. The token self-join path can be removed in a
future cleanup once all deployed databases are rebuilt.

### 6.4 Short query handling

Queries shorter than 3 characters produce zero trigrams. `_compute_trigrams`
returns `[]`, and the query methods return an empty-result query. This matches
the current behavior where single-character tokens were filtered out.

For queries of exactly 3 characters (e.g., "bar"), one trigram is produced.
This is a broad match but the JW threshold filters effectively.

## 7. Performance Expectations

### 7.1 Import time

Trigram generation via `generate_series` + `substr` is pure SQL with no
extension calls. At 321K places:
- Current (DM codes): ~15 seconds (includes `double_metaphone()` UDF calls)
- Expected (trigrams): ~10 seconds (simpler SQL, no UDF overhead)
- The `SELECT DISTINCT` adds a sort/hash step but on a smaller row set than
  the DM code expansion (each token produced 2 DM codes; each name produces
  ~L-2 trigrams, but only one set per name vs. one set per token).

### 7.2 Index size

Average name length ~15 chars -> ~13 trigrams per name (vs. ~4-6 DM code rows
per name in the current schema). The index will be ~2-3x larger in row count.
Column width is smaller (3-char trigram vs. variable-length DM code + token),
partially offsetting the row count increase. Net index size increase: ~1.5-2x.

### 7.3 Query time

- Trigram retrieval: `WHERE trigram IN (...)` with zone map pruning on the
  sorted `trigram` column. Comparable to current `WHERE dm_code IN (...)`.
- JW scoring: Single `jaro_winkler_similarity()` call per candidate (vs.
  N_query_tokens * max(JW) calls in current token-level scoring). Faster.
- Expected: 2-15ms at 321K places (comparable to current phonetic search).

### 7.4 Retrieval quality

Trigrams handle:
- Transpositions: "Chipolte" shares trigrams with "Chipotle" (`chi, hip, tle`)
- Misspellings: "Starbcuks" shares trigrams with "Starbucks" (`sta, tar, arb`)
- Multi-word: "pizza hut" naturally generates cross-word trigrams
- All-short-token queries: "la la" produces `la , a l` — cross-word trigrams
  handle this case that was problematic with per-token generation

Full-string JW scoring handles ranking effectively because it captures both
character-level similarity and positional alignment in a single metric.

**Word-order transposition penalty:** Full-string JW is more sensitive to word
reordering than token-level scoring, because positional alignment matters in
the Jaro similarity computation. A query "pizza hut" against "hut pizza" scores
approximately 0.78 — well above the 0.6 threshold — so common two-word
transpositions still retrieve the correct place. For queries with more words or
less common orderings the penalty grows, but this is an accepted tradeoff for
the simplicity gain of eliminating token-level scoring and the Jaro-Winkler
blend logic.
