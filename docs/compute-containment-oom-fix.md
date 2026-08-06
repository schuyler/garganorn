# compute_containment() OOM: root cause and fix design

Status: both fixes below implemented, reviewed PASSED, and merged. The z6-partitioning fix
(commit `85d569d`) was necessary but not sufficient — a second, distinct OOM surfaced at global
scale after it shipped, root-caused and fixed separately (see "Second OOM" below).

## The problem

The first global build attempt got past the import stage (75.6M Overture places imported
cleanly) and crashed in `compute_containment()` (`garganorn/stages.py:458`, executing
`garganorn/sql/compute_containment.sql`) with a DuckDB out-of-memory error. Raising
`memory_limit` from 32GB to 48GB moved the crash to the same code site at the new ceiling,
ruling out "just needs more RAM."

## Root cause

`compute_containment.sql` builds `to_json({within: list(struct ORDER BY level, boundary_id)})`
grouped by place. `list()` (and `string_agg`) are holistic aggregates DuckDB cannot spill — the
entire per-group array must stay resident in memory until the whole `GROUP BY` finishes. The
query processes one `qk4` (z4 quadkey) cell at a time via a loop in `compute_containment()`
(`stages.py:433-460`); a cell over a dense region (Western Europe, US East Coast) can hold
8-12M places, and the whole cell's hash table (empirically ~700 + ~460·fanout bytes per group)
must be RAM-resident simultaneously. That's the actual failure — not pathological per-place
data.

Verified and ruled out:

- **Reordering the underlying tables** (pre-sorting `matches` by `place_id`, hoping DuckDB
  would use a streaming aggregate): dead end. `EXPLAIN` shows `HASH_GROUP_BY` regardless of
  input order — DuckDB 1.4.4 has no sorted/streaming aggregate operator. OOM'd identically with
  sorted input.
- **Pathological Overture municipality nesting as the primary cause**: ruled out by the math.
  `LEVEL_VOCAB` (`garganorn/levels.py`) has 10 levels, so clean-data fanout per place is ~4-6.
  Explaining a 44.7 GiB OOM this way would require the defect to affect millions of places, not
  a small number of specific ones. Upstream truncation of outlier municipalities would **not**
  have prevented this crash — it's a real, separate data-quality issue, not the memory fix.
- Everything else in the query already spills correctly: hash joins, the final `ORDER BY`, and
  the two 75.6M-row TEMP TABLEs (`places_slim`, `tile_assignments`) all evict to
  `temp_directory` normally. `list()` is the only unspillable piece.

## Recommended fix

1. **Partition the containment loop at z6, not z4** (`stages.py:436` prefix query,
   `stages.py:443` covering lookup, `compute_containment.sql:27-29` WHERE clause). z6 is chosen
   because `stage_tile_assignment` uses `min_zoom=6`, so every `tile_qk` is ≥6 characters — z6
   is the deepest partition depth at which a `tile_qk` never straddles two batches, which keeps
   the concatenated output globally sorted by `(tile_qk, place_id)` for free. Covering files
   stay at z4 (no regeneration needed); the lookup just truncates to `prefix[:4]`. Make
   `partition_zoom` a config knob (default 6) rather than hardcoding it, in case logged
   per-batch counts (below) show a z6 cell is still too large — bump to 7 if so.

2. **Required alongside #1, not separable**: materialize the `cov` CTE
   (`compute_containment.sql:31-33`) into a real TEMP TABLE once per z4 cell, built outside the
   new inner z6 loop. It's currently a non-materialized CTE re-scanned once per zoom level in
   the `interior_arms` UNION ALL (`stages.py:385-391`, 9 arms for `cover_min_zoom..cover_max_zoom`)
   plus the edge arm — 10 full scans of the covering parquet per prefix today. Without
   materializing it, z6 partitioning turns that into 160 scans per z4 cell (16x more loop
   iterations × the same 10 scans each) — an I/O regression trading one bug for another. This
   is the same wide-CTE rematerialization pattern already fixed once for the import stages in
   commit `e5d3723`.

3. One test needs updating: `tests/test_containment_covering.py` (~line 648-655) currently
   asserts output parquet filename stems are exactly 4 characters. Needs to become
   `4 <= len(stem) <= partition_zoom`. The per-file sortedness test (~706-745) and schema test
   are unaffected — each output file is still individually `ORDER BY tile_qk, place_id`.

4. Add one INFO log line: extend the prefix-list query with a `count(*)` per batch (cheap —
   same scan the loop already needs) and log the top ~10 batches by place count. Turns "which
   cell is actually the worst" from a guess into a known fact, which is the real input for
   deciding whether z6 or z7 is the right default.

**Deliberately not part of this fix**: no adaptive per-cell/greedy batch packing, no hard cap on
the `within` array's length. A bounded alternative to `list()` exists if ever needed —
`min_by(struct, sort_key, n)` has genuinely bounded per-group memory (verified: 1000 groups ×
20k inputs succeeds at 500MB where `list()` OOMs) and produces output identical to
`list() ORDER BY ...` truncated to n elements — but shipping it now would silently truncate data
for a problem that isn't the one that actually crashed. Keep it as a known escape hatch, not a
speculative addition.

## Validation before a global re-run

Mirrors the `~/spill-probe/` eu-bbox validation habit used for the earlier import-spill fix:

1. Reproduce the OOM at small scale first — run the `eu` bbox at a deliberately low
   `memory_limit` (4-8GB) against the *current* (unfixed) code. This is the RED test; without
   it there's no way to tell whether the fix worked or the bbox was just small enough to pass
   regardless.
2. Run the same bbox with the fix (partition_zoom=6 + materialized `cov`) at the same low
   memory limit. Must complete.
3. Byte-compare output against a pre-fix run at a generous memory limit:
   `count(*)` and a full-row hash over all containment parquet files must match **exactly** —
   this is a pure repartition of the same rows, so equality is the correct assertion, not a
   tolerance check.
4. Confirm the new per-batch place-count log line appears and the max is where expected.

## Data-quality half (separate concern, do not bundle into the memory fix)

The original idea was a single pass over Overture Divisions to truncate counties/regions with a
disproportionate number of child municipalities. That heuristic is the wrong one:
child-count-vs-siblings conflates legitimate administrative granularity (France has ~35,000
communes, 300-600 per département — entirely normal) with the actual problem, which is multiple
*overlapping* polygons at the same admin level covering the same point. Child count doesn't
measure overlap.

Better detection, using data already built: `stage_covering` (`garganorn/covering.py` ~line 250)
already produces a per-tile inverted index of boundaries. `SELECT tile_qk, level,
count(DISTINCT boundary_id) AS n FROM covering_out GROUP BY 1, 2 HAVING n > threshold` measures
same-level overlap directly and names the offending tiles/boundary ids, for the cost of one
`GROUP BY` over data already in hand. Run it as a diagnostic first to see the real distribution
before picking a threshold.

Recommended placement: `stage_division_import` (`garganorn/stages.py:864-899`, alongside the
existing subtype validator). Flag loudly (`RuntimeError` or `WARNING` + record in the import
`_meta.json`) rather than silently drop — matches this codebase's existing "never default or
guess" convention for bad source data.

## Second OOM (surfaced after the z6 fix shipped, at global scale)

A third global-build attempt with the z6 fix live got much further (past import, tile
assignment, and 58+ containment batches) but still OOM'd in `compute_containment`, same code
site, same symptom. Root-caused via direct experimentation on the deploy box (garganorn-1)
against real data — not a repeat of the `list()`/aggregate-density problem above, a genuinely
different mechanism:

**Cause**: `compute_containment.sql`'s `p` CTE filters the ~75.6M-row `places_slim` TEMP TABLE
live (`WHERE left(qk17, N) = '${prefix}'`). DuckDB plans the query against `places_slim`'s full
size regardless of the filter's actual selectivity. For almost all batches this is harmless. But
when the `edge` arm then joins that plan against a boundary with an extremely complex geometry —
found: Canada (`id=d5654a87...`, 197,834 vertices) and Nunavut (`id=d8b0d60b...`, 226,060
vertices, both real, legitimate geometries reflecting the Canadian Arctic Archipelago's
coastline, not a data defect — the combination caused memory to blow up 100x+ (145MB vs 30GB+)
for a batch of just 126 places.

Confirmed by elimination, each tested directly on real data before accepting it:
- Not scan pattern: rewriting the filter as a proper sorted range (enabling zone-map pruning,
  0.501s → 0.0016s) didn't change the crash at all.
- Not thread count: `SET threads=1` didn't prevent it either.
- Not the TEMP TABLE specifically: reading fresh from `places.parquet` per batch (no persisted
  `places_slim` at all) crashed identically.
- **Is** the source relation's apparent size: materializing `p` into its own small
  `CREATE TEMP TABLE` before the query — exactly the same pattern already used for `cov` —
  fixed it. Same query, same data, memory stayed flat (~10.6→10.9 GiB) instead of exploding.

No confirmed DuckDB-internals explanation for *why* (a plausible mechanism is that DuckDB's
join planner sizes work off the source relation's cardinality rather than the true
post-filter cardinality, and that only matters when what's being over-provisioned for is a
multi-MB geometry) — not pursued further since the fix doesn't depend on knowing the exact
internal mechanism. Two related, currently-open upstream issues make this plausible as a genuine
DuckDB/duckdb-spatial limitation rather than a bug in this codebase:
[duckdb/duckdb#14087](https://github.com/duckdb/duckdb/issues/14087) and
[duckdb/duckdb#18330](https://github.com/duckdb/duckdb/issues/18330) (temp table memory not
reclaimed); DuckDB's own [OOM guide](https://duckdb.org/docs/current/guides/performance/oom)
states some operations "circumvent the database's buffer manager," and duckdb-spatial's internals
docs confirm GEOS-backed functions (`ST_Contains` among them) take an extra allocation path
outside it.

**Fix**: `compute_containment()` now materializes `p` as `CREATE TEMP TABLE p AS SELECT ...
FROM places_slim WHERE qk17 >= '${prefix}' AND qk17 <= '${prefix_upper}'` per batch (range form,
not `left(qk17,N)=`, since it enables zone-map pruning as a free bonus), and drops it after —
symmetric with the existing `cov` handling. `compute_containment.sql`'s `WITH p AS (...)` CTE is
removed; `p` is now a precondition like `cov`, documented in the file header.

## Files involved

- `garganorn/stages.py` — `compute_containment()` (lines 269-490; prefix list 433-438,
  covering lookup 443, per-prefix loop 442-460, interior arms 385-391);
  `stage_division_import()` validator insertion point (864-899)
- `garganorn/sql/compute_containment.sql` — `p` prefix predicate (27-29), `cov` CTE to delete
  (31-33), final `list()` aggregation (57-63)
- `tests/test_containment_covering.py` — 4-char stem assertion (~648-655) must change;
  per-file sortedness (706-745) and schema tests are the contract to preserve
- `garganorn/covering.py` — `stage_covering()` per-z4 file layout (266-281), confirms no
  covering regeneration is needed; `covering_out` is also where the overlap diagnostic belongs
- `garganorn/sql/overture_division_import.sql` — target for the upstream data-quality guard
  (`parent_division_id`, `subtype`, `country` are all available in `division_base`)
