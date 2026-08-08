# P9 — Performance improvements

Tracked separately from `cleanup-punchlist.md` on purpose: that document
converges to empty (see its own header) because every item there is "bring
already-decided-on code/docs into line." Performance work is different —
it's optimizing code that already works correctly, open-ended until each
idea has its own design, so it doesn't belong in a document meant to reach
zero. This doc holds performance-improvement ideas generally, not just
containment; add new sections here rather than starting another file.

Each section below is its own proposal with its own status. Nothing in this
document is scoped for implementation until its section says so.

## P9.1 — Containment computation: polygon tiling

Status: proposed, not started. No design has been reviewed.

### Problem

Log analysis of the 2026-08-08 build (`compute-containment.log`,
`tile-build.log`) shows `compute_containment`'s per-batch cost is
near-linear in candidate count `n` in aggregate (log-log slope 0.71-0.75 for
n≥1000, both the overture_place and osm runs) — but a consistent set of
~20 quadtree cells run **10-38x slower than their own `n` predicts**,
independently, in both runs. Every one of them falls on a complex coastline
or archipelago: Chesapeake Bay, Cuba/Bahamas, Nova Scotia/Gulf of St.
Lawrence, Maine coast, BC fjords, Norway fjords, Finland lakes, Hawaii,
Indonesia, Sicily, Río de la Plata. `032010` (Chesapeake/DC-MD-VA-DE) is the
worst outlier in both runs (~23-35x) and also a top-10 cell by raw `n`.

This is the same mechanism `compute_containment.sql`'s own header already
documents for the Nunavut case (~200k vertices): the edge-arm join's
`ST_Contains`-style test costs scale with boundary polygon **vertex count**,
not candidate point count. Right now every candidate point tested against a
boundary pays that boundary's full vertex count, no matter how far from the
complex part of the coastline the point actually is.

### Idea

Pre-split large/geometrically-complex boundary polygons into non-overlapping
tiles at import time — a one-time cost paid once per boundary — so the
edge-arm test for each candidate point only has to check the polygon
fragment covering that point's tile, not the whole polygon. Trades one-time
import cost for a cost per contained object that no longer scales with the
parent polygon's total complexity.

### Open questions

- [ ] What triggers tiling a polygon — a vertex-count threshold, or the
      observed slowdown-vs-`n` ratio, or both?
- [ ] Tiling scheme: reuse the existing qk17 / `partition_zoom=6` quadtree
      partitioning already used for batching, or something independent?
- [ ] Correctness: tiled fragments must produce identical `relations.within`
      output to the untiled polygon. Needs a parity test — same containment
      result, before and after — not just a speed check.
- [ ] Antimeridian-crossing boundaries (D7 in `compute_containment.sql`) use
      OR-logic across two lobes. Tiling has to preserve that, not just the
      single-lobe case.
- [ ] Where does the one-time tiling cost land — `stage_division_import`, or
      the `boundaries.duckdb` build? Whichever it is has to fit the existing
      artifact pipeline without restructuring `boundaries.duckdb`.
- [ ] Is it worth doing yet? The project has no users yet and full builds
      already complete in ~90 minutes. Not a blocker on writing the design,
      but worth weighing before implementing.

### Evidence

Full cell-by-cell numbers (prefix, n, duration, actual÷predicted) live in
the 2026-08-08 log-analysis session, not reproduced here — re-run the same
analysis against a fresh build log before implementing, since this data is a
snapshot of one build.

## P9.2 — Same-level boundary overlap: measured, not a driver for P9.1's cells

Status: measured, closed as a non-issue for the P9.1 slow cells. Not
proposed for implementation.

A candidate second cost driver for `compute_containment`: does the *number*
of same-level boundaries claiming a tile (as opposed to per-boundary vertex
complexity) also drive cost? Measured directly against the real 2026-08-08
production artifacts on `garganorn-1` (`overture_division/covering/*.parquet`,
`boundaries.duckdb`; 617,734 boundaries, 1,106,134 edge-arm tiles at the
terminal zoom used by the containment test — only the edge arm pays the
`ST_Contains`-style cost, the interior arm is a cheap prefix join).

Distribution of distinct same-level boundaries per tile: mean 3.9, median 3,
p90=8, p99=16, p999=31, max=490.

Cross-referenced against all 15 slow-outlier prefixes from P9.1's log
analysis (rolled up to their z6 batching prefix): every one sits at or below
p90 — unremarkable. Their 10-38x slowdown is already fully explained by
vertex complexity; overlap density adds nothing there. **Conclusion: not
worth pursuing as a fix for the cells P9.1 targets.**

The actual high-overlap tiles are a disjoint set of hotspots, none of which
show up as slow in the logs because they're cheap per-boundary despite the
high count: Taipei/New Taiwan (490 boundaries in one tile — Taiwan's
fine-grained 里/li subdivisions, 5-810 vertices each), Guangxi and Shandong
China, Java Indonesia, Delhi India, Zambia's Copperbelt.

## P9.3 — Literal duplicate boundary records (candidate cleanup, not scoped)

Status: found, not scoped or approved for implementation.

While measuring P9.2, a global exact-bbox self-join (same admin subtype,
identical `min/max_lat/lon`) found 711 pairs of boundaries with
`ST_Equals`-identical geometry stored under two different boundary IDs (and
often two name variants) — e.g. Tunis governorate stored as both `ولاية
تونس` and `تونس` under separate IDs. Concentrated in Thailand (205 pairs),
Japan (153), Russia (141), China (37) — reads as a systematic source-data
artifact in those countries, not random noise. ~0.2% of all boundaries.
A smaller, distinct pattern also turned up in Indonesia: 22 `Kel. <name>` /
`<name>` pairs (Kelurahan-prefix variants of the same feature) with
IoU 0.92-0.98, not exact matches.

This is not the same thing as two boundaries that genuinely, correctly
disagree about covering the same ground — this project reports that
faithfully, it's not a bug. An exact duplicate record is different: it's the
same claim stored twice, so testing a point against it twice is pure
computational waste with zero informational difference between the two
outcomes. Whether this is worth fixing (and how — dedupe at import, or
elsewhere) hasn't been decided.
