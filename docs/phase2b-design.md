# Phase 2b — Design (OQ-P2-1 Record Envelope + OQ-P2-2 Level Vocabulary)

**Status:** design, pre-review (Rule of Two design gate pending for the merged
document; OQ-P2-2's design has independently PASSED its Rule-of-Two design
review as a standalone document).

**Scope:** Phase 2b ships **both** OQ-P2-1 (record envelope adoption) and
OQ-P2-2 (containment level vocabulary) as **one combined change set**.
Backwards compatibility is a non-goal for either sub-change (OQ-P2-1
resolution 2026-07-07, `phase2-artifacts-design.md:937`). The normative
source for both is `docs/atgeo-appview-sdk-design.md` §1: OQ-P2-1 draws on
§1.2 (tile/record envelope) and §1.3 (manifest); OQ-P2-2 draws on §1.7
(containment level vocabulary). Per the "nothing is sacrosanct" steer, §1 is
revisable; proposed protocol amendments from both sub-designs are collected
in one table (§5).

All decisions marked "approved this session" / "APPROVED this session" below
were approved by Schuyler in the sessions that produced the two source
designs (the OQ-P2-2 level-vocabulary design and the OQ-P2-1 envelope
design), prior to this merge. This merge performs no new design work and
changes no decision.

**Byte-parity acceptance is retired for both sub-changes.** Phase 2 preserved
the legacy envelope specifically for byte-comparability
(`phase2-artifacts-design.md` §3.6 step 4); both OQ-P2-1 (new envelope shape)
and OQ-P2-2 (new `level` values replacing `admin_level`) break byte-parity
by design. Phase 2b replaces byte-parity with the combined acceptance
checklist in §6.

---

## MERGE NOTES

No contradictions were found between the two source documents. They are
orthogonal by construction (OQ-P2-1 changes the envelope *around* record
values; OQ-P2-2 changes the division record's `attributes.level` *value*
inside one record type) and their one point of file overlap
(`overture_division_export_tiles.sql`) is explicitly reconciled by both
source docs in the same way (see §4). Cross-checked OQ-P2-2's level table
against `docs/atgeo-appview-sdk-design.md` §1.7 directly: the current
normative table there has `55 → borough`, `60 → neighborhood`, confirming
OQ-P2-2's own premise (macrohood/microhood absent, neighborhood normatively
at 60) and its proposed stride-5 renumbering is a genuine amendment, not a
misreading. No stray "macrohood 57" or "microhood 65" values were found
outside of the one explicitly-rejected alternative in OQ-P2-2 §3 ("the
minimal-change alternative (macrohood 57 in the gap)"), which is preserved
verbatim below since it documents a considered-and-rejected option, not a
live value. The two corrections requested for this merge (fail-loud
`placeholders` construction note; dropping the `id ASC` variant from the
containment tie-break) have been applied in Part A §6 and §7d respectively.

---

# Part A — OQ-P2-2: Containment Level Vocabulary

**Scope of this part:** Replace the raw Overture `admin_level` integer with
the atgeo containment `level` vocabulary throughout the `overture_division`
producer, the `boundaries.duckdb` export, the tile export, and the two
containment-ordering paths.

Authored from a Fable design recommendation; empirical inputs gathered
on-box (garganorn-1) against Overture release `2026-06-17.0`.

## A.1. Motivation

`boundaries.duckdb` and the division tiles currently carry Overture's raw
`admin_level`. Two problems:

1. **`admin_level` is NULL for ~96% of features** (measured, §A.2). It exists
   only for `country`/`county`/`region`/`dependency`. Every "hood",
   `locality`, and `localadmin` has NULL `admin_level`. Ordering containment
   by it is therefore undefined for the majority of boundary types — a
   locality with NULL `admin_level` sorts *last* today, which is wrong.
2. **`admin_level` is semantically inconsistent** across countries
   (OSM-inherited). `atgeo-appview-sdk-design.md` §1.7 makes this explicit
   and defines a normative `level` vocabulary as the protocol replacement.

The server ordering is only *accidentally* correct today: the
`boundaries.duckdb` export filter (`stages.py:740-741`) happens to exclude
everything below locality, so NULL-last coincidentally puts localities last.
The moment `localadmin` enters (§A.5), that coincidence breaks. The
vocabulary makes ordering correct **by design** rather than by filter
accident.

## A.2. Empirical inputs (measured on-box, release 2026-06-17.0)

**Subtype universe (`division` theme, global, ZERO NULLs — subtype is a
complete key):**

| subtype | count |
|---|---:|
| locality | 3,464,208 |
| neighborhood | 715,929 |
| microhood | 233,378 |
| macrohood | 154,804 |
| county | 38,903 |
| localadmin | 21,380 |
| region | 3,917 |
| country | 219 |
| dependency | 53 |

9 distinct subtypes; closed set. `division.subtype`: 0 NULL of 4.63M.
`division_area.admin_level`: 1,022,641 NULL of 1,065,733 (96%).

**(subtype, admin_level) as landed in `places`** (is_land=true, INNER JOIN,
`min(admin_level)`): `country`→0; `county`→0/1/**2**/3 (38,674 of 38,903 at 2);
`dependency`→1; `region`→1/2; `localadmin`/`locality`/`macrohood`/`microhood`/
`neighborhood`→NULL.

**Landed (post-is_land-JOIN) counts per subtype** — the counts that actually
reach `places`, distinct from the global subtype universe above (only the
"hood" subtypes and `locality` shrink; the administrative subtypes and
`localadmin` are identical global-vs-landed because every such division has
a land area): `country` 219, `dependency` 53, `region` 3,917, `county`
38,903, `localadmin` 21,380, `locality` 551,663, `macrohood` 46,007,
`neighborhood` 317,605, `microhood` 85,986. (`borough` 0.)

**County-nesting spot check** (representative-point-in-polygon, all
admin_level pairs, global): al=1⊃al=2 = **0**; the only county-in-county
nesting is same-level (al=1⊃al=1 ×2, al=2⊃al=2 ×497). Conclusion:
collapsing county to a single level loses **no** cross-admin_level
parent/child ordering (§A.4).

Reproduce: `/tmp/subtype_check.py` and `/tmp/county_nest.py` on garganorn-1.

## A.3. The mapping — key on `subtype` alone

Rationale: `subtype` is complete and a closed 9-value set; `admin_level` is
96% NULL and ambiguous within a subtype. §1.7 keys on subtype and explicitly
rejects `admin_level`.

Counts below give both the global subtype universe (§A.2) and the landed
count (post-is_land-JOIN, what actually reaches `places`); they diverge only
for `locality` and the hoods.

| Overture `subtype` | `level` | §1.7 status | universe (global) | landed (places) |
|---|---:|---|---:|---:|
| `country` | 10 | normative | 219 | 219 |
| `dependency` | 15 | normative | 53 | 53 |
| `region` | 25 | normative | 3,917 | 3,917 |
| `county` | 35 | normative | 38,903 | 38,903 |
| `localadmin` | 45 | normative | 21,380 | 21,380 |
| `locality` | 50 | normative | 3,464,208 | 551,663 |
| `borough` | 55 | normative; absent from current Overture | 0 | 0 |
| `macrohood` | **60** | **added (amendment)** | 154,804 | 46,007 |
| `neighborhood` | **65** | **renumbered from normative 60 (stride-5)** | 715,929 | 317,605 |
| `microhood` | **70** | **added (amendment)** | 233,378 | 85,986 |

Level 0 (continent) has no producer entry (§1.7: "not present in divisions").

`borough → 55` is included though absent from current data: it is already
normative, so a future release emitting it maps cleanly rather than tripping
the fail-loud guard. The guard targets *unknown* subtypes, not
known-but-absent ones.

**The added values (macrohood 60, microhood 70) are proposals; neighborhood
moves 60→65.** §1.7 lists levels only through `neighborhood`=60 and does not
define macrohood or microhood; §1.7's implementation note anticipates
exactly this ("any extras … brought back as a table amendment, not silently
mapped"). Placement follows the WoF descent locality → borough → macrohood →
neighbourhood → microhood: a macrohood *contains* neighborhoods (sorts above
them), a microhood sits inside a neighborhood (sorts below). Rather than
wedge macrohood into the narrow 55–60 gap (which breaks §1.7's stride-5 and
leaves no insertion room), the hoods are renumbered on a **uniform
stride-5**: borough 55, macrohood 60, neighborhood 65, microhood 70. This
**moves the normative `neighborhood` value from 60 to 65** and adds
macrohood/microhood. Chosen (Schuyler, this session) over the
minimal-change alternative (macrohood 57 in the gap) because a clean stride
is easier to explain and preserves insertion room; no live consumers depend
on the old neighborhood value. This is a **protocol change** — see §5.

**Single source of truth.** The mapping lives as one Python constant
(`LEVEL_VOCAB: dict[str, int]`, proposed location `garganorn/levels.py`),
used to (a) render a `${level_case}` CASE expression into
`overture_division_import.sql` and (b) drive the fail-loud validator. SQL
and validator cannot drift. This satisfies `pipeline-restructure-design.md`
§3.4: import is the single place the mapping is applied; covering and
containment copy `level` downstream.

## A.4. County: collapse to 35

All counties get `level = 35`; `admin_level` is **not** preserved as a
secondary discriminator.

- §1.7 assigns county exactly one value; re-encoding admin_level would
  reintroduce the inconsistency the vocabulary removes.
- The distinction is 98% degenerate (38,674 of 38,903 at admin_level 2).
- **The spot check (§A.2) shows zero al=1⊃al=2 nesting** — no cross-level
  county hierarchy exists to lose. The 499 same-level nestings
  (independent-city-style enclaves) are already the same admin_level today,
  so the pipeline cannot order them by admin_level now either; the
  deterministic tie-break (§A.7c) resolves them.

Accepted trade-off (Schuyler, this session): even if a handful of nested
counties were collapsed, that is not a catastrophe. The spot check shows
the count is zero.

`subtype` stays in `boundaries.duckdb` and tile attributes, so anyone
needing the raw distinction can consult the source parquet.

## A.5. The `boundaries.duckdb` boundary filter

`admin_level` ceases to exist in the exported schema, so the filter at
`stages.py:740-741` must change:

```sql
-- today
WHERE admin_level BETWEEN 0 AND 2 OR subtype = 'locality'
-- proposed
WHERE level <= 50   -- country .. locality; expressed as level <= LEVEL_VOCAB['locality']
```

The threshold is expressed via the Python constant so it can't drift from
the vocabulary. This states the real intent ("the administrative hierarchy
down to locality; hoods excluded") instead of encoding it through an
admin_level accident.

**Behavior deltas** (both corrections, but they change output — flagged for
sign-off in §A.8):
1. **+200 counties** (the admin_level=3 ones the old filter arbitrarily
   excluded while including identical-subtype counties at admin_level 2).
2. **+21,380 `localadmin`** (+3.6%). `localadmin` is a genuine
   administrative tier (municipal-level; common in FR/JP) at §1.7 level 45,
   i.e. inside the hierarchy. Its exclusion today is a side effect of NULL
   admin_level, not a decision. Recommended: include it. Conservative
   alternative if zero scope change is wanted: `WHERE level <= 50 AND
   subtype <> 'localadmin'` (a wart; not recommended).

Both deltas use the **landed** counts (§A.2/§A.3), not global. `localadmin`'s
landed count equals its global count (21,380) because every localadmin has a
land area, so the +21,380 figure is exact, not inferred.

Row-count reconciliation (all landed counts):
- Proposed (`level <= 50`): country 219 + dependency 53 + region 3,917 +
  county 38,903 + localadmin 21,380 + locality 551,663 = **616,135**
  (≈616.1k).
- Today (`admin_level BETWEEN 0 AND 2 OR subtype='locality'`): the same
  minus the 200 admin_level=3 counties and minus all 21,380 localadmin =
  **594,555** (≈594.6k). Delta = +21,580, matching the two corrections
  above.

Hoods (borough 55, macrohood 60, neighborhood 65, microhood 70) remain
excluded, matching today's containment scope; whether neighborhoods should
ever participate is a future one-constant change.

## A.6. Fail-loud enforcement

**Where:** `stage_division_import()` in `garganorn/stages.py`, after the
`division_all` CTAS and **before** any artifact write (before the `COPY`
and the `boundaries.duckdb` ATTACH). Failing before tmp writes means no
partial output; existing `finalize_artifact` meta-gating guarantees a clean
rerun.

**Mechanics:**

```python
unmapped = con.execute(f"""
    SELECT DISTINCT subtype FROM division_all
    WHERE subtype IS NULL OR subtype NOT IN ({placeholders})
""", list(LEVEL_VOCAB)).fetchall()
if unmapped:
    raise RuntimeError(
        f"overture_division import: unmapped division subtypes "
        f"{sorted(s for (s,) in unmapped)}; the atgeo level vocabulary "
        f"(atgeo-appview-sdk-design.md §1.7) must be amended before import. "
        f"Never default or guess."
    )
```

`placeholders` is constructed as `placeholders = ",".join("?" *
len(LEVEL_VOCAB))`, paired with `list(LEVEL_VOCAB)` as the query params (the
dict's keys — the subtype strings — in insertion order); this is what binds
one `?` per known subtype in the `NOT IN (...)` clause above.

Belt-and-braces: the `${level_case}` CASE has **no ELSE** branch, and the
stage asserts `count(*) WHERE level IS NULL = 0` after the CTAS. The
pre-check makes this unreachable but costs one aggregate; it lists all
unmapped subtypes at once (better than DuckDB `error()` inside a CASE,
which reports one row).

The OQ-P2-2 on-box precondition ("`SELECT DISTINCT subtype` verification")
is **satisfied** by §A.2. The runtime guard covers future release drift.

## A.7. Server and pipeline touch points (all confirmed by file:line)

**a. Producer — `overture_division_import.sql`:** replace `ma.admin_level`
(line 57; sourced from `min(admin_level)` at line 45) with the
`${level_case}` subtype→level CASE (keyed on `d.subtype`, in scope in
`division_base`), producing a `level INTEGER` column in `places`.
`admin_level` is no longer carried into `places`. **Drop the now-dead
admin_level plumbing:** `division_area.admin_level` (line 32), the
`min(admin_level) AS admin_level` (line 45), and the `ma.admin_level` select
(line 57) become unused once `level` is subtype-derived — remove them rather
than leave dead columns. Update the header comments that describe
admin_level (lines 4, 8, 9, 42) so nothing stale is left behind.

**b. `boundaries.duckdb` export — `stages.py:734-741`:** select `level`
instead of `admin_level` (line 734); rewrite the filter (§A.5).

**c. Server runtime — `boundaries.py`:** line 49 `ORDER BY admin_level ASC`
→ `ORDER BY level ASC, id ASC` (deterministic tie-break, §A.4). Update
the class docstring (lines 11-12, 36-43) which names `admin_level` in the
schema contract. `level` is total by construction, so no NULLS-last
handling is needed.

**d. Build-time containment ordering — `covering_seed.sql:21` +
`compute_containment.sql:59`:** `covering_seed.sql` currently renames
`b.admin_level AS level`; change to `b.level` (the real vocabulary column).
The containment relations already `ORDER BY m.level ASC`
(`compute_containment.sql:59`) — today "level" *is* admin_level via that
rename, so this path becomes correct automatically once the seed feeds it
the vocabulary. Add `, boundary_id ASC` as the tie-break there for
determinism.

**e. Division `getRecord` — `database.py:1374,1438,1449-1450`:** select
`level` instead of `admin_level`; emit `attributes["level"]`. Since `level`
is never NULL, the `if admin_level is not None` guard (line 1449) becomes
unconditional.

**f. Tile export — `overture_division_export_tiles.sql:48`:** `admin_level:
p.admin_level` → `level: p.level` in the attributes struct. Update the
header comment (line 8). **Drop** raw `admin_level` rather than carrying
both: it is 96% NULL (so `strip_json_nulls` already removes it from most
records), OSM-inherited, and recoverable from source. Carrying both invites
keying on the wrong one.

**Tile `level` and `boundaries.duckdb` `level` agree by construction.** The
mapping is applied exactly once, in the import CTAS producing
`division_all`, from which both `places.parquet` (→ tiles) and
`boundaries.duckdb` are cut, and from which covering/containment copy the
column. Agreement is structural. `boundaries.py`'s contract (docstring
40-43) already says clients resolve `level` from the admin tile layer, so
the server's sort key and the tile-resolved value must match. Add one cheap
acceptance assertion: for sampled division rkeys, tile `attributes.level`
== `boundaries.duckdb` `level`.

**Test change set (complete — from `grep -rn admin_level tests/`, 13
files).** Do NOT treat this as a blanket `admin_level`→`level` rename:
fixtures need *semantic* migration (the vocabulary values differ from raw
admin_level), and four assertions encode the OLD semantics and must be
inverted/deleted under Red/Green TDD. Three categories:

*(A) Fixture / schema migration — the `places`/boundary fixtures create an
`admin_level INTEGER` column and insert values; these become a `level
INTEGER` column whose values are the subtype-mapped vocabulary (not the old
admin_level integers). Where a fixture's ordering behavior depends on the
specific integers, re-derive them from the vocabulary:*
- `tests/conftest.py:583,603,629,640,646,657,1157` (division/boundary
  fixtures)
- `tests/test_containment_covering.py:77,84,87,333,1017`
- `tests/test_covering.py:48,84,91,94`
- `tests/test_phase3_containment.py:16,36,43,51,56`
- `tests/test_overture_division.py:210,234,248,261`
- `tests/test_tile_flatten.py:232,252,314,316`
- `tests/test_coord_exprs_bug.py:114`
- `tests/test_audit_scoring.py:92,99,106`, `tests/test_audit_spatial_data.py:219`,
  `tests/test_audit_spatial_processing.py:174,207`,
  `tests/test_phase4_density_spatial_join.py:311`

*(B) Assertion inversions / deletions — these assert the OLD behavior and
CANNOT be satisfied by a rename; each needs explicit sign-off (old → new
invariant):*
- `tests/test_phase3_containment.py:158-169` —
  `test_run_pipeline_has_boundary_filter` string-matches `"admin_level
  BETWEEN 0 AND 2"` / `"subtype = 'locality'"` in the stage source. **New:**
  assert the source contains the `level <= 50` filter form.
- `tests/test_boundaries.py:25-33,128` —
  `test_ordered_by_admin_level_ascending` and `assert attrs["admin_level"]
  == 3`. **New:** order-by-`level` assertion; `attrs["level"] ==
  <mapped>`; rename the test.
- `tests/test_overture_division.py:327-328` — `attrs["admin_level"] == 6`,
  `keys == {"subtype","admin_level"}`. **New:** `attrs["level"] == 35`
  (county), `keys == {"subtype","level"}`.
- `tests/test_overture_division.py:364-375` —
  `test_boundaries_duckdb_schema_unchanged` asserts `"admin_level" in
  cols`. **New (inverted):** `"level" in cols` and `"admin_level" not in
  cols`; rewrite docstring.
- `tests/test_covering.py:367-382` — `test_level_equals_boundary_admin_level`
  asserts `level == admin_level`, the exact invariant this design breaks.
  **New:** assert `level == LEVEL_VOCAB[subtype]`; rename the test.
- `tests/test_containment_covering.py:261,321-397` —
  `test_null_admin_level_boundaries_last`. Premise (NULL levels sort last)
  is void since `level` is total. **Decision:** delete, or repurpose to
  assert no NULL levels ever occur. (Recommend the latter — it becomes
  acceptance item §6.5 [combined checklist].)

*(C) Comments only — no assertion change, but update text so it isn't
stale:*
- `tests/test_export.py:1045,1445-1450` — ordering comment and
  fixture-label comments describing admin_level; verify any
  export-ordering assertion these back now keys on `level`.

Also close out `covering-containment-design.md:238` ("level in Phase 1 =
admin_level values") and OQ-2 (~line 686). Per "No dangling tests," the
full suite must be green before the Green gate — grep `admin_level` across
`tests/` must return zero production-relevant hits (only historical
comments may remain, and preferably none).

## A.8. Open risks / decisions (OQ-P2-2-specific)

1. **§1.7 amendment is a protocol change.** The hoods are renumbered
   stride-5: macrohood 60 and microhood 70 are added, and **neighborhood
   moves from its normative 60 to 65** (§A.3). This edits §1.7's table in
   `atgeo-appview-sdk-design.md` and obligates the atgeo.org Lexicon page
   update — including the neighborhood change, not just the additions.
   Approved this session. **Out-of-repo obligation:** atgeo.org Lexicon
   page.
2. **`localadmin` entering containment** (+21,380 boundaries; changes live
   server containment for points in localadmin-using countries). Approved
   this session (include it, per §A.5).
3. **Byte-comparability breaks by design** — already accepted in the
   OQ-P2-1/2 resolutions and `phase2-artifacts-design.md` §3.3 rationale. Phase 2b needs its own small
   acceptance fixtures: new `within` ordering, `attributes.level`,
   boundary-count delta (≈616.1k vs ≈594.6k).
4. **Dropping raw `admin_level` from tile records** (§A.7f) — approved this
   session.
5. **Hood numbering stride — DECIDED (stride-5 renumber).** Chosen
   (Schuyler, this session): renumber the hoods on a uniform stride —
   borough 55, macrohood 60, neighborhood 65, microhood 70 — rather than
   wedge macrohood into the 55–60 gap. Cleaner and easier to explain later;
   the cost is moving the normative neighborhood value 60→65 (§A.8.1),
   acceptable as no consumers depend on it. Reflected throughout §A.3.

---

# Part B — OQ-P2-1: Record Envelope Adoption

**Scope of this part:** Adopt the atgeo v1 tile/record envelope and the
§1.3 manifest fields in the pipeline export.

Authored from a Fable design recommendation. The four escalated decisions
in §B.9 (cid: null; immutable: false; tile_url_template in 2b; REPO
hardcoded) were **approved by Schuyler this session**.

**Normative source:** `docs/atgeo-appview-sdk-design.md` §1.2 (tile/record
envelope) and §1.3 (manifest). Note: an earlier reference to
"`pipeline-restructure-design.md` §1.3" is a mis-cite — that doc's §1 is
Goals; its §3.8 (lines 328–352) *delegates* the envelope to the atgeo doc
§1.2/§1.3. Per the "nothing is sacrosanct" steer, the atgeo §1 spec is
revisable; proposed protocol amendments are collected in §5 and flagged
inline as **[protocol change]**.

## B.1. Current state (grounded)

- **Tile payload** (`stages.py:1338-1343`, duplicated in legacy
  `export_tiles()` at `stages.py:413-418`): `{"collection", "attribution",
  "records": [<bare record values>]}`. Records are DuckDB `to_json()`
  strings built in the four `*_export_tiles.sql` views; shape `{"$type":
  "org.atgeo.place", rkey, name, importance, locations, variants,
  attributes, relations}`.
- **manifest.json** (`stages.py:513-523`): `{"source", "generated_at":
  now(), "quadkeys": sorted([...])}`.
- **manifest.duckdb** (`stages.py:465-510`): `record_tiles(rkey, tile_qk)`
  + `metadata(source, generated_at)`.
- **Server consumption**: `tile_reader.py:40-44` matches `record["rkey"]`
  inside `tile_data["records"]`; `server.py:60-61` builds
  `https://{repo}/{collection}/{rkey}` URIs; `__main__.py:101-118` serves
  tiles via the slug route against a `current`-symlinked dir, deliberately
  **not** `immutable` (`__main__.py:114-117`).
- **Parity harness** (`scripts/tile_parity.py`): sorts records by `rkey`,
  strips manifest `generated_at`.
- Phase 2 deliberately preserved this envelope for byte-comparability
  (`phase2-artifacts-design.md` §3.6 step 4).

## B.2. Target shapes (after OQ-P2-1)

### B.2a. Tile file (`{qk6}/{qk}.json.gz`)

Before:
```json
{ "collection": "org.atgeo.places.overture.place",
  "attribution": "https://docs.overturemaps.org/attribution/",
  "records": [ { "$type": "org.atgeo.place", "rkey": "08f2...", "name": "...", "...": "..." } ] }
```

After (per atgeo §1.2, verbatim field set):
```json
{ "atgeo": 1,
  "collection": "org.atgeo.places.overture.place",
  "attribution": "https://docs.overturemaps.org/attribution/",
  "generated_at": "2026-07-09T18:00:00Z",
  "records": [
    { "uri": "https://places.atgeo.org/org.atgeo.places.overture.place/08f2...",
      "cid": null,
      "value": { "$type": "org.atgeo.place", "rkey": "08f2...", "name": "...",
                 "importance": 62, "locations": ["..."], "variants": ["..."],
                 "attributes": {"...": "..."}, "relations": {"...": "..."} } } ] }
```

`value` is byte-for-byte today's record JSON — §1.2 is explicit that value
schemas are the existing lexicons and unchanged. (OQ-P2-2 changes
`attributes` for divisions independently; §5.)

Spec-silent points, resolved here:
- **Record order within a tile.** §1.2 is silent; §2.5 has the AppView
  flush `ORDER BY rkey`. Keep the pipeline's `ORDER BY tile_qk, place_id`
  (`stages.py:1328-1331`, determinism per Phase 2 §9.1) and **[protocol
  change P5]** spec that record order is producer-defined and consumers
  must not rely on it (the conformance canonicalizer already sorts).
- **Attribution content.** §1.2's example shows human-readable text; the
  producer emits URLs (`database.py:319,649,985,1353`). The spec never
  constrains it beyond "a string". Keep the URLs; note the mismatch for
  whoever generates the `envelope.json` conformance vectors so the fixtures
  reflect reality.

### B.2b. Single record

`{uri, cid, value}` exactly — three keys, always present. See §B.3.

### B.2c. manifest.json

Before (`stages.py:514-518`):
```json
{ "source": "overture_place",
  "generated_at": "2026-07-09T18:00:00.123456+00:00",
  "quadkeys": ["023010", "..."] }
```

After (per atgeo §1.3, plus one proposed addition):
```json
{ "atgeo": 1,
  "source": "overture_place",
  "collection": "org.atgeo.places.overture.place",
  "attribution": "https://docs.overturemaps.org/attribution/",
  "generated_at": "2026-07-09T18:00:00Z",
  "tile_url_template": "{base}/{qk6}/{qk}.json.gz",
  "cache": { "max_age": 86400, "immutable": false },
  "quadkeys": ["023010", "..."] }
```

- `atgeo`, `collection`, `tile_url_template`, `cache`, `quadkeys` are §1.3
  verbatim. `source` is an existing producer field §1.3 also shows; keep.
- `attribution` in the manifest is **[protocol change P3]** — §1.3 omits
  it; adding it lets an SDK render attribution without fetching a tile.
  Optional field, unknown-field tolerance (§1.6) makes it safe.
- `cache.immutable: false` deliberately contradicts §1.3's example — see
  §B.6; this is **[protocol change P2]**.
- Ship `tile_url_template` now rather than Phase 3 as
  `pipeline-restructure-design.md` §7.2 sequenced it: it is a static
  string, and the atgeo doc's own sequencing (§4 item 1, "protocol freeze
  v1") wants the format frozen in one event before SDK conformance vectors
  are generated.

## B.3. `{uri, cid, value}` — the crux

**`uri`** = `https://{repo}/{collection}/{rkey}`, where `repo = REPO =
"places.atgeo.org"` (`stages.py:38`), `collection` = the source class
constant, `rkey` = the record's rkey *after* source transforms — for OSM
that is the `node:|way:|relation:` form (`osm_export_tiles.sql:7-11`), not
the raw `place_id`. This is the canonical dereferenceable form: it is
exactly what `server.py:60-61` already returns from `getRecord`, so tile
URIs and XRPC URIs agree by construction. Colons in OSM rkeys are legal in
a URI path segment (RFC 3986); no encoding needed. Not `at://` — §1.2 is
emphatic and correct that gazetteer records are not repository data (no
MST, no signed commit) and must not mint `at://` URIs they cannot verify;
`pipeline-restructure-design.md` §3.8 restates this.

**`cid` is literally `null`. It is not computed. [APPROVED this session.]**
This is specced (§1.2: gazetteer records carry `cid: null`; only AppView
records carry a genuine commit-verified CID) and recommended, having
costed the alternative:

- A "real" CID would be atproto-style: CIDv1, `dag-cbor` codec, sha2-256
  multihash, base32 — hashing the record value's canonical DAG-CBOR
  encoding. That requires parsing every DuckDB-emitted JSON string back
  into Python and re-encoding with DAG-CBOR's canonical map-key ordering,
  ~10⁸ times per global run, in the flush threads.
- The atproto data model **bans floats**. Today's values mostly comply by
  accident (coordinates and confidence are `DECIMAL::VARCHAR` strings;
  `importance`, `population`, `level` are integers) — but `attributes`
  passes raw Overture structs through
  (`overture_place_export_tiles.sql:60-72`, e.g. `sources`), and any float
  inside them makes the record unencodable as compliant DAG-CBOR. That
  would turn a caching nicety into an import-data audit.
- Most decisively: the hash verifies nothing. There is no signed commit
  chaining it to a DID; a client recomputing it learns only that the
  producer hashed what TLS already delivered intact. §1.2's own reasoning
  applies.

If content-addressing is ever wanted (mirror dedup, offline verification),
the honest route is §1.2's marked-speculative batch-MST idea — a separate
project, explicitly not v1.

**Null vs omitted.** Keep the explicit `null`: every record object has
exactly three keys regardless of producer (gazetteer or AppView), matching
§0 goal 1 and the `com.atproto.repo.listRecords` shape where `cid` is
required; the byte cost vanishes under gzip. No spec change.

**Considered and rejected:** dropping per-record `uri` for gazetteer tiles
(derivable from tile-level `collection` + `value.rkey`). Rejected because
it forks the envelope into gazetteer/AppView variants — the exact thing the
unified contract prevents — and gzip reduces the repetition to a few bytes
per record. Size delta measured in acceptance (§6, combined checklist item 11), not estimated.

## B.4. `generated_at` — placement and determinism

**Recommendation: per-tile and per-manifest, one shared run-scoped value;
never per-record; parity canonicalizer strips it.**

§1.2 puts `generated_at` in the tile payload; §1.3 keeps it in the
manifest. The spec does not say the two are equal, and a naive
implementation (call `now()` in `flush_tile`) would stamp every tile
differently, making two exports of identical inputs byte-different in
every file — destroying exactly the property the Phase 2 parity harness,
`gzip.compress(mtime=0)` (`stages.py:1346`), and the `ORDER BY tile_qk,
place_id` determinism work (§9.1) protect. The harness already treats
manifest `generated_at` as the *only* nondeterminism and strips it
(`tile_parity.py:36-44`).

So:

1. Capture **one** timestamp per export run, derived from the run-dir name
   (`stages.py:1278` already produces `%Y%m%dT%H%M%S` UTC): `generated_at =
   datetime.strptime(ts, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc).strftime(
   "%Y-%m-%dT%H:%M:%SZ")`. Every tile, `manifest.json`, and
   `manifest.duckdb.metadata` carry this same value; it is recoverable from
   the directory path.
2. Normalize the format to RFC 3339 `Z`-suffixed seconds precision (current
   code emits `+00:00` with microseconds, `stages.py:516`). Do it now,
   while the format is breaking anyway.
3. Update `scripts/tile_parity.py`: `canonical_tile()` strips tile-level
   `generated_at` and sorts records by `value.rkey`. Tiles remain a pure
   function of (inputs, run timestamp); the Phase 2b acceptance can still
   assert cross-run determinism by injecting a fixed timestamp.

**[protocol change P1]** Amend §1.2: *`generated_at` is a run-scoped
timestamp, identical across all tiles of a run and equal to the manifest's
`generated_at`; RFC 3339 UTC, seconds precision.* Without this, a second
producer (the AppView, §2.5) could legitimately stamp per-flush times and
consumers could wrongly infer per-tile freshness ordering.

Considered: fully deterministic `generated_at` derived from input-artifact
metadata ("data as-of"). Rejected for now (freshness gate already prevents
unchanged re-exports; changing the field's semantics is a bigger spec
conversation). Flag as a possible future `data_as_of` addition, not v1.
Per-record `generated_at`: no.

## B.5. `atgeo: 1` version marker

- **Placement:** tile payload (§1.2) and manifest (§1.3), as integer `1`.
  Also add an `atgeo` column to `manifest.duckdb.metadata` (internal; lets
  the Phase 3 server assert compatibility at startup).
- **Consumer semantics** (§1.6): unknown JSON fields ignored (forward
  compat); unknown `atgeo` **major** version rejected. No minor-version
  field, no negotiation handshake — the manifest is fetched first, so a
  client rejects before fetching tiles. Sufficient for v1.
- **Producer single source of truth:** one constant `ATGEO_VERSION = 1` in
  the new envelope module (§B.7), used by tile payload, manifest.json,
  manifest.duckdb.

## B.6. Manifest changes and `write_manifest_db()`

**`write_manifest()`** (`stages.py:513-523`): gains `atgeo`, `collection`,
`attribution`, `tile_url_template`, `cache`; `generated_at` becomes the
passed-in run timestamp. Signature becomes `write_manifest(manifest,
output_dir, source, *, generated_at)` (collection/attribution resolved
from `_SOURCES[source]` internally, as `flush_tile` already does).

**`cache` and the `immutable` contradiction. [immutable: false APPROVED
this session.]** §1.3 prescribes `immutable: true` for pipeline outputs,
rationalized by "URL changes when content changes, via the `current`
symlink". That rationale does not survive the deployed serving path: the
OQ-P2-5 slug route serves `tiles_dir` pointed at `current`, so the *same*
tile URL returns new bytes after a run — and `__main__.py:114-117` already
documents exactly this. `tile_url_template` as specced contains no
run-unique segment, so `immutable: true` would let CDNs serve stale tiles
for the full max-age.

Ship `"cache": {"max_age": 86400, "immutable": false}` and **[protocol
change P2]** amend §1.3: *`immutable: true` is permitted only when
`tile_url_template` embeds a run-unique path segment.* The
genuinely-immutable design (template =
`{base}/<run-timestamp>/{qk6}/{qk}.json.gz`, manifest fetched via
`current/` as the sole mutable object) is attractive but is a serving-path
change that belongs with the Phase 3 server work, decided then. The
manifest format doesn't change when it flips; only the template string and
the cache object do.

**`write_manifest_db()`** (`stages.py:465-510`): `record_tiles` unchanged
(backs `getRecord`; rkeys already carry the OSM transform,
`stages.py:484-495`, so they match the new `uri` rkeys). `metadata` gains
`collection` and `atgeo` columns; `generated_at` uses the shared run
timestamp instead of a second `now()` (`stages.py:506`). No consumer reads
`metadata` today; this is self-description plus the Phase 3 startup
assert.

## B.7. Producer change set, file-by-file

**Where the wrapping happens: Python, not SQL.** The four export SQLs
would each need `${collection}` substitution and a duplicated
`to_json({uri: …, cid: NULL, value: …})` wrapper; Python centralizes the
envelope in one function, is where the run timestamp and version constant
live, is the only place CID computation could ever go, and is the code
§2.7 expects the AppView to share. SQL's only new job is exposing `rkey`
as a column.

1. **NEW `garganorn/envelope.py`** — the shared envelope module:
   - `ATGEO_VERSION = 1`; `record_uri(repo, collection, rkey) -> str`.
   - `wrap_record(uri, record_json: str) -> str`: string composition
     `'{"uri":%s,"cid":null,"value":%s}' % (json.dumps(uri), record_json)`
     — no `json.loads` per record. This drops the current round-trip
     (`json.loads(f"[{joined}]")` then `json.dumps`, `stages.py:1341-1343`),
     which exists only for attribution escaping (EXPORT-14) — now handled
     by `json.dumps` on header fields alone. Side effect for review:
     DuckDB's UTF-8 output is preserved verbatim instead of being
     `ensure_ascii`-escaped; JSON-equivalent, byte-different, and
     byte-parity is already dead by design.
   - `build_tile_payload(collection, attribution, generated_at,
     wrapped_records: list[str]) -> bytes`.
   - `build_manifest(source, collection, attribution, generated_at,
     quadkeys) -> dict`.
   - `server.py:60-61`'s `record_uri` should delegate here (no import
     cycle: envelope.py imports nothing from garganorn).

2. **`garganorn/sql/*_export_tiles.sql`** (all four): add an `rkey` output
   column to the `tile_export` view — `p.id` (overture_place,
   overture_division), `p.fsq_place_id` (foursquare), and for OSM the
   existing `CASE` (`osm_export_tiles.sql:7-11`) hoisted so the view emits
   it both inside `record_json` and as the column. `ORDER BY ta.tile_qk,
   ta.place_id` unchanged.

3. **`garganorn/stages.py` `stage_export()`** (`stages.py:1221-1400`):
   - Derive `generated_at` from the step-3 timestamp (one `datetime`
     captured, formats both the dir name and the RFC 3339 string).
   - Cursor gains the column: `SELECT tile_qk, place_id, rkey,
     record_json …` (`stages.py:1328-1331`); `accumulated` holds `(rkey,
     record_json)` tuples (place_id retained solely as the deterministic
     sort key).
   - `flush_tile` calls `envelope.wrap_record` + `build_tile_payload`;
     gzip/tmp-write mechanics unchanged.
   - Pass `generated_at` into `write_manifest_db()` and `write_manifest()`.

4. **Legacy `export_tiles()`/`write_manifest()` module functions**
   (`stages.py:390-462, 513-523`): not on the production path
   (`quadtree.py:95` calls `stage_export`; the imports at
   `quadtree.py:15-17` are back-compat re-exports; callers are tests only).
   Two envelope implementations in-tree is a drift hazard. Retarget legacy
   `export_tiles()` onto the same `envelope.py` helpers in this change set
   (small), and file its outright deletion (with test migration to
   `stage_export`) as separate cleanup — deletion is out of OQ-P2-1's scope
   but tracked.

5. **`garganorn/tile_reader.py:40-44`**: match
   `record["value"]["rkey"]`, return `copy.copy(record["value"])` — server
   envelope construction (`server.py:113-116`) then works unchanged,
   including the `importance` hoist. Add a one-line tolerance for the
   deployment window (§B.8): `value = record.get("value", record)`.

6. **`scripts/tile_parity.py`**: canonicalizer strips tile `generated_at`;
   sorts records by `value["rkey"]`; its unit tests (Phase 2 §7.8) updated.

7. **Tests**: `test_export.py` (payload-shape assertions),
   `test_audit_export.py`, `test_tile_flatten.py`, `test_tile_reader.py`,
   `test_pipeline.py`, any server test reading `records[...]` — mechanical
   fallout, red-first per Standard tier. (Reviewer: grep `records\[` and
   `"rkey"` across `tests/` for a complete inventory.)

## B.8. Server coupling (must ship atomically)

`getRecord` on places.atgeo.org reads live tiles through `tile_reader.py`.
New server code + old tiles → `KeyError: 'value'`; old server code + new
tiles → rkey match fails. Since server and pipeline deploy from one repo,
the risk is the window between deploying code and the next export run. The
`record.get("value", record)` tolerance in `tile_reader` (one line, remove
after the first production re-export) covers new-code/old-tiles; deploy
order (code first, then re-export) covers the rest. Backwards
compatibility being a non-goal covers external consumers, not the server's
own coupling — this is internal correctness.

Also confirmed unaffected: `__main__.py:95-96` (`result["value"]`)
consumes the XRPC envelope, not tiles; the `lru_cache` in
`_cached_read_tile` keys on path and tiles remain immutable-once-written.

## B.9. Open risks / decisions (OQ-P2-1-specific)

1. **`cid: null` (no computation)** — the central decision (§B.3).
   **APPROVED this session.** If content-addressing is later wanted for
   mirrors/dedup, that reopens the atproto float ban against raw Overture
   `sources` structs and the batch-MST question.
2. **`cache.immutable: false` now, run-stamped immutable URLs later
   (Phase 3)** — accepts weaker CDN caching until the serving path
   changes. **APPROVED this session.**
3. **`tile_url_template` ships in 2b**, overriding
   `pipeline-restructure-design.md` §7.2's Phase 3 sequencing. **APPROVED
   this session.**
4. **`REPO` hardcoded** (`stages.py:38`) baked into every record's `uri`.
   Correct for the canonical origin today; becomes a config parameter the
   day a second deployment exists. **APPROVED this session** (noted, not
   solved here).
5. **Deployment window** (§B.8, "Server coupling"): tolerant read + deploy-then-re-export
   ordering; the tolerance is temporary code with a removal note.
6. **Legacy `export_tiles()` deletion** — recommended as tracked follow-up
   cleanup, not part of this change set (§B.7.4).

---

# Shared

## §4. Change-set sequencing

OQ-P2-1 and OQ-P2-2 are orthogonal by construction: OQ-P2-2 changes the
division record's *value* (`attributes.level` replacing `admin_level`,
Part A §A.7f); OQ-P2-1 changes everything *around* the value. §1.2 is
explicit that value schemas are out of the envelope's scope.

**One file overlaps**: `overture_division_export_tiles.sql` — OQ-P2-2
edits the attributes struct (line 48) and header comment; OQ-P2-1 adds the
`rkey` column and leaves the struct alone. Trivially mergeable.

**Unified implementation order**: sequence OQ-P2-2's producer changes
first within the combined change set — they sit deeper in the pipeline
(import → boundaries → export) — then apply the OQ-P2-1 envelope wrapping
at the leaf (the export/tile-flush stage). Concretely: `level` vocabulary
work (Part A §A.7 a–f: import CTAS, `boundaries.duckdb` export, server
ordering, containment ordering, `getRecord`, tile export attributes struct)
lands first; the envelope module and its integration into `stage_export()`
(Part B §B.7) lands on top, at `overture_division_export_tiles.sql` and
the other three `*_export_tiles.sql` views, once the `level`-bearing
attributes struct is already in place.

## §5. Protocol amendments

Combined table of all proposed amendments to `docs/atgeo-appview-sdk-design.md`
§1, tracked together because both sub-designs edit the normative spec:

| # | Section | Amendment | Rationale | Source |
|---|---|---|---|---|
| §1.7-renumber | §1.7 | Hoods renumbered on uniform stride-5: `borough`=55, `macrohood`=60 (added), `neighborhood`=65 (moved from normative 60), `microhood`=70 (added) | Macrohood/microhood are unrepresented in the normative table; wedging macrohood into the narrow 55–60 gap breaks the stride and leaves no insertion room (Part A §A.3) | OQ-P2-2 |
| P1 | §1.2 | `generated_at` is run-scoped: identical across all tiles of a run, equal to the manifest's; RFC 3339 UTC `Z`, seconds precision | Determinism; prevents per-flush stamping by other producers (Part B §B.4) | OQ-P2-1 |
| P2 | §1.3 | `cache.immutable: true` permitted only when `tile_url_template` embeds a run-unique segment; current pipeline ships `false` | Spec's example presumes run-stamped URLs; deployed slug route serves via `current` (`__main__.py:114-117`) (Part B §B.6) | OQ-P2-1 |
| P3 | §1.3 | Add optional `attribution` to the manifest | Attribution display without a tile fetch; unknown-field-tolerant (Part B §B.2c) | OQ-P2-1 |
| P4 | §1.2 (clarify, no behavior change) | `cid` stays required-nullable; explicitly note it is never computed for gazetteer records | Records the §B.3 decision so no future producer "helpfully" hashes | OQ-P2-1 |
| P5 | §1.2 | Record order within a tile is producer-defined; consumers must not rely on it | §1.2 silent, §2.5 implies rkey order; pipeline orders by place_id (Part B §B.2a) | OQ-P2-1 |

P1/P2 are load-bearing; P3–P5 are hygiene. The §1.7-renumber amendment is
also load-bearing (it changes live values, not just documentation).

**Out-of-repo obligations:**
- §1.7-renumber: the atgeo.org Lexicon page must be updated to reflect the
  hood renumbering, including the neighborhood 60→65 move, not just the
  additions (Part A §A.8.1).
- P1–P5: none, but the conformance corpus (§1.6 `envelope.json`) must be
  generated from post-2b output — this change set is atgeo §4 item 1's
  "protocol freeze v1" producer half, so it must land before any SDK
  vectors are cut (Part B §5, as originally numbered).

## §6. Acceptance (combined change set)

Byte-parity vs Phase 2 does not apply to either sub-change (both break it
by design). Phase 2b fixtures are written **once**, against the combined
output: a division tile fixture asserts both
`records[i].value.attributes.level` (OQ-P2-2) **and** the `{uri, cid,
value}` wrapping (OQ-P2-1) in the same fixture. The cross-artifact level
assertion (tile `level` == `boundaries.duckdb` `level`, Part A §A.7) now
dereferences through `.value` (i.e. `records[i].value.attributes.level`).
One canonicalizer update in `scripts/tile_parity.py` covers both: record
sort key moves to `value.rkey` (OQ-P2-1), and — orthogonally — any
level-based ordering fixtures pick up vocabulary values (OQ-P2-2).

Combined acceptance checklist:

1. **Vocabulary correctness** (OQ-P2-2): unit-test `LEVEL_VOCAB` covers
   exactly the 9 observed subtypes + `borough`; fail-loud raises on an
   injected unknown subtype.
2. **Ordering** (OQ-P2-2): a containment query returning multiple levels is
   ordered ascending by `level`, ties broken by id; assert against a
   fixture with a known nested set (e.g. country ⊃ region ⊃ county ⊃
   locality).
3. **Filter delta** (OQ-P2-2): assert `boundaries.duckdb` now includes
   `localadmin` and admin_level=3 counties; assert hoods excluded.
4. **Cross-artifact agreement** (OQ-P2-2 + OQ-P2-1): sampled rkeys —
   tile `records[i].value.attributes.level` == `boundaries.duckdb`
   `level`.
5. **No NULL levels** (OQ-P2-2): `count(*) WHERE level IS NULL` = 0 in
   `places`.
6. **Envelope shape** (OQ-P2-1): unit fixtures assert tile top-level =
   exactly `{atgeo, collection, attribution, generated_at, records}` with
   `atgeo == 1`; each record = exactly `{uri, cid, value}` with `cid is
   None`; `uri` form per source, including the OSM `node:/way:/relation:`
   rkey (and `record_tiles.rkey` == uri rkey for sampled records).
7. **Determinism** (OQ-P2-1): two `stage_export` runs over identical
   inputs with an injected fixed timestamp are byte-identical, tile-for-
   tile; the updated parity canonicalizer (strip tile `generated_at`, sort
   by `value.rkey`) round-trips.
8. **Timestamp coherence** (OQ-P2-1): every tile's `generated_at` ==
   manifest `generated_at` == manifest.duckdb `metadata.generated_at` ==
   run-dir name, RFC 3339 `Z` form.
9. **Manifest** (OQ-P2-1): manifest.json matches §B.2c's field set;
   manifest.duckdb `metadata` has `atgeo`/`collection`.
10. **Server round-trip** (OQ-P2-1): `getRecord` via `tile_reader` against
    new-shape tiles returns the value with `rkey`/`importance` handling
    intact; tolerance path covered against an old-shape fixture while it
    exists.
11. **Size** (OQ-P2-1): record gzipped-bytes delta per source on the
    SF-bbox run (measure, don't assert).
12. **On-box smoke** (joint, both sub-changes): SF-bbox rerun; confirm no
    fail-loud from the level-vocabulary guard; inspect one place tile and
    one division tile — envelope fields present,
    `records[].value.attributes.level` present, `cid` null throughout.

## §7. Open decisions / risks (combined, de-duplicated)

1. **§1.7 amendment is a protocol change** (hood stride-5 renumber:
   macrohood 60 and microhood 70 added, neighborhood moves 60→65).
   Obligates the atgeo.org Lexicon page update. Approved this session.
   (Part A §A.8.1)
2. **`localadmin` entering containment** (+21,380 boundaries; changes live
   server containment for points in localadmin-using countries). Approved
   this session. (Part A §A.8.2)
3. **Byte-comparability breaks by design** for both sub-changes — replaced
   by the combined acceptance checklist above (§6), including the new
   `within` ordering, `attributes.level`, and boundary-count delta
   (≈616.1k vs ≈594.6k). (Part A §A.8.3)
4. **Dropping raw `admin_level` from tile records** — approved this
   session. (Part A §A.8.4, Part A §A.7f)
5. **Hood numbering stride — DECIDED (stride-5 renumber)**, chosen over
   wedging macrohood into the 55–60 gap. (Part A §A.8.5)
6. **`cid: null` (no computation)** — the central OQ-P2-1 decision.
   APPROVED this session. Reopening content-addressing later reopens the
   atproto float ban against raw Overture `sources` structs and the
   batch-MST question. (Part B §B.9.1)
7. **`cache.immutable: false` now, run-stamped immutable URLs later
   (Phase 3)** — accepts weaker CDN caching until the serving path
   changes. APPROVED this session. (Part B §B.9.2)
8. **`tile_url_template` ships in 2b**, overriding
   `pipeline-restructure-design.md` §7.2's Phase 3 sequencing. APPROVED
   this session. (Part B §B.9.3)
9. **`REPO` hardcoded** (`stages.py:38`) baked into every record's `uri`.
   Correct today; becomes a config parameter the day a second deployment
   exists. APPROVED this session (noted, not solved here). (Part B
   §B.9.4)
10. **Deployment window** for the envelope change: tolerant read
    (`record.get("value", record)`) + deploy-then-re-export ordering; the
    tolerance is temporary code with a removal note. (Part B §B.9.5)
11. **Legacy `export_tiles()` deletion** — recommended as tracked
    follow-up cleanup, not part of this change set. (Part B §B.9.6)
</content>
