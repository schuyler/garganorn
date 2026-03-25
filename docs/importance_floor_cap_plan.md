# Importance Floor Cap Plan

**Status:** Implemented. Cap raised from 45 to 50 based on floor validation results.

## Problem

Text-only queries apply `importance_floor = 52` (the maximum the formula
produces for globe-scale area). This excludes local favorites:

| Place | Importance | Excluded? |
|---|---|---|
| Chez Panisse (Berkeley) | 51 | Yes (1 point short) |
| Palo Alto Caltrain Station | 47 | Yes |
| Zachary's Chicago Pizza | 43 | Yes |
| Golden Gate Bridge | 39 | Yes |

## Analysis: IDF Reweighting Does Not Help

Shifting the 60/40 density/IDF split toward IDF makes things *worse*
for most problem places, because their density scores are higher than
their normalized IDF scores:

| Place | 60/40 (current) | 50/50 | 45/55 |
|---|---|---|---|
| Chez Panisse (idf=8.2) | 51 | 50 | 50 |
| Palo Alto Caltrain (idf=6.3) | 47 | 45 | 44 |
| Zachary's (idf=4.7) | 43 | 40 | 39 |
| Golden Gate Bridge (idf=7.6) | 39 | 40 | 40 |

## Recommendation: Cap the Floor at 50

Based on floor validation results (2026-03-25), the cap was set to 50
(was 45, originally 100). Floor=50 delivers a 1.4-1.5x speedup over
floor=45 with minimal quality impact.

One-line change in `garganorn/database.py`:

```python
# Was (cap=45):
return min(int(4 * math.log(1 + area_km2 / K)), 45)

# Current (cap=50):
return min(int(4 * math.log(1 + area_km2 / K)), 50)
```

This only affects areas larger than ~77M km² (15% of the globe). No
real bounding box is that large. The cap exclusively targets the
text-only / globe-scale case. Intermediate bounding boxes (city,
country, continent) are unaffected.

## Impact

| Metric | Floor=52 | Floor=45 |
|---|---|---|
| Candidate places | 4.4M | ~10.9M |
| Chez Panisse (51) | Excluded | **Included** |
| Palo Alto Caltrain (47) | Excluded | **Included** |
| Zachary's (43) | Excluded | Excluded |
| Golden Gate Bridge (39) | Excluded | Excluded |

Zachary's and Golden Gate Bridge would require floor=40 (15.5M candidates)
or lower. Deploy with 45 first, measure performance, then decide.

## Performance Risk

Candidate set grows 2.5x (4.4M → 10.9M), but the trigram IN filter
constrains the actual working set. For distinctive queries (most real
searches), the trigram pre-filter is the bottleneck, not the importance
floor. Risk is low for typical queries; short/common queries like "The"
may be slower.

## Files to Modify

- `garganorn/database.py` — change cap in `compute_importance_floor` from 100 to 45
- `tests/test_importance.py` — update `test_compute_floor_globe` assertion from 52 to 45

## What This Does NOT Fix

- Zachary's Chicago Pizza (43) — still below floor
- Golden Gate Bridge (39) — still below floor
- These require either a lower cap or a different importance signal

## Floor Validation Results (2026-03-25)

Warm-cache baselines and speedups at each floor value (median of 10 iterations):

| Category | floor=45 | floor=50 | floor=55 | floor=60 |
|---|---|---|---|---|
| exact_chain | 48ms | 40ms 1.2x | 34ms 1.4x | 29ms 1.6x |
| misspelled | 90ms | 68ms 1.3x | 52ms 1.7x | 41ms 2.2x |
| multi_word | 287ms | 198ms 1.4x | 136ms 2.1x | 98ms 2.9x |
| local_unique | 233ms | 161ms 1.4x | 111ms 2.1x | 81ms 2.9x |
| ambiguous | 269ms | 183ms 1.5x | 127ms 2.1x | 92ms 2.9x |
| long_query | 651ms | 446ms 1.5x | 300ms 2.2x | 212ms 3.1x |
| short_query | 19ms | 16ms 1.2x | 13ms 1.5x | 11ms 1.8x |

Key findings:
- floor=50: 1.4-1.5x speedup, minimal quality loss (mostly sort-order churn on high-cardinality exact matches)
- floor=55: meets <500ms target (long_query 651ms→300ms), but loses exact matches for local favorites (Tartine Bakery, Zachary's Chicago Pizza)
- floor=60: unacceptable — loses Chez Panisse, Bi-Rite Creamery, UCSF Medical Center exact matches (score=1.0)
- Spatial path unaffected (uses bbox-derived floor, not the cap)
