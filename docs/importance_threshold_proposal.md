# Importance Threshold Optimization

## Status

**Scoring formula**: Implemented. importance is now a normalized 0–100
INTEGER stored in the `places` table and propagated to `name_index`.

**Threshold filtering**: Proposed. Not yet implemented. Validated as
necessary for global-scale datasets (100M+ places) but not testable on
the NorCal 321K extract because the importance distribution is too flat
(99% of places >= 13 on the 0–100 scale).

## Problem

Trigram Jaccard search scans all name_index rows matching any query
trigram, then GROUP BY + HAVING filters to relevant places. At 321K
places this takes 2-90ms. At 100M places, common trigrams like "sta"
or "the" would match millions of rows, pushing latency to seconds.

## Proposed Approach

Add an importance floor to the trigram query based on the search area
size. Larger search areas require higher importance to qualify as a
candidate. This prunes low-importance places from the scan before the
expensive GROUP BY.

### Formula

```
importance_floor = min(4 * ln(1 + area_km2 / K), 100)
```

Where:
- `area_km2` is the bounding box area in square kilometers
- For text-only queries (no bbox), use the full globe: ~510,000,000 km²
- `K` is a tuning constant controlling pruning aggressiveness
- The `4 *` scaling factor maps the raw log value to the 0–100 importance scale

### Example thresholds (K=1000)

| Search scope       | Area (km²)     | Floor | Effect                        |
|--------------------|----------------|-------|-------------------------------|
| 5km local bbox     | ~100           | 0     | All places qualify            |
| City-scale bbox    | ~2,500         | 5     | Filters lowest-importance     |
| NorCal region      | ~70,000        | 17    | Most qualify                  |
| Global (no bbox)   | 510,000,000    | 52    | Only prominent places         |

### Why this works

Importance is a normalized 0–100 INTEGER score:

```
density_norm = min(ln(1 + pt_count) / 10.0, 1.0)
idf_norm     = min(max_category_idf / 18.0, 1.0)
importance   = round(60 * density_norm + 40 * idf_norm)
```

60% weight on S2 level-12 cell density (prominence signal), 40% on
maximum category IDF (distinctiveness signal). Soft caps at 10.0 (density)
and 18.0 (IDF), slightly above observed maxima.

Typical values on the global dataset:
- Sparse + common category: ~13
- Sparse + rare category: ~43
- Dense + common category: ~64
- Dense + rare category: ~94
- Global average: ~31

When searching globally for "Starbucks", you want the top Starbucks by
importance — not all 6000+. When searching near your location, you want
the closest ones regardless of importance. The bbox area encodes this
intent.

### Implementation

Add to the text-only query methods:

```sql
WHERE trigram IN ($g0, $g1, ...)
  AND importance >= {importance_floor}
GROUP BY ...
```

For spatial queries, the bbox already constrains the candidate set, so
the importance floor is less critical. It could still be applied but
with a lower floor (the bbox area is small).

`nearest()` would compute the floor:

```python
import math

if latitude is not None and longitude is not None:
    area_km2 = (xmax - xmin) * 111 * math.cos(math.radians(latitude)) * (ymax - ymin) * 111
else:
    area_km2 = 510_000_000  # globe

K = 1000  # tuning constant
importance_floor = min(int(4 * math.log(1 + area_km2 / K)), 100)
params["importance_floor"] = importance_floor
```

### Tuning K

K controls the tradeoff:
- Small K (100): aggressive pruning, risk of missing relevant results
- Large K (10000): lenient pruning, less performance benefit
- K=1000 is a starting point; tune against the global dataset

### Prerequisites

- Rebuild density scores against a global dataset (current NorCal extract
  has a flat importance distribution that doesn't exercise this filter)
- The importance column is now INTEGER (0–100) in both `places` and
  `name_index` — the floor values in the Implementation section are
  calibrated to this scale
- The importance column should be included in the sort order or have
  zone map coverage for efficient filtering

### Validation Plan

1. Import the full global FSQ dataset
2. Run importance-stats.py to verify distribution spread
3. Pick K value based on desired pruning at global scale
4. Run perf-test.py to compare latency with and without the floor
5. Verify that the correct places are still returned (no important
   results pruned)

## Related Documents

- `docs/trigram_search_design.md` — trigram retrieval design
- `scripts/importance-stats.py` — importance distribution analysis script
