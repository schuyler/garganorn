# Pipeline Audit: Deferred Items

This document tracks issues identified during the pipeline audit that were deferred for future work. Items are organized by category and include rationale for deferral and requirements for addressing them.

## Observability & Logging

These items are not correctness issues but represent gaps in observability that make it harder to understand system behavior or diagnose problems.

### SCORE-6: JW_THRESHOLD=0.6 undocumented
- **Severity**: Important
- **Description**: The 0.6 threshold for Jaro-Winkler similarity lacks empirical rationale documentation
- **Why deferred**: Not a correctness issue; threshold value works in practice
- **To address**:
  - Add documentation in `docs/` explaining empirical basis for 0.6 threshold
  - Consider making threshold configurable via environment variable
  - Document trade-offs: lower threshold increases recall but decreases precision

### DATA-2: Division population validation/logging
- **Severity**: Important
- **Description**: Negative population values in Overture divisions are silently accepted
- **Why deferred**: Doesn't cause failures; negative populations are rare
- **To address**:
  - Add logging during division import to count and flag negative population values
  - Consider validation step to reject or clamp negative values
  - Document expected population value ranges per source

### DATA-5: Division is_land=true drops maritime divisions
- **Severity**: Important
- **Description**: Filtering `is_land=true` excludes maritime divisions (bays, straits, seas)
- **Why deferred**: Current behavior may be intentional; need product decision
- **To address**:
  - Add logging to count how many divisions are dropped by this filter
  - Evaluate whether maritime areas should be included in the gazetteer
  - If included, determine whether they should be searchable via different criteria
  - Document product decision either way

### DATA-6: Overture categories.primary NULL coverage
- **Severity**: Important
- **Description**: No logging on how many places lack `categories.primary` values
- **Why deferred**: Doesn't cause failures; impact on categorization unclear
- **To address**:
  - Add logging during Overture import to track NULL `categories.primary` rate
  - Analyze whether places without primary categories are adequately searchable
  - Consider fallback categorization logic if coverage is poor

### DATA-11: OSM way node reference resolution
- **Severity**: Important
- **Description**: No logging on how many OSM way node references fail to resolve
- **Why deferred**: QuackOSM handles this internally; impact unclear
- **To address**:
  - Add resolution quality logging during OSM import
  - Track percentage of ways with unresolved node references
  - Document expected failure rate and whether it affects result quality

## Architectural Issues

These items require significant design or implementation work beyond simple bug fixes.

### DATA-4: Antimeridian-spanning features excluded
- **Severity**: Important
- **Description**: Features crossing the antimeridian are dropped by bbox validation
- **Why deferred**: Rare edge case; fix requires spatial logic changes
- **To address**:
  - Implement antimeridian-aware bbox validation using OR logic: `xmin > xmax` indicates antimeridian crossing
  - Update spatial joins to handle features that span the ±180° meridian
  - Add test coverage for antimeridian-spanning geometries
  - Document which stages/processes handle antimeridian cases

### SPATIAL-3: Density bbox-overlap join over-includes tiles
- **Severity**: Important
- **Description**: Density computation includes tiles that only minimally overlap division bboxes, skewing density values
- **Why deferred**: Requires algorithm redesign; current approach is conservative
- **To address** (choose one):
  1. **Weight by intersection area**: Multiply counts by `(intersection_area / tile_area)` before summing
  2. **Accept as known limitation**: Document that density is approximate and tends to overestimate near division edges
  3. **Centroid-based inclusion**: Only include tiles whose centroid falls within the division
  - If implementing weighted approach, profile performance impact
  - Add test coverage comparing weighted vs unweighted density values

### SPATIAL-6: Phase 2 containment has no R-tree on temp table
- **Severity**: Important
- **Description**: DuckDB temp tables don't support R-tree indexes; containment query performance suffers
- **Why deferred**: Bbox pre-filter provides adequate performance for current data volumes
- **To address**:
  - Evaluate whether current performance is acceptable as data scales
  - If not, consider using a regular table instead of temp table for phase 2 containment
  - Alternative: materialize phase 2 results in a persistent table with R-tree
  - Document performance characteristics and scaling expectations

### EXPORT-4: strip_json_nulls vulnerable to special key chars
- **Severity**: Important
- **Description**: Custom `strip_json_nulls()` function may fail on keys containing special characters like `{`, `}`, `"`, or `,`
- **Why deferred**: Edge case; no known failures in production; native function in progress
- **To address**:
  - Monitor DuckDB PR #21748 for native `json_strip_nulls()` implementation
  - When available, replace custom function with native implementation
  - In the meantime, add validation for special characters in JSON keys
  - Add test coverage for keys with problematic characters

## Cosmetic Issues

These items are low-priority polish or edge cases that don't significantly impact functionality.

### SCORE-7: Importance theoretically exceeds 100 with non-default constants
- **Severity**: Cosmetic
- **Description**: Importance score can exceed 100.0 if JW_THRESHOLD or POPULATION_LOG_SCALE constants are changed
- **Why deferred**: Only occurs with non-default constants; current implementation bounds correctly
- **To address**: Document that importance is normalized to 0-100 with default constants; non-default values may produce scores outside this range

### SCORE-8: Negative density/IDF inputs produce negative importance
- **Severity**: Cosmetic
- **Description**: Missing or corrupted density/IDF values can produce negative importance scores
- **Why deferred**: Should be prevented by validation; indicates data corruption if it occurs
- **To address**: Add validation to ensure density and IDF are non-negative before importance computation

### SCORE-9: IDF ln(N/0) theoretically possible
- **Severity**: Cosmetic
- **Description**: Division by zero in IDF calculation if a trigram appears in zero places (should be impossible)
- **Why deferred**: Trigram filtering prevents this; indicates logic error if it occurs
- **To address**: Add defensive check for zero denominator; log error if encountered

### SPATIAL-7: Points exactly on boundary edges
- **Severity**: Cosmetic
- **Description**: Points exactly on division boundaries may be assigned to one division arbitrarily
- **Why deferred**: Rare edge case; any consistent assignment is acceptable
- **To address**: Document that boundary cases are resolved by whichever division returns first from spatial query

### SPATIAL-8: ST_Union_Agg memory pressure
- **Severity**: Cosmetic
- **Description**: Unioning many geometries may cause memory pressure during export
- **Why deferred**: Hasn't caused issues in practice; DuckDB handles spilling to disk
- **To address**: Monitor memory usage during exports; consider batching unions if needed

### DATA-7: Overture names struct empty containers
- **Severity**: Cosmetic
- **Description**: `names.common`, `names.alt`, etc. may contain empty arrays or objects rather than NULL
- **Why deferred**: Doesn't cause failures; minor inefficiency
- **To address**: Normalize empty containers to NULL during import for cleaner JSON

### DATA-9: OSM synthetic bbox at coordinate boundaries
- **Severity**: Cosmetic
- **Description**: OSM nodes at exact coordinate boundaries (±90°, ±180°) may produce invalid bboxes
- **Why deferred**: Extremely rare; QuackOSM handles this
- **To address**: Add validation for bbox bounds after QuackOSM import; clamp to valid ranges if needed

### DATA-10: Empty/whitespace-only names not filtered
- **Severity**: Cosmetic
- **Description**: Names that are empty strings or only whitespace are not filtered during import
- **Why deferred**: Doesn't cause failures; rare in real data
- **To address**: Add validation to filter `TRIM(name) != ''` during name extraction

### DATA-12: Coordinate range validation
- **Severity**: Cosmetic
- **Description**: No explicit validation that coordinate values are within valid ranges (latitude: ±90°, longitude: ±180°)
- **Why deferred**: Overlaps SPATIAL-1; DuckDB spatial functions typically reject invalid coordinates
- **To address**: (If SPATIAL-1 is addressed) Ensure same validation covers this case

### EXPORT-5: Coordinate precision DECIMAL(10,6)
- **Severity**: Cosmetic
- **Description**: Coordinates are stored with DECIMAL(10,6), providing ~0.1m precision but may not match GeoJSON standard
- **Why deferred**: Precision is adequate for gazetteer use; no complaints
- **To address**: Document precision choice; evaluate whether higher precision is needed for specific use cases

### EXPORT-9: Variant deduplication allows same name with different metadata
- **Severity**: Cosmetic
- **Description**: Name variants with identical names but different language/origin are kept as separate entries
- **Why deferred**: Current behavior may be intentional; language metadata is valuable
- **To address**: Evaluate whether variants should be de-duplicated by name alone; document decision either way

### EXPORT-10: Linear scan for record lookup in tiles
- **Severity**: Cosmetic
- **Description**: Looking up records within tiles uses linear scan rather than index
- **Why deferred**: Tiles are small; linear scan is fast enough
- **To address**: If tiles grow large, consider adding index on tile_id; document current expected tile sizes

### EXPORT-11: Debug print() in production code
- **Severity**: Cosmetic
- **Description**: Debug print statements may remain in production code
- **Why deferred**: Doesn't affect functionality
- **To address**: Remove debug prints or replace with proper logging

### EXPORT-13: Coordinate range validation in tile assignment
- **Severity**: Cosmetic
- **Description**: No explicit validation that coordinates are within valid ranges during tile assignment
- **Why deferred**: Overlaps SPATIAL-1 and DATA-12; same validation applies
- **To address**: (If SPATIAL-1 is addressed) Ensure same validation covers this case
