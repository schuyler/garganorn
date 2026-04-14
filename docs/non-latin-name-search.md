# Non-Latin Name Search Limitation

## Problem

The gazetteer does not support searching for place names in non-Latin scripts (CJK, Arabic, Hebrew, Cyrillic, Thai, etc.). The trigram-based search system relies on Latin-script trigrams, and the fix for EXPORT-1 (empty trigram detection) now returns empty results for these queries instead of causing a database error.

## Current Behavior

When a user searches for a place name in a non-Latin script:

1. The query is processed through `_strip_accents()` which only removes diacritical marks from Latin characters
2. No trigrams are generated because the text contains no word boundaries
3. The trigram list is empty
4. EXPORT-1 fix detects this and returns an empty result set
5. A warning is logged: "No valid trigrams generated for query"

## Impact

- **User impact**: International users cannot search for places in their native scripts
- **Geographic coverage**: Major regions affected include East Asia (CJK), Middle East (Arabic, Hebrew, Eastern Europe (Cyrillic), South Asia (Devanagari, Thai, etc.)
- **Data availability**: Many Overture and OSM places have names in these scripts, but they're not searchable

## Root Cause

The trigram search system was designed for Latin-script text with explicit word boundaries. Non-Latin scripts have different characteristics:

- **CJK**: No spaces between words; characters are logograms
- **Arabic**: Connected script with optional vowel marks
- **Cyrillic**: Different character set but similar word-boundary behavior (could work with minor changes)

The `_strip_accents()` function in `garganorn/database.py` only handles Latin diacritics (á → a, ö → o, etc.) and doesn't transliterate or normalize non-Latin scripts.

## Proposed Solutions

### Option 1: Fallback to Substring Search

When trigram generation produces no results, fall back to a LIKE-based substring search.

**Pros**:
- Simple to implement
- Works for all scripts
- No schema changes required

**Cons**:
- No trigram index usage = slow full-text scan
- No relevance ranking
- Performance degrades with dataset size

**Implementation**:
1. Add new query path in `garganorn/database.py` for non-trigram search
2. Use `LIKE '%query%'` with case-insensitive collation
3. Consider adding `LIMIT` to prevent runaway queries
4. Log performance metrics to evaluate viability

**Code location**: `garganorn/database.py:places_query()` method

### Option 2: Script Detection and Specialized Handling

Detect the script of the query and apply appropriate search logic.

**Pros**:
- Optimal search per script
- Can add indexing strategies for specific scripts

**Cons**:
- More complex implementation
- Requires script detection library (e.g., `python-langdetect`, `unicodedata` script detection)
- Maintenance burden for multiple code paths

**Implementation**:
1. Use `unicodedata.name()` to detect character scripts
2. Route to appropriate search method:
   - Latin: Trigram search (current)
   - CJK: Bigram or character-based search
   - Arabic/Hebrew: Substring with right-to-left awareness
   - Cyrillic: Could use trigrams with character set expansion

### Option 3: Phonetic Transliteration

Normalize non-Latin scripts to phonetic Latin equivalents before trigram generation.

**Pros**:
- Leverages existing trigram infrastructure
- Cross-script search (用户 can find "Beijing" by searching "北京")

**Cons**:
- Requires transliteration library (e.g., `cjkvi`, `arabic-transliteration`)
- Loss of fidelity in transliteration
- May not work for all scripts
- Complex dependency management

**Libraries to evaluate**:
- CJK: `jieba` for segmentation, `kakasi` or `zhon` for conversion
- Arabic: `arabic-transliteration` or `pyarabic`
- Hebrew: `heb-eng-transliterator`
- Cyrillic: `transliterate` library

### Option 4: Postpone and Document

Defer international search support while clearly documenting the limitation.

**Pros**:
- No implementation cost
- Focus on core functionality

**Cons**:
- Poor user experience for international users
- Data exists but isn't accessible

## Recommendation

**Short-term**: Implement Option 1 (fallback substring search) as a stopgap. This makes non-Latin search functional, if slow, while a better solution is designed.

**Medium-term**: Evaluate Option 2 (script detection) based on query patterns and performance data from Option 1. If non-Latin queries are rare, the fallback may be sufficient. If they're common, invest in specialized handling.

**Long-term**: Consider Option 3 (phonetic transliteration) if cross-script search is valuable (e.g., allowing Latin-script users to find places by their native names).

## Implementation Priority

**High** - This is a correctness issue for international users. The current behavior (returning empty results) is technically better than crashing (the pre-EXPORT-1 behavior), but it's still a functional gap.

## Related Issues

- **EXPORT-1**: Empty trigram guard (introduces the empty result behavior)
- **EXPORT-2**: Whitespace-only queries (related edge case)
- **EXPORT-12**: Query string normalization (should handle non-Latin scripts)

## Testing Requirements

Any implementation must include tests for:

1. **Script detection**: Verify queries are correctly classified by script
2. **Fallback behavior**: Confirm substring search works when trigrams fail
3. **Performance**: Ensure substring queries don't timeout on full dataset
4. **Relevance**: Evaluate whether substring results are useful to users
5. **Edge cases**: Mixed-script queries, Latin queries with non-Latin place names

## Open Questions

1. What percentage of production queries are in non-Latin scripts?
2. Is acceptable performance for substring search feasible with current dataset size?
3. Should we prioritize specific scripts based on user geography?
4. Is cross-script search (finding "Beijing" via "北京") a requirement?
