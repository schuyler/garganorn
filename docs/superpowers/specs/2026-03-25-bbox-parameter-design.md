# Bbox Parameter for searchRecords

## Summary

Add a `bbox` query parameter (`"xmin,ymin,xmax,ymax"`) to `org.atgeo.searchRecords`. Refactor the internals so bbox is the canonical spatial input — lat/lon becomes a convenience that gets converted to bbox before reaching the query layer.

## Parameter Behavior

- **Format:** Comma-separated string `"xmin,ymin,xmax,ymax"` (WGS84 decimal degrees)
- **Validation:** Four parseable floats, xmin < xmax, ymin < ymax. Error: `InvalidBbox`.
- **Precedence:** When bbox is present, lat/lon are silently ignored.
- **Query validity:** At least one of `q`, `bbox`, or `lat/lon` must be provided.

## Query Modes (after change)

| bbox | q | Behavior |
|------|---|----------|
| yes  | no  | Return places contained in bbox, ordered by importance DESC. `distance_m` calculated from bbox center. |
| yes  | yes | Return places contained in bbox matching text query, ordered by score DESC then distance. Importance floor applies based on bbox area. |
| no   | yes | Text-only search (unchanged). Globe-sized area, highest importance floor. |
| no   | no  | Error: `InvalidQuery` (unchanged, assuming no lat/lon either). |

When lat/lon is provided without bbox, the server converts to a bbox by expanding ~5km (existing behavior), then proceeds as if bbox were provided.

## Internal Refactor

### server.py (`search_records`)

1. Accept new `bbox` parameter (string, optional).
2. If bbox provided: parse into `(xmin, ymin, xmax, ymax)` tuple, validate.
3. If lat/lon provided without bbox: compute bbox using existing expand logic (5km).
4. Derive centroid as bbox midpoint: `((xmin+xmax)/2, (ymin+ymax)/2)`.
5. Call `nearest(bbox, q, limit)` — no more lat/lon args.
6. Include bbox in `_query.parameters` response.
7. Include `q` in `_query.parameters` response (currently missing).

### database.py (`Database.nearest`)

Change signature from `(latitude=None, longitude=None, q=None, expand_m=5000, limit=50)` to `(bbox=None, q=None, limit=50)`. The `expand_m` parameter moves to `server.py` where lat/lon→bbox conversion happens.

- `bbox` is a `(xmin, ymin, xmax, ymax)` tuple, or None for text-only.
- Centroid derived internally as bbox midpoint.
- Area calculation for importance floor uses bbox dimensions (existing math).
- Downstream query construction is mostly unchanged — the three backends' `query_nearest()` already consume `xmin`, `ymin`, `xmax`, `ymax`, `centroid` params. However, the spatial-only path (bbox without q) currently orders by `distance_m`. This changes to `importance DESC, distance_m ASC` for bbox-only queries, requiring a new query variant in each backend.

### Lexicon (searchRecords.json)

Add `bbox` property:
```json
"bbox": {
    "type": "string",
    "description": "Bounding box as xmin,ymin,xmax,ymax in WGS84 decimal degrees"
}
```

Add error:
```json
{"name": "InvalidBbox", "description": "The bbox parameter must be four comma-separated numbers with xmin < xmax and ymin < ymax"}
```

Update description and `InvalidQuery` to mention bbox as an alternative spatial input.

## Tests

- bbox-only query returns places ordered by importance with distance from center
- bbox + q returns text-matched places within bbox
- bbox overrides lat/lon when both provided
- Invalid bbox formats raise `InvalidBbox`
- bbox with xmin >= xmax raises error
- Existing lat/lon tests continue to pass (converted to bbox internally)
- All direct callers of `nearest()` in tests updated to pass bbox tuples instead of lat/lon kwargs

## Out of Scope

- No bbox size constraints beyond xmin < xmax, ymin < ymax.
- No changes to pagination/cursor behavior.
- No changes to the three collection backends' `query_nearest()` SQL.
- Antimeridian-crossing bboxes (e.g. `170,-10,-170,10`) are not supported. This is a pre-existing limitation.
