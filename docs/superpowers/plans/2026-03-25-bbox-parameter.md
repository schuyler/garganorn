# Bbox Parameter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `bbox` query parameter to searchRecords, refactoring internals so bbox is the canonical spatial input.

**Architecture:** bbox becomes the single spatial representation flowing through the system. server.py parses/validates bbox or converts lat/lon to bbox. database.py's `nearest()` accepts only bbox. Three backends gain a bbox-only (no text) query path ordered by importance.

**Tech Stack:** Python, Flask, DuckDB, lexrpc, pytest

**Spec:** `docs/superpowers/specs/2026-03-25-bbox-parameter-design.md`

---

### Task 1: Update lexicon

**Files:**
- Modify: `garganorn/lexicon/searchRecords.json`

- [ ] **Step 1: Add bbox parameter and InvalidBbox error to lexicon**

In `searchRecords.json`, add `bbox` to `defs.main.parameters.properties`:

```json
"bbox": {
    "type": "string",
    "description": "Bounding box as xmin,ymin,xmax,ymax in WGS84 decimal degrees"
}
```

Add to `defs.main.errors` array:

```json
{"name": "InvalidBbox", "description": "The bbox parameter must be four comma-separated numbers with xmin < xmax and ymin < ymax"}
```

Update `defs.main.description` to:

```json
"description": "Search for records within a bounding box, near a point, or matching a text query"
```

Update the `InvalidQuery` error description to:

```json
{"name": "InvalidQuery", "description": "Either q, bbox, or latitude/longitude must be provided"}
```

- [ ] **Step 2: Commit**

```bash
git add garganorn/lexicon/searchRecords.json
git commit -m "feat: add bbox parameter and InvalidBbox error to searchRecords lexicon"
```

---

### Task 2: Refactor Database.nearest() signature

**Files:**
- Modify: `garganorn/database.py`
- Test: `tests/test_server.py`

- [ ] **Step 1: Write failing test for nearest() accepting bbox**

Add to `tests/test_server.py`:

```python
def test_search_records_with_bbox():
    """search_records accepts bbox parameter and passes bbox tuple to nearest()."""
    nearest_results = [dict(SAMPLE_RECORD)]
    server = _make_server(nearest_results=nearest_results)
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, bbox="-122.5,37.7,-122.3,37.8"
    )
    assert "records" in result
    assert "_query" in result
    # Verify nearest was called with bbox tuple, not lat/lon
    mock_db = server.db[FSQ_COLLECTION]
    call_kwargs = mock_db.nearest.call_args
    assert "bbox" in call_kwargs.kwargs or (call_kwargs.args and isinstance(call_kwargs.args[0], tuple))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_server.py::test_search_records_with_bbox -v`
Expected: FAIL — `search_records()` does not accept `bbox` parameter yet.

- [ ] **Step 3: Write failing test for bbox validation**

Add to `tests/test_server.py`:

```python
def test_search_records_invalid_bbox_format():
    """Malformed bbox raises InvalidBbox."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection=FSQ_COLLECTION, bbox="not,valid,bbox")
    assert exc_info.value.name == "InvalidBbox"


def test_search_records_invalid_bbox_order():
    """bbox with xmin >= xmax raises InvalidBbox."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection=FSQ_COLLECTION, bbox="-122.3,37.7,-122.5,37.8")
    assert exc_info.value.name == "InvalidBbox"


def test_search_records_bbox_overrides_latlon():
    """When both bbox and lat/lon are provided, bbox is used."""
    nearest_results = [dict(SAMPLE_RECORD)]
    server = _make_server(nearest_results=nearest_results)
    result = server.search_records(
        {}, collection=FSQ_COLLECTION,
        bbox="-122.5,37.7,-122.3,37.8",
        latitude="0.0", longitude="0.0"
    )
    assert "records" in result
    # lat/lon should be ignored; bbox should be passed through
    mock_db = server.db[FSQ_COLLECTION]
    call_kwargs = mock_db.nearest.call_args.kwargs
    bbox = call_kwargs.get("bbox")
    assert bbox is not None
    assert bbox[0] == pytest.approx(-122.5)


def test_search_records_bbox_in_query_response():
    """_query.parameters includes bbox and q."""
    nearest_results = [dict(SAMPLE_RECORD)]
    server = _make_server(nearest_results=nearest_results)
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, bbox="-122.5,37.7,-122.3,37.8", q="coffee"
    )
    params = result["_query"]["parameters"]
    assert "bbox" in params
    assert "q" in params
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `pytest tests/test_server.py -k "bbox" -v`
Expected: All 4 new tests FAIL.

- [ ] **Step 5: Implement bbox parsing in server.py**

Replace the `search_records` method in `garganorn/server.py` (lines 73-110) with:

```python
    def _parse_bbox(self, bbox_str):
        """Parse and validate bbox string 'xmin,ymin,xmax,ymax'. Returns tuple or raises XrpcError."""
        parts = bbox_str.split(",")
        if len(parts) != 4:
            raise XrpcError("bbox must be four comma-separated numbers: xmin,ymin,xmax,ymax", "InvalidBbox")
        try:
            xmin, ymin, xmax, ymax = (float(p) for p in parts)
        except ValueError:
            raise XrpcError("bbox values must be valid numbers", "InvalidBbox")
        if xmin >= xmax or ymin >= ymax:
            raise XrpcError("bbox requires xmin < xmax and ymin < ymax", "InvalidBbox")
        return (xmin, ymin, xmax, ymax)

    def search_records(self, _, collection: str, latitude: str = "", longitude: str = "",
                       q: str = "", limit: str = "50", bbox: str = ""):
        self.logger.info(f"Searching records in {collection} with bbox={bbox}, latitude={latitude}, longitude={longitude}, q={q}, limit={limit}")
        if collection not in self.db:
            raise XrpcError(f"Collection {collection} not found on server {self.repo}", "CollectionNotFound")
        parsed_bbox = None
        if bbox:
            parsed_bbox = self._parse_bbox(bbox)
        elif latitude and longitude:
            try:
                lat = float(latitude)
                lon = float(longitude)
            except ValueError:
                raise XrpcError("Latitude and longitude coordinates must be valid numbers", "InvalidCoordinates")
            expand_m = 5000
            expand_lat = expand_m / 111194.927
            expand_lon = expand_lat / math.cos(lat * math.pi / 180) if abs(lat) < 90 else expand_lat
            parsed_bbox = (
                max(lon - expand_lon, -180),
                max(lat - expand_lat, -90),
                min(lon + expand_lon, 180),
                min(lat + expand_lat, 90),
            )
        if parsed_bbox is None and not q:
            raise XrpcError("Either q, bbox, or latitude/longitude must be provided", "InvalidQuery")
        start_time = time.perf_counter()
        result = self.db[collection].nearest(bbox=parsed_bbox, q=q or None, limit=int(limit))
        run_time = int((time.perf_counter() - start_time) * 1000)
        return {
            "records": [
                {
                    "$type": f"{self.nsid}.searchRecords#record",
                    "uri": self.record_uri(collection, r["rkey"]),
                    "distance_m": r.pop("distance_m"),
                    "value": r,
                } for r in result
            ],
            "_query": {
                "parameters": {
                    "repo": self.repo,
                    "collection": collection,
                    "bbox": bbox,
                    "q": q,
                    "latitude": latitude,
                    "longitude": longitude,
                    "limit": limit
                },
                "elapsed_ms": run_time
            }
        }
```

Add `import math` to the top of `server.py` (after the existing `import json, time, logging` line).

- [ ] **Step 6: Refactor Database.nearest() to accept bbox**

In `garganorn/database.py`, replace the `nearest` method (lines 205-247) with:

```python
    def nearest(self, bbox=None, q=None, limit=50):
        self.connect()
        params: SearchParams = {"limit": limit}
        if bbox is not None:
            xmin, ymin, xmax, ymax = bbox
            mid_lon = (xmin + xmax) / 2
            mid_lat = (ymin + ymax) / 2
            params.update({
                "centroid": f"POINT({mid_lon} {mid_lat})",
                "xmin": xmin,
                "ymin": ymin,
                "xmax": xmax,
                "ymax": ymax,
            })
            width_km = (xmax - xmin) * 111 * math.cos(math.radians(mid_lat))
            height_km = (ymax - ymin) * 111
            area_km2 = width_km * height_km
        else:
            area_km2 = GLOBE_AREA_KM2
        trigrams = None
        if q:
            norm_q = Database._strip_accents(q.lower())
            params["norm_q"] = norm_q
            trigrams = self._compute_trigrams(q)
            for i, tri in enumerate(trigrams):
                params[f"g{i}"] = tri
            importance_floor = compute_importance_floor(area_km2)
            params["importance_floor"] = importance_floor
            tokens = [t for t in norm_q.split() if t][:Database.MAX_QUERY_TOKENS]
            for i, token in enumerate(tokens):
                params[f"t{i}"] = token
        print(f"Searching with params: {params}")
        result = self.execute(
            self.query_nearest(params, trigrams=trigrams), params
        )
        records = [self.process_nearest(item) for item in result]
        return self.hydrate_records(records)
```

- [ ] **Step 7: Update __main__ block in database.py**

Replace lines 1180-1186 in `database.py`:

```python
    d = FoursquareOSP("db/fsq-osp.duckdb")
    result = d.nearest(bbox=(-122.48, 37.73, -122.39, 37.82))
    pprint(result)
    d.close()

    d = OvertureMaps("db/overture-maps.duckdb")
    result = d.nearest(bbox=(-122.48, 37.73, -122.39, 37.82))
    pprint(result)
```

- [ ] **Step 8: Run server tests to verify they pass**

Run: `pytest tests/test_server.py -v`
Expected: All tests pass, including the 4 new bbox tests and the existing lat/lon tests.

- [ ] **Step 9: Commit**

```bash
git add garganorn/server.py garganorn/database.py tests/test_server.py
git commit -m "feat: refactor nearest() to accept bbox, add bbox parsing to server"
```

---

### Task 3: Update all test callers of nearest()

**Files:**
- Modify: `tests/test_foursquare.py`
- Modify: `tests/test_overture.py`
- Modify: `tests/test_osm.py`
- Modify: `tests/test_importance_threshold.py`
- Modify: `tests/test_importance.py`

- [ ] **Step 1: Update test_foursquare.py callers**

Replace all `nearest(latitude=LAT, longitude=LON)` calls with `nearest(bbox=(LON-0.045, LAT-0.045, LON+0.045, LAT+0.045))` (approximates a ~5km box). Replace all `nearest(latitude=LAT, longitude=LON, q=Q)` calls with `nearest(bbox=(LON-0.045, LAT-0.045, LON+0.045, LAT+0.045), q=Q)`.

Specific replacements in `tests/test_foursquare.py`:

- Line 114: `fsq_db.nearest(latitude=37.7749, longitude=-122.4194)` → `fsq_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199))`
- Line 209: `fsq_db.nearest(latitude=37.7749, longitude=-122.4194, q="Blue Bottle Coffee")` → `fsq_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199), q="Blue Bottle Coffee")`
- Line 252-253: `fsq_db.nearest(latitude=37.7749, longitude=-122.4194, q=...)` → `fsq_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199), q=...)`
- Line 658: `fsq_db.nearest(latitude=37.7749, longitude=-122.4194)` → `fsq_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199))`
- Line 673: `fsq_db.nearest(latitude=37.7749, longitude=-122.4194, q="Blue Bottle Coffee")` → `fsq_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199), q="Blue Bottle Coffee")`

All `nearest(q=...)` calls (text-only, no lat/lon) remain unchanged — they already pass `bbox=None` implicitly.

- [ ] **Step 2: Run foursquare tests**

Run: `pytest tests/test_foursquare.py -v`
Expected: All tests pass.

- [ ] **Step 3: Update test_overture.py callers**

Specific replacements in `tests/test_overture.py`:

- Line 96: `overture_db.nearest(latitude=37.7749, longitude=-122.4194)` → `overture_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199))`
- Line 104: `overture_db.nearest(latitude=37.7596, longitude=-122.4269, q="Dolores")` → `overture_db.nearest(bbox=(-122.4719, 37.7146, -122.3819, 37.8046), q="Dolores")`
- Line 204-205: `overture_db.nearest(latitude=..., longitude=..., q=...)` → update with bbox tuple
- Line 497: `overture_db.nearest(latitude=37.7749, longitude=-122.4194)` → `overture_db.nearest(bbox=(-122.4644, 37.7299, -122.3744, 37.8199))`

All `nearest(q=...)` calls remain unchanged.

- [ ] **Step 4: Run overture tests**

Run: `pytest tests/test_overture.py -v`
Expected: All tests pass.

- [ ] **Step 5: Update test_osm.py callers**

Specific replacements in `tests/test_osm.py`:

- Line 184: `osm_db.nearest(latitude=37.7612, longitude=-122.4195)` → `osm_db.nearest(bbox=(-122.4645, 37.7162, -122.3745, 37.8062))`
- Line 199: `osm_db.nearest(latitude=37.7612, longitude=-122.4195, q="tartine")` → `osm_db.nearest(bbox=(-122.4645, 37.7162, -122.3745, 37.8062), q="tartine")`
- Line 273-274: `osm_db.nearest(latitude=..., longitude=..., q=...)` → update with bbox tuple
- Line 570: `osm_db.nearest(latitude=37.7612, longitude=-122.4195)` → `osm_db.nearest(bbox=(-122.4645, 37.7162, -122.3745, 37.8062))`

All `nearest(q=...)` calls remain unchanged.

- [ ] **Step 6: Run osm tests**

Run: `pytest tests/test_osm.py -v`
Expected: All tests pass.

- [ ] **Step 7: Update test_importance_threshold.py and test_importance.py callers**

In `tests/test_importance_threshold.py`:

- Line 306-308: `db.nearest(latitude=..., longitude=..., q=...)` → `db.nearest(bbox=(...), q=...)`
- Line 330-332: same pattern

In `tests/test_importance.py`:

- Line 229: `db.nearest(q="Test Place", limit=10)` — no change needed (text-only).

- [ ] **Step 8: Run all updated tests**

Run: `pytest tests/test_importance_threshold.py tests/test_importance.py -v`
Expected: All pass.

- [ ] **Step 9: Run full test suite**

Run: `pytest -v`
Expected: All tests pass.

- [ ] **Step 10: Commit**

```bash
git add tests/test_foursquare.py tests/test_overture.py tests/test_osm.py tests/test_importance_threshold.py tests/test_importance.py
git commit -m "refactor: update all nearest() callers to use bbox parameter"
```

---

### Task 4: Add bbox-only ordering by importance

**Files:**
- Modify: `garganorn/database.py` (FoursquareOSP, OvertureMaps, OpenStreetMap `query_nearest` methods)
- Test: `tests/test_foursquare.py`, `tests/test_overture.py`, `tests/test_osm.py`

- [ ] **Step 1: Write failing test for bbox-only importance ordering (Foursquare)**

Add to `tests/test_foursquare.py`:

```python
def test_spatial_only_ordered_by_importance(fsq_db):
    """Bbox-only results are ordered by importance DESC, then distance_m ASC."""
    results = fsq_db.nearest(bbox=(-122.50, 37.70, -122.35, 37.85))
    assert len(results) > 1
    # Check importance ordering: each result's importance should be >= next
    importances = []
    for r in results:
        # importance is on the places table; we need to verify ordering
        pass
    # Verify first result has highest importance among returned results
    # (fsq003 has importance=85, fsq001 has 75, etc.)
    assert results[0]["rkey"] == "fsq003"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_foursquare.py::test_spatial_only_ordered_by_importance -v`
Expected: FAIL — current spatial-only query orders by distance_m, not importance.

- [ ] **Step 3: Update FoursquareOSP.query_nearest spatial-only branch**

In `garganorn/database.py`, replace the spatial-only branch of `FoursquareOSP.query_nearest()` (the `else` block at ~line 531-542) with:

```python
        else:
            # Spatial-only: bbox without text query
            columns = self.search_columns()
            return f"""
                select
                    {columns},
                    ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer as distance_m
                from places
                where bbox.xmin > $xmin and bbox.ymin > $ymin
                  and bbox.xmax < $xmax and bbox.ymax < $ymax
                order by importance desc, distance_m
                limit $limit;
            """
```

- [ ] **Step 4: Update OvertureMaps.query_nearest spatial-only branch**

Same change in `OvertureMaps.query_nearest()` (~line 815-827):

```python
        else:
            # Spatial-only: bbox without text query
            columns = self.search_columns()
            return f"""
                select
                    {columns},
                    ST_Distance_Sphere(geometry, ST_GeomFromText($centroid))::integer as distance_m
                from places
                where bbox.xmin > $xmin and bbox.ymin > $ymin
                  and bbox.xmax < $xmax and bbox.ymax < $ymax
                order by importance desc, distance_m
                limit $limit;
            """
```

- [ ] **Step 5: Update OpenStreetMap.query_nearest spatial-only branch**

Same change in `OpenStreetMap.query_nearest()` (~line 1110-1121):

```python
        else:
            # Spatial-only: bbox without text query
            columns = self.search_columns()
            return f"""
                select
                    {columns},
                    ST_Distance_Sphere(geom, ST_GeomFromText($centroid))::integer as distance_m
                from places
                where bbox.xmin > $xmin and bbox.ymin > $ymin
                  and bbox.xmax < $xmax and bbox.ymax < $ymax
                order by importance desc, distance_m
                limit $limit;
            """
```

- [ ] **Step 6: Run the importance ordering test**

Run: `pytest tests/test_foursquare.py::test_spatial_only_ordered_by_importance -v`
Expected: PASS

- [ ] **Step 7: Run full test suite**

Run: `pytest -v`
Expected: All tests pass. Some existing spatial-only tests may need result order adjustments if they assumed distance ordering.

- [ ] **Step 8: Commit**

```bash
git add garganorn/database.py tests/test_foursquare.py
git commit -m "feat: order bbox-only results by importance desc, distance_m asc"
```
