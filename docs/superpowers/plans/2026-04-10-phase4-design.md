# Phase 4 Design: Division Database Class and Server Integration

## Summary

Phase 4 introduces the `OvertureDivision(Database)` class in `garganorn/boundaries.py` to enable serving division records via the XRPC `getRecord` API. It also registers the class in `config.py`, updates `config.yaml` with the division tile collection, and removes the now-superseded `WhosOnFirst` class and its fixtures.

## Research Findings

**Database subclass pattern** (from `garganorn/database.py`):
- All Database subclasses define `collection`, `attribution` class attributes
- Override `record_columns()` returning SQL column expressions
- Override `query_record()` returning a SELECT with `$rkey` parameter
- Override `process_record(result)` transforming the dict into the `org.atgeo.place` record shape
- Base `get_record()` calls `execute(query_record(), {"rkey": rkey})`, pops `importance`, calls `process_record()`
- Base `connect()` validates `name_index` table existence — **OvertureDivision must override this** since divisions have no name_index

**WhosOnFirst pattern** (from `garganorn/boundaries.py`):
- Overrides `connect()` to skip name_index validation
- `record_columns()` returns explicit column list from `boundaries` table
- `query_record()` selects from `boundaries WHERE rkey = $rkey` with `0 AS importance`
- `process_record()` builds locations (geo + bbox), parses `names_json` into variants, builds attributes
- `query_nearest()` raises `NotImplementedError`

**Division boundary DB schema** (from import SQL and quadtree.py boundary export):
- Work DB `places` table: `id, geometry, names, subtype, country, region, admin_level, wikidata, population, parent_division_id, bbox, qk17, min_latitude, max_latitude, min_longitude, max_longitude, importance, variants`
- Boundary DB `places` table: `id, geometry, admin_level, min_latitude, max_latitude, min_longitude, max_longitude` only

**Key finding**: The boundary DB does NOT contain `names`, `subtype`, `country`, `region`, `wikidata`, `population`, or `variants`. The `OvertureDivision` class cannot serve full records from the boundary DB alone.

**Server get_record flow** (from `garganorn/server.py`):
1. Checks `tile_collections` first (divisions served from tiles)
2. Falls back to `self.db` (Database instances)
3. Containment only computed for records with `community.lexicon.location.geo` locations — divisions have bbox-only, so containment naturally skipped

## Prerequisite: Boundary DB Export Enrichment

The boundary DB export in `quadtree.py` must be updated to include record-serving columns:

```sql
CREATE TABLE bnd.places AS
SELECT id, geometry, admin_level,
       names, subtype, country, region, wikidata, population,
       min_latitude, max_latitude,
       min_longitude, max_longitude,
       importance, variants
FROM places
ORDER BY ST_Hilbert(geometry,
    {'min_x': -180.0, 'min_y': -90.0,
     'max_x': 180.0, 'max_y': 90.0}::BOX_2D)
```

The geometry column (by far the largest) is already present, so the size increase is modest. The R-tree index on `geometry` is unaffected.

## Class Definition: `OvertureDivision(Database)`

Location: `garganorn/boundaries.py`, replacing `WhosOnFirst`

```python
class OvertureDivision(Database):
    """Minimal Database subclass for Overture division record resolution.

    Supports get_record only. No search, no name_index.
    """

    collection = "org.atgeo.places.overture.division"
    attribution = "https://docs.overturemaps.org/attribution/"

    def connect(self):
        """Connect to boundary database (no name_index validation)."""
        if self.conn is None:
            self.conn = duckdb.connect(str(self.db_path), read_only=True)
            self.temp_dir = tempfile.mkdtemp(prefix='duckdb_temp_')
            self.conn.execute(f"SET temp_directory='{self.temp_dir}'")
            # Spatial extension is required to open files containing GEOMETRY
            # columns — DuckDB cannot deserialize the type without it, even if
            # no ST_* functions are called.
            self._load_extension("spatial")
        return self.conn

    def record_columns(self):
        return """
            id AS rkey,
            names,
            subtype,
            country,
            region,
            admin_level,
            wikidata,
            population,
            min_latitude::decimal(10,6)::varchar AS min_latitude,
            min_longitude::decimal(10,6)::varchar AS min_longitude,
            max_latitude::decimal(10,6)::varchar AS max_latitude,
            max_longitude::decimal(10,6)::varchar AS max_longitude,
            variants
        """

    def query_record(self):
        columns = self.record_columns()
        return f"""
            SELECT {columns}, importance
            FROM places
            WHERE id = $rkey
        """

    def process_record(self, result):
        # Locations: bbox only (divisions are areas, not points)
        locations = []
        min_lat = result.pop("min_latitude", None)
        min_lon = result.pop("min_longitude", None)
        max_lat = result.pop("max_latitude", None)
        max_lon = result.pop("max_longitude", None)
        if all(v is not None for v in [min_lat, min_lon, max_lat, max_lon]):
            locations.append({
                "$type": "community.lexicon.location.bbox",
                "north": max_lat,
                "west": min_lon,
                "south": min_lat,
                "east": max_lon,
            })

        # Parse names struct into primary name + variants
        names = result.pop("names", None)
        name = None
        variants = []
        if names:
            name = names.get("primary")
            common = names.get("common")
            if common and isinstance(common, dict):
                for lang, lang_name in common.items():
                    if lang_name and lang_name != name:
                        variants.append({"name": lang_name, "language": lang})
            rules = names.get("rules")
            if rules:
                for rule in rules:
                    entry = {"name": rule["value"]}
                    if rule.get("language"):
                        entry["language"] = rule["language"]
                    if rule.get("variant"):
                        entry["type"] = rule["variant"]
                    variants.append(entry)

        # Note: pre-computed variants column is intentionally ignored to avoid
        # duplication if the import pipeline later adds variant extraction.
        # Names struct is the single source of truth for variants.
        result.pop("variants", None)

        # Build attributes
        subtype = result.pop("subtype", None)
        country = result.pop("country", None)
        region = result.pop("region", None)
        admin_level = result.pop("admin_level", None)
        wikidata = result.pop("wikidata", None)
        population = result.pop("population", None)

        attributes = {}
        if subtype:
            attributes["subtype"] = subtype
        if country:
            attributes["country"] = country
        if region:
            attributes["region"] = region
        if admin_level is not None:
            attributes["admin_level"] = admin_level
        if wikidata:
            attributes["wikidata"] = wikidata
        if population is not None and population > 0:
            attributes["population"] = population

        return {
            "$type": "org.atgeo.place",
            "collection": self.collection,
            "rkey": result.pop("rkey"),
            "locations": locations,
            "name": name or "",
            "variants": variants,
            "attributes": attributes,
        }

    def query_nearest(self, _params, trigrams=None):
        raise NotImplementedError("Division collection does not support search")
```

### Key design decisions in process_record()

- **Locations are bbox-only** (no geo point) — divisions are areas. Matches the tile export SQL.
- **Names struct parsed at query time** — the `names` struct has `.primary`, `.common` (MAP), `.rules` (list of structs). The import SQL currently sets `variants` to empty; parsing `names` at query time provides variant data without needing a variants pipeline stage. **Only `names` struct is parsed — the pre-computed `variants` column is NOT used**, to avoid duplication if the import pipeline later adds variant extraction. This matches a single-source-of-truth approach.
- **NULL names handled** — when `names` is NULL, `name` defaults to `""` and `variants` is empty.
- **Attributes**: subtype, country, region, admin_level, wikidata, population — matching the tile export attributes block.
- **Containment naturally skipped** — server.py only computes containment for records with `community.lexicon.location.geo` type locations.

## Config Changes

### `garganorn/config.py`

```python
from .boundaries import OvertureDivision  # remove WhosOnFirst import

DATABASE_TYPES = {
    "foursquare": FoursquareOSP,
    "overture": OvertureMaps,
    "osm": OpenStreetMap,
    "overture_division": OvertureDivision,  # replaces "wof": WhosOnFirst
}
```

### `garganorn/config.yaml`

No changes needed from Phase 4. The tile collection for divisions was configured in Phase 2. The `databases` list in config.yaml would optionally include an `overture_division` entry pointing to the boundary DB path, but this is only needed if TileBackedCollection is not configured.

### `garganorn/__main__.py`

No structural changes needed. `load_config()` already instantiates Database subclasses from the YAML `databases` list. If an `overture_division` entry is added to config.yaml, it will automatically create an `OvertureDivision` instance.

### `garganorn/server.py`

No changes needed. The get_record() flow already handles the OvertureDivision pattern correctly.

## WhosOnFirst Removal

Remove:
- `WhosOnFirst` class from `garganorn/boundaries.py`
- `WhosOnFirst` from `garganorn/__init__.py` export, add `OvertureDivision`
- `"wof": WhosOnFirst` from `garganorn/config.py` DATABASE_TYPES
- `WOF_BOUNDARIES` data, `_create_wof_db()`, `wof_db_path`, `wof_db` fixtures from `tests/conftest.py`
- `TestWhosOnFirstGetRecord` class from `tests/test_boundaries.py`
- `from garganorn.boundaries import WhosOnFirst` import in test_boundaries.py
- `test_wof_type_creates_whos_on_first` test and `WhosOnFirst` import from `tests/test_config.py`
- Update `test_config.yaml` and `test_config_missing_boundaries.yaml`: change `boundaries: /nonexistent/wof.duckdb` to `boundaries: /nonexistent/boundaries.duckdb`

## Test Changes

### New tests in `tests/test_boundaries.py`

```python
class TestOvertureDivisionGetRecord:
    def test_returns_correct_record_structure(self, division_db):
        ...  # bbox location, name, collection, rkey
    def test_bbox_location_values(self, division_db):
        ...  # north/south/east/west from extent
    def test_no_geo_location(self, division_db):
        ...  # divisions have bbox only, no geo point
    def test_variants_from_names_struct(self, division_db):
        ...  # common names parsed into variants
    def test_attributes(self, division_db):
        ...  # subtype, country, region, admin_level, wikidata, population
    def test_null_names_returns_empty_name_and_variants(self, division_db):
        ...  # record with names=NULL → name="", variants=[]
    def test_not_found_returns_none(self, division_db):
        ...
    def test_importance_is_zero(self, division_db):
        ...  # pipeline initializes importance to 0 (not structural)
    def test_query_nearest_raises(self, division_db):
        ...
```

### Fixture changes in `tests/conftest.py`

**Shared fixture note**: `_create_division_db()` is used by both `division_db_path` (which feeds `boundary_lookup`) and the new `division_db` fixture. Enriching the schema with record-serving columns (names, subtype, etc.) must preserve compatibility with `TestBoundaryLookupContainment`, which only queries `id`, `geometry`, and `admin_level`. Adding columns is safe — `BoundaryLookup.containment()` ignores extra columns. The INSERT statements must include values for all new columns in every row.

- Enrich `DIVISION_BOUNDARIES` data to include `names`, `subtype`, `country`, `region`, `wikidata`, `population`, `importance`, `variants`
- Include at least one row with `names=NULL` for the null-names test case
- Enrich `_create_division_db()` to create the full schema. The `CREATE TABLE` column order **must exactly match** the enriched boundary DB export SQL above:
  `id, geometry, admin_level, names, subtype, country, region, wikidata, population, min_latitude, max_latitude, min_longitude, max_longitude, importance, variants`
  INSERT statements **must use named-column syntax** (`INSERT INTO places (id, geometry, ...) VALUES (...)`) rather than positional syntax. The existing positional INSERT in `_create_division_db()` must be replaced. Positional INSERT with mismatched column order will cause `record_columns()` to silently read wrong values.
- Add `division_db` fixture (function-scoped, parallel to existing `wof_db`) yielding an `OvertureDivision` instance backed by `division_db_path`
- Remove `WOF_BOUNDARIES`, `_create_wof_db`, `wof_db_path`, `wof_db`

### New test in `tests/test_config.py`

- Add `test_overture_division_type_creates_overture_division` (parallel to removed `test_wof_type_creates_whos_on_first`)
- Remove `test_wof_type_creates_whos_on_first` and `WhosOnFirst` import

## Resolved Design Questions

1. **Boundary DB enrichment**: Yes — enrich the boundary DB with record-serving columns so `OvertureDivision` can serve full records as a fallback when tiles aren't configured. The geometry column dominates DB size, so adding metadata columns has modest impact.

2. **Variants strategy**: Parse `names` struct at query time only. Ignore the pre-computed `variants` column to avoid duplication if the import pipeline later adds variant extraction. Single source of truth.

3. **WhosOnFirst removal scope**: Yes — update `test_config.yaml` and `test_config_missing_boundaries.yaml` WoF path references alongside the Python code removal.

## Files to Create/Modify

| File | Action |
|------|--------|
| `garganorn/boundaries.py` | Add `OvertureDivision` class, remove `WhosOnFirst` class |
| `garganorn/config.py` | Replace `"wof": WhosOnFirst` with `"overture_division": OvertureDivision` |
| `garganorn/quadtree.py` | Enrich boundary DB export to include names, subtype, country, region, wikidata, population, importance, variants |
| `tests/conftest.py` | Enrich division fixtures; add division_db fixture; remove WoF fixtures |
| `tests/test_boundaries.py` | Add TestOvertureDivisionGetRecord; remove TestWhosOnFirstGetRecord |
| `garganorn/__init__.py` | Remove `WhosOnFirst` export, add `OvertureDivision` |
| `tests/test_config.py` | Remove `test_wof_type_creates_whos_on_first`, add `test_overture_division_type_creates_overture_division` |
| `test_config.yaml` | Update `boundaries:` path from wof.duckdb to boundaries.duckdb |
| `test_config_missing_boundaries.yaml` | Update `boundaries:` path from wof.duckdb to boundaries.duckdb |

No changes needed to:
- `garganorn/server.py` — containment naturally skipped for bbox-only records
- `garganorn/__main__.py` — config loading already handles new DATABASE_TYPES entries
