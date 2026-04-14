# Design Document: Integration Tests for Quadtree Tile Export

## File

`tests/test_integration_quadtree.py`

## Purpose

Test the full chain that no existing test covers: `run_pipeline()` output consumed by `TileManifest`, `TileBackedCollection`, and `Server` endpoints. Existing tests either mock the manifest/tiles or test pipeline output properties in isolation.

## Required Imports

```python
import gzip
import json
import logging

import duckdb
import pytest
from lexrpc.base import XrpcError

from garganorn.quadtree import ATTRIBUTION, BboxTooLarge, TileManifest, run_pipeline
from garganorn.tile_reader import TileBackedCollection
from garganorn.server import Server
```

## Fixture Strategy

**Module-scoped pipeline output** (runs once per test module):

```python
@pytest.fixture(scope="module")
def pipeline_output(fsq_parquet, tmp_path_factory):
    """Run FSQ pipeline once; return resolved current/ directory path."""
    from garganorn.quadtree import run_pipeline
    output_dir = tmp_path_factory.mktemp("integration")
    run_pipeline("fsq", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                 str(output_dir), memory_limit="4GB", max_per_tile=100)
    current = output_dir / "fsq" / "current"
    assert current.exists()
    return current
```

This fixture depends on the session-scoped `fsq_parquet` from `conftest.py`. The pipeline takes a few seconds on 8 rows — acceptable for a module-scoped fixture.

**Edge-case fixtures** (function-scoped, each runs its own pipeline):

- `empty_pipeline_output` — Uses `fsq_parquet` but with an empty-ocean bbox `(0.0, 0.0, 0.01, 0.01)` where no FSQ_ROWS places exist.
- `single_place_parquet` + `single_place_output` — Creates a one-row parquet inline, runs pipeline. Verifies zoom-6 tile behavior.
- `dense_cluster_output` — Uses `fsq_parquet` with `max_per_tile=1` to force quadtree subdivision.

## Constants

```python
FSQ_COLLECTION = "org.atgeo.places.foursquare"
SF_BBOX_STR = "-122.55,37.60,-122.30,37.85"  # 2 decimal places, passes precision check
BASE_URL = "https://tiles.test.example.com"
REPO = "places.atgeo.org"
# rkeys expected to survive FSQ import filtering
EXPECTED_RKEYS = {"fsq001", "fsq002", "fsq008"}
```

## Test Classes

### Class 1: `TestPipelineToCoverage`

Tests chain: pipeline → TileManifest → Server.get_coverage → tile files on disk.

All tests use the module-scoped `pipeline_output` fixture.

| Test Method | What It Asserts |
|---|---|
| `test_coverage_returns_tile_urls` | Construct `TileManifest` from `pipeline_output / "manifest.duckdb"` and wire into `Server`. Call `server.get_coverage({}, collection=FSQ_COLLECTION, bbox=SF_BBOX_STR)`. Result has `"tiles"` key with at least one URL string. |
| `test_tile_urls_resolve_to_files` | For each URL in getCoverage result, strip `BASE_URL + "/"` prefix, join with `pipeline_output` path → file exists on disk and ends with `.json.gz`. |
| `test_tile_files_are_valid_gzipped_json` | `gzip.open` + `json.load` each tile file. Top-level keys include `"attribution"` (str) and `"records"` (list). |
| `test_tile_records_match_expected_schema` | For each record in each tile: `record["uri"]` starts with `"https://places.atgeo.org/"`, `record["value"]["$type"]` == `"org.atgeo.place"`, `record["value"]["rkey"]` is non-empty string, `record["value"]["name"]` is string, `record["value"]["importance"]` is int, `record["value"]["locations"]` is non-empty list, first location has `"$type"` == `"community.lexicon.location.geo"` with string `"latitude"` and `"longitude"` parseable as float. |
| `test_manifest_rkeys_match_tiles` | Read all rkeys from `manifest.duckdb` record_tiles table. Read all rkeys from tile JSON files. Assert the two sets are equal. Also assert they equal `EXPECTED_RKEYS`. |
| `test_manifest_metadata` | Open `manifest.duckdb`, query `metadata` table. Assert `source` == `"fsq"` and `generated_at` is a non-empty ISO timestamp string. (Task 9 explicitly requires metadata verification.) |

### Class 2: `TestPipelineToGetRecord`

Tests chain: pipeline → TileBackedCollection → Server.get_record.

All tests use the module-scoped `pipeline_output` fixture. Each test must call `TileBackedCollection._cached_read_tile.cache_clear()` in setup (or use a fixture that does so).

| Test Method | What It Asserts |
|---|---|
| `test_get_record_returns_known_place` | Build `TileBackedCollection` from pipeline output. Wire into `Server` as `tile_collections`. Call `server.get_record({}, repo=REPO, collection=FSQ_COLLECTION, rkey="fsq001")`. Result has `"uri"` containing `"fsq001"`, `"value"` dict with `"name"` == `"Blue Bottle Coffee"`. |
| `test_get_record_value_has_geo_location` | Same setup. Result `"value"["locations"]` has at least one entry with `$type` == `"community.lexicon.location.geo"`. |
| `test_get_record_nonexistent_rkey_raises` | `server.get_record({}, repo=REPO, collection=FSQ_COLLECTION, rkey="nonexistent")` raises `XrpcError` with name `"RecordNotFound"`. |
| `test_all_expected_rkeys_retrievable` | For each rkey in `EXPECTED_RKEYS`, `server.get_record` succeeds and returns a value with that rkey. |

### Class 3: `TestEdgeCases`

Function-scoped fixtures for each edge case scenario.

| Test Method | Fixture | What It Asserts |
|---|---|---|
| `test_empty_bbox_no_tiles` | `empty_pipeline_output` | `manifest.duckdb` exists, `record_tiles` has 0 rows. `TileManifest.get_tiles_for_bbox` returns `[]`. `Server.get_coverage` returns `{"tiles": []}`. No `.json.gz` files exist. |
| `test_single_place_one_zoom6_tile` | `single_place_output` | Exactly 1 `.json.gz` file. The quadkey in manifest has length 6. Tile contains exactly 1 record. |
| `test_dense_cluster_subdivides` | `dense_cluster_output` (fsq_parquet + max_per_tile=1) | More than 1 tile. At least one quadkey has length > 6. Each tile has <= 1 record (honoring max_per_tile). |
| `test_bbox_too_large_real_manifest` | `pipeline_output` | Construct `Server` with `max_coverage_tiles=0`. Call `get_coverage` with SF bbox. Raises `XrpcError` with name `"BboxTooLarge"`. Uses real `TileManifest`, not a mock. |

## Helper Functions

```python
def _build_server(pipeline_dir, max_coverage_tiles=50):
    """Build a Server with TileManifest and TileBackedCollection from pipeline output."""
    manifest_path = str(pipeline_dir / "manifest.duckdb")
    manifest = TileManifest(manifest_path, BASE_URL)
    collection = TileBackedCollection(
        collection=FSQ_COLLECTION,
        manifest_db_path=manifest_path,
        tiles_dir=str(pipeline_dir),
        attribution=ATTRIBUTION["fsq"],
    )
    return Server(
        REPO, dbs=[], logger=logging.getLogger("test"),
        tile_manifests={FSQ_COLLECTION: manifest},
        tile_collections={FSQ_COLLECTION: collection},
        max_coverage_tiles=max_coverage_tiles,
    )

def _collect_tile_records(pipeline_dir):
    """Read all records from all tile .json.gz files. Returns list of record dicts."""
    records = []
    for gz_path in pipeline_dir.rglob("*.json.gz"):
        with gzip.open(gz_path, "rt") as f:
            tile = json.load(f)
        records.extend(tile["records"])
    return records

def _make_single_place_parquet(tmp_path):
    """Create a parquet with exactly one FSQ place. Returns glob path.

    Follows the same schema as conftest.py's fsq_parquet fixture (tmp_fsq table).
    Must include: fsq_place_id, name, latitude, longitude, bbox struct,
    geom (VARCHAR WKT), date_refreshed, date_closed, fsq_category_ids (VARCHAR[]),
    and all other columns from fsq_import.sql expectations.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    parquet_path = tmp_path / "solo.parquet"
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    # Schema must match conftest.py's fsq_parquet fixture exactly
    conn.execute("""
        CREATE TABLE tmp_fsq (
            fsq_place_id VARCHAR, name VARCHAR, latitude DOUBLE, longitude DOUBLE,
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            geom VARCHAR, date_refreshed DATE, date_closed DATE, date_created DATE,
            address VARCHAR, locality VARCHAR, region VARCHAR, postcode VARCHAR,
            country VARCHAR, admin_region VARCHAR, post_town VARCHAR, po_box VARCHAR,
            tel VARCHAR, website VARCHAR, email VARCHAR, facebook_id VARCHAR,
            instagram VARCHAR, twitter VARCHAR,
            fsq_category_ids VARCHAR[], fsq_category_labels VARCHAR[],
            placemaker_url VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO tmp_fsq VALUES (
            'solo001', 'Solo Place', 37.7749, -122.4194,
            {'xmin': -122.4204, 'ymin': 37.7739, 'xmax': -122.4184, 'ymax': 37.7759},
            'POINT(-122.4194 37.7749)', '2023-01-01', NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL,
            ['13065143']::VARCHAR[], NULL::VARCHAR[], NULL
        )
    """)
    conn.execute(f"COPY tmp_fsq TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()
    return str(tmp_path / "*.parquet")
```

### Edge-case fixture patterns

All edge-case fixtures follow the same `run_pipeline()` pattern as `pipeline_output`:

```python
@pytest.fixture
def empty_pipeline_output(fsq_parquet, tmp_path):
    """Pipeline with bbox in open ocean — no places survive import."""
    output_dir = tmp_path / "empty"
    output_dir.mkdir()
    run_pipeline("fsq", fsq_parquet, (0.0, 0.0, 0.01, 0.01),
                 str(output_dir), memory_limit="4GB", max_per_tile=100)
    return output_dir / "fsq" / "current"

@pytest.fixture
def single_place_output(tmp_path):
    """Pipeline with exactly one place."""
    parquet_glob = _make_single_place_parquet(tmp_path / "parquet")
    output_dir = tmp_path / "single"
    output_dir.mkdir()
    run_pipeline("fsq", parquet_glob, (-122.55, 37.60, -122.30, 37.85),
                 str(output_dir), memory_limit="4GB", max_per_tile=100)
    return output_dir / "fsq" / "current"

@pytest.fixture
def dense_cluster_output(fsq_parquet, tmp_path):
    """Pipeline with max_per_tile=1 to force quadtree subdivision."""
    output_dir = tmp_path / "dense"
    output_dir.mkdir()
    run_pipeline("fsq", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                 str(output_dir), memory_limit="4GB", max_per_tile=1)
    return output_dir / "fsq" / "current"
```

## What Is NOT Tested Here (already covered elsewhere)

- `manifest.duckdb` metadata table fields (`test_pipeline.py::test_fsq_manifest_db`) — integration tests verify metadata exists but `test_pipeline.py` has the detailed assertions
- `manifest.json` content and format (`test_pipeline.py`)
- Atomic symlink and old-directory cleanup (`test_pipeline.py`)
- Individual SQL export query correctness (`test_export.py`)
- `quadkey_to_bbox`, `bboxes_intersect` math (`test_quadtree_functions.py`)
- `TileBackedCollection` cache behavior (`test_tile_reader.py`)
- `getCoverage` with mock manifests, bbox precision, error paths (`test_get_coverage.py`)
- CLI argument parsing (`test_pipeline.py`)

## Dependencies Between Tests

The `pipeline_output` fixture is shared by `TestPipelineToCoverage` and `TestPipelineToGetRecord`. Both classes are read-only consumers of the pipeline output. The `TileBackedCollection._cached_read_tile.cache_clear()` call in `TestPipelineToGetRecord` prevents stale cache entries but does not affect `TestPipelineToCoverage` (which reads tile files directly, not through the cache).

`TestEdgeCases` tests are fully independent — each creates its own pipeline output.
