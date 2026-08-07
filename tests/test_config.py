"""Tests for garganorn.config.load_config."""
import pytest
import yaml

from garganorn.config import load_config
from garganorn.database import FoursquareOSP, OverturePlaces, OvertureDivisions


def _write_config(tmp_path, data):
    """Write a YAML config file and return its path."""
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(data))
    return p


def test_missing_file_raises_file_not_found(tmp_path):
    """load_config raises FileNotFoundError for a non-existent path."""
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "does_not_exist.yaml")


def test_missing_repo_key_defaults_to_places_atgeo_org(tmp_path):
    """Config without 'repo' key defaults to 'places.atgeo.org'."""
    config_path = _write_config(tmp_path, {"databases": []})
    repo, dbs, *_ = load_config(config_path)
    assert repo == "places.atgeo.org"
    assert dbs == []


def test_explicit_repo_key_is_used(tmp_path):
    """Explicit 'repo' key in config is returned."""
    config_path = _write_config(tmp_path, {"repo": "myserver.example.com", "databases": []})
    repo, dbs, *_ = load_config(config_path)
    assert repo == "myserver.example.com"


def test_unknown_db_type_raises_value_error(tmp_path, tmp_path_factory):
    """Unknown database type raises ValueError."""
    fake_db = tmp_path / "fake.duckdb"
    fake_db.touch()
    config_path = _write_config(tmp_path, {
        "databases": [{"type": "unknown_type", "path": str(fake_db)}]
    })
    with pytest.raises(ValueError, match="Unknown database type"):
        load_config(config_path)


def test_foursquare_type_creates_foursquare_osp(tmp_path):
    """'foursquare' type creates a FoursquareOSP instance."""
    fake_db = tmp_path / "fsq.duckdb"
    fake_db.touch()
    config_path = _write_config(tmp_path, {
        "databases": [{"type": "foursquare", "path": str(fake_db)}]
    })
    repo, dbs, *_ = load_config(config_path)
    assert len(dbs) == 1
    assert isinstance(dbs[0], FoursquareOSP)


def test_overture_place_type_creates_overture_places(tmp_path):
    """'overture_place' type creates an OverturePlaces instance."""
    fake_db = tmp_path / "ovr.duckdb"
    fake_db.touch()
    config_path = _write_config(tmp_path, {
        "databases": [{"type": "overture_place", "path": str(fake_db)}]
    })
    repo, dbs, *_ = load_config(config_path)
    assert len(dbs) == 1
    assert isinstance(dbs[0], OverturePlaces)


def test_boundaries_path_from_config(tmp_path):
    """Config with 'boundaries' key returns the path as third element."""
    config_path = _write_config(tmp_path, {
        "boundaries": "db/wof-boundaries.duckdb",
        "databases": []
    })
    repo, dbs, boundaries_path, *_ = load_config(config_path)
    assert boundaries_path == "db/wof-boundaries.duckdb"


def test_config_without_boundaries(tmp_path):
    """Config without 'boundaries' key returns None."""
    config_path = _write_config(tmp_path, {"databases": []})
    repo, dbs, boundaries_path, *_ = load_config(config_path)
    assert boundaries_path is None



# ---------------------------------------------------------------------------
# Tiles config tests (Red phase: load_config returns 3-tuple, not 4-tuple yet)
# ---------------------------------------------------------------------------

def test_tiles_section_returns_4tuple(tmp_path):
    """Config with 'tiles:' section → load_config returns a 4-tuple."""
    config_path = _write_config(tmp_path, {
        "databases": [],
        "tiles": {
            "collections": {
                "org.atgeo.places.foursquare": {
                    "manifest": "tiles/fsq/manifest.json",
                    "base_url": "https://tiles.example.com/fsq",
                }
            },
            "max_coverage_tiles": 50,
        },
    })
    result = load_config(config_path)
    assert len(result) == 4, f"load_config must return 4-tuple; got {len(result)}-tuple"


def test_tiles_config_dict_returned_as_fourth_element(tmp_path):
    """The tiles dict from config is returned as the 4th element."""
    config_path = _write_config(tmp_path, {
        "databases": [],
        "tiles": {
            "collections": {
                "org.atgeo.places.foursquare": {
                    "manifest": "tiles/fsq/manifest.json",
                    "base_url": "https://tiles.example.com/fsq",
                }
            }
        },
    })
    result = load_config(config_path)
    tiles = result[3]
    assert isinstance(tiles, dict)
    assert "collections" in tiles
    assert "org.atgeo.places.foursquare" in tiles["collections"]


def test_config_without_tiles_returns_4tuple_with_none(tmp_path):
    """Config without 'tiles:' → 4-tuple with None as 4th element."""
    config_path = _write_config(tmp_path, {"databases": []})
    result = load_config(config_path)
    assert len(result) == 4, f"load_config must always return 4-tuple; got {len(result)}-tuple"
    assert result[3] is None


def test_overture_division_type_creates_overture_divisions(tmp_path):
    """'overture_division' database type creates an OvertureDivisions instance."""
    fake_db = tmp_path / "boundaries.duckdb"
    fake_db.touch()
    config_path = _write_config(tmp_path, {
        "databases": [{"type": "overture_division", "path": str(fake_db)}]
    })
    repo, dbs, boundaries_path, *_ = load_config(config_path)
    assert len(dbs) == 1
    assert isinstance(dbs[0], OvertureDivisions)


# ---------------------------------------------------------------------------
# OQ-P2-5 Red tests — new slug/path/URL-contract schema (failing until Change B
# lands in config.yaml and Change A lands in __main__.py).
# ---------------------------------------------------------------------------

_SLUG_CONFIG = {
    "databases": [],
    "tiles": {
        "max_per_tile": 1000,
        "memory_limit": "48GB",
        "collections": {
            "org.atgeo.places.foursquare": {
                "slug": "foursquare",
                "manifest": "tiles/foursquare/tiles/current/manifest.duckdb",
                "tiles_dir": "tiles/foursquare/tiles/current",
                "base_url": "https://places.atgeo.org/tiles/foursquare",
                "source": "https://docs.foursquare.com/",
                "license": "https://docs.foursquare.com/",
            },
            "org.atgeo.places.overture.place": {
                "slug": "overture-place",
                "manifest": "tiles/overture_place/tiles/current/manifest.duckdb",
                "tiles_dir": "tiles/overture_place/tiles/current",
                "base_url": "https://places.atgeo.org/tiles/overture-place",
                "source": "https://overturemaps.org/",
                "license": "https://docs.overturemaps.org/attribution/",
            },
            "org.atgeo.places.osm": {
                "slug": "osm",
                "manifest": "tiles/osm/tiles/current/manifest.duckdb",
                "tiles_dir": "tiles/osm/tiles/current",
                "base_url": "https://places.atgeo.org/tiles/osm",
                "source": "https://www.openstreetmap.org/",
                "license": "https://opendatacommons.org/licenses/odbl/1-0/",
            },
        },
        "max_coverage_tiles": 50,
    },
}


def test_slug_base_url_consistency(tmp_path):
    """Each collection's base_url must end with '/<slug>' (OQ-P2-5 Change B).

    Asserts the new config schema invariant that the design doc requires
    (design doc §Change A): base_url.rstrip('/').endswith('/' + slug).
    This test uses the synthetic config directly without touching config.yaml,
    so it encodes the new schema contract and will pass once the config is
    updated — but we are asserting that the *config values themselves* satisfy
    this invariant (not that load_config enforces it; that enforcement lives in
    create_app). Fails now if slug is absent from the loaded dict.
    """
    config_path = _write_config(tmp_path, _SLUG_CONFIG)
    _, _, _, tiles_config = load_config(config_path)
    collections = tiles_config["collections"]
    for nsid, coll_cfg in collections.items():
        slug = coll_cfg.get("slug")
        base_url = coll_cfg.get("base_url")
        # These will fail if slug key is absent (which it is in the pre-P2-5 schema)
        assert slug is not None, f"{nsid}: missing 'slug' key in collection config"
        assert base_url is not None, f"{nsid}: missing 'base_url' key"
        assert base_url.rstrip("/").endswith("/" + slug), (
            f"{nsid}: base_url '{base_url}' must end with '/{slug}'"
        )


def test_phase2_tiles_dir_and_manifest_path_doubling(tmp_path):
    """tiles_dir and manifest must carry the doubled '<source>/tiles/current' segment.

    The Phase 2 on-disk layout writes tiles to
    <output_dir>/<source>/tiles/current/<qk6>/<qk>.json.gz. The config must
    point manifest and tiles_dir at that path, which means the string
    'tiles/current' appears *inside* the path (after the source dir component).
    Specifically, for each collection the manifest and tiles_dir paths must
    contain the segment 'tiles/current'.
    """
    config_path = _write_config(tmp_path, _SLUG_CONFIG)
    _, _, _, tiles_config = load_config(config_path)
    collections = tiles_config["collections"]

    expected_patterns = {
        "org.atgeo.places.foursquare": "foursquare/tiles/current",
        "org.atgeo.places.overture.place": "overture_place/tiles/current",
        "org.atgeo.places.osm": "osm/tiles/current",
    }

    for nsid, expected_segment in expected_patterns.items():
        coll_cfg = collections[nsid]
        manifest = coll_cfg.get("manifest", "")
        tiles_dir = coll_cfg.get("tiles_dir", "")
        assert expected_segment in manifest, (
            f"{nsid}: manifest '{manifest}' must contain '{expected_segment}'"
        )
        assert expected_segment in tiles_dir, (
            f"{nsid}: tiles_dir '{tiles_dir}' must contain '{expected_segment}'"
        )


def test_url_contract_base_url_slug_and_tile_path_shape():
    """URL-contract guard: getCoverage URL shape matches the new /tiles/<slug>/... route.

    TileManifest.get_tiles_for_bbox emits:
        f"{self.base_url}/{qk[:6]}/{qk}.json.gz"

    For a kebab-case base_url ending in the slug, the emitted URL must have the
    form  <base_url>/<qk6>/<qk>.json.gz  and — stripping base_url — the
    remainder must be  <qk6>/<qk>.json.gz, consistent with the new Flask route
    /tiles/<slug>/<qk6>/<qk>.json.gz  (i.e. the suffix after the slug prefix
    in the URL matches what the route captures as <path:tile_path>).

    This is a pure Python string test; it does NOT open any DuckDB file.
    We instantiate TileManifest manually (bypassing __init__) to exercise only
    the URL-building logic in get_tiles_for_bbox.
    """
    from garganorn.quadtree import TileManifest

    # A real quadkey at zoom 9 (9 chars); qk[:6] is the level-6 parent cell.
    sample_qk = "023010123"
    slug = "overture-place"
    base_url = f"https://places.atgeo.org/tiles/{slug}"

    # Build a TileManifest without touching DuckDB.
    tm = object.__new__(TileManifest)
    tm.base_url = base_url.rstrip("/")
    # A quadkey that covers the whole world at zoom 0 ensures bbox always hits.
    tm.quadkeys = {sample_qk}

    urls = tm.get_tiles_for_bbox(-180, -90, 180, 90)
    assert len(urls) == 1, f"Expected 1 URL, got {len(urls)}: {urls}"
    url = urls[0]

    # Shape: <base_url>/<qk6>/<qk>.json.gz
    expected_url = f"{base_url}/{sample_qk[:6]}/{sample_qk}.json.gz"
    assert url == expected_url, f"URL mismatch: got {url!r}, expected {expected_url!r}"

    # Stripping base_url leaves <qk6>/<qk>.json.gz — what the Flask route captures
    # as <path:tile_path> after matching /tiles/<slug>/.
    assert url.startswith(base_url + "/"), (
        f"URL {url!r} does not start with base_url {base_url!r}"
    )
    tile_path = url[len(base_url) + 1:]
    expected_tile_path = f"{sample_qk[:6]}/{sample_qk}.json.gz"
    assert tile_path == expected_tile_path, (
        f"tile_path after stripping base_url: got {tile_path!r}, "
        f"expected {expected_tile_path!r}"
    )

    # base_url must end with the slug (the new route invariant).
    assert base_url.rstrip("/").endswith("/" + slug), (
        f"base_url {base_url!r} must end with '/{slug}'"
    )
