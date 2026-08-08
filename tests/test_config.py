"""Tests for garganorn.config.load_config."""
import pytest
import yaml

from garganorn.config import load_config


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
    config_path = _write_config(tmp_path, {})
    repo, tiles = load_config(config_path)
    assert repo == "places.atgeo.org"


def test_explicit_repo_key_is_used(tmp_path):
    """Explicit 'repo' key in config is returned."""
    config_path = _write_config(tmp_path, {"repo": "myserver.example.com"})
    repo, tiles = load_config(config_path)
    assert repo == "myserver.example.com"


# ---------------------------------------------------------------------------
# Tiles config tests
# ---------------------------------------------------------------------------

def test_tiles_section_returned_as_second_element(tmp_path):
    """Config with 'tiles:' section is returned as the 2nd element."""
    config_path = _write_config(tmp_path, {
        "tiles": {
            "collections": {
                "org.atgeo.places.osm": {
                    "manifest": "tiles/osm/manifest.json",
                    "base_url": "https://tiles.example.com/osm",
                }
            },
            "max_coverage_tiles": 50,
        },
    })
    repo, tiles = load_config(config_path)
    assert isinstance(tiles, dict)
    assert "collections" in tiles
    assert "org.atgeo.places.osm" in tiles["collections"]


def test_config_without_tiles_returns_none(tmp_path):
    """Config without 'tiles:' key returns None as the 2nd element."""
    config_path = _write_config(tmp_path, {})
    repo, tiles = load_config(config_path)
    assert tiles is None


# ---------------------------------------------------------------------------
# OQ-P2-5 slug/path/URL-contract schema
# ---------------------------------------------------------------------------

_SLUG_CONFIG = {
    "tiles": {
        "max_per_tile": 1000,
        "memory_limit": "48GB",
        "collections": {
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

    Asserts the config schema invariant that the design doc requires (design
    doc §Change A): base_url.rstrip('/').endswith('/' + slug). This test uses
    the synthetic config directly without touching config.yaml, so it encodes
    the schema contract — enforcement of the invariant lives in create_app,
    not load_config.
    """
    config_path = _write_config(tmp_path, _SLUG_CONFIG)
    _, tiles_config = load_config(config_path)
    collections = tiles_config["collections"]
    for nsid, coll_cfg in collections.items():
        slug = coll_cfg.get("slug")
        base_url = coll_cfg.get("base_url")
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
    _, tiles_config = load_config(config_path)
    collections = tiles_config["collections"]

    expected_patterns = {
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
    """URL-contract guard: getCoverage URL shape matches the /tiles/<slug>/... route.

    TileManifest.get_tiles_for_bbox emits:
        f"{self.base_url}/{qk[:6]}/{qk}.json.gz"

    For a kebab-case base_url ending in the slug, the emitted URL must have the
    form  <base_url>/<qk6>/<qk>.json.gz  and — stripping base_url — the
    remainder must be  <qk6>/<qk>.json.gz, consistent with the Flask route
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
