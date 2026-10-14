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
    repo, tiles, _ = load_config(config_path)
    assert repo == "places.atgeo.org"


def test_explicit_repo_key_is_used(tmp_path):
    """Explicit 'repo' key in config is returned."""
    config_path = _write_config(tmp_path, {"repo": "myserver.example.com"})
    repo, tiles, _ = load_config(config_path)
    assert repo == "myserver.example.com"


# ---------------------------------------------------------------------------
# Tiles config tests
# ---------------------------------------------------------------------------

def test_tiles_section_returned_as_second_element(tmp_path):
    """Config with 'tiles:' section is returned as the 2nd element."""
    config_path = _write_config(tmp_path, {
        "tiles": {
            "tilesets": [{
                "run_dir": "tiles/osm/tiles/current",
                "base_url": "https://tiles.example.com/osm",
            }],
            "max_tiles": 50,
        },
    })
    repo, tiles, _ = load_config(config_path)
    assert isinstance(tiles, dict)
    assert tiles["tilesets"][0]["run_dir"] == "tiles/osm/tiles/current"
    assert tiles["max_tiles"] == 50


def test_config_without_tiles_returns_none(tmp_path):
    """Config without 'tiles:' key returns None as the 2nd element."""
    config_path = _write_config(tmp_path, {})
    repo, tiles, _ = load_config(config_path)
    assert tiles is None


# ---------------------------------------------------------------------------
# public_base
# ---------------------------------------------------------------------------

def test_public_base_defaults_to_https_repo(tmp_path):
    """Without 'public_base', it is https://{repo}."""
    config_path = _write_config(tmp_path, {"repo": "myserver.example.com"})
    _, _, public_base = load_config(config_path)
    assert public_base == "https://myserver.example.com"


def test_explicit_public_base_is_used(tmp_path):
    """Explicit 'public_base' is returned, independent of 'repo'."""
    config_path = _write_config(tmp_path, {
        "repo": "myserver.example.com",
        "public_base": "https://other.example.org",
    })
    _, _, public_base = load_config(config_path)
    assert public_base == "https://other.example.org"


def test_public_base_trailing_slash_stripped(tmp_path):
    config_path = _write_config(tmp_path, {"public_base": "https://other.example.org/"})
    _, _, public_base = load_config(config_path)
    assert public_base == "https://other.example.org"


@pytest.mark.parametrize("base", [
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://[::1]:8000",
    "http://192.168.1.20:8000",
    "http://169.254.1.1",
    "http://dev.localhost",
])
def test_public_base_accepts_http_development_hosts(tmp_path, base):
    config_path = _write_config(tmp_path, {"public_base": base})
    _, _, public_base = load_config(config_path)
    assert public_base == base


@pytest.mark.parametrize("base", [
    "http://example.org",
    "http://8.8.8.8",
    "ftp://localhost",
])
def test_public_base_rejects_non_development_http_and_other_schemes(tmp_path, base):
    config_path = _write_config(tmp_path, {"public_base": base})
    with pytest.raises(ValueError, match="public_base"):
        load_config(config_path)
