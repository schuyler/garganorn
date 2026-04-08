"""Tests for garganorn.config.load_config."""
import pytest
import yaml

from garganorn.config import load_config
from garganorn.database import FoursquareOSP, OvertureMaps
from garganorn.boundaries import WhosOnFirst


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


def test_overture_type_creates_overture_maps(tmp_path):
    """'overture' type creates an OvertureMaps instance."""
    fake_db = tmp_path / "ovr.duckdb"
    fake_db.touch()
    config_path = _write_config(tmp_path, {
        "databases": [{"type": "overture", "path": str(fake_db)}]
    })
    repo, dbs, *_ = load_config(config_path)
    assert len(dbs) == 1
    assert isinstance(dbs[0], OvertureMaps)


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


def test_wof_type_creates_whos_on_first(tmp_path):
    """'wof' database type creates a WhosOnFirst instance."""
    fake_db = tmp_path / "wof.duckdb"
    fake_db.touch()
    config_path = _write_config(tmp_path, {
        "databases": [{"type": "wof", "path": str(fake_db)}]
    })
    repo, dbs, boundaries_path, *_ = load_config(config_path)
    assert len(dbs) == 1
    assert isinstance(dbs[0], WhosOnFirst)


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
