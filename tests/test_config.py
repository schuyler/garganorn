"""Tests for garganorn.config.load_config."""
import pytest
import yaml

from garganorn.config import load_config
from garganorn.database import FoursquareOSP, OvertureMaps, OpenStreetMap


def _write_config(tmp_path, data):
    """Write a YAML config file and return its path."""
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(data))
    return p


def test_missing_file_raises_file_not_found(tmp_path):
    """load_config raises FileNotFoundError for a non-existent path."""
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "does_not_exist.yaml")


def test_missing_repo_key_defaults_to_gazetteer_social(tmp_path):
    """Config without 'repo' key defaults to 'gazetteer.social'."""
    config_path = _write_config(tmp_path, {"databases": []})
    repo, dbs = load_config(config_path)
    assert repo == "gazetteer.social"
    assert dbs == []


def test_explicit_repo_key_is_used(tmp_path):
    """Explicit 'repo' key in config is returned."""
    config_path = _write_config(tmp_path, {"repo": "myserver.example.com", "databases": []})
    repo, dbs = load_config(config_path)
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
    repo, dbs = load_config(config_path)
    assert len(dbs) == 1
    assert isinstance(dbs[0], FoursquareOSP)


def test_overture_type_creates_overture_maps(tmp_path):
    """'overture' type creates an OvertureMaps instance."""
    fake_db = tmp_path / "ovr.duckdb"
    fake_db.touch()
    config_path = _write_config(tmp_path, {
        "databases": [{"type": "overture", "path": str(fake_db)}]
    })
    repo, dbs = load_config(config_path)
    assert len(dbs) == 1
    assert isinstance(dbs[0], OvertureMaps)


def test_osm_type_creates_openstreetmap(tmp_path):
    """'osm' type creates an OpenStreetMap instance."""
    fake_db = tmp_path / "osm.duckdb"
    fake_db.touch()
    config_path = _write_config(tmp_path, {
        "databases": [{"type": "osm", "path": str(fake_db)}]
    })
    repo, dbs = load_config(config_path)
    assert len(dbs) == 1
    assert isinstance(dbs[0], OpenStreetMap)
