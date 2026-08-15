"""Lexicon schema cleanup and Overture collection naming.

Part 1: Lexicon schema (garganorn/lexicon/place.json)
  - rkey description must use a generic example format (not WoF-specific)

Part 2: Collection naming
  - OverturePlaces.collection must equal "org.atgeo.places.overture.place"
  - config.yaml must have key "org.atgeo.places.overture.place"
"""
import json
import os

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLACE_JSON_PATH = os.path.join(REPO_ROOT, "garganorn", "lexicon", "place.json")
CONFIG_YAML_PATH = os.path.join(REPO_ROOT, "config.yaml.example")


def _load_place_json():
    with open(PLACE_JSON_PATH) as f:
        return json.load(f)


def _load_config_yaml():
    with open(CONFIG_YAML_PATH) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Part 1: Lexicon schema
# ---------------------------------------------------------------------------

def test_relation_rkey_description_is_generic():
    """rkey description in #relation must use a generic example, not WoF-specific.

    The relation schema is source-neutral, so its rkey description must not
    name a specific data source.
    """
    schema = _load_place_json()
    rkey_desc = schema["defs"]["relation"]["properties"]["rkey"]["description"]
    assert "wof" not in rkey_desc.lower(), (
        f"place.json #relation.properties.rkey description must not reference WoF. "
        f"Got: {rkey_desc!r}. "
        "Replace with a generic example that does not name a specific data source."
    )


# ---------------------------------------------------------------------------
# Part 2: Collection naming
# ---------------------------------------------------------------------------

def test_overture_places_collection_attribute():
    """OverturePlaces.collection must equal 'org.atgeo.places.overture.place'.

    Matches the Overture divisions collection's naming convention
    ('org.atgeo.places.overture.division').
    """
    from garganorn.database import OverturePlaces
    db = OverturePlaces(":memory:")
    assert db.collection == "org.atgeo.places.overture.place", (
        f"OverturePlaces.collection must equal 'org.atgeo.places.overture.place'; "
        f"got {db.collection!r}"
    )


def test_config_yaml_has_overture_place_key():
    """config.yaml must have key 'org.atgeo.places.overture.place' under tiles.collections.

    The tile collection config key must match OverturePlaces.collection.
    """
    config = _load_config_yaml()
    tile_collections = config.get("tiles", {}).get("collections", {})
    assert "org.atgeo.places.overture.place" in tile_collections, (
        f"config.yaml tiles.collections must have key 'org.atgeo.places.overture.place'. "
        f"Found keys: {list(tile_collections.keys())}"
    )


def test_config_yaml_old_overture_key_gone():
    """config.yaml must not have the bare key 'org.atgeo.places.overture' under tiles.collections.

    Only 'org.atgeo.places.overture.place' identifies the Overture places collection.
    """
    config = _load_config_yaml()
    tile_collections = config.get("tiles", {}).get("collections", {})
    assert "org.atgeo.places.overture" not in tile_collections, (
        "config.yaml tiles.collections must not contain the bare key "
        "'org.atgeo.places.overture' (only 'org.atgeo.places.overture.place' is valid)"
    )
