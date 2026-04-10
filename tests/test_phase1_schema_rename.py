"""Phase 1 migration tests: lexicon schema cleanup and Overture collection rename.

These tests are written RED — they FAIL against the current codebase and
PASS once Phase 1 changes are implemented.

Part 1: Lexicon schema (garganorn/lexicon/place.json)
  - #relation must NOT have a `level` property
  - rkey description must use a generic example format (not WoF-specific)
  - `within` description must NOT contain the word "level"

Part 2: Collection rename
  - OvertureMaps.collection must equal "org.atgeo.places.overture.place"
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
CONFIG_YAML_PATH = os.path.join(REPO_ROOT, "config.yaml")


def _load_place_json():
    with open(PLACE_JSON_PATH) as f:
        return json.load(f)


def _load_config_yaml():
    with open(CONFIG_YAML_PATH) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Part 1: Lexicon schema
# ---------------------------------------------------------------------------

def test_relation_def_has_no_level_property():
    """#relation definition must NOT have a `level` property.

    The `level` property is WoF-specific and belongs in the WoF boundaries
    collection, not in the generic place relation schema.
    FAILS until `level` is removed from the #relation def in place.json.
    """
    schema = _load_place_json()
    relation_def = schema["defs"]["relation"]
    properties = relation_def.get("properties", {})
    assert "level" not in properties, (
        "place.json #relation.properties must not contain 'level'. "
        "Remove it — level is WoF-specific and does not belong in the generic relation schema."
    )


def test_relation_rkey_description_is_generic():
    """rkey description in #relation must use a generic example, not WoF-specific.

    The current description references `org.atgeo.places.wof:85922583`, which
    couples the generic relation schema to a specific data source.
    FAILS until the rkey description uses a source-neutral example.
    """
    schema = _load_place_json()
    rkey_desc = schema["defs"]["relation"]["properties"]["rkey"]["description"]
    assert "wof" not in rkey_desc.lower(), (
        f"place.json #relation.properties.rkey description must not reference WoF. "
        f"Got: {rkey_desc!r}. "
        "Replace with a generic example that does not name a specific data source."
    )


def test_within_description_has_no_level_word():
    """The `within` array description must NOT contain the word 'level'.

    Once `level` is removed from #relation, ordering by level is no longer
    expressed in the schema; references to it should be removed from descriptions.
    FAILS until 'level' is removed from the `within` description in place.json.
    """
    schema = _load_place_json()
    within_desc = (
        schema["defs"]["main"]["record"]["properties"]
        ["relations"]["properties"]["within"]["description"]
    )
    assert "level" not in within_desc.lower(), (
        f"place.json relations.within description must not contain 'level'. "
        f"Got: {within_desc!r}. "
        "Remove the level reference — level no longer exists on #relation."
    )


# ---------------------------------------------------------------------------
# Part 2: Collection rename
# ---------------------------------------------------------------------------

def test_overture_maps_collection_attribute():
    """OvertureMaps.collection must equal 'org.atgeo.places.overture.place'.

    The Overture collection is being renamed from 'org.atgeo.places.overture'
    to 'org.atgeo.places.overture.place' to align with the Overture divisions
    migration naming convention.
    FAILS until OvertureMaps.collection is updated in garganorn/database.py.
    """
    from garganorn.database import OvertureMaps
    db = OvertureMaps(":memory:")
    assert db.collection == "org.atgeo.places.overture.place", (
        f"OvertureMaps.collection should be 'org.atgeo.places.overture.place'. "
        f"Got: {db.collection!r}. "
        "Update the collection class attribute in garganorn/database.py."
    )


def test_config_yaml_has_overture_place_key():
    """config.yaml must have key 'org.atgeo.places.overture.place' under tiles.collections.

    The tile collection config key must match the renamed collection identifier.
    FAILS until config.yaml is updated to use 'org.atgeo.places.overture.place'.
    """
    config = _load_config_yaml()
    tile_collections = config.get("tiles", {}).get("collections", {})
    assert "org.atgeo.places.overture.place" in tile_collections, (
        f"config.yaml tiles.collections must have key 'org.atgeo.places.overture.place'. "
        f"Found keys: {list(tile_collections.keys())}. "
        "Rename 'org.atgeo.places.overture' to 'org.atgeo.places.overture.place'."
    )


def test_config_yaml_old_overture_key_gone():
    """config.yaml must NOT have the old key 'org.atgeo.places.overture' under tiles.collections.

    After the rename, the old key should be removed to avoid ambiguity.
    FAILS until the old key is removed from config.yaml.
    """
    config = _load_config_yaml()
    tile_collections = config.get("tiles", {}).get("collections", {})
    assert "org.atgeo.places.overture" not in tile_collections, (
        "config.yaml tiles.collections must not contain the old key 'org.atgeo.places.overture'. "
        "Remove it after adding 'org.atgeo.places.overture.place'."
    )
