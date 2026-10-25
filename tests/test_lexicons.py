"""The org.atgeo.* lexicons are publishable (T1-R9) and require only what a
reader needs (T1-R15)."""
import json
import subprocess

import pytest

from tests.lexicon_helpers import LEXICON_DIR, goat_validate, validate_def


def _atgeo_lexicon_files():
    return sorted(
        p for p in LEXICON_DIR.glob("*.json")
        if json.loads(p.read_text())["id"].startswith("org.atgeo.")
    )


def test_atgeo_lexicons_found():
    assert len(_atgeo_lexicon_files()) >= 7


# goat warns that `type` is a commonly reserved keyword. #variant.type and
# #relation.type are wire fields every SDK reads; renaming them is a lexicon
# break, not a lint fix.
ALLOWED_LINTS = {
    ("org.atgeo.place", "field-name-syntax", "name is a commonly reserved keyword: type"),
}


def test_atgeo_lexicons_parse_with_goat():
    """community.lexicon.location.* are excluded: they fail lint with
    [unlimited-string] and mirror the Lexicon Community schemas."""
    paths = [str(p) for p in _atgeo_lexicon_files()]
    result = subprocess.run(["goat", "lex", "parse", *paths], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_atgeo_lexicons_pass_goat_lint():
    paths = [str(p) for p in _atgeo_lexicon_files()]
    result = subprocess.run(
        ["goat", "lex", "lint", "--json", *paths], capture_output=True, text=True
    )
    lints = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    unexpected = [
        lint for lint in lints
        if (lint["nsid"], lint["lint-name"], lint["message"]) not in ALLOWED_LINTS
    ]
    assert unexpected == [], unexpected
    # A non-zero exit with no lints is goat failing, not a clean lint.
    assert result.returncode == 0 or lints, result.stdout + result.stderr


MINIMAL_PLACE = {"$type": "org.atgeo.place", "name": "x"}


def test_minimal_user_place_validates():
    goat_validate(MINIMAL_PLACE)


def test_tile_of_repo_records_validates():
    """A tile an app publishes: records with no importance and no origin,
    identified by their at:// uri, and no attribution header."""
    validate_def("org.atgeo.tilePayload", {
        "records": [{"value": MINIMAL_PLACE, "uri": "at://did:plc:x/org.atgeo.place/abc"}],
    })


def test_minimal_tileset_descriptor_validates():
    validate_def("org.atgeo.defs#tileset", {
        "slug": "x",
        "collections": [{"dataset": "example.org", "collection": "spots"}],
    })


# ---------------------------------------------------------------------------
# The lexicon break (#14 #15 #16 #18 #19)
# ---------------------------------------------------------------------------

def _lexicon(name):
    return json.loads((LEXICON_DIR / f"{name}.json").read_text())["defs"]


PLACE_VIEW = {
    "uri": "urn:atgeo:openstreetmap.org:way:5",
    "url": "https://places.example.org/openstreetmap.org/way/5",
    "importance": 62,
    "value": {"$type": "org.atgeo.place", "name": "x"},
    "attribution": [{"dataset": "openstreetmap.org", "collection": "way"}],
}


def test_place_view_new_shape_validates():
    """#15: getPlace#placeView is {uri, url, importance?, value, attribution}."""
    validate_def("org.atgeo.getPlace#placeView", PLACE_VIEW)


def test_place_view_old_shape_is_rejected():
    """#15: a view with `record` and no url/value/attribution no longer validates."""
    old = {"uri": PLACE_VIEW["uri"], "importance": 62, "record": PLACE_VIEW["value"]}
    with pytest.raises(Exception):
        validate_def("org.atgeo.getPlace#placeView", old)


def test_place_view_required_and_properties():
    """#15: required is exactly uri, url, value, attribution; `record` is gone."""
    view = _lexicon("getPlace")["placeView"]
    assert set(view["required"]) == {"uri", "url", "value", "attribution"}
    assert set(view["properties"]) == {"uri", "url", "importance", "value", "attribution"}
    assert view["properties"]["attribution"]["items"]["ref"] == "org.atgeo.defs#collectionAttribution"


TILESET = {
    "slug": "x",
    "collections": [{"dataset": "example.org", "collection": "spots"}],
}


def test_tileset_overview_records_validates():
    """#19: defs#tileset takes an optional overview {records: integer}."""
    validate_def("org.atgeo.defs#tileset", {**TILESET, "overview": {"records": 10}})


@pytest.mark.parametrize("overview", [{}, {"records": "x"}])
def test_tileset_overview_must_carry_integer_records(overview):
    """#19: overview without records, or with a non-integer count, is invalid."""
    with pytest.raises(Exception):
        validate_def("org.atgeo.defs#tileset", {**TILESET, "overview": overview})


def test_describe_service_has_max_tiles_and_informational_did():
    """#19: describeService.maxTiles is an optional integer (other producers
    need not send it); did is described as informational."""
    out = _lexicon("describeService")["main"]["output"]["schema"]
    assert out["properties"]["maxTiles"]["type"] == "integer"
    assert "maxTiles" not in out["required"]
    assert "Informational" in out["properties"]["did"]["description"]


def test_get_tiles_bbox_is_documented_west_south_east_north():
    """#16: getTiles.bbox is described as west,south,east,north."""
    desc = _lexicon("getTiles")["main"]["parameters"]["properties"]["bbox"]["description"]
    assert "west,south,east,north" in desc


def test_get_tiles_overview_points_at_the_descriptor_count():
    """#19: getTiles.overview refers to the descriptor's overview.records."""
    out = _lexicon("getTiles")["main"]["output"]["schema"]
    assert "overview.records" in out["properties"]["overview"]["description"]


def test_tile_record_uri_is_never_the_gazetteer_url():
    """#15: tileRecord.uri says it is never the gazetteer's URL for the record."""
    assert "never" in _lexicon("tilePayload")["tileRecord"]["properties"]["uri"]["description"]


def test_importance_text_scopes_comparability_to_one_producer():
    """#14: both importance descriptions say scores are comparable only across
    what one producer builds from one dataset."""
    tile = _lexicon("tilePayload")["tileRecord"]["properties"]["importance"]
    view = _lexicon("getPlace")["placeView"]["properties"]["importance"]
    assert "one producer" in tile["description"]
    assert "one producer" in view["description"]
