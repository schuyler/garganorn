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


@pytest.mark.parametrize("command", ["lint", "parse"])
def test_atgeo_lexicons_pass_goat(command):
    """community.lexicon.location.* are excluded: they fail lint with
    [unlimited-string] and are not ours to change."""
    paths = [str(p) for p in _atgeo_lexicon_files()]
    result = subprocess.run(["goat", "lex", command, *paths], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


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
