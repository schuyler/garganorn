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
