"""Tests for scripts/tiles_smoke.py."""
import os
import sys

import pytest

_SCRIPTS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
sys.path.insert(0, _SCRIPTS)
from tiles_smoke import quadkey_from_tile_url  # noqa: E402


@pytest.mark.parametrize("suffix", [".json", ".json.gz"])
def test_quadkey_from_tile_url_handles_both_suffixes(suffix):
    """#18: getTiles URLs end in .json; the older .json.gz URLs still parse."""
    assert quadkey_from_tile_url(f"https://x.example/tiles/s/20260101/012301/012301{suffix}") == "012301"
