"""Tests for quadtree Python functions: quadkey_to_bbox, bboxes_intersect,
BboxTooLarge, and TileManifest.

Red phase: these names don't exist in garganorn/quadtree.py yet, so the
module-level import raises ImportError at pytest collection time.
"""
import json
import math

import duckdb
import pytest

from garganorn.quadtree import (
    BboxTooLarge,
    TileManifest,
    bboxes_intersect,
    quadkey_to_bbox,
)

MERC_YMAX = 85.05112877980659
_BASE_URL = "https://tiles.example.com"

# Four zoom-6 quadkeys in distinct geographic areas.
_MANIFEST_QUADKEYS = [
    "023010",   # Pacific Northwest
    "023011",   # adjacent east of 023010
    "120301",   # eastern US
    "200000",   # Europe
]


def _make_manifest_file(tmp_path, quadkeys=None):
    """Write a manifest.json (matching write_manifest() format) and return its path."""
    if quadkeys is None:
        quadkeys = _MANIFEST_QUADKEYS
    data = {
        "source": "fsq",
        "generated_at": "2026-01-01T00:00:00+00:00",
        "quadkeys": sorted(quadkeys),
    }
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(data))
    return p


# ---------------------------------------------------------------------------
# quadkey_to_bbox
# ---------------------------------------------------------------------------

class TestQuadkeyToBbox:
    """quadkey_to_bbox(quadkey) -> (xmin, ymin, xmax, ymax)."""

    def test_empty_string_is_whole_world(self):
        xmin, ymin, xmax, ymax = quadkey_to_bbox("")
        assert xmin == pytest.approx(-180.0, abs=0.01)
        assert xmax == pytest.approx(180.0, abs=0.01)
        assert ymin == pytest.approx(-MERC_YMAX, abs=0.01)
        assert ymax == pytest.approx(MERC_YMAX, abs=0.01)

    def test_quadkey_0_is_nw_quadrant(self):
        """'0' = northwest quadrant: west hemisphere, upper half."""
        xmin, ymin, xmax, ymax = quadkey_to_bbox("0")
        assert xmin == pytest.approx(-180.0, abs=0.01)
        assert xmax == pytest.approx(0.0, abs=0.01)
        assert ymin == pytest.approx(0.0, abs=0.01)
        assert ymax == pytest.approx(MERC_YMAX, abs=0.01)

    def test_quadkey_3_is_se_quadrant(self):
        """'3' = southeast quadrant: east hemisphere, lower half."""
        xmin, ymin, xmax, ymax = quadkey_to_bbox("3")
        assert xmin == pytest.approx(0.0, abs=0.01)
        assert xmax == pytest.approx(180.0, abs=0.01)
        assert ymin == pytest.approx(-MERC_YMAX, abs=0.01)
        assert ymax == pytest.approx(0.0, abs=0.01)

    def test_zoom17_key_is_tiny_region(self):
        """A zoom-17 quadkey covers a very small geographic area."""
        qk = "12021302232332100"
        xmin, ymin, xmax, ymax = quadkey_to_bbox(qk)
        assert xmax - xmin < 0.01, f"lon span {xmax - xmin:.6f} should be < 0.01"
        assert ymax - ymin < 0.01, f"lat span {ymax - ymin:.6f} should be < 0.01"

    def test_zoom17_bbox_contains_original_point(self):
        """Roundtrip: ST_QuadKey → quadkey_to_bbox must contain the original point."""
        lat, lon = 37.7749, -122.4194  # San Francisco
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        qk = con.execute("SELECT ST_QuadKey(?, ?, 17)", [lon, lat]).fetchone()[0]
        xmin, ymin, xmax, ymax = quadkey_to_bbox(qk)
        assert xmin <= lon <= xmax, f"lon {lon} not in [{xmin}, {xmax}]"
        assert ymin <= lat <= ymax, f"lat {lat} not in [{ymin}, {ymax}]"

    def test_return_is_four_tuple(self):
        result = quadkey_to_bbox("13")
        assert len(result) == 4

    def test_bbox_values_are_finite_floats(self):
        xmin, ymin, xmax, ymax = quadkey_to_bbox("021")
        for v in (xmin, ymin, xmax, ymax):
            assert isinstance(v, float)
            assert math.isfinite(v)

    def test_xmin_less_than_xmax(self):
        xmin, ymin, xmax, ymax = quadkey_to_bbox("120")
        assert xmin < xmax

    def test_ymin_less_than_ymax(self):
        xmin, ymin, xmax, ymax = quadkey_to_bbox("120")
        assert ymin < ymax


# ---------------------------------------------------------------------------
# bboxes_intersect
# ---------------------------------------------------------------------------

class TestBboxesIntersect:
    """bboxes_intersect(a, b) -> bool."""

    def test_overlapping(self):
        assert bboxes_intersect((0, 0, 2, 2), (1, 1, 3, 3)) is True

    def test_symmetric(self):
        a, b = (0, 0, 2, 2), (1, 1, 3, 3)
        assert bboxes_intersect(a, b) == bboxes_intersect(b, a)

    def test_touching_edge(self):
        assert bboxes_intersect((0, 0, 1, 1), (1, 0, 2, 1)) is True

    def test_touching_corner(self):
        assert bboxes_intersect((0, 0, 1, 1), (1, 1, 2, 2)) is True

    def test_separate_x_axis(self):
        assert bboxes_intersect((0, 0, 1, 1), (2, 0, 3, 1)) is False

    def test_separate_y_axis(self):
        assert bboxes_intersect((0, 0, 1, 1), (0, 2, 1, 3)) is False

    def test_separate_symmetric(self):
        a, b = (0, 0, 1, 1), (2, 2, 3, 3)
        assert bboxes_intersect(a, b) == bboxes_intersect(b, a)

    def test_contained(self):
        assert bboxes_intersect((0, 0, 10, 10), (2, 2, 4, 4)) is True

    def test_same_bbox(self):
        a = (1, 1, 5, 5)
        assert bboxes_intersect(a, a) is True


# ---------------------------------------------------------------------------
# BboxTooLarge
# ---------------------------------------------------------------------------

class TestBboxTooLarge:
    """BboxTooLarge is a plain Exception (not XrpcError)."""

    def test_is_exception_subclass(self):
        assert issubclass(BboxTooLarge, Exception)

    def test_is_not_xrpc_error(self):
        """BboxTooLarge must NOT be an XrpcError — it's caught and re-raised by the server."""
        from lexrpc.base import XrpcError
        assert not issubclass(BboxTooLarge, XrpcError), (
            "BboxTooLarge should be a plain Exception; the server layer converts it to XrpcError"
        )

    def test_can_be_raised_and_caught(self):
        with pytest.raises(BboxTooLarge):
            raise BboxTooLarge()

    def test_message_preserved(self):
        with pytest.raises(BboxTooLarge) as exc_info:
            raise BboxTooLarge("too many tiles")
        assert "too many tiles" in str(exc_info.value)


# ---------------------------------------------------------------------------
# TileManifest
# ---------------------------------------------------------------------------

class TestTileManifest:
    """TileManifest(manifest_path, base_url).get_tiles_for_bbox(...)."""

    def test_url_format(self, tmp_path):
        """URLs follow {base_url}/{qk[:6]}/{qk}.json.gz format."""
        path = _make_manifest_file(tmp_path, quadkeys=["023010"])
        tm = TileManifest(str(path), _BASE_URL)
        urls = tm.get_tiles_for_bbox(-180, -85, 180, 85)
        assert urls == [f"{_BASE_URL}/023010/023010.json.gz"]

    def test_returns_all_intersecting_urls(self, tmp_path):
        """All intersecting tiles are returned (caller is responsible for sorting)."""
        path = _make_manifest_file(tmp_path)
        tm = TileManifest(str(path), _BASE_URL)
        urls = tm.get_tiles_for_bbox(-180, -85, 180, 85)
        assert set(urls) == {f"{_BASE_URL}/{qk[:6]}/{qk}.json.gz" for qk in _MANIFEST_QUADKEYS}

    def test_non_intersecting_excluded(self, tmp_path):
        """Tiles outside the query bbox are excluded."""
        qk = _MANIFEST_QUADKEYS[0]
        xmin, ymin, xmax, ymax = quadkey_to_bbox(qk)
        mid_lon = (xmin + xmax) / 2
        mid_lat = (ymin + ymax) / 2
        tiny = (mid_lon - 0.001, mid_lat - 0.001, mid_lon + 0.001, mid_lat + 0.001)
        path = _make_manifest_file(tmp_path)
        tm = TileManifest(str(path), _BASE_URL)
        urls = tm.get_tiles_for_bbox(*tiny)
        assert f"{_BASE_URL}/{qk[:6]}/{qk}.json.gz" in urls
        for other in _MANIFEST_QUADKEYS[1:]:
            assert f"{_BASE_URL}/{other[:6]}/{other}.json.gz" not in urls

    def test_raises_when_exceeds_max_tiles(self, tmp_path):
        """Raises BboxTooLarge when count exceeds max_tiles."""
        path = _make_manifest_file(tmp_path)
        tm = TileManifest(str(path), _BASE_URL)
        # 4 tiles match; max_tiles=2 should raise
        with pytest.raises(BboxTooLarge):
            tm.get_tiles_for_bbox(-180, -85, 180, 85, max_tiles=2)

    def test_no_raise_when_exactly_max_tiles(self, tmp_path):
        """Does NOT raise when count equals max_tiles (> not >= semantics)."""
        path = _make_manifest_file(tmp_path)
        tm = TileManifest(str(path), _BASE_URL)
        # Exactly 4 tiles; max_tiles=4 must succeed
        urls = tm.get_tiles_for_bbox(-180, -85, 180, 85, max_tiles=4)
        assert len(urls) == 4

    def test_empty_result_when_manifest_empty(self, tmp_path):
        """Empty manifest returns empty list."""
        path = _make_manifest_file(tmp_path, quadkeys=[])
        tm = TileManifest(str(path), _BASE_URL)
        urls = tm.get_tiles_for_bbox(-180, -85, 180, 85)
        assert urls == []

    def test_default_max_tiles_is_50(self, tmp_path):
        """Default max_tiles=50; 4 tiles don't raise."""
        path = _make_manifest_file(tmp_path)
        tm = TileManifest(str(path), _BASE_URL)
        urls = tm.get_tiles_for_bbox(-180, -85, 180, 85)
        assert isinstance(urls, list)
        assert len(urls) == 4
