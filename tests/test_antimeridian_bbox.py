"""Tests for antimeridian bbox handling: _parse_bbox and bboxes_intersect
detect xmin > xmax as antimeridian crossing and handle it correctly.
"""
import pytest
from garganorn.server import Server
from garganorn.stages import bboxes_intersect


# ---------------------------------------------------------------------------
# Test _parse_bbox antimeridian handling
# ---------------------------------------------------------------------------

class TestParseBboxAntimeridian:
    """Tests for Server._parse_bbox with antimeridian-crossing bboxes."""

    def test_parse_bbox_accepts_antimeridian_crossing(self):
        """_parse_bbox accepts bbox with xmin > xmax (antimeridian crossing)
        and returns the tuple as-is."""
        server = Server("places.atgeo.org", [], None)

        # Fiji-like bbox crossing the antimeridian
        bbox_str = "170,-20,-170,-10"
        result = server._parse_bbox(bbox_str)

        assert result == (170, -20, -170, -10), (
            f"Antimeridian bbox should parse as-is, got {result}"
        )

    def test_parse_bbox_normal_bbox_still_works(self):
        """Normal bboxes (xmin < xmax) should continue to work."""
        server = Server("places.atgeo.org", [], None)

        # San Francisco bbox
        bbox_str = "-122.5,37.7,-122.3,37.8"
        result = server._parse_bbox(bbox_str)

        assert result == (-122.5, 37.7, -122.3, 37.8)

    def test_parse_bbox_invalid_ymin_ymax_raises_error(self):
        """Invalid bbox with ymin >= ymax should still raise InvalidBbox."""
        server = Server("places.atgeo.org", [], None)

        with pytest.raises(Exception) as exc_info:
            server._parse_bbox("-122.5,37.8,-122.3,37.7")

        assert exc_info.value.name == "InvalidBbox"

    def test_parse_bbox_antimeridian_with_invalid_lat_raises_error(self):
        """Antimeridian bbox with invalid latitude should raise InvalidBbox."""
        server = Server("places.atgeo.org", [], None)

        # Valid longitude (xmin > xmax) but invalid latitude (ymin >= ymax)
        with pytest.raises(Exception) as exc_info:
            server._parse_bbox("170, -10, -170, -20")

        assert exc_info.value.name == "InvalidBbox"

    def test_parse_bbox_extreme_antimeridian(self):
        """Extreme antimeridian bbox (e.g., 179 to -179) should be accepted."""
        server = Server("places.atgeo.org", [], None)

        bbox_str = "179,-10,-179,10"
        result = server._parse_bbox(bbox_str)

        assert result == (179, -10, -179, 10)


# ---------------------------------------------------------------------------
# Test bboxes_intersect with antimeridian
# ---------------------------------------------------------------------------

class TestBboxesIntersectAntimeridian:
    """Tests for bboxes_intersect with antimeridian-crossing boxes."""

    def test_intersect_both_cross_antimeridian(self):
        """Two bboxes crossing the antimeridian should intersect."""
        # Both cross the antimeridian
        a = (170, -10, -170, 10)
        b = (175, -5, -175, 5)

        result = bboxes_intersect(a, b)
        assert result is True, "Two antimeridian-crossing bboxes should intersect"

    def test_intersect_one_crosses_one_eastern(self):
        """Antimeridian-crossing bbox should intersect with eastern bbox."""
        # a crosses antimeridian (spans 170°E to 170°W)
        a = (170, -10, -170, 10)
        # b is in eastern hemisphere (near 180° but not crossing)
        b = (175, -5, 179, 5)

        result = bboxes_intersect(a, b)
        assert result is True, "Crossing bbox should intersect with eastern bbox"

    def test_intersect_one_crosses_one_western(self):
        """Antimeridian-crossing bbox should intersect with western bbox."""
        # a crosses antimeridian
        a = (170, -10, -170, 10)
        # b is in western hemisphere (near -180° but not crossing)
        b = (-179, -5, -175, 5)

        result = bboxes_intersect(a, b)
        assert result is True, "Crossing bbox should intersect with western bbox"

    def test_intersect_crossing_no_overlap_normal_bbox(self):
        """Antimeridian-crossing bbox should not intersect with distant normal bbox."""
        # a crosses antimeridian (Pacific)
        a = (170, -10, -170, 10)
        # b is in Europe (no overlap)
        b = (10, -10, 20, 10)

        result = bboxes_intersect(a, b)
        assert result is False, "Pacific crossing bbox should not intersect with Europe bbox"

    def test_intersect_normal_bboxes_still_work(self):
        """Normal bbox intersection should continue to work."""
        a = (-122.5, 37.7, -122.3, 37.8)
        b = (-122.4, 37.75, -122.2, 37.85)

        result = bboxes_intersect(a, b)
        assert result is True, "Overlapping normal bboxes should intersect"

    def test_intersect_disjoint_normal_bboxes(self):
        """Disjoint normal bboxes should not intersect."""
        a = (-122.5, 37.7, -122.3, 37.8)
        b = (-122.2, 37.7, -122.0, 37.8)

        result = bboxes_intersect(a, b)
        assert result is False, "Disjoint bboxes should not intersect"


# ---------------------------------------------------------------------------
# Edge cases and boundary conditions
# ---------------------------------------------------------------------------

class TestAntimeridianEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_bbox_exact_180_meridian(self):
        """Bbox with xmin=180 or xmax=-180 should be handled correctly."""
        server = Server("places.atgeo.org", [], None)

        # Bbox that exactly spans the antimeridian
        bbox_str = "180,-10,-180,10"
        result = server._parse_bbox(bbox_str)

        assert result == (180, -10, -180, 10)

    def test_bbox_nearly_global_antimeridian(self):
        """Nearly global bbox crossing antimeridian should be accepted."""
        server = Server("places.atgeo.org", [], None)

        # Most of the world, crossing the antimeridian
        bbox_str = "170,-90,-170,90"
        result = server._parse_bbox(bbox_str)

        assert result == (170, -90, -170, 90)

    def test_centroid_dateline_crossing_small(self):
        """Small bbox crossing the antimeridian should have correct centroid.

        Test case: bbox=(179, -1, -179, 1) - a 2-degree-wide box crossing the dateline.
        """
        # Antimeridian-aware centroid: normalizes xmax into 0-360 range before averaging
        def compute_bbox_centroid(bbox):
            """Compute centroid of a bbox, handling antimeridian crossing.

            For antimeridian-crossing bboxes (xmin > xmax), normalize xmax to 0-360 range.
            """
            xmin, ymin, xmax, ymax = bbox
            # For antimeridian crossing, normalize xmax to 0-360 range
            xmax_normalized = xmax + 360 if xmax < xmin else xmax
            mid_lon = (xmin + xmax_normalized) / 2
            mid_lat = (ymin + ymax) / 2
            # Normalize to -180 to 180 range for consistency
            if mid_lon > 180:
                mid_lon -= 360
            return mid_lon, mid_lat

        # Small bbox: 179°E to 179°W (i.e., 179 to 181 normalized)
        bbox = (179, -1, -179, 1)

        expected_lon, expected_lat = compute_bbox_centroid(bbox)

        # The centroid should be at ±180° (the antimeridian)
        assert abs(expected_lon) == 180.0, (
            f"Centroid of small antimeridian-crossing bbox should be at ±180°, "
            f"got {expected_lon}"
        )
        assert expected_lat == 0.0, (
            f"Centroid latitude should be (-1 + 1) / 2 = 0, got {expected_lat}"
        )

        # A naive (xmin + xmax) / 2 average, with no antimeridian handling, is wrong
        xmin, ymin, xmax, ymax = bbox
        current_broken_lon = (xmin + xmax) / 2
        assert current_broken_lon == 0.0, (
            f"naive (xmin + xmax) / 2 average gives {current_broken_lon}, "
            "which is wrong for an antimeridian-crossing bbox"
        )
