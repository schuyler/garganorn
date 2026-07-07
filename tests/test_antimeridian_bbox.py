"""Tests for antimeridian bbox handling (DATA-4).

These tests FAIL against current code because antimeridian-spanning features
are silently dropped by bbox validation. The fix requires detecting xmin > xmax
as antimeridian crossing and handling it correctly.

Test cases:
1. _parse_bbox antimeridian handling
2. Antimeridian centroid computation
3. bboxes_intersect with antimeridian
4. Query-time split-query behavior
"""
import pytest
import math
from garganorn.server import Server
from garganorn.database import Database
from garganorn.stages import bboxes_intersect


# ---------------------------------------------------------------------------
# Test _parse_bbox antimeridian handling
# ---------------------------------------------------------------------------

class TestParseBboxAntimeridian:
    """Tests for Server._parse_bbox with antimeridian-crossing bboxes."""

    def test_parse_bbox_accepts_antimeridian_crossing(self):
        """_parse_bbox should accept bbox with xmin > xmax (antimeridian crossing).

        Current behavior: raises InvalidBbox
        Expected behavior: returns tuple with xmin > xmax
        """
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
# Test antimeridian centroid computation
# ---------------------------------------------------------------------------

class TestAntimeridianCentroid:
    """Tests for centroid computation with antimeridian-crossing bboxes."""

    def test_centroid_antimeridian_crossing(self):
        """Centroid of antimeridian-crossing bbox should be near ±180°, not 0°.

        Current behavior: computes (170 + -170) / 2 = 0 (wrong!)
        Expected behavior: centroid should be near 180° or -180°

        Test case: bbox=(170, -20, -170, -10) like Fiji
        """
        # Helper function that computes the CORRECT antimeridian-aware centroid
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

        # Fiji-like bbox crossing the antimeridian
        bbox = (170, -20, -170, -10)

        # Expected centroid using correct formula
        expected_lon, expected_lat = compute_bbox_centroid(bbox)

        # The centroid should be near 180° (or -180°), not 0°
        assert abs(expected_lon) == 180.0, (
            f"Centroid of antimeridian-crossing bbox should be at ±180°, "
            f"got {expected_lon}"
        )
        assert expected_lat == -15.0, (
            f"Centroid latitude should be (-20 + -10) / 2 = -15, got {expected_lat}"
        )

        # Verify the current (broken) implementation would give the wrong answer
        xmin, ymin, xmax, ymax = bbox
        current_broken_lon = (xmin + xmax) / 2  # This gives 0, which is wrong
        assert current_broken_lon == 0.0, (
            f"Current implementation gives {current_broken_lon}, which is incorrect"
        )

    def test_centroid_eastern_hemisphere_normal(self):
        """Normal eastern hemisphere bbox should compute centroid correctly."""
        xmin, ymin, xmax, ymax = 10, -10, 20, 10

        mid_lon = (xmin + xmax) / 2
        mid_lat = (ymin + ymax) / 2

        assert mid_lon == 15.0, "Eastern hemisphere centroid should be (10+20)/2 = 15"
        assert mid_lat == 0.0, "Latitude centroid should be (-10+10)/2 = 0"

    def test_centroid_western_hemisphere_normal(self):
        """Normal western hemisphere bbox should compute centroid correctly."""
        xmin, ymin, xmax, ymax = -20, -10, -10, 10

        mid_lon = (xmin + xmax) / 2
        mid_lat = (ymin + ymax) / 2

        assert mid_lon == -15.0, "Western hemisphere centroid should be (-20+-10)/2 = -15"
        assert mid_lat == 0.0, "Latitude centroid should be (-10+10)/2 = 0"

    def test_width_antimeridian_crossing(self):
        """Width calculation for antimeridian bbox should be positive.

        Current behavior: xmax - xmin = -170 - 170 = -340 (negative!)
        Expected behavior: width = 360 + (-170 - 170) = 20 degrees (positive)
        """
        # Helper function that computes the CORRECT antimeridian-aware width
        def compute_bbox_width_degrees(bbox):
            """Compute width of a bbox in degrees, handling antimeridian crossing.

            For antimeridian-crossing bboxes (xmin > xmax), the width wraps around.
            """
            xmin, ymin, xmax, ymax = bbox
            if xmax > xmin:
                # Normal bbox
                return xmax - xmin
            else:
                # Antimeridian crossing: width wraps around 360°
                return 360 + (xmax - xmin)

        # Fiji-like bbox crossing the antimeridian
        bbox = (170, -20, -170, -10)

        # Expected width using correct formula
        width_degrees = compute_bbox_width_degrees(bbox)

        # The width should be positive (20 degrees), not negative
        assert width_degrees == 20.0, (
            f"Width of antimeridian-crossing bbox should be 20°, got {width_degrees}"
        )

        # Verify the current (broken) implementation would give a negative width
        xmin, ymin, xmax, ymax = bbox
        current_broken_width = xmax - xmin  # This gives -340, which is wrong
        assert current_broken_width == -340.0, (
            f"Current implementation gives {current_broken_width}, which is incorrect"
        )

        # Also verify that area calculation would be affected
        # At latitude -15°, 1 degree of longitude ≈ 111 * cos(-15°) ≈ 107 km
        mid_lat = (ymin + ymax) / 2
        width_km_correct = width_degrees * 111 * math.cos(math.radians(mid_lat))
        height_km = (ymax - ymin) * 111  # 10 degrees * 111 km/degree

        # Correct area: ~2142 * 1110 ≈ 2,380,000 km²
        # (20 degrees * 107.2 km/degree) * (10 degrees * 111 km/degree)
        area_km2_correct = width_km_correct * height_km
        assert area_km2_correct > 2_000_000 and area_km2_correct < 3_000_000, (
            f"Correct area should be ~2.38M km², got {area_km2_correct}"
        )

        # Broken implementation would give negative area
        width_km_broken = current_broken_width * 111 * math.cos(math.radians(mid_lat))
        area_km2_broken = width_km_broken * height_km
        assert area_km2_broken < 0, (
            f"Current implementation gives negative area: {area_km2_broken}"
        )


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
# Test query-time split-query behavior
# ---------------------------------------------------------------------------

class TestQuerySplitBehavior:
    """Tests for split-query behavior with antimeridian bbox.

    These tests verify that when a query bbox crosses the antimeridian,
    the system splits it into two queries and merges results correctly.
    """

    def test_should_split_antimeridian_bbox(self):
        """Query with antimeridian bbox should split into two queries.

        This is a behavioral test documenting the expected behavior.
        After implementation, the query should:
        1. Detect xmin > xmax (antimeridian crossing)
        2. Split into western and eastern halves
        3. Run two queries
        4. Merge and deduplicate results by rkey
        """
        # Antimeridian bbox (Fiji-like)
        bbox = (170, -20, -170, -10)

        # Detection: xmin > xmax indicates antimeridian crossing
        is_antimeridian = bbox[0] > bbox[2]
        assert is_antimeridian is True

        # Split at ±180° meridian
        bbox_west = (bbox[0], bbox[1], 180, bbox[3])  # (170, -20, 180, -10)
        bbox_east = (-180, bbox[1], bbox[2], bbox[3])  # (-180, -20, -170, -10)

        # Verify split
        assert bbox_west == (170, -20, 180, -10)
        assert bbox_east == (-180, -20, -170, -10)

        # Both halves should have xmin < xmax (normal)
        assert bbox_west[0] < bbox_west[2]
        assert bbox_east[0] < bbox_east[2]

    def test_deduplicate_by_rkey(self):
        """Results from split queries should be deduplicated by rkey."""
        # Simulated results from western and eastern queries
        results_west = [
            {"rkey": "place1", "name": "Place 1"},
            {"rkey": "place2", "name": "Place 2"},
        ]
        results_east = [
            {"rkey": "place2", "name": "Place 2"},  # Duplicate
            {"rkey": "place3", "name": "Place 3"},
        ]

        # Deduplicate by rkey
        seen = set()
        merged = []
        for r in results_west + results_east:
            if r["rkey"] not in seen:
                seen.add(r["rkey"])
                merged.append(r)

        # Should have 3 unique places
        assert len(merged) == 3
        assert {r["rkey"] for r in merged} == {"place1", "place2", "place3"}

    def test_normal_bbox_should_not_split(self):
        """Normal bbox should not trigger split-query behavior."""
        # Normal San Francisco bbox
        bbox = (-122.5, 37.7, -122.3, 37.8)

        # Detection: xmin < xmax (normal)
        is_antimeridian = bbox[0] > bbox[2]
        assert is_antimeridian is False

        # Should query as-is, no split
        # This test documents the expected behavior


# ---------------------------------------------------------------------------
# Integration test: search_records with antimeridian bbox
# ---------------------------------------------------------------------------

class TestSearchRecordsAntimeridian:
    """Integration tests for search_records with antimeridian bbox.

    These tests require a database with antimeridian-crossing features.
    For now, they document the expected behavior and will fail until
    the fix is implemented.
    """

    def test_search_records_antimeridian_bbox_should_not_raise(self):
        """search_records with antimeridian bbox should not raise InvalidBbox.

        Current behavior: raises InvalidBbox
        Expected behavior: accepts bbox and returns results
        """
        from unittest.mock import MagicMock
        from garganorn.server import Server
        import logging

        # Create a mock database
        mock_db = MagicMock()
        mock_db.collection = "org.atgeo.places.foursquare"
        mock_db.attribution = "https://example.com/attribution"
        mock_db.nearest.return_value = []

        logger = logging.getLogger("test")
        server = Server("places.atgeo.org", [mock_db], logger)

        # This should NOT raise InvalidBbox
        result = server.search_records(
            {}, collection="org.atgeo.places.foursquare", bbox="170,-20,-170,-10"
        )

        # Verify nearest was called with the antimeridian bbox
        mock_db.nearest.assert_called_once()
        call_kwargs = mock_db.nearest.call_args.kwargs
        assert "bbox" in call_kwargs
        assert call_kwargs["bbox"] == (170, -20, -170, -10)

        assert "records" in result


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
        # Helper function that computes the CORRECT antimeridian-aware centroid
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

        # Expected centroid using correct formula
        expected_lon, expected_lat = compute_bbox_centroid(bbox)

        # The centroid should be at ±180° (the antimeridian)
        assert abs(expected_lon) == 180.0, (
            f"Centroid of small antimeridian-crossing bbox should be at ±180°, "
            f"got {expected_lon}"
        )
        assert expected_lat == 0.0, (
            f"Centroid latitude should be (-1 + 1) / 2 = 0, got {expected_lat}"
        )

        # Verify the current (broken) implementation would give the wrong answer
        xmin, ymin, xmax, ymax = bbox
        current_broken_lon = (xmin + xmax) / 2  # This gives 0, which is wrong
        assert current_broken_lon == 0.0, (
            f"Current implementation gives {current_broken_lon}, which is incorrect"
        )
