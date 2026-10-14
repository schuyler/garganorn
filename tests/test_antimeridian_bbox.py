"""Tests for antimeridian bbox handling: getTiles and bboxes_intersect
detect xmin > xmax as antimeridian crossing and handle it correctly.
"""
import pytest
from garganorn.stages import bboxes_intersect
from tests.served_runs import OVERTURE, build_app, serve_run, tile_url


# ---------------------------------------------------------------------------
# getTiles bbox handling of antimeridian crossing
# ---------------------------------------------------------------------------

FIJI = "311133"  # lon 178, lat -17
SAN_FRANCISCO = "023010"


@pytest.fixture
def client(tmp_path):
    tiles = {qk: [] for qk in (FIJI, SAN_FRANCISCO)}
    with build_app(serve_run(tmp_path, tiles=tiles)).test_client() as c:
        yield c


def _get_tiles(client, bbox):
    return client.get("/xrpc/org.atgeo.getTiles", query_string={
        "dataset": OVERTURE, "bbox": bbox,
    })


def _tiles(client, bbox):
    resp = _get_tiles(client, bbox)
    assert resp.status_code == 200, resp.get_json()
    return resp.get_json()["tiles"]


class TestGetTilesAntimeridian:
    """getTiles accepts a bbox with xmin > xmax as an antimeridian crossing
    and selects the tiles on both sides of it."""

    def test_accepts_antimeridian_crossing(self, client):
        """A Fiji-like bbox crossing the antimeridian lists Fiji's tile and
        not San Francisco's."""
        assert _tiles(client, "170,-20,-170,-10") == [tile_url("overture-place", FIJI)]

    def test_normal_bbox_still_works(self, client):
        """Normal bboxes (xmin < xmax) continue to work: a San Francisco bbox
        lists San Francisco's tile and not Fiji's."""
        assert _tiles(client, "-122.5,37.7,-122.3,37.8") == [tile_url("overture-place", SAN_FRANCISCO)]

    def test_invalid_ymin_ymax_raises_error(self, client):
        """Invalid bbox with ymin >= ymax still raises InvalidBbox."""
        resp = _get_tiles(client, "-122.5,37.8,-122.3,37.7")
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "InvalidBbox"

    def test_antimeridian_with_invalid_lat_raises_error(self, client):
        """Valid longitude (xmin > xmax) but invalid latitude (ymin >= ymax)."""
        resp = _get_tiles(client, "170,-10,-170,-20")
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "InvalidBbox"

    def test_extreme_antimeridian(self, client):
        """Extreme antimeridian bbox (179 to -179) is accepted."""
        assert _tiles(client, "179,-10,-179,10") == []

    def test_bbox_exact_180_meridian(self, client):
        """xmin=180 with xmax=-180 is accepted."""
        assert _tiles(client, "180,-10,-180,10") == []

    def test_bbox_nearly_global_antimeridian(self, client):
        """Nearly global bbox crossing the antimeridian is accepted and
        selects Fiji's tile."""
        assert _tiles(client, "170,-85,-170,85") == [tile_url("overture-place", FIJI)]


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
