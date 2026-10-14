"""Tests for quadtree Python functions: quadkey_to_bbox, bboxes_intersect,
and TileManifest.
"""
import fcntl
import math
import os
import subprocess
import sys

import duckdb
import pytest

from garganorn.quadtree import (
    TileManifest,
    bboxes_intersect,
    quadkey_to_bbox,
    _acquire_build_lock,
)

POLE_YMAX = 90.0
WORLD = (-180, -85, 180, 85)
_BASE_URL = "https://tiles.example.com"

# Four zoom-6 quadkeys in distinct geographic areas.
_MANIFEST_QUADKEYS = [
    "023010",   # Pacific Northwest
    "023011",   # adjacent east of 023010
    "120301",   # eastern US
    "200000",   # Europe
]


def _make_manifest_db(tmp_path, quadkeys=None):
    """Create a manifest.duckdb with the given quadkeys and return its path."""
    if quadkeys is None:
        quadkeys = _MANIFEST_QUADKEYS
    p = tmp_path / "manifest.duckdb"
    con = duckdb.connect(str(p))
    con.execute("CREATE TABLE record_tiles (key VARCHAR, tile_qk VARCHAR)")
    for i, qk in enumerate(quadkeys):
        con.execute("INSERT INTO record_tiles VALUES (?, ?)", [f"overturemaps.org:place:p{i}", qk])
    con.close()
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
        assert ymin == pytest.approx(-POLE_YMAX, abs=0.01)
        assert ymax == pytest.approx(POLE_YMAX, abs=0.01)

    def test_quadkey_0_is_nw_quadrant(self):
        """'0' = northwest quadrant: west hemisphere, upper half."""
        xmin, ymin, xmax, ymax = quadkey_to_bbox("0")
        assert xmin == pytest.approx(-180.0, abs=0.01)
        assert xmax == pytest.approx(0.0, abs=0.01)
        assert ymin == pytest.approx(0.0, abs=0.01)
        assert ymax == pytest.approx(POLE_YMAX, abs=0.01)

    def test_quadkey_3_is_se_quadrant(self):
        """'3' = southeast quadrant: east hemisphere, lower half."""
        xmin, ymin, xmax, ymax = quadkey_to_bbox("3")
        assert xmin == pytest.approx(0.0, abs=0.01)
        assert xmax == pytest.approx(180.0, abs=0.01)
        assert ymin == pytest.approx(-POLE_YMAX, abs=0.01)
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
# TileManifest
# ---------------------------------------------------------------------------

class TestTileManifest:
    """TileManifest(manifest_path, base_url).regular_tiles(bbox) and
    .overview_tiles(bbox)."""

    def test_url_format(self, tmp_path):
        """URLs follow {base_url}/{qk[:6]}/{qk}.json.gz format."""
        path = _make_manifest_db(tmp_path, quadkeys=["023010"])
        tm = TileManifest(str(path), _BASE_URL)
        assert tm.regular_tiles(WORLD) == [f"{_BASE_URL}/023010/023010.json.gz"]

    def test_returns_all_intersecting_urls(self, tmp_path):
        """All intersecting tiles are returned (caller is responsible for sorting)."""
        path = _make_manifest_db(tmp_path)
        tm = TileManifest(str(path), _BASE_URL)
        urls = tm.regular_tiles(WORLD)
        assert set(urls) == {f"{_BASE_URL}/{qk[:6]}/{qk}.json.gz" for qk in _MANIFEST_QUADKEYS}

    def test_non_intersecting_excluded(self, tmp_path):
        """Tiles outside the query bbox are excluded."""
        qk = _MANIFEST_QUADKEYS[0]
        xmin, ymin, xmax, ymax = quadkey_to_bbox(qk)
        mid_lon = (xmin + xmax) / 2
        mid_lat = (ymin + ymax) / 2
        tiny = (mid_lon - 0.001, mid_lat - 0.001, mid_lon + 0.001, mid_lat + 0.001)
        path = _make_manifest_db(tmp_path)
        tm = TileManifest(str(path), _BASE_URL)
        urls = tm.regular_tiles(tiny)
        assert f"{_BASE_URL}/{qk[:6]}/{qk}.json.gz" in urls
        for other in _MANIFEST_QUADKEYS[1:]:
            assert f"{_BASE_URL}/{other[:6]}/{other}.json.gz" not in urls

    def test_regular_tiles_are_not_capped(self, tmp_path):
        """The cap belongs to the server, which sums across tilesets."""
        qks = [f"023{a}{b}{c}" for a in "0123" for b in "0123" for c in "0123"]
        path = _make_manifest_db(tmp_path, quadkeys=qks)
        tm = TileManifest(str(path), _BASE_URL)
        assert len(tm.regular_tiles(WORLD)) == 64

    def test_empty_result_when_manifest_empty(self, tmp_path):
        """Empty manifest returns an empty list and has no overview tiles."""
        path = _make_manifest_db(tmp_path, quadkeys=[])
        tm = TileManifest(str(path), _BASE_URL)
        assert tm.regular_tiles(WORLD) == []
        assert tm.overview_tiles(WORLD) is None


# ---------------------------------------------------------------------------
# Overview tiles are keyed by quadkey length < 6 (the regular band's
# min_zoom); regular_tiles never lists them and overview_tiles lists only them.
# ---------------------------------------------------------------------------

class TestOverviewTiles:
    def test_overview_tiles_are_the_short_keys(self, tmp_path):
        """Overview keys of length 1, 3, and 5 exercise the qk[:6]/qk URL
        shape for keys shorter than 6 characters; regular keys are not listed."""
        overview = ["0", "023", "20111"]
        path = _make_manifest_db(tmp_path, quadkeys=list(_MANIFEST_QUADKEYS) + overview)
        tm = TileManifest(str(path), _BASE_URL)

        assert set(tm.overview_tiles(WORLD)) == {f"{_BASE_URL}/{qk}/{qk}.json.gz" for qk in overview}

    def test_overview_tiles_exclude_non_intersecting(self, tmp_path):
        path = _make_manifest_db(tmp_path, quadkeys=["0", "3"])
        tm = TileManifest(str(path), _BASE_URL)
        northwest = (-170.0, 10.0, -100.0, 60.0)

        assert tm.overview_tiles(northwest) == [f"{_BASE_URL}/0/0.json.gz"]

    def test_no_overview_leak_into_regular_tiles(self, tmp_path):
        regular = _MANIFEST_QUADKEYS[:2]
        path = _make_manifest_db(tmp_path, quadkeys=regular + ["0"])
        tm = TileManifest(str(path), _BASE_URL)

        assert set(tm.regular_tiles(WORLD)) == {f"{_BASE_URL}/{qk[:6]}/{qk}.json.gz" for qk in regular}

    def test_no_overview_tiles_is_none(self, tmp_path):
        """None, not an empty list: the server tells 'no overview tier' from
        'nothing in this bbox'."""
        path = _make_manifest_db(tmp_path)
        tm = TileManifest(str(path), _BASE_URL)

        assert tm.overview_tiles(WORLD) is None

    def test_overview_tiles_empty_list_when_none_intersect(self, tmp_path):
        path = _make_manifest_db(tmp_path, quadkeys=["3"])
        tm = TileManifest(str(path), _BASE_URL)

        assert tm.overview_tiles((-170.0, 10.0, -100.0, 60.0)) == []


class TestAcquireBuildLock:
    """_acquire_build_lock guards concurrent `quadtree all` runs with an
    OS-level flock, so a killed holder can never wedge the next run."""

    def test_succeeds_when_no_lock_held(self, tmp_path):
        fd = _acquire_build_lock(str(tmp_path))
        assert os.path.exists(tmp_path / ".quadtree-all.lock")
        os.close(fd)

    def test_raises_when_another_process_holds_it(self, tmp_path):
        holder = subprocess.Popen(
            [sys.executable, "-c",
             "import fcntl, time; "
             f"fd = open({str(tmp_path / '.quadtree-all.lock')!r}, 'w'); "
             "fcntl.flock(fd, fcntl.LOCK_EX); time.sleep(30)"]
        )
        try:
            for _ in range(50):
                if (tmp_path / ".quadtree-all.lock").exists():
                    break
                import time as _time
                _time.sleep(0.1)
            with pytest.raises(SystemExit):
                _acquire_build_lock(str(tmp_path))
        finally:
            holder.kill()
            holder.wait()

    def test_recovers_after_holder_is_killed(self, tmp_path):
        """The requirement this guards: a crashed/killed run must not
        permanently wedge the next `quadtree all` attempt."""
        holder = subprocess.Popen(
            [sys.executable, "-c",
             "import fcntl, time; "
             f"fd = open({str(tmp_path / '.quadtree-all.lock')!r}, 'w'); "
             "fcntl.flock(fd, fcntl.LOCK_EX); time.sleep(30)"]
        )
        for _ in range(50):
            if (tmp_path / ".quadtree-all.lock").exists():
                break
            import time as _time
            _time.sleep(0.1)
        holder.kill()
        holder.wait()

        fd = _acquire_build_lock(str(tmp_path))
        os.close(fd)
