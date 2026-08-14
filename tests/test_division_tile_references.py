"""stage_division_tile_references: divisions are referenced by overlap.

A division used to reach exactly one tile — whichever one held its interior
point — so a bbox query missing that tile missed the division the user was
standing in. These tests are written against the outcomes that fix states,
not against how the stage reaches them:

  discoverability   every tile the geometry reaches references the division
  bounded overshoot no tile outside the division's bbox references it
  record shape      (place_id, tile_qk), sorted, one row per referencing tile
  placement         a division is placed by its own size, not by record density
  cost              the artifact is rebuilt only when its inputs change
"""
import random

import duckdb
import pytest

from garganorn.stages import (
    DIVISION_REFERENCE_ZOOM,
    bboxes_intersect,
    quadkey_to_bbox,
    stage_division_tile_references,
)


# ---------------------------------------------------------------------------
# Fixture geometry, built from real quadkey envelopes so the expected
# references are exact rather than approximately-somewhere-around-there.
# ---------------------------------------------------------------------------

# A z3 cell, i.e. a 2x2 block of reference-zoom cells.
_BLOCK = "122"
_BLOCK_CELLS = [_BLOCK + d for d in "0123"]  # NW, NE, SW, SE

# A cell deep enough that a division sized to it lands well below the
# reference zoom.
_DEEP_CELL = _BLOCK_CELLS[0] + "00000000"    # z12


def _l_shape_wkt():
    """A polygon filling three of _BLOCK's four reference cells.

    Its bbox covers all four. Everything is inset from a cell edge rather
    than laid along one — the notch, so nothing turns on how an edge-touching
    intersection is classified, and the outer bounds, so nothing turns on
    which side of a tile boundary a coordinate exactly on it falls.
    """
    x0, y0, x1, y1 = quadkey_to_bbox(_BLOCK)
    _, ym, xm, _ = quadkey_to_bbox(_BLOCK + "0")  # NW child: (x0, ym, xm, y1)
    dx = (x1 - x0) * 0.02
    dy = (y1 - y0) * 0.02
    x0, y0, x1, y1 = x0 + dx, y0 + dy, x1 - dx, y1 - dy
    return (
        f"POLYGON(({x0} {y0}, {xm - dx} {y0}, {xm - dx} {ym + dy}, "
        f"{x1} {ym + dy}, {x1} {y1}, {x0} {y1}, {x0} {y0}))"
    )


def _inset_box(quadkey, *, fraction=0.99):
    """A bbox concentric with `quadkey`'s cell, scaled by `fraction`."""
    x0, y0, x1, y1 = quadkey_to_bbox(quadkey)
    mx, my = (x0 + x1) / 2, (y0 + y1) / 2
    hw, hh = (x1 - x0) / 2 * fraction, (y1 - y0) / 2 * fraction
    return (mx - hw, my - hh, mx + hw, my + hh)


def _seam_box(west, east, *, fraction):
    """A bbox centred on the edge two horizontally adjacent cells share.

    Sized as `fraction` of one cell in both axes, so `fraction` decides which
    zoom the box is small enough to be placed at, independently of the fact
    that it sits across a seam at that zoom.
    """
    wx0, wy0, wx1, wy1 = quadkey_to_bbox(west)
    ex0, _, _, _ = quadkey_to_bbox(east)
    assert abs(wx1 - ex0) < 1e-9, f"{west} and {east} are not side by side"
    cy = (wy0 + wy1) / 2
    half_w = (wx1 - wx0) * fraction / 2
    half_h = (wy1 - wy0) * fraction / 2
    return (wx1 - half_w, cy - half_h, wx1 + half_w, cy + half_h)


def _box_wkt(box):
    x0, y0, x1, y1 = box
    return f"POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, {x0} {y1}, {x0} {y0}))"


_TINY_BOX = _inset_box(_DEEP_CELL)
# Small enough to be placed one zoom below the reference zoom, but sitting
# across the seam between two reference cells.
_SEAM_BOX = _seam_box(_BLOCK_CELLS[0] + "1", _BLOCK_CELLS[1] + "0", fraction=0.8)

DIVISIONS = [
    # (id, wkt)
    ("big_l", _l_shape_wkt()),
    ("tiny", _box_wkt(_TINY_BOX)),
    ("seam", _box_wkt(_SEAM_BOX)),
]


def _make_boundaries_db(db_path, divisions):
    """Write a boundaries.duckdb holding just what the stage reads.

    The stage reads the flattened bbox extents; the geometry is here because
    the discoverability test needs a truth oracle, not because the stage
    touches it.
    """
    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("""
            CREATE TABLE places (
                id VARCHAR,
                geometry GEOMETRY,
                min_latitude DOUBLE,
                max_latitude DOUBLE,
                min_longitude DOUBLE,
                max_longitude DOUBLE
            )
        """)
        for place_id, wkt in divisions:
            con.execute("""
                INSERT INTO places
                SELECT ?, g, ST_YMin(g), ST_YMax(g), ST_XMin(g), ST_XMax(g)
                FROM (SELECT ST_GeomFromText(?) AS g)
            """, [place_id, wkt])
    finally:
        con.close()
    return str(db_path)


@pytest.fixture(scope="module")
def division_refs(tmp_path_factory):
    """(references, boundaries_db, artifact) for DIVISIONS.

    `references` maps place_id → set of referencing tile quadkeys.
    """
    base = tmp_path_factory.mktemp("division_refs")
    boundaries_db = _make_boundaries_db(base / "boundaries.duckdb", DIVISIONS)
    artifact = str(base / "tile_assignments.parquet")
    stage_division_tile_references(boundaries_db, artifact)

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT place_id, tile_qk FROM read_parquet('{artifact}')"
        ).fetchall()
    finally:
        con.close()

    references = {}
    for place_id, tile_qk in rows:
        references.setdefault(place_id, set()).add(tile_qk)
    return references, boundaries_db, artifact


def _bbox_of(place_id):
    wkt = dict(DIVISIONS)[place_id]
    x, y = [], []
    for pair in wkt[wkt.index("((") + 2:wkt.index("))")].split(", "):
        lon, lat = pair.split()
        x.append(float(lon))
        y.append(float(lat))
    return (min(x), min(y), max(x), max(y))


# ---------------------------------------------------------------------------
# Discoverability: every tile the geometry reaches references the division
# ---------------------------------------------------------------------------

def test_every_bbox_query_that_finds_the_geometry_finds_a_tile(division_refs):
    """The invariant the single-point assignment violated.

    For query bboxes swept across the fixture region, any query whose box
    intersects a division's real geometry must intersect at least one tile
    referencing that division — otherwise the client is told the region is
    empty. Truth comes from ST_Intersects, not from the same bbox arithmetic
    the stage used.
    """
    references, boundaries_db, _artifact = division_refs
    x0, y0, x1, y1 = quadkey_to_bbox(_BLOCK)
    rng = random.Random(20260814)

    queries = []
    for _ in range(200):
        qx = rng.uniform(x0, x1)
        qy = rng.uniform(y0, y1)
        qw = (x1 - x0) * rng.uniform(0.005, 0.3)
        qh = (y1 - y0) * rng.uniform(0.005, 0.3)
        queries.append((qx, qy, min(qx + qw, x1), min(qy + qh, y1)))

    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
        con.execute(f"ATTACH '{boundaries_db}' AS bnd (READ_ONLY)")
        misses = []
        for query in queries:
            hits = con.execute("""
                SELECT id FROM bnd.places
                WHERE ST_Intersects(geometry, ST_MakeEnvelope(?, ?, ?, ?))
            """, list(query)).fetchall()
            for (place_id,) in hits:
                if not any(bboxes_intersect(quadkey_to_bbox(tile_qk), query)
                           for tile_qk in references.get(place_id, ())):
                    misses.append((place_id, query))
    finally:
        con.close()

    assert not misses, (
        f"{len(misses)} bbox queries intersect a division but no tile "
        f"referencing it; first: {misses[0]}"
    )


def test_a_division_across_a_seam_is_referenced_from_both_sides(division_refs):
    references, *_ = division_refs
    assert len(references["seam"]) == 2, (
        f"a division across one cell edge must be referenced by the cells "
        f"on both sides, got {sorted(references['seam'])}"
    )


# ---------------------------------------------------------------------------
# Bounded overshoot: references may exceed the geometry, never the bbox
# ---------------------------------------------------------------------------

def test_no_division_is_referenced_by_a_tile_its_bbox_never_touches(division_refs):
    references, *_ = division_refs
    for place_id, tiles in references.items():
        bbox = _bbox_of(place_id)
        for tile_qk in tiles:
            assert bboxes_intersect(quadkey_to_bbox(tile_qk), bbox), (
                f"{place_id} is referenced by {tile_qk}, whose cell "
                f"{quadkey_to_bbox(tile_qk)} lies outside its bbox {bbox}"
            )


def test_references_come_from_the_bbox_not_the_geometry(division_refs):
    """The accepted overshoot, asserted so it isn't later read as a bug.

    The L-shape's geometry reaches three reference cells; its bbox reaches
    four, and four is what it gets. Deriving references from real geometry
    instead would cost a covering read to save a fourth of one record in one
    coarse tile.
    """
    references, *_ = division_refs
    assert references["big_l"] == set(_BLOCK_CELLS), (
        f"expected every cell the bbox touches, got {sorted(references['big_l'])}"
    )


def test_a_division_inside_one_cell_is_referenced_exactly_once(division_refs):
    """Overshoot has a floor as well as a ceiling: a duplicate reference is
    a duplicated record in another tile."""
    references, *_ = division_refs
    assert references["tiny"] == {_DEEP_CELL}, (
        f"expected the one cell containing it, got {sorted(references['tiny'])}"
    )


# ---------------------------------------------------------------------------
# Placement: by the division's own size, not by record density or by seams
# ---------------------------------------------------------------------------

def test_placement_follows_size_not_seams(division_refs):
    """A division small enough to place deep is placed deep even when it sits
    across a shallow seam — the case a containment-based placement would drag
    up into a continent-sized tile."""
    references, *_ = division_refs
    assert all(len(tile_qk) == DIVISION_REFERENCE_ZOOM + 1
               for tile_qk in references["seam"]), (
        f"expected placement one zoom below the reference zoom, "
        f"got {sorted(references['seam'])}"
    )
    assert all(len(tile_qk) > DIVISION_REFERENCE_ZOOM
               for tile_qk in references["tiny"]), (
        f"a division far smaller than a reference cell must be placed "
        f"deeper than one, got {sorted(references['tiny'])}"
    )


def test_an_antimeridian_bbox_is_measured_as_two_lobes():
    """min_lon > max_lon is a narrow box across ±180, not a world-wide one.

    Measured as max - min it is a negative span that fits at every zoom,
    which would place a Fiji-shaped division at z17 and lose half of it.
    """
    from garganorn.covering import placement_zoom
    assert placement_zoom(179.0, 0.0, -179.0, 0.5, min_zoom=0, max_zoom=17) == \
        placement_zoom(0.0, 0.0, 2.0, 0.5, min_zoom=0, max_zoom=17)


# ---------------------------------------------------------------------------
# Record shape and cost
# ---------------------------------------------------------------------------

def test_artifact_matches_tile_assignments_in_schema_and_sort(division_refs):
    """stage_export detects a tile boundary by watching tile_qk change, so
    the sort is the grouping mechanism, not an optimization."""
    _references, _bnd, artifact = division_refs
    con = duckdb.connect()
    try:
        described = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{artifact}')"
        ).fetchall()
        rows = con.execute(
            f"SELECT place_id, tile_qk FROM read_parquet('{artifact}')"
        ).fetchall()
    finally:
        con.close()

    assert [(name, dtype) for name, dtype, *_ in described] == [
        ("place_id", "VARCHAR"), ("tile_qk", "VARCHAR")
    ], f"schema drifted from tile_assignments.parquet: {described}"
    assert rows == sorted(rows, key=lambda r: (r[1], r[0])), (
        "artifact is not sorted by (tile_qk, place_id)"
    )
    assert len(rows) == len(set(rows)), (
        "a repeated (place_id, tile_qk) would export the same record twice "
        "into one tile"
    )


def test_a_second_run_over_unchanged_inputs_does_no_work(division_refs):
    _references, boundaries_db, artifact = division_refs
    assert stage_division_tile_references(boundaries_db, artifact) == {}


# ---------------------------------------------------------------------------
# The fork: point sources keep the record-density splitter
# ---------------------------------------------------------------------------

def _run_fork(source, tmp_path):
    from unittest.mock import patch
    import garganorn.quadtree as _qt

    with patch.object(_qt, "stage_import"), \
            patch.object(_qt, "stage_covering"), \
            patch.object(_qt, "compute_containment"), \
            patch.object(_qt, "stage_export"), \
            patch.object(_qt, "stage_tile_assignment") as by_density, \
            patch.object(_qt, "stage_division_tile_references") as by_overlap:
        _qt.run_pipeline(source, "glob", None, str(tmp_path))
    return by_density, by_overlap


def test_divisions_are_assigned_by_overlap(tmp_path):
    by_density, by_overlap = _run_fork("overture_division", tmp_path)
    assert by_overlap.called and not by_density.called


def test_point_sources_are_assigned_by_record_density(tmp_path):
    by_density, by_overlap = _run_fork("overture_place", tmp_path)
    assert by_density.called and not by_overlap.called
