"""Tests for stage_import() freshness-meta contract (density/idf inputs+params).

Design contract (see docs/pipeline-implementation-decisions.md, "Phase 2" — freshness via meta sidecars):

    | `<src>/places.parquet` | `bbox, density_norm, idf_norm` | source parquet, density, idf |

For a non-division source (overture_place/osm), stage_import's places.parquet
.meta.json must record:
  - inputs: source parquet paths + resolved density_parquet path + resolved
    idf_parquet path (when those are provided)
  - params: bbox, density_norm, idf_norm (in addition to source)

stage_division_import already does this correctly (garganorn/stages.py ~656-668)
and is the reference shape. stage_import (~891-898) currently only records
input_files = source parquet glob and params = {"source", "bbox"}, omitting
density_parquet/idf_parquet entirely. This means rebuilding density_tiles.parquet
(which feeds importance scoring for ALL sources, including osm which doesn't
own it) does not invalidate a source's places.parquet: artifact_fresh() returns
True and stale importance scores are silently reused.

These tests encode the CONTRACT (the meta shape + the invalidation behavior it
must produce), not the current implementation.
"""
import json
import logging
import os
import pathlib
import time

import duckdb
import pytest

import garganorn.stages as _stages
from tests.quadtree_helpers import write_minimal_overture_parquet

_BBOX = (-122.55, 37.60, -122.30, 37.85)


def _make_overture_parquet(tmp_path, name="overture_data.parquet"):
    """Minimal Overture-schema parquet, private to a single test.

    Used (instead of the shared session-scoped `overture_parquet` fixture) in
    the mtime-invalidation tests below, so os.utime() manipulation of the
    source parquet's mtime — needed to isolate the density/idf-specific
    invalidation path from the source-parquet invalidation path, which is
    already correctly tracked even by the buggy code — cannot leak into
    other tests that also depend on the shared overture_parquet fixture.
    """
    path = tmp_path / name
    write_minimal_overture_parquet(path, [
        ("ovfresh001", -122.4194, 37.7749, "coffee_shop"),
    ])
    return str(path)


def _make_density_parquet(tmp_path, name="density.parquet"):
    """Minimal density_tiles parquet, function-scoped and private to a single test.

    Schema matches stage_density_extract's output (garganorn/stages.py ~1040-1046:
    tile_qk15 VARCHAR, density_score DOUBLE, tile_xmin/ymin/xmax/ymax DOUBLE),
    which is what stage_import's ${density_cte} reads via read_parquet(...).

    A dedicated per-test parquet (rather than the shared session-scoped
    `density_parquet` fixture) is used in the mtime-invalidation tests below
    so that os.utime() manipulation cannot leak mtime changes into the shared
    fixture and affect other tests that also depend on it.
    """
    path = tmp_path / name
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE tmp_density (
            tile_qk15 VARCHAR, density_score DOUBLE,
            tile_xmin DOUBLE, tile_ymin DOUBLE, tile_xmax DOUBLE, tile_ymax DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO tmp_density VALUES
            ('03000000000000000', 1.5, -122.5, 37.6, -122.3, 37.85)
    """)
    conn.execute(f"COPY tmp_density TO '{path}' (FORMAT PARQUET)")
    conn.close()
    return str(path)


def _make_idf_parquet(tmp_path, name="idf_scores.parquet"):
    """Minimal idf_scores parquet with schema (category VARCHAR, idf_score DOUBLE).

    This is the schema stage_import's ${idf_cte} reads from (garganorn/stages.py
    ~918-922: 'CREATE TEMP TABLE idf_scores AS SELECT * FROM read_parquet(...)').
    Built directly rather than via the full stage_idf pipeline since only the
    two consumed columns matter here.
    """
    path = tmp_path / name
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE tmp_idf (category VARCHAR, idf_score DOUBLE)
    """)
    conn.execute("""
        INSERT INTO tmp_idf VALUES ('cafe', 1.5), ('park', 2.0)
    """)
    conn.execute(f"COPY tmp_idf TO '{path}' (FORMAT PARQUET)")
    conn.close()
    return str(path)


def _source_parquet_and_glob(source, overture_parquet, osm_parquet):
    """Return (parquet_glob_arg_for_stage_import, list_of_source_parquet_paths)."""
    if source == "overture_place":
        return overture_parquet, [overture_parquet]
    else:  # osm
        node_glob, way_glob = osm_parquet["node"], osm_parquet["way"]
        return (node_glob, way_glob), [node_glob, way_glob]


# ---------------------------------------------------------------------------
# 1. Meta shape: inputs/params must include density + idf (contract, not impl)
# ---------------------------------------------------------------------------

class TestStageImportFreshnessMetaShape:
    """places.parquet .meta.json must record density/idf inputs and norm params.

    Reference shape: stage_division_import records
        input_files = division parquet + division_area parquet + density_parquet
        params = {"source", "bbox", "density_norm", "pop_norm"}
    (garganorn/stages.py ~656-668). stage_import must do the analogous thing
    for density_parquet AND idf_parquet, per the design doc freshness table.
    """

    @pytest.mark.parametrize("source", ["overture_place", "osm"])
    def test_meta_inputs_include_density_and_idf_paths(
        self, source, overture_parquet, osm_parquet,
        density_parquet, tmp_path,
    ):
        """meta['inputs'] must include the resolved density and idf paths."""
        idf_parquet = _make_idf_parquet(tmp_path, name=f"{source}_idf.parquet")
        parquet_glob, _source_paths = _source_parquet_and_glob(
            source, overture_parquet, osm_parquet
        )
        output = str(tmp_path / f"{source}_places.parquet")

        _stages.stage_import(
            source, parquet_glob, _BBOX, output,
            density_parquet=density_parquet, idf_parquet=idf_parquet,
        )

        meta = json.loads(pathlib.Path(output + ".meta.json").read_text())
        inputs = meta.get("inputs", [])

        assert density_parquet in inputs, (
            f"meta['inputs'] must include resolved density_parquet path "
            f"({density_parquet!r}); got inputs={inputs!r}. Rebuilding "
            f"density_tiles.parquet must invalidate {source}/places.parquet."
        )
        assert idf_parquet in inputs, (
            f"meta['inputs'] must include resolved idf_parquet path "
            f"({idf_parquet!r}); got inputs={inputs!r}."
        )

    @pytest.mark.parametrize("source", ["overture_place", "osm"])
    def test_meta_params_include_density_norm_and_idf_norm(
        self, source, overture_parquet, osm_parquet,
        density_parquet, tmp_path,
    ):
        """meta['params'] must include density_norm and idf_norm with the passed values."""
        idf_parquet = _make_idf_parquet(tmp_path, name=f"{source}_idf.parquet")
        parquet_glob, _ = _source_parquet_and_glob(
            source, overture_parquet, osm_parquet
        )
        output = str(tmp_path / f"{source}_places.parquet")

        _stages.stage_import(
            source, parquet_glob, _BBOX, output,
            density_parquet=density_parquet, idf_parquet=idf_parquet,
            density_norm=12.5, idf_norm=21.0,
        )

        meta = json.loads(pathlib.Path(output + ".meta.json").read_text())
        params = meta.get("params", {})

        assert "density_norm" in params, (
            f"meta['params'] must include density_norm (design doc freshness "
            f"table: '<src>/places.parquet' params = bbox, density_norm, "
            f"idf_norm); got params={params!r}"
        )
        assert params["density_norm"] == 12.5, (
            f"meta['params']['density_norm'] must equal the value passed to "
            f"stage_import (12.5); got {params.get('density_norm')!r}"
        )
        assert "idf_norm" in params, (
            f"meta['params'] must include idf_norm; got params={params!r}"
        )
        assert params["idf_norm"] == 21.0, (
            f"meta['params']['idf_norm'] must equal the value passed to "
            f"stage_import (21.0); got {params.get('idf_norm')!r}"
        )


# ---------------------------------------------------------------------------
# 2. Behavioral invalidation: newer density/idf must trigger a rebuild
# ---------------------------------------------------------------------------

class TestStageImportFreshnessInvalidation:
    """A newer density_parquet or idf_parquet must invalidate places.parquet.

    Mirrors the mtime-manipulation pattern in tests/test_idf_stage.py's
    TestStageIdfCaching (lines ~476-680): move artifact+meta to the past,
    leave the input at its current (newer) mtime via os.utime, and assert
    the stage logs a "start" (rebuild) rather than "skip" message.

    Currently these fail because stage_import's params/inputs never include
    density_parquet/idf_parquet at all, so artifact_fresh() compares against
    a params/inputs list that doesn't mention them — mutating those files'
    mtimes has no effect on the freshness decision and the stage always skips.
    """

    def test_newer_density_parquet_triggers_rebuild(self, tmp_path, caplog):
        # Private (non-session-shared) source/idf parquets: os.utime() below
        # must not leak mtime changes into fixtures other tests depend on.
        overture_parquet = _make_overture_parquet(tmp_path)
        density_parquet = _make_density_parquet(tmp_path)
        idf_parquet = _make_idf_parquet(tmp_path)
        output = str(tmp_path / "places.parquet")
        meta_path = output + ".meta.json"

        _stages.stage_import(
            "overture_place", overture_parquet, _BBOX, output,
            density_parquet=density_parquet, idf_parquet=idf_parquet,
        )
        assert os.path.exists(output)
        assert os.path.exists(meta_path)

        # Move artifact+meta+source-parquet+idf all to the past, so the ONLY
        # newer input is density_parquet — isolates the density-specific
        # invalidation path from the (already-correctly-tracked) source input.
        past_time = time.time() - 3600
        os.utime(output, (past_time, past_time))
        os.utime(meta_path, (past_time, past_time))
        os.utime(overture_parquet, (past_time - 1, past_time - 1))
        os.utime(idf_parquet, (past_time - 1, past_time - 1))
        # Explicitly bump density_parquet to "now" (newer than meta) —
        # mind second-granularity mtimes, set explicitly rather than relying
        # on wall-clock drift.
        now = time.time()
        os.utime(density_parquet, (now, now))

        caplog.clear()
        with caplog.at_level(logging.INFO):
            _stages.stage_import(
                "overture_place", overture_parquet, _BBOX, output,
                density_parquet=density_parquet, idf_parquet=idf_parquet,
            )

        assert any("start" in r.message.lower() for r in caplog.records), (
            "stage_import must rebuild (log a 'starting' message) when "
            "density_parquet ALONE is newer than the recorded meta (source "
            "parquet and idf_parquet held fixed in the past) — a stale "
            "density_tiles.parquet rebuild must not be silently reused for "
            "importance scoring."
        )
        assert not any("skip" in r.message.lower() for r in caplog.records), (
            "stage_import must not skip when density_parquet changed"
        )

    def test_newer_idf_parquet_triggers_rebuild(self, tmp_path, caplog):
        overture_parquet = _make_overture_parquet(tmp_path)
        density_parquet = _make_density_parquet(tmp_path)
        idf_parquet = _make_idf_parquet(tmp_path)
        output = str(tmp_path / "places.parquet")
        meta_path = output + ".meta.json"

        _stages.stage_import(
            "overture_place", overture_parquet, _BBOX, output,
            density_parquet=density_parquet, idf_parquet=idf_parquet,
        )
        assert os.path.exists(output)
        assert os.path.exists(meta_path)

        # Move artifact+meta+source-parquet+density all to the past, so the
        # ONLY newer input is idf_parquet — isolates the idf-specific
        # invalidation path from the (already-correctly-tracked) source input.
        past_time = time.time() - 3600
        os.utime(output, (past_time, past_time))
        os.utime(meta_path, (past_time, past_time))
        os.utime(overture_parquet, (past_time - 1, past_time - 1))
        os.utime(density_parquet, (past_time - 1, past_time - 1))
        now = time.time()
        os.utime(idf_parquet, (now, now))

        caplog.clear()
        with caplog.at_level(logging.INFO):
            _stages.stage_import(
                "overture_place", overture_parquet, _BBOX, output,
                density_parquet=density_parquet, idf_parquet=idf_parquet,
            )

        assert any("start" in r.message.lower() for r in caplog.records), (
            "stage_import must rebuild (log a 'starting' message) when "
            "idf_parquet ALONE is newer than the recorded meta (source "
            "parquet and density_parquet held fixed in the past)."
        )
        assert not any("skip" in r.message.lower() for r in caplog.records), (
            "stage_import must not skip when idf_parquet changed"
        )

    def test_unchanged_density_and_idf_still_skips(
        self, overture_parquet, density_parquet, tmp_path, caplog,
    ):
        """Sanity/control: with density+idf tracked but genuinely unchanged, a
        second call must still skip (guards against an overcorrection that
        always rebuilds).
        """
        idf_parquet = _make_idf_parquet(tmp_path)
        output = str(tmp_path / "places.parquet")
        meta_path = output + ".meta.json"

        _stages.stage_import(
            "overture_place", overture_parquet, _BBOX, output,
            density_parquet=density_parquet, idf_parquet=idf_parquet,
        )
        assert os.path.exists(output)
        assert os.path.exists(meta_path)

        # Advance BOTH artifact and meta into the future, matching the
        # skip-path pattern in test_idf_stage.py::test_skips_when_output_fresh.
        future_time = time.time() + 3600
        os.utime(output, (future_time, future_time))
        os.utime(meta_path, (future_time + 1, future_time + 1))

        caplog.clear()
        with caplog.at_level(logging.INFO):
            _stages.stage_import(
                "overture_place", overture_parquet, _BBOX, output,
                density_parquet=density_parquet, idf_parquet=idf_parquet,
            )

        assert any("skip" in r.message.lower() for r in caplog.records), (
            "stage_import must skip when density_parquet/idf_parquet are "
            "tracked but unchanged and artifact+meta are fresh"
        )


# ---------------------------------------------------------------------------
# 3. Edge: density_parquet/idf_parquet NOT provided (None) — no phantom entries
# ---------------------------------------------------------------------------

class TestStageImportFreshnessMetaNoDensityIdf:
    """When density_parquet/idf_parquet are None, meta must not contain phantom
    entries, and stage_import must still work (guards against overcorrection —
    e.g. always appending 'None' or an empty-string path to inputs/params).
    """

    def test_no_phantom_density_idf_inputs_when_not_provided(
        self, overture_parquet, tmp_path,
    ):
        output = str(tmp_path / "places.parquet")

        _stages.stage_import("overture_place", overture_parquet, _BBOX, output)

        assert pathlib.Path(output).exists(), (
            "stage_import must still succeed with density_parquet/idf_parquet "
            "left at their default (None)"
        )
        meta = json.loads(pathlib.Path(output + ".meta.json").read_text())
        inputs = meta.get("inputs", [])
        params = meta.get("params", {})

        assert None not in inputs, f"meta['inputs'] must not contain None: {inputs!r}"
        assert "" not in inputs, f"meta['inputs'] must not contain empty string: {inputs!r}"
        assert "None" not in inputs, (
            f"meta['inputs'] must not contain the string 'None': {inputs!r}"
        )
        # density_norm/idf_norm are stage_import's default-valued kwargs (10.0,
        # 18.0); they may legitimately appear in params since they were passed
        # (with defaults) to the call. What must NOT happen is a density/idf
        # *path* entry (None-derived) leaking into params or inputs.
        for key in ("density_norm", "idf_norm"):
            if key in params:
                assert params[key] is not None, (
                    f"meta['params'][{key!r}] must not be None when "
                    f"density_parquet/idf_parquet were not provided"
                )

    def test_meta_params_no_density_idf_path_keys_when_not_provided(
        self, overture_parquet, tmp_path,
    ):
        """params must not contain a 'density_parquet'/'idf_parquet' path key at all
        (paths belong in inputs, not params — matching stage_division_import's
        shape, where params holds only norm constants, not paths).
        """
        output = str(tmp_path / "places.parquet")

        _stages.stage_import("overture_place", overture_parquet, _BBOX, output)

        meta = json.loads(pathlib.Path(output + ".meta.json").read_text())
        params = meta.get("params", {})

        assert "density_parquet" not in params, (
            f"params must not contain a 'density_parquet' path key: {params!r}"
        )
        assert "idf_parquet" not in params, (
            f"params must not contain an 'idf_parquet' path key: {params!r}"
        )
