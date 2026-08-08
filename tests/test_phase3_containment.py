"""Phase 3 tests: compute_containment collection_prefix + boundary export filter."""
import inspect


# ---------------------------------------------------------------------------
# Test 4: compute_containment() accepts collection_prefix parameter
# ---------------------------------------------------------------------------

class TestComputeContainmentSignature:
    def test_accepts_collection_prefix_parameter(self):
        """compute_containment must accept a 'collection_prefix' keyword argument."""
        from garganorn.quadtree import compute_containment
        sig = inspect.signature(compute_containment)
        assert "collection_prefix" in sig.parameters, \
            f"compute_containment signature missing 'collection_prefix': {sig}"

    def test_collection_prefix_defaults_to_division(self):
        """Default value of collection_prefix should be 'org.atgeo.places.overture.division'."""
        from garganorn.quadtree import compute_containment
        sig = inspect.signature(compute_containment)
        param = sig.parameters["collection_prefix"]
        assert param.default == "org.atgeo.places.overture.division", \
            f"Expected default 'org.atgeo.places.overture.division', got {param.default!r}"


# ---------------------------------------------------------------------------
# Tests 5-9 (TestComputeContainmentOutput, TestComputeContainmentAdaptive):
# Deleted with the covering-rewrite (Phase 1).
#   - TestComputeContainmentOutput: superseded by TestContainmentBehaviorPorts
#     in tests/test_containment_covering.py (called without covering_dir, which
#     the new implementation treats as Q3 graceful degradation → empty result).
#   - TestComputeContainmentAdaptive: asserted max_boundaries/max_zoom on the
#     deleted recursion; behavior superseded by TestBruteForceOracle.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test 10: run_pipeline() boundary export filter
# ---------------------------------------------------------------------------

class TestBoundaryExportFilter:
    def test_run_pipeline_has_boundary_filter(self):
        """export_boundaries_db CTAS should include a level-vocabulary filter.

        The filter is the level-vocabulary threshold
        `level <= 50` (country..locality), not admin_level BETWEEN 0 AND 2
        OR subtype = 'locality'. The filter is expressed via the
        LEVEL_VOCAB constant so it can't drift from the vocabulary, but the
        source text still contains the literal comparison against 50
        (LEVEL_VOCAB['locality']) since that's what the CASE/threshold compiles
        to in the CTAS.
        """
        from garganorn import stages
        source = inspect.getsource(stages.stage_division_import)
        assert "level <= 50" in source or "level<=50" in source.lower(), \
            "stage_division_import should filter by level <= 50 (country..locality)"
        assert "admin_level BETWEEN 0 AND 2" not in source, \
            "stage_division_import must no longer filter by raw admin_level"
