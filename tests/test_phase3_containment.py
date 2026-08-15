"""Tests for compute_containment's collection_prefix parameter."""
import inspect


# ---------------------------------------------------------------------------
# compute_containment() accepts collection_prefix parameter
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
