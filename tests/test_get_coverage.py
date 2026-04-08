"""Tests for the getCoverage XRPC endpoint on garganorn.server.Server.

Red phase: Server.__init__ does not accept tile_manifests yet → TypeError
on every test that constructs a Server with tile_manifests.
The BboxTooLarge import will also fail until quadtree.py is updated.
"""
import logging
from unittest.mock import MagicMock

import pytest
from lexrpc.base import XrpcError

from garganorn.server import Server

FSQ_COLLECTION = "org.atgeo.places.foursquare"
SAMPLE_TILES = [
    "https://tiles.example.com/023010/023010.json.gz",
    "https://tiles.example.com/023011/023011.json.gz",
]


def _make_mock_db(collection=FSQ_COLLECTION):
    mock = MagicMock()
    mock.collection = collection
    mock.attribution = "https://example.com/attribution"
    mock.get_record.return_value = None
    mock.nearest.return_value = []
    return mock


def _make_mock_manifest(tiles=None, raises=None):
    """Return a mock TileManifest.

    If raises is an exception class, get_tiles_for_bbox raises an instance.
    Otherwise returns tiles (default SAMPLE_TILES).
    """
    mock = MagicMock()
    if raises is not None:
        mock.get_tiles_for_bbox.side_effect = raises()
    else:
        mock.get_tiles_for_bbox.return_value = list(tiles if tiles is not None else SAMPLE_TILES)
    return mock


def _make_server(tile_manifests=None, max_coverage_tiles=50):
    dbs = [_make_mock_db(FSQ_COLLECTION)]
    logger = logging.getLogger("test")
    return Server("places.atgeo.org", dbs, logger,
                  tile_manifests=tile_manifests, max_coverage_tiles=max_coverage_tiles)


class TestGetCoverage:
    def test_valid_collection_and_bbox_returns_tiles(self):
        """Valid collection + bbox → {'tiles': sorted URLs}."""
        server = _make_server(tile_manifests={FSQ_COLLECTION: _make_mock_manifest(tiles=SAMPLE_TILES)})
        result = server.get_coverage({}, collection=FSQ_COLLECTION, bbox="-122.55,37.60,-122.30,37.85")
        assert "tiles" in result
        assert result["tiles"] == sorted(SAMPLE_TILES)

    def test_tiles_are_sorted(self):
        """Response tiles are sorted even if manifest returns them unsorted."""
        unsorted = [
            "https://tiles.example.com/zzz/zzz.json.gz",
            "https://tiles.example.com/aaa/aaa.json.gz",
        ]
        server = _make_server(tile_manifests={FSQ_COLLECTION: _make_mock_manifest(tiles=unsorted)})
        result = server.get_coverage({}, collection=FSQ_COLLECTION, bbox="-180,-85,180,85")
        assert result["tiles"] == sorted(unsorted)

    def test_unknown_collection_raises_collection_not_found(self):
        """Unknown collection → XrpcError('CollectionNotFound')."""
        server = _make_server(tile_manifests={FSQ_COLLECTION: _make_mock_manifest()})
        with pytest.raises(XrpcError) as exc_info:
            server.get_coverage({}, collection="org.atgeo.places.unknown", bbox="-180,-85,180,85")
        assert exc_info.value.name == "CollectionNotFound"

    def test_invalid_bbox_raises_invalid_bbox(self):
        """Malformed bbox string → XrpcError('InvalidBbox')."""
        server = _make_server(tile_manifests={FSQ_COLLECTION: _make_mock_manifest()})
        with pytest.raises(XrpcError) as exc_info:
            server.get_coverage({}, collection=FSQ_COLLECTION, bbox="not,a,valid,bbox,extra,values")
        assert exc_info.value.name == "InvalidBbox"

    def test_bbox_too_large_raises_xrpc_bbox_too_large(self):
        """BboxTooLarge from manifest → XrpcError('BboxTooLarge')."""
        from garganorn.quadtree import BboxTooLarge
        server = _make_server(tile_manifests={FSQ_COLLECTION: _make_mock_manifest(raises=BboxTooLarge)})
        with pytest.raises(XrpcError) as exc_info:
            server.get_coverage({}, collection=FSQ_COLLECTION, bbox="-180,-85,180,85")
        assert exc_info.value.name == "BboxTooLarge"

    def test_no_tile_manifests_raises_collection_not_found(self):
        """Server with tile_manifests=None → CollectionNotFound for any collection."""
        server = _make_server(tile_manifests=None)
        with pytest.raises(XrpcError) as exc_info:
            server.get_coverage({}, collection=FSQ_COLLECTION, bbox="-180,-85,180,85")
        assert exc_info.value.name == "CollectionNotFound"

    def test_collection_missing_from_manifests_raises_collection_not_found(self):
        """Collection in db but absent from tile_manifests → CollectionNotFound."""
        server = _make_server(tile_manifests={})
        with pytest.raises(XrpcError) as exc_info:
            server.get_coverage({}, collection=FSQ_COLLECTION, bbox="-180,-85,180,85")
        assert exc_info.value.name == "CollectionNotFound"

    def test_max_coverage_tiles_forwarded_to_manifest(self):
        """Server passes max_coverage_tiles to get_tiles_for_bbox as max_tiles."""
        mock_manifest = _make_mock_manifest(tiles=[])
        server = _make_server(
            tile_manifests={FSQ_COLLECTION: mock_manifest},
            max_coverage_tiles=7,
        )
        server.get_coverage({}, collection=FSQ_COLLECTION, bbox="-180,-85,180,85")
        mock_manifest.get_tiles_for_bbox.assert_called_once()
        call_args = mock_manifest.get_tiles_for_bbox.call_args
        assert call_args.kwargs.get("max_tiles") == 7, (
            f"Expected max_tiles=7 in kwargs; got {call_args.kwargs}"
        )
