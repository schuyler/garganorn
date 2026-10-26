"""Tests for the org.atgeo.getTiles XRPC endpoint, through the Flask client."""
import pytest

from tests.served_runs import (
    OSM, OVERTURE, build_app, serve_run, tile_url,
)

WORLD = "-180,-85,180,85"
SAMPLE_QKS = ["023010", "023011"]


def _get_tiles(client, bbox=WORLD, dataset=OVERTURE, **params):
    return client.get("/xrpc/org.atgeo.getTiles", query_string={
        "dataset": dataset, "bbox": bbox, **params,
    })


def _overture(tmp_path, qks=SAMPLE_QKS, **kwargs):
    return serve_run(tmp_path, tiles={qk: [] for qk in qks}, **kwargs)


def _osm_tilesets(tmp_path, way_tiles, node_tiles):
    """Two OSM tilesets, ways configured before nodes, so config order is not
    URL order."""
    return (
        serve_run(tmp_path, "osm-way", [(OSM, "way")], {qk: [] for qk in way_tiles}),
        serve_run(tmp_path, "osm-node", [(OSM, "node")], {qk: [] for qk in node_tiles}),
    )


class TestGetTiles:
    def test_valid_dataset_and_bbox_returns_tiles(self, tmp_path):
        """Valid dataset + bbox -> {'tiles': sorted URLs}."""
        with build_app(_overture(tmp_path)).test_client() as client:
            resp = _get_tiles(client)
        assert resp.status_code == 200
        assert resp.get_json() == {
            "tiles": sorted(tile_url("overture-place", qk) for qk in SAMPLE_QKS),
        }

    def test_overview_absent_for_regular_answer(self, tmp_path):
        """A regular answer has no 'overview' key."""
        with build_app(_overture(tmp_path)).test_client() as client:
            assert "overview" not in _get_tiles(client).get_json()

    def test_tiles_are_sorted(self, tmp_path):
        """Response tiles are in ascending string order."""
        qks = ["200000", "120301", "023011", "023010"]
        with build_app(_overture(tmp_path, qks)).test_client() as client:
            tiles = _get_tiles(client).get_json()["tiles"]
        assert tiles == sorted(tile_url("overture-place", qk) for qk in qks)

    def test_non_intersecting_tiles_excluded(self, tmp_path):
        """Only tiles intersecting the bbox are listed."""
        with build_app(_overture(tmp_path, ["023010", "200000"])).test_client() as client:
            tiles = _get_tiles(client, bbox="-122.55,37.60,-122.30,37.85").get_json()["tiles"]
        assert tiles == [tile_url("overture-place", "023010")]

    def test_invalid_bbox_raises_invalid_bbox(self, tmp_path):
        """Malformed bbox string -> InvalidBbox."""
        with build_app(_overture(tmp_path)).test_client() as client:
            resp = _get_tiles(client, bbox="not,a,valid,bbox,extra,values")
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "InvalidBbox"

    @pytest.mark.parametrize("bbox", [
        "-300,-100,300,100",
        "200,10,201,11",
        "10,95,11,96",
        "-181,10,-179,11",
        "179,10,181,11",
        "10,-91,11,-89",
        "10,89,11,91",
    ])
    def test_out_of_range_coordinates_raise_invalid_bbox(self, tmp_path, bbox):
        """A longitude outside [-180, 180] or a latitude outside [-90, 90] is
        InvalidBbox (#26), not a quiet answer."""
        with build_app(_overture(tmp_path)).test_client() as client:
            resp = _get_tiles(client, bbox=bbox)
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "InvalidBbox"

    @pytest.mark.parametrize("bbox", [
        "-180,-90,180,90",
        "-180,10,-179,11",
        "10,-90,11,-89",
        "179,89,180,90",
        "179,10,-179,11",
    ])
    def test_range_bounds_are_inclusive(self, tmp_path, bbox):
        """±180 and ±90 are on Earth; antimeridian boxes still cross."""
        with build_app(_overture(tmp_path)).test_client() as client:
            resp = _get_tiles(client, bbox=bbox)
        assert resp.status_code == 200

    def test_unknown_dataset_raises_dataset_not_found(self, tmp_path):
        with build_app(_overture(tmp_path)).test_client() as client:
            resp = _get_tiles(client, dataset="example.org")
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "DatasetNotFound"

    def test_no_tilesets_raises_dataset_not_found(self):
        """A server with no tilesets -> DatasetNotFound for any dataset."""
        with build_app().test_client() as client:
            resp = _get_tiles(client)
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "DatasetNotFound"

    def test_bbox_too_large_without_overview_tiles(self, tmp_path):
        """More regular tiles than max_tiles, and no overview tiles -> BboxTooLarge."""
        qks = ["023010", "023011", "120301", "200000"]
        with build_app(_overture(tmp_path, qks), max_tiles=2).test_client() as client:
            resp = _get_tiles(client)
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "BboxTooLarge"

    def test_exactly_max_tiles_is_answered(self, tmp_path):
        qks = ["023010", "023011", "120301", "200000"]
        with build_app(_overture(tmp_path, qks), max_tiles=4).test_client() as client:
            resp = _get_tiles(client)
        assert resp.status_code == 200
        assert len(resp.get_json()["tiles"]) == 4

    def test_default_max_tiles_is_75(self, tmp_path):
        qks = [f"{p}{a}{b}" for p in ("0230", "1203", "2000", "3000", "0320")
               for a in "0123" for b in "0123"]
        with build_app(_overture(tmp_path, qks[:75])).test_client() as client:
            assert len(_get_tiles(client).get_json()["tiles"]) == 75
        with build_app(_overture(tmp_path, qks[:76], subdir="over")).test_client() as client:
            assert _get_tiles(client).get_json()["error"] == "BboxTooLarge"

    def test_overview_tiles_replace_regular_tiles_over_max_tiles(self, tmp_path):
        """4 regular tiles > max_tiles=3, with overview tiles present, answers
        the (uncapped) overview tiles with overview: true. Overview keys of
        length 1, 3 and 5 also exercise the qk[:6]/qk URL shape for keys
        shorter than 6 characters."""
        regular = ["023010", "023011", "120301", "200000"]
        overview = ["0", "023", "20111"]
        app = build_app(_overture(tmp_path, regular + overview), max_tiles=3)
        with app.test_client() as client:
            body = _get_tiles(client).get_json()
        assert body == {
            "tiles": sorted(tile_url("overture-place", qk) for qk in overview),
            "overview": True,
        }

    def test_no_overview_leak_within_max_tiles(self, tmp_path):
        """Overview tiles are not listed when the regular tiles answer within
        max_tiles -- the fallback is 'instead of', not 'in addition to'."""
        app = build_app(_overture(tmp_path, SAMPLE_QKS + ["0"]), max_tiles=3)
        with app.test_client() as client:
            body = _get_tiles(client).get_json()
        assert body == {"tiles": sorted(tile_url("overture-place", qk) for qk in SAMPLE_QKS)}


class TestDatasetAndCollection:
    def test_dataset_returns_its_tiles(self, tmp_path):
        """A dataset returns the tiles of the tilesets carrying it, not another dataset's."""
        osm = serve_run(tmp_path, "osm", [(OSM, "node"), (OSM, "way"), (OSM, "relation")],
                        {"023010": []})
        overture = _overture(tmp_path, ["120301"])
        with build_app(osm, overture).test_client() as client:
            tiles = _get_tiles(client, dataset=OSM).get_json()["tiles"]
        assert tiles == [tile_url("osm", "023010")]

    def test_dataset_with_collection_returns_the_same_tiles(self, tmp_path):
        osm = serve_run(tmp_path, "osm", [(OSM, "node"), (OSM, "way"), (OSM, "relation")],
                        {"023010": [], "023011": []})
        with build_app(osm).test_client() as client:
            by_dataset = _get_tiles(client, dataset=OSM).get_json()
            by_collection = _get_tiles(client, dataset=OSM, collection="way").get_json()
        assert by_collection == by_dataset
        assert len(by_dataset["tiles"]) == 2

    def test_unknown_collection_raises_collection_not_found(self, tmp_path):
        """The dataset is served but no tileset carries the collection."""
        osm = serve_run(tmp_path, "osm", [(OSM, "node"), (OSM, "way")], {"023010": []})
        with build_app(osm).test_client() as client:
            resp = _get_tiles(client, dataset=OSM, collection="relation")
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "CollectionNotFound"

    def test_collection_selects_only_tilesets_carrying_it(self, tmp_path):
        ways, nodes = _osm_tilesets(tmp_path, ["023010"], ["120301"])
        with build_app(ways, nodes).test_client() as client:
            tiles = _get_tiles(client, dataset=OSM, collection="way").get_json()["tiles"]
        assert tiles == [tile_url("osm-way", "023010")]

    def test_two_tilesets_of_one_dataset_combine_into_one_sorted_list(self, tmp_path):
        ways, nodes = _osm_tilesets(tmp_path, ["200000", "023010"], ["120301"])
        with build_app(ways, nodes).test_client() as client:
            tiles = _get_tiles(client, dataset=OSM).get_json()["tiles"]
        expected = [tile_url("osm-way", "200000"), tile_url("osm-way", "023010"),
                    tile_url("osm-node", "120301")]
        assert tiles == sorted(expected)

    def test_combined_cap_falls_back_to_overview_tiles_of_both(self, tmp_path):
        """Each tileset alone is within max_tiles; together they exceed it."""
        ways, nodes = _osm_tilesets(
            tmp_path, ["023010", "023011", "02"], ["120301", "200000", "1"],
        )
        with build_app(ways, nodes, max_tiles=3).test_client() as client:
            body = _get_tiles(client, dataset=OSM).get_json()
        assert body == {
            "tiles": sorted([tile_url("osm-way", "02"), tile_url("osm-node", "1")]),
            "overview": True,
        }

    def test_selected_tileset_without_overview_tiles_raises_bbox_too_large(self, tmp_path):
        ways, nodes = _osm_tilesets(
            tmp_path, ["023010", "023011", "02"], ["120301", "200000"],
        )
        with build_app(ways, nodes, max_tiles=3).test_client() as client:
            resp = _get_tiles(client, dataset=OSM)
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "BboxTooLarge"


class TestBboxPrecision:
    """BboxTooPrecise validation in getTiles.

    Enforces the 0.01 degree grid policy: coordinates with more than 2
    decimal places (including trailing zeros) or scientific notation are
    rejected. Coordinates with at most 2 decimal places pass.
    """

    @pytest.fixture
    def client(self, tmp_path):
        with build_app(_overture(tmp_path)).test_client() as c:
            yield c

    @pytest.mark.parametrize("bbox", [
        "-122.41,37.77,-122.30,37.85",   # two decimal places
        "-122,37,-121,38",               # integers
        "-122.4,37.7,-122.3,37.8",       # one decimal place
        "-122.,37.,-121.,38.",           # bare decimal point
    ])
    def test_accepted(self, client, bbox):
        resp = _get_tiles(client, bbox=bbox)
        assert resp.status_code == 200
        assert "tiles" in resp.get_json()

    @pytest.mark.parametrize("bbox", [
        "-122.419,37.77,-122.30,37.85",           # three decimal places
        "-122.41942,37.77493,-122.30,37.85",      # many decimal places
        "-122.410,37.770,-122.300,37.850",        # trailing zeros count
        "1e2,37.77,101.00,37.85",                 # scientific notation
        "-122.419,37.77,not_a_number,37.85",      # precision is checked before parsing
        "-122.41,37.77,-122.419,37.85",           # one bad coordinate rejects the bbox
    ])
    def test_rejected(self, client, bbox):
        resp = _get_tiles(client, bbox=bbox)
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "BboxTooPrecise"
