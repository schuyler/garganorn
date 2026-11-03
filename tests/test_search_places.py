"""Tests for the org.atgeo.searchPlaces XRPC endpoint, through the Flask client."""
import json
import logging

import gunicorn.config
import gunicorn.glogging
import pytest

from tests.lexicon_helpers import LEXICON_DIR, validate_def
from tests.served_runs import (
    OVERTURE, box, build_app, geo, place, serve_run,
)

QK = "012301"
REGION = "-123,37,-122,38"
FIJI_REGION = "170,-20,-170,-10"
NONCE = "ZanzibarQuokka"


def _serve(tmp_path, records, **kwargs):
    return build_app(serve_run(tmp_path, tiles={QK: records}, **kwargs))


def _record(id, name, **kwargs):
    return place(OVERTURE, "place", id, name, **kwargs)


def _search(client, **params):
    params.setdefault("dataset", OVERTURE)
    return client.get("/xrpc/org.atgeo.searchPlaces", query_string=params)


def _names(resp):
    assert resp.status_code == 200, resp.get_json()
    return [p["value"]["name"] for p in resp.get_json()["places"]]


def _assert_error(resp, error):
    assert resp.status_code == 400
    assert resp.get_json()["error"] == error


def _found(tmp_path, records, **params):
    with _serve(tmp_path, records).test_client() as client:
        return _names(_search(client, **params))


def test_keyword_finds_record_without_region(tmp_path):
    """R1: no region, no tile knowledge; the keyword finds the record."""
    records = [_record("a", "Golden Gate Bridge"), _record("b", "Eiffel Tower")]
    assert _found(tmp_path, records, q="Golden Gate Bridge") == ["Golden Gate Bridge"]


@pytest.mark.parametrize("name,q", [
    ("Café Rouge", "CAFE"),
    ("CAFÉ ROUGE", "café"),
    ("McDonald's", "mcdonalds"),
    ("McDonalds", "McDonald's"),
    ("St. Louis Bar", "st louis"),
    ("St Louis Bar", "St. Louis"),
])
def test_matching_ignores_case_accents_and_punctuation(tmp_path, name, q):
    assert _found(tmp_path, [_record("a", name)], q=q) == [name]


def test_keyword_words_match_by_prefix(tmp_path):
    records = [_record("a", "Blue Bottle Coffee")]
    assert _found(tmp_path, records, q="blu bott") == ["Blue Bottle Coffee"]
    assert _found(tmp_path / "x", records, q="lue") == []


def test_one_character_word_beside_a_longer_one_still_finds_the_record(tmp_path):
    assert _found(tmp_path, [_record("a", "Y Zed Bar")], q="y zed") == ["Y Zed Bar"]


def test_every_keyword_word_is_required(tmp_path):
    records = [_record("a", "Blue Bottle Coffee"), _record("b", "Blue Heron")]
    assert _found(tmp_path, records, q="blue coffee") == ["Blue Bottle Coffee"]


def test_words_must_match_within_one_name(tmp_path):
    """'golden' is in the name and 'puerta' in a variant: no single name has both."""
    records = [_record("a", "Golden Gate", variants=["Puerta Dorada"])]
    assert _found(tmp_path, records, q="golden puerta") == []


def test_variant_name_matches(tmp_path):
    records = [_record("a", "Golden Gate", variants=["Puerta Dorada"])]
    assert _found(tmp_path, records, q="puerta dorada") == ["Golden Gate"]


def test_missing_q_is_invalid_request(tmp_path):
    with _serve(tmp_path, [_record("a", "Ur")]).test_client() as client:
        _assert_error(client.get("/xrpc/org.atgeo.searchPlaces",
                                 query_string={"dataset": OVERTURE}), "InvalidRequest")


@pytest.mark.parametrize("q", ["", "  ", "&", "Å", "a b"])
def test_keyword_without_a_two_character_word_is_refused(tmp_path, q):
    with _serve(tmp_path, [_record("a", "Å"), _record("b", "A B")]).test_client() as client:
        resp = _search(client, q=q)
    _assert_error(resp, "KeywordRequired")
    assert "places" not in resp.get_json()


def test_two_character_keyword_finds_two_character_name(tmp_path):
    assert _found(tmp_path, [_record("a", "Ur")], q="ur") == ["Ur"]


WORDS = ("alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima"
         .split())


def test_only_first_twelve_distinct_words_are_used(tmp_path):
    """A 13th word that matches nothing is cut."""
    records = [_record("a", " ".join(WORDS))]
    assert _found(tmp_path, records, q=" ".join(WORDS + ["zulu"])) == [" ".join(WORDS)]


def test_duplicate_words_do_not_count_toward_the_cap(tmp_path):
    """alpha twice, ten more distinct words, then a non-matching word: the
    non-matching word is the 12th distinct word, so it is used."""
    records = [_record("a", " ".join(WORDS))]
    q = " ".join(["alpha"] + WORDS[:11] + ["zulu"])
    assert _found(tmp_path, records, q=q) == []


@pytest.mark.parametrize("bbox", ["10.5,10,11,11", "10.0,10,11,11", "1e1,10,11,11"])
def test_region_off_the_degree_grid_is_refused(tmp_path, bbox):
    with _serve(tmp_path, [_record("a", "Zed")]).test_client() as client:
        _assert_error(_search(client, q="zed", bbox=bbox), "BboxTooPrecise")


@pytest.mark.parametrize("bbox", ["10,10,10,11", "180,10,-180,11"])
def test_zero_width_region_is_invalid(tmp_path, bbox):
    with _serve(tmp_path, [_record("a", "Zed")]).test_client() as client:
        _assert_error(_search(client, q="zed", bbox=bbox), "InvalidBbox")


REGION_RECORDS = {
    "inside": [geo("37.500000", "-122.500000")],
    "north edge": [geo("38.000000", "-122.500000")],
    "east edge": [geo("37.500000", "-122.000000")],
    "outside": [geo("37.500000", "-121.500000")],
    "just north": [geo("38.000001", "-122.500000")],
    "just east": [geo("37.500000", "-121.999999")],
    "box meets": [box("-122.500000", "37.200000", "-121.500000", "37.400000")],
    "box corner outside": [box("-123.500000", "36.500000", "-122.500000", "37.500000")],
    "box beyond": [box("-121.500000", "37.200000", "-121.000000", "37.400000")],
}


@pytest.mark.parametrize("name,kept", [
    ("inside", True), ("north edge", True), ("east edge", True),
    ("outside", False), ("just north", False), ("just east", False),
    ("box meets", True), ("box corner outside", True), ("box beyond", False),
])
def test_region_keeps_places_it_contains_or_meets(tmp_path, name, kept):
    records = [_record(f"r{i}", f"Zed {n}", locations=loc)
               for i, (n, loc) in enumerate(REGION_RECORDS.items())]
    found = _found(tmp_path, records, q="zed", bbox=REGION)
    assert (f"Zed {name}" in found) == kept


@pytest.mark.parametrize("lon,kept", [
    ("179.500000", True), ("-179.500000", True), ("0.000000", False), ("160.000000", False),
])
def test_antimeridian_region(tmp_path, lon, kept):
    records = [_record("a", "Zed", locations=[geo("-15.000000", lon)])]
    assert (_found(tmp_path, records, q="zed", bbox=FIJI_REGION) == ["Zed"]) == kept


def test_lexicon_declares_no_point():
    lexicon = json.loads((LEXICON_DIR / "searchPlaces.json").read_text())
    props = lexicon["defs"]["main"]["parameters"]["properties"]
    assert set(props) == {"dataset", "collection", "q", "bbox"}


def test_extra_point_parameter_is_invalid_request(tmp_path):
    app = _serve(tmp_path, [_record("a", "Zed")])
    app.config["PROPAGATE_EXCEPTIONS"] = False
    app.testing = False
    with app.test_client() as client:
        _assert_error(_search(client, q="zed", lat="37.7"), "InvalidRequest")


def test_results_are_bounded_ordered_and_getplace_shaped(tmp_path):
    """25 matches: 20 results, importance descending then key ascending, each a
    valid placeView equal to getPlace's answer for its key."""
    records = [_record(f"p{i:02d}", f"Match {i}", importance=(i % 5) * 10)
               for i in range(25)]
    expected = sorted(records, key=lambda r: (-r["importance"], r["value"]["origin"]))[:20]
    with _serve(tmp_path, records).test_client() as client:
        resp = _search(client, q="match")
        assert resp.status_code == 200
        places = resp.get_json()["places"]
        assert [p["uri"] for p in places] == [r["value"]["origin"] for r in expected]
        for view in places:
            validate_def("org.atgeo.getPlace#placeView", view)
            got = client.get("/xrpc/org.atgeo.getPlace", query_string={"uri": view["uri"]})
            assert got.get_json() == view


def _two_tilesets(tmp_path, n=15):
    """Importances 100, 99, ...: even ranks in 'place', odd in 'division'."""
    places = [place(OVERTURE, "place", f"p{i}", f"Merge p{i}", importance=100 - 2 * i)
              for i in range(n)]
    divisions = [place(OVERTURE, "division", f"d{i}", f"Merge d{i}", importance=99 - 2 * i)
                 for i in range(n)]
    return build_app(
        serve_run(tmp_path, "overture-place", [(OVERTURE, "place")], {QK: places}),
        serve_run(tmp_path, "overture-division", [(OVERTURE, "division")], {QK: divisions}),
    )


def test_dataset_wide_search_merges_tilesets(tmp_path):
    with _two_tilesets(tmp_path).test_client() as client:
        names = _names(_search(client, q="merge"))
    assert names == [f"Merge {'pd'[i % 2]}{i // 2}" for i in range(20)]


def test_collection_narrows_results(tmp_path):
    records = [place(OVERTURE, "place", "p", "Zed Place"),
               place(OVERTURE, "division", "d", "Zed Division")]
    app = _serve(tmp_path, records, collections=((OVERTURE, "place"), (OVERTURE, "division")))
    with app.test_client() as client:
        assert _names(_search(client, q="zed", collection="division")) == ["Zed Division"]
        assert sorted(_names(_search(client, q="zed"))) == ["Zed Division", "Zed Place"]


def test_unknown_dataset_and_collection(tmp_path):
    with _serve(tmp_path, [_record("a", "Zed")]).test_client() as client:
        _assert_error(_search(client, q="zed", dataset="nowhere.example"), "DatasetNotFound")
        _assert_error(_search(client, q="zed", collection="nothing"), "CollectionNotFound")


def test_missing_dataset_is_invalid_request(tmp_path):
    with _serve(tmp_path, [_record("a", "Zed")]).test_client() as client:
        _assert_error(client.get("/xrpc/org.atgeo.searchPlaces", query_string={"q": "zed"}),
                      "InvalidRequest")


def test_run_without_search_index_is_not_served(tmp_path):
    tileset = serve_run(tmp_path, tiles={QK: [_record("a", "Zed")]})
    (tmp_path / "overture-place" / "tiles" / "current" / "search.duckdb").unlink()
    with build_app(tileset).test_client() as client:
        assert client.get("/xrpc/org.atgeo.describeService").get_json()["tilesets"] == []


def test_search_is_not_cacheable(tmp_path):
    with _serve(tmp_path, [_record("a", "Zed")]).test_client() as client:
        resp = _search(client, q="zed")
    assert resp.status_code == 200
    assert "Cache-Control" not in resp.headers


# R10: the keyword reaches no log.

def _logged_text(caplog):
    return "\n".join(logging.Formatter().format(r) for r in caplog.records).lower()


def test_search_logs_no_keyword(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    with _serve(tmp_path, [_record("a", "Zanzibar Quokka")]).test_client() as client:
        assert _names(_search(client, q="Zanzibar Quokka")) == ["Zanzibar Quokka"]
        _assert_error(_search(client, q="Zanzibar Quokka", bbox="10.5,10,11,11"),
                      "BboxTooPrecise")
    text = _logged_text(caplog)
    assert "zanzibar" not in text and "quokka" not in text


def test_gunicorn_error_log_strips_the_query(tmp_path, capfd):
    """The shape of Worker.handle_error's log call."""
    _serve(tmp_path, [_record("a", "Zed")])
    glogger = gunicorn.glogging.Logger(gunicorn.config.Config())
    glogger.exception("Error handling request %s %s", "GET",
                      f"/xrpc/org.atgeo.searchPlaces?dataset=x&q={NONCE}")
    err = capfd.readouterr().err
    assert "/xrpc/org.atgeo.searchPlaces" in err
    assert NONCE not in err
