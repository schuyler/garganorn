"""Tests for garganorn.envelope, the atgeo tile envelope module.

Envelope contract exercised here:
  - build_tile_payload(published_at, attribution, records_json) -> bytes whose
    top-level parses to exactly {publishedAt, attribution, records}.
  - records_json is a list of record JSON strings spliced in verbatim (string
    composition, not a json.loads/json.dumps round trip), so UTF-8 output and
    unusual formatting survive unchanged.

Determinism and timestamp coherence are covered end-to-end against the
real stage_export production path in tests/test_stages.py -- this module
is unit-level only, exercising envelope.py's pure functions in isolation.
"""
import gzip
import json

from garganorn import envelope
from tests.lexicon_helpers import validate_def


_PUBLISHED_AT = "2026-07-09T18:00:00Z"
_ATTRIBUTION = [{
    "dataset": "overturemaps.org",
    "collection": "place",
    "release": "2026-09-17.0",
    "source": "https://overturemaps.org/",
    "licenses": ["https://www.apache.org/licenses/LICENSE-2.0"],
    "attribution": "https://docs.overturemaps.org/attribution/",
}]


def _record(place_id="ov001", name="Test"):
    value = {
        "$type": "org.atgeo.place",
        "origin": f"urn:atgeo:overturemaps.org:place:{place_id}",
        "name": name,
    }
    return json.dumps({"value": value, "importance": 10})


class TestRecordSurface:
    def test_record_uri_and_wrap_record_removed(self):
        assert not hasattr(envelope, "record_uri")
        assert not hasattr(envelope, "wrap_record")


class TestBuildTilePayload:
    def test_top_level_keys_exact(self):
        payload = envelope.build_tile_payload(_PUBLISHED_AT, _ATTRIBUTION, [_record()])
        assert isinstance(payload, bytes), f"build_tile_payload must return bytes; got {type(payload)}"
        parsed = json.loads(payload)
        assert set(parsed.keys()) == {"publishedAt", "attribution", "records"}, (
            f"tile top-level must be exactly {{publishedAt, attribution, records}}; got {list(parsed)}"
        )

    def test_header_fields_match_inputs(self):
        parsed = json.loads(envelope.build_tile_payload(_PUBLISHED_AT, _ATTRIBUTION, [_record()]))
        assert parsed["publishedAt"] == _PUBLISHED_AT
        assert parsed["attribution"] == _ATTRIBUTION

    def test_records_spliced_in_order(self):
        records = [_record("ov001", "A"), _record("ov002", "B")]
        parsed = json.loads(envelope.build_tile_payload(_PUBLISHED_AT, _ATTRIBUTION, records))
        assert [r["value"]["name"] for r in parsed["records"]] == ["A", "B"]

    def test_records_embedded_verbatim(self):
        """Unusual spacing and key order survive: no json round trip."""
        record = '{"importance":3,"value":{"$type":"org.atgeo.place","name":"Café"}}'
        payload = envelope.build_tile_payload(_PUBLISHED_AT, _ATTRIBUTION, [record])
        assert record.encode("utf-8") in payload

    def test_utf8_not_ascii_escaped(self):
        record = json.dumps(
            {"value": {"$type": "org.atgeo.place", "name": "Café"}}, ensure_ascii=False,
        )
        payload = envelope.build_tile_payload(_PUBLISHED_AT, _ATTRIBUTION, [record])
        assert "Café".encode("utf-8") in payload
        assert b"\\u00e9" not in payload

    def test_empty_records(self):
        parsed = json.loads(envelope.build_tile_payload(_PUBLISHED_AT, _ATTRIBUTION, []))
        assert parsed["records"] == []

    def test_gzip_roundtrip(self):
        payload = envelope.build_tile_payload(_PUBLISHED_AT, _ATTRIBUTION, [_record()])
        assert json.loads(gzip.decompress(gzip.compress(payload, mtime=0))) == json.loads(payload)

    def test_validates_against_lexicon_schema(self):
        payload = envelope.build_tile_payload(
            _PUBLISHED_AT, _ATTRIBUTION, [_record("ov001", "A"), _record("ov002", "B")],
        )
        validate_def("org.atgeo.tilePayload", json.loads(payload))

    def test_empty_records_validates_against_lexicon_schema(self):
        payload = envelope.build_tile_payload(_PUBLISHED_AT, _ATTRIBUTION, [])
        validate_def("org.atgeo.tilePayload", json.loads(payload))
