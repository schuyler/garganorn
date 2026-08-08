"""RED tests: garganorn.envelope — the atgeo v1 tile/record envelope module.

pipeline-implementation-decisions.md ("OQ-P2-1 — record envelope adoption").
garganorn/envelope.py does not exist yet
(per those decisions), so every test in this module that imports from it fails at
collection/setup with ImportError until it is implemented -- that failure IS
the RED signal for this feature. Mirrors the import-guard pattern used by
tests/test_levels.py for garganorn.levels.

Covers the §6 combined acceptance checklist (pipeline-implementation-decisions.md
"OQ-P2-1 — record envelope adoption") items 6 and 9,
plus the module-level contract from those same decisions:
  - record_uri(repo, collection, rkey) -> "https://{repo}/{collection}/{rkey}"
  - wrap_record(uri, record_json) -> '{"uri":...,"cid":null,"value":...}' string,
    cid is literally null, never computed
  - build_tile_payload(collection, source_url, license_url, generated_at,
    wrapped_records) -> bytes; top-level == exactly {collection, source,
    license, generated_at, records}

Item 7 (determinism), item 8 (timestamp coherence), and item 10 (server
round-trip) are covered end-to-end against the real stage_export/tile_reader
production path in tests/test_stages.py, tests/test_integration_quadtree.py,
and tests/test_tile_reader.py -- this module is unit-level only, exercising
envelope.py's pure functions in isolation.
"""
import gzip
import json

import pytest

try:
    from garganorn import envelope
    _ENVELOPE_ERROR = None
except ImportError as _exc:
    envelope = None
    _ENVELOPE_ERROR = _exc


def _check_envelope():
    """Call at the start of every test that needs garganorn.envelope; surfaces the ImportError."""
    if _ENVELOPE_ERROR is not None:
        pytest.fail(
            f"garganorn.envelope not importable: {_ENVELOPE_ERROR}",
            pytrace=False,
        )


# ---------------------------------------------------------------------------
# record_uri() (envelope decisions, see module docstring)
# ---------------------------------------------------------------------------

class TestRecordUri:
    def test_record_uri_form(self):
        """record_uri(repo, collection, rkey) == https://{repo}/{collection}/{rkey} (per the envelope decisions)."""
        _check_envelope()
        uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.overture.place", "ov001"
        )
        assert uri == "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"

    def test_record_uri_osm_node_rkey(self):
        """OSM rkey is the node:/way:/relation: transformed form, not the raw
        place_id -- per the envelope decisions: 'for OSM that is the node:|way:|relation: form
        ... not the raw place_id'. Colons are legal in a URI path segment
        (RFC 3986); no encoding needed."""
        _check_envelope()
        uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.osm", "node:12345"
        )
        assert uri == "https://places.atgeo.org/org.atgeo.places.osm/node:12345"
        assert "%3A" not in uri, "colon must not be percent-encoded (per the envelope decisions)"

    def test_record_uri_osm_way_and_relation(self):
        """way: and relation: rkeys form URIs analogously to node:."""
        _check_envelope()
        way_uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.osm", "way:67890"
        )
        rel_uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.osm", "relation:11111"
        )
        assert way_uri == "https://places.atgeo.org/org.atgeo.places.osm/way:67890"
        assert rel_uri == "https://places.atgeo.org/org.atgeo.places.osm/relation:11111"

    def test_record_uri_not_at_protocol(self):
        """URIs are https://, never at:// -- the envelope decisions are emphatic that gazetteer
        records are not repository data and must not mint at:// URIs."""
        _check_envelope()
        uri = envelope.record_uri("places.atgeo.org", "org.atgeo.places.overture.place", "ov001")
        assert uri.startswith("https://"), f"URI must be https://, got {uri!r}"
        assert not uri.startswith("at://")


# ---------------------------------------------------------------------------
# wrap_record(): {uri, cid, value} exactly, cid is literally null
# ---------------------------------------------------------------------------

class TestWrapRecord:
    def test_wrap_record_produces_exactly_three_keys(self):
        """wrap_record(uri, record_json) -> a JSON string whose parsed object
        has exactly {uri, cid, value} -- three keys, always present (per the envelope decisions)."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "ov001", "name": "Test"})
        wrapped = envelope.wrap_record(uri, record_json)
        parsed = json.loads(wrapped)
        assert set(parsed.keys()) == {"uri", "cid", "value"}, (
            f"wrapped record must have exactly {{uri, cid, value}}; got {list(parsed)}"
        )

    def test_wrap_record_cid_is_none(self):
        """cid is literally null -- never computed, per the APPROVED envelope decision."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "ov001"})
        parsed = json.loads(envelope.wrap_record(uri, record_json))
        assert parsed["cid"] is None

    def test_wrap_record_uri_matches_input(self):
        """The uri field is exactly the uri passed in."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.osm/node:12345"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "node:12345"})
        parsed = json.loads(envelope.wrap_record(uri, record_json))
        assert parsed["uri"] == uri

    def test_wrap_record_value_is_the_record(self):
        """value is byte-for-byte today's record JSON, parsed back losslessly (per the envelope decisions)."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        record = {
            "$type": "org.atgeo.place", "rkey": "ov001", "name": "Blue Bottle Coffee",
            "importance": 72, "locations": [{"$type": "community.lexicon.location.geo",
                                              "latitude": "37.774900", "longitude": "-122.419400"}],
            "variants": [], "attributes": {"tel": None}, "relations": {},
        }
        record_json = json.dumps(record)
        parsed = json.loads(envelope.wrap_record(uri, record_json))
        assert parsed["value"] == record

    def test_wrap_record_no_json_loads_per_record(self):
        """Per the envelope decisions: wrap_record is string composition, not json.loads + json.dumps,
        to avoid a per-record parse/reserialize round trip. Verify by checking
        that malformed-but-well-formed-looking JSON text is passed through
        verbatim rather than being re-serialized (e.g. key order / spacing
        would change under a round trip through json.loads->json.dumps with
        default separators). This is a white-box characterization of the
        envelope decisions: wrap_record must not alter the byte content of
        record_json, only wrap it.
        """
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        # Deliberately unusual spacing/ordering that json.dumps(json.loads(...))
        # with default args would normalize away.
        record_json = '{"$type":"org.atgeo.place","rkey":"ov001","name":"Café"}'
        wrapped = envelope.wrap_record(uri, record_json)
        assert record_json in wrapped, (
            "wrap_record must embed record_json verbatim (string composition, "
            "not a json.loads/json.dumps round trip) -- per the envelope decisions"
        )

    def test_wrap_record_utf8_not_ascii_escaped(self):
        """Per the envelope decisions (review note): DuckDB's UTF-8 output is preserved verbatim
        instead of being ensure_ascii-escaped."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        record_json = json.dumps({"name": "Café"}, ensure_ascii=False)
        wrapped = envelope.wrap_record(uri, record_json)
        assert "Café" in wrapped, (
            f"UTF-8 characters must be preserved verbatim, not \\u-escaped; got {wrapped!r}"
        )
        assert "\\u00e9" not in wrapped


# ---------------------------------------------------------------------------
# build_tile_payload() (envelope decisions, see module docstring)
# ---------------------------------------------------------------------------

class TestBuildTilePayload:
    _COLLECTION = "org.atgeo.places.overture.place"
    _SOURCE_URL = "https://overturemaps.org/"
    _LICENSE_URL = "https://docs.overturemaps.org/attribution/"
    _GENERATED_AT = "2026-07-09T18:00:00Z"

    def _wrapped(self, rkey="ov001", name="Test"):
        uri = f"https://places.atgeo.org/{self._COLLECTION}/{rkey}"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": rkey, "name": name})
        return envelope.wrap_record(uri, record_json) if envelope else None

    def test_build_tile_payload_top_level_keys_exact(self):
        """Tile top-level == exactly {collection, source, license,
        generated_at, records} (§6 item 6; per the envelope decisions)."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT,
            [self._wrapped()],
        )
        assert isinstance(payload, bytes), f"build_tile_payload must return bytes; got {type(payload)}"
        parsed = json.loads(payload)
        assert set(parsed.keys()) == {"collection", "source", "license", "generated_at", "records"}, (
            f"tile top-level must be exactly {{collection, source, license, generated_at, records}}; "
            f"got {list(parsed)}"
        )

    def test_build_tile_payload_fields_match_inputs(self):
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT, [self._wrapped()],
        )
        parsed = json.loads(payload)
        assert parsed["collection"] == self._COLLECTION
        assert parsed["source"] == self._SOURCE_URL
        assert parsed["license"] == self._LICENSE_URL
        assert parsed["generated_at"] == self._GENERATED_AT

    def test_build_tile_payload_records_each_exactly_three_keys(self):
        """Each record == exactly {uri, cid, value} with cid is None (§6 item 6)."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT,
            [self._wrapped("ov001", "A"), self._wrapped("ov002", "B")],
        )
        parsed = json.loads(payload)
        assert len(parsed["records"]) == 2
        for rec in parsed["records"]:
            assert set(rec.keys()) == {"uri", "cid", "value"}, (
                f"record must be exactly {{uri, cid, value}}; got {list(rec)}"
            )
            assert rec["cid"] is None

    def test_build_tile_payload_record_uri_rkey_matches_value_rkey(self):
        """uri's trailing rkey segment matches value.rkey for sampled records (§6 item 6)."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT,
            [self._wrapped("ov001", "A")],
        )
        parsed = json.loads(payload)
        rec = parsed["records"][0]
        assert rec["uri"].rsplit("/", 1)[-1] == rec["value"]["rkey"]

    def test_build_tile_payload_osm_rkey_form_in_uri(self):
        """OSM node:/way:/relation: rkey form appears verbatim in the record uri (§6 item 6)."""
        _check_envelope()
        collection = "org.atgeo.places.osm"
        uri = f"https://places.atgeo.org/{collection}/node:12345"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "node:12345", "name": "Cafe"})
        wrapped = envelope.wrap_record(uri, record_json)
        payload = envelope.build_tile_payload(
            collection, "https://www.openstreetmap.org/",
            "https://opendatacommons.org/licenses/odbl/1-0/", self._GENERATED_AT, [wrapped],
        )
        parsed = json.loads(payload)
        rec = parsed["records"][0]
        assert rec["uri"].endswith("/node:12345")
        assert rec["value"]["rkey"] == "node:12345"

    def test_build_tile_payload_empty_records(self):
        """A tile with zero records still has the full envelope with records == []."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT, [],
        )
        parsed = json.loads(payload)
        assert parsed["records"] == []

    def test_build_tile_payload_gzip_roundtrip(self):
        """Payload bytes gzip-compress and decompress back to the same JSON
        (sanity check for the flush_tile integration point, per the envelope decisions)."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT, [self._wrapped()],
        )
        compressed = gzip.compress(payload, mtime=0)
        decompressed = gzip.decompress(compressed)
        assert json.loads(decompressed) == json.loads(payload)
