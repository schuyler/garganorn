"""Tests for garganorn.envelope, the atgeo v1 tile/record envelope module.

Envelope contract exercised here:
  - record_uri(repo, collection, rkey) -> "https://{repo}/{collection}/{rkey}",
    never at://; for OSM, rkey is the node:/way:/relation: form, not the raw
    place_id, and its colon is not percent-encoded.
  - wrap_record(uri, record_json) -> a JSON string parsing to exactly
    {uri, cid, value}; cid is literally null, never computed; record_json is
    embedded verbatim (string composition, not a json.loads/json.dumps round
    trip), so UTF-8 output and unusual formatting survive unchanged.
  - build_tile_payload(collection, source_url, license_url, generated_at,
    wrapped_records) -> bytes whose top-level parses to exactly {collection,
    source, license, generated_at, records}.

Determinism, timestamp coherence, and server round-trip are covered
end-to-end against the real stage_export/tile_reader production path in
tests/test_stages.py and tests/test_tile_reader.py -- this module is
unit-level only, exercising envelope.py's pure functions in isolation.
"""
import gzip
import json

import lexrpc

from garganorn.server import load_lexicons

from garganorn import envelope


# ---------------------------------------------------------------------------
# record_uri()
# ---------------------------------------------------------------------------

class TestRecordUri:
    def test_record_uri_form(self):
        """record_uri(repo, collection, rkey) == https://{repo}/{collection}/{rkey}."""
        uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.overture.place", "ov001"
        )
        assert uri == "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"

    def test_record_uri_osm_node_rkey(self):
        """OSM rkey is the node:/way:/relation: transformed form, not the raw
        place_id. Colons are legal in a URI path segment (RFC 3986); no
        encoding needed."""
        uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.osm", "node:12345"
        )
        assert uri == "https://places.atgeo.org/org.atgeo.places.osm/node:12345"
        assert "%3A" not in uri, "colon must not be percent-encoded"

    def test_record_uri_osm_way_and_relation(self):
        """way: and relation: rkeys form URIs analogously to node:."""
        way_uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.osm", "way:67890"
        )
        rel_uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.osm", "relation:11111"
        )
        assert way_uri == "https://places.atgeo.org/org.atgeo.places.osm/way:67890"
        assert rel_uri == "https://places.atgeo.org/org.atgeo.places.osm/relation:11111"

    def test_record_uri_not_at_protocol(self):
        """URIs are https://, never at:// -- gazetteer records are not
        repository data and must not mint at:// URIs."""
        uri = envelope.record_uri("places.atgeo.org", "org.atgeo.places.overture.place", "ov001")
        assert uri.startswith("https://"), f"URI must be https://, got {uri!r}"
        assert not uri.startswith("at://")


# ---------------------------------------------------------------------------
# wrap_record(): {uri, cid, value} exactly, cid is literally null
# ---------------------------------------------------------------------------

class TestWrapRecord:
    def test_wrap_record_produces_exactly_three_keys(self):
        """wrap_record(uri, record_json) -> a JSON string whose parsed object
        has exactly {uri, cid, value} -- three keys, always present."""
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "ov001", "name": "Test"})
        wrapped = envelope.wrap_record(uri, record_json)
        parsed = json.loads(wrapped)
        assert set(parsed.keys()) == {"uri", "cid", "value"}, (
            f"wrapped record must have exactly {{uri, cid, value}}; got {list(parsed)}"
        )

    def test_wrap_record_cid_is_none(self):
        """cid is literally null -- never computed."""
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "ov001"})
        parsed = json.loads(envelope.wrap_record(uri, record_json))
        assert parsed["cid"] is None

    def test_wrap_record_uri_matches_input(self):
        """The uri field is exactly the uri passed in."""
        uri = "https://places.atgeo.org/org.atgeo.places.osm/node:12345"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "node:12345"})
        parsed = json.loads(envelope.wrap_record(uri, record_json))
        assert parsed["uri"] == uri

    def test_wrap_record_value_is_the_record(self):
        """value is byte-for-byte today's record JSON, parsed back losslessly."""
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
        """wrap_record is string composition, not json.loads + json.dumps, to
        avoid a per-record parse/reserialize round trip. Verify by checking
        that malformed-but-well-formed-looking JSON text is passed through
        verbatim rather than being re-serialized (e.g. key order / spacing
        would change under a round trip through json.loads->json.dumps with
        default separators). This is a white-box check that wrap_record does
        not alter the byte content of record_json, only wraps it.
        """
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        # Deliberately unusual spacing/ordering that json.dumps(json.loads(...))
        # with default args would normalize away.
        record_json = '{"$type":"org.atgeo.place","rkey":"ov001","name":"Café"}'
        wrapped = envelope.wrap_record(uri, record_json)
        assert record_json in wrapped, (
            "wrap_record must embed record_json verbatim (string composition, "
            "not a json.loads/json.dumps round trip)"
        )

    def test_wrap_record_utf8_not_ascii_escaped(self):
        """DuckDB's UTF-8 output is preserved verbatim instead of being
        ensure_ascii-escaped."""
        uri = "https://places.atgeo.org/org.atgeo.places.overture.place/ov001"
        record_json = json.dumps({"name": "Café"}, ensure_ascii=False)
        wrapped = envelope.wrap_record(uri, record_json)
        assert "Café" in wrapped, (
            f"UTF-8 characters must be preserved verbatim, not \\u-escaped; got {wrapped!r}"
        )
        assert "\\u00e9" not in wrapped


# ---------------------------------------------------------------------------
# build_tile_payload()
# ---------------------------------------------------------------------------

class TestBuildTilePayload:
    _COLLECTION = "org.atgeo.places.overture.place"
    _SOURCE_URL = "https://overturemaps.org/"
    _LICENSE_URL = "https://docs.overturemaps.org/attribution/"
    _GENERATED_AT = "2026-07-09T18:00:00Z"

    def _wrapped(self, rkey="ov001", name="Test"):
        uri = f"https://places.atgeo.org/{self._COLLECTION}/{rkey}"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": rkey, "name": name})
        return envelope.wrap_record(uri, record_json)

    def test_build_tile_payload_top_level_keys_exact(self):
        """Tile top-level == exactly {collection, source, license,
        generated_at, records}."""
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
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT, [self._wrapped()],
        )
        parsed = json.loads(payload)
        assert parsed["collection"] == self._COLLECTION
        assert parsed["source"] == self._SOURCE_URL
        assert parsed["license"] == self._LICENSE_URL
        assert parsed["generated_at"] == self._GENERATED_AT

    def test_build_tile_payload_records_each_exactly_three_keys(self):
        """Each record == exactly {uri, cid, value} with cid is None."""
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
        """uri's trailing rkey segment matches value.rkey for sampled records."""
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT,
            [self._wrapped("ov001", "A")],
        )
        parsed = json.loads(payload)
        rec = parsed["records"][0]
        assert rec["uri"].rsplit("/", 1)[-1] == rec["value"]["rkey"]

    def test_build_tile_payload_osm_rkey_form_in_uri(self):
        """OSM node:/way:/relation: rkey form appears verbatim in the record uri."""
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
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT, [],
        )
        parsed = json.loads(payload)
        assert parsed["records"] == []

    def test_build_tile_payload_gzip_roundtrip(self):
        """Payload bytes gzip-compress and decompress back to the same JSON
        (sanity check for the flush_tile integration point)."""
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT, [self._wrapped()],
        )
        compressed = gzip.compress(payload, mtime=0)
        decompressed = gzip.decompress(compressed)
        assert json.loads(decompressed) == json.loads(payload)

    def test_build_tile_payload_validates_against_lexicon_schema(self):
        """build_tile_payload's output validates against org.atgeo.tilePayload.

        org.atgeo.tilePayload#main is an object def, not a query/procedure
        output, so there is no method NSID to call lexrpc.Server.validate()
        with (per its ('input', 'output', 'message', 'parameters', 'record')
        contract); _validate_schema is the same code validate() delegates
        to, invoked directly against the def.
        """
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT,
            [self._wrapped("ov001", "A"), self._wrapped("ov002", "B")],
        )
        server = lexrpc.Server(lexicons=load_lexicons())
        schema = server.defs["org.atgeo.tilePayload"]
        server._validate_schema(
            name="output", val=json.loads(payload), type_name="org.atgeo.tilePayload",
            lexicon="org.atgeo.tilePayload", schema=schema,
        )

    def test_build_tile_payload_empty_records_validates_against_lexicon_schema(self):
        """records is required and must validate even when empty."""
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._SOURCE_URL, self._LICENSE_URL, self._GENERATED_AT, [],
        )
        server = lexrpc.Server(lexicons=load_lexicons())
        schema = server.defs["org.atgeo.tilePayload"]
        server._validate_schema(
            name="output", val=json.loads(payload), type_name="org.atgeo.tilePayload",
            lexicon="org.atgeo.tilePayload", schema=schema,
        )
