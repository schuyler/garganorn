"""RED tests: garganorn.envelope — the atgeo v1 tile/record envelope module.

phase2b-design.md Part B (OQ-P2-1). garganorn/envelope.py does not exist yet
(§B.7.1), so every test in this module that imports from it fails at
collection/setup with ImportError until it is implemented -- that failure IS
the RED signal for this feature. Mirrors the import-guard pattern used by
tests/test_levels.py for garganorn.levels.

Covers phase2b-design.md §6 (combined acceptance checklist) items 6 and 9,
plus the module-level contract from §B.5/§B.7.1:
  - ATGEO_VERSION == 1 (§B.5)
  - record_uri(repo, collection, rkey) -> "https://{repo}/{collection}/{rkey}" (§B.3)
  - wrap_record(uri, record_json) -> '{"uri":...,"cid":null,"value":...}' string,
    cid is literally null, never computed (§B.3)
  - build_tile_payload(collection, attribution, generated_at, wrapped_records)
    -> bytes; top-level == exactly {atgeo, collection, attribution,
    generated_at, records} (§B.2a, §B.5)
  - build_manifest(source, collection, attribution, generated_at, quadkeys)
    -> dict matching §B.2c's field set exactly, including cache.immutable=false
    (§B.6, protocol change P2)

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
# §B.5 — ATGEO_VERSION
# ---------------------------------------------------------------------------

class TestAtgeoVersion:
    def test_atgeo_version_is_1(self):
        """ATGEO_VERSION is the integer 1, the single source of truth for both
        the tile payload and the manifest (§B.5)."""
        _check_envelope()
        assert envelope.ATGEO_VERSION == 1
        assert isinstance(envelope.ATGEO_VERSION, int)


# ---------------------------------------------------------------------------
# §B.3 — record_uri()
# ---------------------------------------------------------------------------

class TestRecordUri:
    def test_record_uri_form(self):
        """record_uri(repo, collection, rkey) == https://{repo}/{collection}/{rkey} (§B.3)."""
        _check_envelope()
        uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.foursquare", "fsq001"
        )
        assert uri == "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001"

    def test_record_uri_osm_node_rkey(self):
        """OSM rkey is the node:/way:/relation: transformed form, not the raw
        place_id -- §B.3: 'for OSM that is the node:|way:|relation: form
        ... not the raw place_id'. Colons are legal in a URI path segment
        (RFC 3986); no encoding needed."""
        _check_envelope()
        uri = envelope.record_uri(
            "places.atgeo.org", "org.atgeo.places.osm", "node:12345"
        )
        assert uri == "https://places.atgeo.org/org.atgeo.places.osm/node:12345"
        assert "%3A" not in uri, "colon must not be percent-encoded (§B.3)"

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
        """URIs are https://, never at:// -- §B.3 is emphatic that gazetteer
        records are not repository data and must not mint at:// URIs."""
        _check_envelope()
        uri = envelope.record_uri("places.atgeo.org", "org.atgeo.places.foursquare", "fsq001")
        assert uri.startswith("https://"), f"URI must be https://, got {uri!r}"
        assert not uri.startswith("at://")


# ---------------------------------------------------------------------------
# §B.3 — wrap_record(): {uri, cid, value} exactly, cid is literally null
# ---------------------------------------------------------------------------

class TestWrapRecord:
    def test_wrap_record_produces_exactly_three_keys(self):
        """wrap_record(uri, record_json) -> a JSON string whose parsed object
        has exactly {uri, cid, value} -- three keys, always present (§B.3)."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "fsq001", "name": "Test"})
        wrapped = envelope.wrap_record(uri, record_json)
        parsed = json.loads(wrapped)
        assert set(parsed.keys()) == {"uri", "cid", "value"}, (
            f"wrapped record must have exactly {{uri, cid, value}}; got {list(parsed)}"
        )

    def test_wrap_record_cid_is_none(self):
        """cid is literally null -- never computed, per the APPROVED §B.3 decision."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": "fsq001"})
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
        """value is byte-for-byte today's record JSON, parsed back losslessly (§B.2a)."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001"
        record = {
            "$type": "org.atgeo.place", "rkey": "fsq001", "name": "Blue Bottle Coffee",
            "importance": 72, "locations": [{"$type": "community.lexicon.location.geo",
                                              "latitude": "37.774900", "longitude": "-122.419400"}],
            "variants": [], "attributes": {"tel": None}, "relations": {},
        }
        record_json = json.dumps(record)
        parsed = json.loads(envelope.wrap_record(uri, record_json))
        assert parsed["value"] == record

    def test_wrap_record_no_json_loads_per_record(self):
        """§B.7.1: wrap_record is string composition, not json.loads + json.dumps,
        to avoid a per-record parse/reserialize round trip. Verify by checking
        that malformed-but-well-formed-looking JSON text is passed through
        verbatim rather than being re-serialized (e.g. key order / spacing
        would change under a round trip through json.loads->json.dumps with
        default separators). This is a white-box characterization of the
        §B.7.1 design: wrap_record must not alter the byte content of
        record_json, only wrap it.
        """
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001"
        # Deliberately unusual spacing/ordering that json.dumps(json.loads(...))
        # with default args would normalize away.
        record_json = '{"$type":"org.atgeo.place","rkey":"fsq001","name":"Café"}'
        wrapped = envelope.wrap_record(uri, record_json)
        assert record_json in wrapped, (
            "wrap_record must embed record_json verbatim (string composition, "
            "not a json.loads/json.dumps round trip) -- §B.7.1"
        )

    def test_wrap_record_utf8_not_ascii_escaped(self):
        """§B.7.1 review note: DuckDB's UTF-8 output is preserved verbatim
        instead of being ensure_ascii-escaped."""
        _check_envelope()
        uri = "https://places.atgeo.org/org.atgeo.places.foursquare/fsq001"
        record_json = json.dumps({"name": "Café"}, ensure_ascii=False)
        wrapped = envelope.wrap_record(uri, record_json)
        assert "Café" in wrapped, (
            f"UTF-8 characters must be preserved verbatim, not \\u-escaped; got {wrapped!r}"
        )
        assert "\\u00e9" not in wrapped


# ---------------------------------------------------------------------------
# §B.2a / §B.5 — build_tile_payload()
# ---------------------------------------------------------------------------

class TestBuildTilePayload:
    _COLLECTION = "org.atgeo.places.foursquare"
    _ATTRIBUTION = "https://docs.foursquare.com/data-products/docs/access-fsq-os-places"
    _GENERATED_AT = "2026-07-09T18:00:00Z"

    def _wrapped(self, rkey="fsq001", name="Test"):
        uri = f"https://places.atgeo.org/{self._COLLECTION}/{rkey}"
        record_json = json.dumps({"$type": "org.atgeo.place", "rkey": rkey, "name": name})
        return envelope.wrap_record(uri, record_json) if envelope else None

    def test_build_tile_payload_top_level_keys_exact(self):
        """Tile top-level == exactly {atgeo, collection, attribution,
        generated_at, records} (§6 item 6, §B.2a)."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT,
            [self._wrapped()],
        )
        assert isinstance(payload, bytes), f"build_tile_payload must return bytes; got {type(payload)}"
        parsed = json.loads(payload)
        assert set(parsed.keys()) == {"atgeo", "collection", "attribution", "generated_at", "records"}, (
            f"tile top-level must be exactly {{atgeo, collection, attribution, generated_at, records}}; "
            f"got {list(parsed)}"
        )

    def test_build_tile_payload_atgeo_is_1(self):
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, [self._wrapped()],
        )
        parsed = json.loads(payload)
        assert parsed["atgeo"] == 1

    def test_build_tile_payload_fields_match_inputs(self):
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, [self._wrapped()],
        )
        parsed = json.loads(payload)
        assert parsed["collection"] == self._COLLECTION
        assert parsed["attribution"] == self._ATTRIBUTION
        assert parsed["generated_at"] == self._GENERATED_AT

    def test_build_tile_payload_records_each_exactly_three_keys(self):
        """Each record == exactly {uri, cid, value} with cid is None (§6 item 6)."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT,
            [self._wrapped("fsq001", "A"), self._wrapped("fsq002", "B")],
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
            self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT,
            [self._wrapped("fsq001", "A")],
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
            collection, "https://www.openstreetmap.org/copyright", self._GENERATED_AT, [wrapped],
        )
        parsed = json.loads(payload)
        rec = parsed["records"][0]
        assert rec["uri"].endswith("/node:12345")
        assert rec["value"]["rkey"] == "node:12345"

    def test_build_tile_payload_empty_records(self):
        """A tile with zero records still has the full envelope with records == []."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, [],
        )
        parsed = json.loads(payload)
        assert parsed["records"] == []
        assert parsed["atgeo"] == 1

    def test_build_tile_payload_gzip_roundtrip(self):
        """Payload bytes gzip-compress and decompress back to the same JSON
        (sanity check for the flush_tile integration point, §B.7.3)."""
        _check_envelope()
        payload = envelope.build_tile_payload(
            self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, [self._wrapped()],
        )
        compressed = gzip.compress(payload, mtime=0)
        decompressed = gzip.decompress(compressed)
        assert json.loads(decompressed) == json.loads(payload)


# ---------------------------------------------------------------------------
# §B.2c / §B.6 — build_manifest()
# ---------------------------------------------------------------------------

class TestBuildManifest:
    _SOURCE = "foursquare"
    _COLLECTION = "org.atgeo.places.foursquare"
    _ATTRIBUTION = "https://docs.foursquare.com/data-products/docs/access-fsq-os-places"
    _GENERATED_AT = "2026-07-09T18:00:00Z"
    _QUADKEYS = ["023130", "023131"]

    def test_build_manifest_field_set_exact(self):
        """manifest.json matches §B.2c's field set exactly (§6 item 9)."""
        _check_envelope()
        manifest = envelope.build_manifest(
            self._SOURCE, self._COLLECTION, self._ATTRIBUTION,
            self._GENERATED_AT, self._QUADKEYS,
        )
        expected_keys = {
            "atgeo", "source", "collection", "attribution", "generated_at",
            "tile_url_template", "cache", "quadkeys",
        }
        assert set(manifest.keys()) == expected_keys, (
            f"manifest must match §B.2c field set exactly; got {sorted(manifest.keys())}, "
            f"expected {sorted(expected_keys)}"
        )

    def test_build_manifest_atgeo_is_1(self):
        _check_envelope()
        manifest = envelope.build_manifest(
            self._SOURCE, self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, self._QUADKEYS,
        )
        assert manifest["atgeo"] == 1

    def test_build_manifest_scalar_fields(self):
        _check_envelope()
        manifest = envelope.build_manifest(
            self._SOURCE, self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, self._QUADKEYS,
        )
        assert manifest["source"] == self._SOURCE
        assert manifest["collection"] == self._COLLECTION
        assert manifest["attribution"] == self._ATTRIBUTION
        assert manifest["generated_at"] == self._GENERATED_AT

    def test_build_manifest_quadkeys_sorted(self):
        """quadkeys remain sorted (pre-existing invariant, §B.1)."""
        _check_envelope()
        manifest = envelope.build_manifest(
            self._SOURCE, self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT,
            ["023133", "023130", "023132"],
        )
        assert manifest["quadkeys"] == ["023130", "023132", "023133"]

    def test_build_manifest_tile_url_template(self):
        """tile_url_template ships in 2b, per §B.9.3 (protocol change, approved)."""
        _check_envelope()
        manifest = envelope.build_manifest(
            self._SOURCE, self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, self._QUADKEYS,
        )
        assert manifest["tile_url_template"] == "{base}/{qk6}/{qk}.json.gz"

    def test_build_manifest_cache_immutable_false(self):
        """cache == {max_age: 86400, immutable: false} -- [protocol change P2],
        APPROVED this session (§B.6, §B.9.2). Deliberately contradicts §1.3's
        example because the deployed slug route serves via `current`."""
        _check_envelope()
        manifest = envelope.build_manifest(
            self._SOURCE, self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, self._QUADKEYS,
        )
        assert manifest["cache"] == {"max_age": 86400, "immutable": False}

    def test_build_manifest_generated_at_rfc3339_z_form(self):
        """generated_at is RFC 3339 UTC, seconds precision, Z suffix (§B.4).

        build_manifest() does not reformat generated_at -- it is captured once
        per run by the caller (stage_export) and passed through verbatim
        (§B.4 item 1) -- so this asserts the passed-through value is preserved
        exactly, not merely that a pre-formatted string round-trips.
        """
        _check_envelope()
        manifest = envelope.build_manifest(
            self._SOURCE, self._COLLECTION, self._ATTRIBUTION, self._GENERATED_AT, self._QUADKEYS,
        )
        assert manifest["generated_at"] == self._GENERATED_AT, (
            f"generated_at must be preserved verbatim from the caller-supplied "
            f"run timestamp; got {manifest.get('generated_at')!r}"
        )
        assert manifest["generated_at"].endswith("Z")
        assert "+00:00" not in manifest["generated_at"]
        assert "." not in manifest["generated_at"], (
            "generated_at must be seconds precision, no microseconds"
        )
