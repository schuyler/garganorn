"""The atgeo v1 tile/record envelope.

Shared by the pipeline export path (garganorn.stages) and the legacy
export_tiles() module function, so there is exactly one implementation of
the envelope shape.

Imports nothing from garganorn — no import cycle with server.py or stages.py.
"""
import json


def record_uri(repo: str, collection: str, rkey: str) -> str:
    """Build the canonical dereferenceable record URI.

    https://{repo}/{collection}/{rkey} -- not at://; gazetteer records are not
    repository data. rkey is the record's rkey *after* source transforms (for
    OSM, the node:/way:/relation: form). Colons in rkeys are legal in a URI
    path segment (RFC 3986); no encoding is applied.
    """
    return f"https://{repo}/{collection}/{rkey}"


def wrap_record(uri: str, record_json: str) -> str:
    """Wrap a record's flat JSON string into the {uri, cid, value} envelope.

    String composition, not json.loads + json.dumps: record_json is
    already valid JSON text (from DuckDB's to_json()), so it is embedded
    verbatim rather than being parsed and reserialized. This avoids a
    per-record round trip and preserves DuckDB's UTF-8 output exactly
    (no ensure_ascii escaping).

    cid is literally null -- never computed.
    """
    return '{"uri":%s,"cid":null,"value":%s}' % (json.dumps(uri), record_json)


def build_tile_payload(collection: str, source_url: str, license_url: str,
                        generated_at: str, wrapped_records: list) -> bytes:
    """Build the top-level tile payload.

    Top-level keys are exactly {collection, source, license, generated_at,
    records}. wrapped_records is a list of already-wrap_record()-wrapped JSON
    strings; they are embedded verbatim via string composition, matching
    wrap_record's no-parse-round-trip contract.
    """
    header = json.dumps({
        "collection": collection,
        "source": source_url,
        "license": license_url,
        "generated_at": generated_at,
    })
    # header is '{"collection": ..., ..., "generated_at": "..."}' -- splice
    # "records" in before the closing brace, joining the wrapped record
    # strings verbatim.
    joined_records = ",".join(wrapped_records)
    payload = header[:-1] + ',"records":[' + joined_records + ']}'
    return payload.encode("utf-8")
