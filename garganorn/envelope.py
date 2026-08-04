"""The atgeo v1 tile/record envelope (pipeline-implementation-decisions.md
"OQ-P2-1 — record envelope adoption").

Shared by the pipeline export path (garganorn.stages) and the legacy
export_tiles() module function, so there is exactly one implementation of
the envelope shape (pipeline-implementation-decisions.md
"OQ-P2-1 — record envelope adoption").

Imports nothing from garganorn — no import cycle with server.py or stages.py.
"""
import json

ATGEO_VERSION = 1


def record_uri(repo: str, collection: str, rkey: str) -> str:
    """Build the canonical dereferenceable record URI (pipeline-implementation-decisions.md
    "OQ-P2-1 — record envelope adoption").

    https://{repo}/{collection}/{rkey} -- not at://; gazetteer records are not
    repository data. rkey is the record's rkey *after* source transforms (for
    OSM, the node:/way:/relation: form). Colons in rkeys are legal in a URI
    path segment (RFC 3986); no encoding is applied.
    """
    return f"https://{repo}/{collection}/{rkey}"


def wrap_record(uri: str, record_json: str) -> str:
    """Wrap a record's flat JSON string into the {uri, cid, value} envelope.

    String composition, not json.loads + json.dumps (per the envelope decisions above): record_json is
    already valid JSON text (from DuckDB's to_json()), so it is embedded
    verbatim rather than being parsed and reserialized. This avoids a
    per-record round trip and preserves DuckDB's UTF-8 output exactly
    (no ensure_ascii escaping).

    cid is literally null -- never computed (per the envelope decisions above, APPROVED).
    """
    return '{"uri":%s,"cid":null,"value":%s}' % (json.dumps(uri), record_json)


def build_tile_payload(collection: str, attribution: str, generated_at: str,
                        wrapped_records: list) -> bytes:
    """Build the top-level tile payload (per the envelope decisions above).

    Top-level keys are exactly {atgeo, collection, attribution, generated_at,
    records}. wrapped_records is a list of already-wrap_record()-wrapped JSON
    strings; they are embedded verbatim via string composition, matching
    wrap_record's no-parse-round-trip contract.
    """
    header = json.dumps({
        "atgeo": ATGEO_VERSION,
        "collection": collection,
        "attribution": attribution,
        "generated_at": generated_at,
    })
    # header is '{"atgeo": 1, ..., "generated_at": "..."}' -- splice "records"
    # in before the closing brace, joining the wrapped record strings verbatim.
    joined_records = ",".join(wrapped_records)
    payload = header[:-1] + ',"records":[' + joined_records + ']}'
    return payload.encode("utf-8")


def build_manifest(source: str, collection: str, attribution: str,
                    generated_at: str, quadkeys: list) -> dict:
    """Build the manifest.json dict (per the envelope decisions above).

    Field set is exactly {atgeo, source, collection, attribution,
    generated_at, tile_url_template, cache, quadkeys}. generated_at is
    passed through verbatim -- callers capture one run-scoped timestamp
    (per the envelope decisions above) and this function does not reformat it. quadkeys is sorted.
    """
    return {
        "atgeo": ATGEO_VERSION,
        "source": source,
        "collection": collection,
        "attribution": attribution,
        "generated_at": generated_at,
        "tile_url_template": "{base}/{qk6}/{qk}.json.gz",
        "cache": {"max_age": 86400, "immutable": False},
        "quadkeys": sorted(quadkeys),
    }
